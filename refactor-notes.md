# 模块分割评估与改进建议

> 日期：2026-05-07
> 状态：已完成的 PoC 重构记录 + 后续改进路线图

---

## 已完成重构

### 1. Consumer 订阅缺失修复

**问题**：`consumer_sim.rs` 中 consumer 只通过 Queryable 声明 interest、通过 Liveliness Token 让 gateway 发现自己，但从未在 downstream session 上真正 subscribe 感兴趣的 topic。Gateway 转发消息后 consumer 收不到。

**修复**：在声明 Queryable 和 Liveliness Token 之前，为每个 topic 调用 `session.declare_subscriber()` 注册真正的订阅。

### 2. Unsubscribe 未真正执行修复

**问题**：`sync_shard_subscriptions()` 中 `subs.remove(shard)` 只是从 HashMap 中移除 `Subscriber` 对象。虽然 `Subscriber` 的 `Drop` 会尝试 undeclare，但 `Drop::drop()` 是同步的，无法 `.await` 确认完成。

**修复**：显式调用 `sub.undeclare().await` 确保 Zenoh session 真正取消上游订阅。

### 3. `shard_subs` 移入 `GatewayState`

**问题**：`shard_subs: Arc<Mutex<HashMap<String, Subscriber>>>` 与 `GatewayState.subscribed_shards: BTreeSet<String>` 是同一事物的两面——逻辑意图 vs 实际句柄，分开存储容易状态不一致，且每个调用点都要传递两个 Arc。

**修复**：
- `GatewayState` 中用 `active_subscribers: HashMap<String, Subscriber>` 替代 `subscribed_shards: BTreeSet<String>`
- HashMap 的 keys 即为已订阅 shard 集合，values 持有实际句柄
- `compute_subscription_diff()` 改为 `&self`，通过 `active_subscribers.keys()` 推算当前订阅集合
- 新增 `take_subscribers_for_undeclare()` 和 `insert_subscriber()` 让调用方在锁外执行 async 操作
- `sync_shard_subscriptions()` 签名从 4 个 Arc 参数简化为 3 个

---

### 4. TOCTOU Race Condition Fix (2026-05-12)

**Problem**: Multiple concurrent calls to `sync_shard_subscriptions()` created a TOCTOU race condition where:
- Step 1: `compute_subscription_diff()` computed the diff based on a snapshot
- Step 2: `take_subscribers_for_undeclare()` extracted handles based on the diff
- Between steps, another concurrent call could modify `active_subscribers`

**Impact**: Potential subscriber leaks or duplicate subscriptions.

**Fix**: Implemented atomic operations in [`GatewayState`](src/interest.rs:18):
- [`compute_diff_and_take_undeclare()`](src/interest.rs:179) - Computes diff AND extracts handles in one lock operation
- [`handle_cluster_change()`](src/interest.rs:204) - Handles cluster changes atomically, returns snapshot for lock-free printing
- [`stats_snapshot()`](src/interest.rs:220) - Returns a snapshot for lock-free statistics printing

**Modified files**:
- [`src/interest.rs`](src/interest.rs) - Added three atomic operation methods and `StatsSnapshot` struct
- [`src/main.rs`](src/main.rs) - Updated `sync_shard_subscriptions()`, cluster callback, and stats loop to use atomic operations

**Result**: All three lock-related issues resolved:
1. ✅ TOCTOU race in `sync_shard_subscriptions` - Fixed by atomic operation
2. ✅ Multi-step lock in cluster callback - Fixed by atomic operation
3. ✅ Long lock holding in stats loop - Fixed by snapshot pattern

---

## 当前模块分割评估

| 模块 | 职责 | 评价 |
|------|------|------|
| `hashing.rs` | SHA256 分片 + Rendezvous Hashing | ✅ 纯计算，无状态，边界清晰 |
| `cluster.rs` | 集群成员 + shard 所有权 | ✅ 独立关注点，接口干净 |
| `interest.rs` | 三层索引 + 订阅句柄 + pull 逻辑 | ⚠️ 职责偏多，见下方建议 |
| `main.rs` | 事件驱动编排 + 转发回调 | ⚠️ 回调内逻辑较重，样板代码多 |

---

## 后续改进建议

### 建议 1：`interest.rs` 拆分 — 分离索引与订阅管理

`GatewayState` 当前承担了两种职责：
- **三层索引维护**：`client_topics` / `topic_subscribers` / `shard_topics` 的增删查
- **订阅句柄管理**：`active_subscribers` 的插入、提取、undeclare

这两种职责的变化频率和生命周期不同：索引随 consumer 上下线频繁变化，订阅句柄随 shard 所有权变化而变化。

**建议方案**：将 `active_subscribers` 及其操作提取为独立的 `SubscriptionManager`：

```rust
// subscription.rs (新模块)
pub struct SubscriptionManager {
    active_subscribers: HashMap<String, zenoh::pubsub::Subscriber<()>>,
}

impl SubscriptionManager {
    pub fn current_shards(&self) -> BTreeSet<String> { ... }
    pub fn take_for_undeclare(&mut self, shards: &[String]) -> Vec<Subscriber<()>> { ... }
    pub fn insert(&mut self, shard: String, sub: Subscriber<()>) { ... }
}
```

`GatewayState` 通过组合持有 `SubscriptionManager`，保持三层索引的纯粹性。

### 建议 2：`main.rs` 回调提取 — 消除重复的 clone-and-spawn 模式

当前 `main.rs` 中 liveliness callback 和 consumer callback 中的 clone-and-spawn 模式重复了 3 次：

```rust
// 模式重复：cluster change / consumer online / consumer offline
let s_sync = state.clone();
let up_sync = up.clone();
let ds_sync = ds.clone();
tokio::spawn(async move {
    sync_shard_subscriptions(s_sync, up_sync, ds_sync).await;
});
```

**建议方案**：提取为两个高层函数：

```rust
// event_handlers.rs (新模块)
pub async fn on_cluster_change(state: Arc<Mutex<GatewayState>>, up: Session, ds: Session) { ... }
pub async fn on_consumer_change(
    state: Arc<Mutex<GatewayState>>, up: Session, ds: Session,
    client_id: String, kind: SampleKind,
) { ... }
```

回调闭包只负责解析事件参数，业务逻辑委托给这两个函数。

### 建议 3：`pull_consumer_interests` 位置调整

`pull_consumer_interests` 在 `interest.rs` 中但依赖外部 `zenoh::Session`，与纯数据结构的 `GatewayState` 不太协调。`interest.rs` 的其他部分都是对 `GatewayState` 的同步操作，唯独这个函数是异步的且涉及网络 I/O。

**建议方案**：移到独立的 `discovery.rs` 模块，或移入 `main.rs` 旁的 `event_handlers.rs`：

```rust
// discovery.rs (新模块)
pub async fn pull_consumer_interests(
    client_id: String,
    session: zenoh::Session,
    state: Arc<Mutex<GatewayState>>,
) { ... }
```

这样 `interest.rs` 保持为纯数据结构模块，便于单元测试。

### 建议 4：转发回调提取 — 核心数据面逻辑可测试化

`sync_shard_subscriptions` 中 `declare_subscriber` 的 callback 闭包（检查 interest → 转发）是核心数据面逻辑，目前内联在编排代码中，无法单独测试。

**建议方案**：提取为 `ForwardingHandler` 结构体：

```rust
// forwarding.rs (新模块)
pub struct ForwardingHandler {
    state: Arc<Mutex<GatewayState>>,
    downstream: zenoh::Session,
}

impl ForwardingHandler {
    pub fn on_sample(&self, sample: Sample) {
        if let Some(attr) = sample.attachment() {
            let okey = String::from_utf8_lossy(&attr.to_bytes()).to_string();
            if self.state.lock().unwrap().topic_subscribers.contains_key(&okey) {
                let payload = sample.payload().clone();
                let ds = self.downstream.clone();
                tokio::spawn(async move {
                    let _ = ds.put(okey, payload).await;
                });
            }
        }
    }
}
```

这样转发逻辑可以独立测试，且 `sync_shard_subscriptions` 只负责编排订阅/取消订阅的生命周期。

---

## 锁使用分析与改进建议

### 当前锁调用点清单

`main.rs` 中所有 `state.lock().unwrap()` 调用点：

| 行号 | 位置 | 操作 | 持锁时长 | 风险 |
|------|------|------|----------|------|
| 21 | `sync_shard_subscriptions` | `compute_subscription_diff()` | 短 | ✅ |
| 29 | `sync_shard_subscriptions` | `take_subscribers_for_undeclare()` | 短 | ⚠️ TOCTOU |
| 46 | 转发回调 | `topic_subscribers.contains_key()` | 短 | ✅ |
| 58 | `sync_shard_subscriptions` | `insert_subscriber()` | 短 | ✅ |
| 75 | 初始化 | `refresh_load_stats()` | 短 | ✅ |
| 87 | cluster 回调 | `add_node / remove_node` | 短 | ⚠️ 分步 lock |
| 99 | cluster 回调 | `println + refresh_load_stats()` | 短 | ⚠️ I/O 在锁内 |
| 122 | 初始节点查询 | `add_node + refresh_load_stats` | 短 | ✅ |
| 147 | consumer 回调 | `mark_pulling()` | 短 | ✅ |
| 164 | consumer 回调 | `cleanup_interests()` | 短 | ✅ |
| 185 | 初始 consumer 同步 | `mark_pulling()` | 短 | ✅ |
| 206 | 统计循环 | 整个打印期间持锁 | **长** | ❌ |

### 问题 1：`sync_shard_subscriptions` 中的 TOCTOU 竞态（最严重）

```
Line 21: lock → compute_diff → unlock        ← diff 基于 snapshot
          ⚡ 另一个 tokio::spawn 的 sync_shard_subscriptions 可能在此时修改 active_subscribers
Line 29: lock → take_subscribers → unlock     ← 操作基于过时的 diff 结果
```

多个事件（cluster change、consumer online、consumer offline）都会 `tokio::spawn` 调用 `sync_shard_subscriptions`，它们并发执行时，步骤 1 和步骤 2 之间的状态可能已经被另一个调用修改。

**影响**：可能导致 subscriber 泄漏（diff 说要 unsub，但另一个调用已经改了 active_subscribers）或重复订阅。

### 问题 2：cluster 回调中两次分步 lock

```rust
// 第一次 lock
let (changed, node_id) = {
    let mut s = member_state.lock().unwrap();
    // add_node / remove_node
};
// ⚡ 此处锁释放，其他线程可修改 state

// 第二次 lock
if changed {
    let mut s = member_state.lock().unwrap();
    println!(...);  // I/O 在锁内，不必要
    s.cluster.refresh_load_stats();
}
```

两次 lock 之间状态可能变化，且 `println` 这种 I/O 不应在锁内执行。

### 问题 3：统计循环长时间持锁

```rust
let s = stats_state.lock().unwrap();
println!(...);  // 多次 println + 字符串格式化
// 锁直到作用域结束才释放
```

5 秒一次的统计打印期间，所有回调都被阻塞。

### 根因

所有问题的根源是同一个：**锁的粒度由调用方随意控制，没有统一的访问模式**。每个调用点都自己决定何时 lock、lock 多久、做几步操作，缺乏一致性。

### 建议 5：提供高层原子操作接口，收拢锁策略

核心思路是让 `GatewayState` 提供更高层的**原子操作接口**，而不是暴露内部字段让调用方自己 lock + 操作：

```rust
impl GatewayState {
    /// 原子操作：计算 diff + 取出 subscriber 句柄，一步完成
    /// 消除 TOCTOU 竞态
    pub fn compute_diff_and_take_undeclare(&mut self)
        -> (Vec<String>, Vec<zenoh::pubsub::Subscriber<()>>)
    {
        let desired_shards: BTreeSet<String> = self.shard_topics.iter()
            .filter(|(shard_id, topics)| {
                !topics.is_empty() && self.cluster.is_owner(shard_id)
            })
            .map(|(shard_id, _)| shard_id.clone())
            .collect();

        let current_shards: BTreeSet<String> = self.active_subscribers.keys().cloned().collect();
        let to_subscribe: Vec<String> = desired_shards.difference(&current_shards).cloned().collect();
        let to_unsubscribe: Vec<String> = current_shards.difference(&desired_shards).cloned().collect();

        let to_undeclare: Vec<Subscriber<()>> = to_unsubscribe.iter()
            .filter_map(|shard| self.active_subscribers.remove(shard))
            .collect();

        (to_subscribe, to_undeclare)
    }

    /// 原子操作：添加/移除节点 + 刷新统计，返回快照供锁外打印
    /// 调用方无需自己分步 lock
    pub fn handle_cluster_change(&mut self, node_id: String, kind: SampleKind)
        -> (bool, Vec<String>)
    {
        let changed = match kind {
            SampleKind::Put => self.cluster.add_node(node_id.clone()),
            SampleKind::Delete => self.cluster.remove_node(&node_id),
        };
        if changed {
            self.cluster.refresh_load_stats();
        }
        let nodes: Vec<String> = self.cluster.nodes().iter().cloned().collect();
        (changed, nodes)
    }

    /// 快照：克隆需要的数据，释放锁后再打印
    pub fn stats_snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            my_id: self.my_id().to_string(),
            cluster_size: self.cluster.nodes().len(),
            nodes: self.cluster.nodes().clone(),
            owned_shards: self.cluster.owned_shards_cache(),
            total_interests: self.topic_subscribers.len(),
            active_details: self.topic_subscribers.keys()
                .filter(|topic| {
                    let shard = hashing::get_shard_id(topic);
                    self.cluster.is_owner(&shard)
                })
                .map(|topic| {
                    let shard = hashing::get_shard_id(topic);
                    format!("{} ({})", topic, shard)
                })
                .collect(),
        }
    }
}

pub struct StatsSnapshot {
    pub my_id: String,
    pub cluster_size: usize,
    pub nodes: BTreeSet<String>,
    pub owned_shards: usize,
    pub total_interests: usize,
    pub active_details: Vec<String>,
}
```

调用方改造后：

```rust
// sync_shard_subscriptions — 一次 lock 完成 diff + take
let (to_sub, to_undeclare) = {
    let mut s = state_arc.lock().unwrap();
    s.compute_diff_and_take_undeclare()
};
// 锁外执行 async 操作
for sub in to_undeclare {
    let _ = sub.undeclare().await;
}
for shard in to_sub {
    let sub = upstream.declare_subscriber(&shard)...;
    state_arc.lock().unwrap().insert_subscriber(shard, sub);
}

// cluster 回调 — 一次 lock 完成
let (changed, nodes) = {
    let mut s = member_state.lock().unwrap();
    s.handle_cluster_change(node_id, kind)
};
// 锁外打印
if changed {
    println!("Cluster changed! Current nodes: {:?}", nodes);
}

// 统计循环 — 快照模式
let snapshot = stats_state.lock().unwrap().stats_snapshot();
// 锁已释放，安全打印
println!("Cluster Size: {}", snapshot.cluster_size);
```

---

## 建议的目标模块结构

```
src/
├── hashing.rs          # 纯计算：SHA256 分片 + Rendezvous Hashing（不变）
├── cluster.rs          # 集群成员 + shard 所有权（不变）
├── interest.rs         # 三层索引：纯数据结构 + 同步操作
├── subscription.rs     # 订阅句柄管理：active_subscribers 的 CRUD（从 interest.rs 拆出）
├── forwarding.rs       # 数据面转发逻辑：interest 检查 + downstream put（从 main.rs 拆出）
├── discovery.rs        # Consumer 发现 + interest 拉取（从 interest.rs 拆出）
├── event_handlers.rs   # 高层事件处理：on_cluster_change / on_consumer_change（从 main.rs 拆出）
├── main.rs             # 精简编排：session 创建 + subscriber 声明 + 事件分发
├── consumer_sim.rs     # Consumer 模拟器（不变）
└── producer_sim.rs     # Producer 模拟器（不变）
```

每个模块职责单一，便于独立测试和演进。
