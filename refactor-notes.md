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
