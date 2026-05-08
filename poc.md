# Zenoh Gateway PoC 设计文档 - Rendezvous Hashing 验证

## 1. 背景与目标
本 PoC 旨在验证在千万级 Topic 压力下，通过 **Source Sharding** 将流量收敛至 10,000 个分片，并使用 **Rendezvous Hashing** 在 Gateway 集群中实现无状态、自组织的负载均衡。

## 2. 核心功能设计

### 2.1 节点身份与集群发现 (Node ID & Liveliness)
- 每个实例启动时通过命令行参数 `--id` 指定唯一标识。
- 使用 Zenoh `Liveliness Token` 机制：
    - 路径：`gateway/cluster/<node_id>`
    - 行为：启动时 `declare_liveliness`，同时监听 `gateway/cluster/**`。
    - 产出：每个节点维护一个实时更新的 `BTreeSet<NodeID>`。
- 启动时主动查询 `liveliness.get()` 同步已有节点，避免遗漏先启动的成员。

### 2.2 双 Session 架构 (Dual Session Architecture)
为实现网络隔离（Backbone 与 Local Mesh 分离），Gateway 采用双 Session 设计：

| Session | 连接目标 | 职责 |
|---------|----------|------|
| **Upstream** | Backbone Router | 声明 Subscriber 订阅分片数据，接收来自 Producer 的消息 |
| **Downstream** | Local Mesh Router | 监听 Liveliness 事件、声明自身 Liveliness Token、转发消息给本地 Consumer |

此架构确保 Backbone 流量不会泄漏到本地网络，本地 Consumer 的发现流量不会干扰 Backbone。

### 2.3 兴趣感知机制 (Interest Management — Liveliness + Pull)
采用 **Liveliness + Pull** 方案，完全基于 Zenoh 原语实现，无需外部存储插件：

- **Consumer 端行为**：
    1. 为每个感兴趣的 Topic 调用 `session.declare_subscriber()` 注册真正的订阅（接收 Gateway 转发的消息）。
    2. 声明 Queryable：路径 `gateway/interest/<client_id>`，回复内容为逗号分隔的 Topic 列表。
    3. 声明 Liveliness Token：路径 `gateway/consumer/<client_id>`，宣告自身在线。

- **Gateway 端行为**：
    1. 监听 `gateway/consumer/**` 的 Liveliness 事件。
    2. **Consumer 上线**（`SampleKind::Put`）：
        - 去重检查：`mark_pulling(client_id)` 防止重复拉取。
        - 异步拉取：`pull_consumer_interests()` 通过 `session.get("gateway/interest/<client_id>")` 查询 Consumer 的 Queryable，获取 Topic 列表。
        - 注册兴趣：将拉取到的 Topic 列表写入三层索引。
        - 同步订阅：`sync_shard_subscriptions()` 根据索引变更调整上游订阅。
    3. **Consumer 下线**（`SampleKind::Delete`）：
        - 清理索引：`cleanup_interests()` 从三层索引中移除该 Consumer 的所有记录。
        - 同步订阅：`sync_shard_subscriptions()` 释放不再需要的上游订阅。
    4. 启动时主动查询 `liveliness.get("gateway/consumer/**")` 同步已有 Consumer。

### 2.4 三层索引结构 (Three-Layer Interest Index)
`GatewayState` 维护三层索引，保证各操作的时间复杂度最优：

| 层级 | 数据结构 | 用途 | 操作复杂度 |
|------|----------|------|------------|
| Layer 1 | `client_topics: HashMap<ClientID, BTreeSet<Topic>>` | Consumer 下线时 O(M) 清理（M = 该 Consumer 的 Topic 数） | O(M) 删除 |
| Layer 2 | `topic_subscribers: HashMap<Topic, BTreeSet<ClientID>>` | 转发时 O(1) 判断是否有本地订阅者 | O(1) 查询 |
| Layer 3 | `shard_topics: HashMap<ShardID, BTreeSet<Topic>>` | 动态订阅时 O(1) 判断分片是否有兴趣 | O(1) 查询 |

此外还有两个辅助字段：
- `pulling_consumers: HashSet<ClientID>` — 去重守卫，防止同一 Consumer 被重复拉取。
- `active_subscribers: HashMap<ShardID, Subscriber>` — 活跃的上游订阅句柄，keys 即为当前已订阅分片集合。

三层索引在 `register_interests()` 和 `cleanup_interests()` 中原子维护，保证一致性。

### 2.5 分片映射与所有权仲裁 (Sharding & Rendezvous Hashing)
- **Topic 到分片的映射**：
    - `ShardMapper(topic) → SHA256(topic) % 10000` → 产生 `shard/p0000` ~ `shard/p9999`（4 位零填充）。
- **所有权算法 (Rendezvous Hashing)**：
    - 对于每一个活动分片 $S$，计算所有存活节点 $N_i$ 的分值：$Score = SHA256(N_i + "|" + S)$。
    - 拥有最高 $Score$ 的节点被认定为该分片的 **Owner**。
    - Gateway 只对满足 `is_owner(shard_id)` 的分片激活上游订阅。

### 2.6 动态订阅管理 (Dynamic Shard Subscription)
`sync_shard_subscriptions()` 是核心编排函数，负责将三层索引的"逻辑意图"与上游 Zenoh 的"物理订阅"保持一致：

1. **计算 Diff**：`compute_subscription_diff()` 对比 `desired_shards`（有本地兴趣 + 本节点 Owner）与 `current_shards`（`active_subscribers` 的 keys），输出 `to_subscribe` / `to_unsubscribe` 列表。
2. **取消订阅**：`take_subscribers_for_undeclare()` 从 `active_subscribers` 中取出句柄，锁外执行 `sub.undeclare().await`。
3. **新建订阅**：对每个新分片调用 `upstream.declare_subscriber(shard)` 注册回调，成功后 `insert_subscriber()` 存入句柄。

### 2.7 消息转发 (Message Forwarding)
完整的数据面转发链路已实现：

- **Producer → Gateway**：Producer 将消息发送到分片 Key（如 `shard/p0042`），原始 Topic 通过 Zenoh Attachment 携带。
- **Gateway 转发逻辑**（在 `declare_subscriber` 的回调中）：
    1. 从 `sample.attachment()` 提取原始 Topic Key（`okey`）。
    2. 查询 `topic_subscribers.contains_key(&okey)` 判断本地是否有订阅者。
    3. 若有，通过 `downstream.put(okey, payload)` 转发到本地 Mesh。
- **Gateway → Consumer**：Consumer 在启动时已通过 `declare_subscriber(okey)` 订阅了原始 Topic，直接接收消息。

### 2.8 分片负载统计 (Shard Distribution Stats)
- **监控维度**：集群大小、节点列表、本节点负责的分片总数、活跃兴趣数、活跃 Topic 详情。
- **实现方式**：5 秒定时器，打印当前负载快照至控制台。
- **演进方向**：未来通过 HTTP 服务或 Zenoh Queryable 提供 JSON 格式状态数据，接入可视化 Dashboard。

## 3. PoC 验证流程
1. **启动 Router**：充当 Backbone 中心。
2. **启动 Gateway A & B**：
    - 观察 `Liveliness` 触发的成员更新。
    - 记录 A 和 B 各自计算出的"初始负责分片集"。
    - **观察控制台输出**：确认分片在两个节点间是否分配均匀。
3. **模拟 Consumer 订阅**：`cargo run --bin consumer-sim -- c1 topics.txt`
    - 观察 Gateway 日志中的 `Pulled Interest` 输出。
    - 观察 `Dynamic Subscribe` 输出，确认分片订阅已建立。
4. **模拟 Producer 发送**：`cargo run --bin producer-sim -- topics.txt 500`
    - 观察 Consumer 端是否收到转发的消息。
    - 验证 Attachment 中的原始 Topic 是否正确匹配。
5. **动态再平衡与容错验证**：
    - 杀死 A，观察 B 的 `Active Handled Topics` 增加。
    - 杀掉 Consumer，观察 Gateway 日志中的 `Cleaning up interests`。
6. **精确匹配验证**：
    - 模拟分片 `shard/p0001` 中混杂了多个原始 Topic 的数据，验证 Gateway 是否仅转发了 Consumer 声明过的 Topic。

## 4. 关键评估指标 (KPI)
- **收敛率**：验证 46M Topic 是否能准确收敛到 10k Shard 空间。
- **分配均匀度**：集群节点间分片分配的标准差。
- **再平衡速度**：成员变更到哈希重新计算完成的延迟。
- **转发精确度**：仅转发本地有订阅者的 Topic，无冗余流量。

## 5. 模块结构

```
src/
├── hashing.rs          # 纯计算：SHA256 分片 + Rendezvous Hashing
├── cluster.rs          # 集群成员 + shard 所有权
├── interest.rs         # 三层索引 + 订阅句柄 + pull 逻辑
├── lib.rs              # 模块声明
├── main.rs             # 事件驱动编排 + 转发回调 + 统计循环
├── consumer_sim.rs     # Consumer 模拟器
└── producer_sim.rs     # Producer 模拟器
```

## 6. 暂不涉及范围
- 存储插件 (Storage Plugin) 的集成（当前使用 Liveliness + Queryable Pull 替代）。
- 复杂的 ACL 安全认证。
- 消息持久化与重传。
- 多级级联（Gateway 嵌套）。
