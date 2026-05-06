# Zenoh Gateway PoC 设计文档 - Rendezvous Hashing 验证

## 1. 背景与目标
本 PoC 旨在验证在千万级 Topic 压力下，通过 **Source Sharding** 将流量收敛至 10,000 个分片，并使用 **Rendezvous Hashing** 在 Gateway 集群中实现无状态、自组织的负载均衡。

## 2. 核心功能设计

### 2.1 节点身份与集群发现 (Node ID & Liveliness)
- 每个实例启动时通过命令行参数 `--id` 指定唯一标识。
- 使用 Zenoh `Liveliness Token` 机制：
    - 路径：`gateway/cluster/<node_id>`
    - 行为：启动时 `declare_liveliness`，同时监听 `gateway/cluster/*`。
    - 产出：每个节点维护一个实时更新的 `BTreeSet<NodeID>`。

### 2.2 兴趣感知机制 (Interest Management)
为了避免全局订阅 `client/**` 带来的性能问题，采用 **"Active Announcement"** 方案：
- **Key Space**: `gateway/announcement/<client_id>`
- **数据结构**: 客户端发布其感兴趣的 Topic 列表。
- **网关行为**: 
    - 订阅本地网络的 `gateway/announcement/*`。
    - 维护 `LocalInterestTable`: `OriginalTopic -> Vec<ClientID>`。
    - 将 `OriginalTopic` 哈希映射到 `ShardID` (p0 - p9999)。

### 2.3 分片映射与所有权仲裁 (Sharding & Rendezvous Hashing)
- **Topic 到分片的映射**: 
    - `ShardMapper(topic) -> hash(topic) % 10000` -> 产生 `shard/p_xx`。
- **所有权算法 (Rendezvous Hashing)**:
    - 对于每一个活动分片 $S$，计算所有存活节点 $N_i$ 的分值：$Score = hash(N_i + S)$。
    - 拥有最高 $Score$ 的节点被认定为该分片的 **Owner**。
    - **PoC 逻辑**: 网关只对满足 `is_owner(shard_id)` 的分片逻辑上“激活”订阅。

### 2.4 分片负载统计 (Shard Distribution Stats)
- **监控维度**: 每个节点负责的分片总数、具体分片 ID 列表。
- **实现方式**: 
    - PoC 阶段：设置定时器（如每 5 秒），将当前节点的负载状态打印至控制台。
    - 演进方向：未来通过内置的 HTTP 服务或 Zenoh Queryable 提供 JSON 格式的状态数据，接入可视化 Dashboard。

### 2.5 本地兴趣精确匹配 (Interest Refinement)
- **核心逻辑**: 在分片所有权的基础上，进一步维护 `ShardID -> Set<OriginalTopic>` 的映射关系。
- **价值**: 当数据包从 Backbone 到达 Gateway 时，Gateway 不仅检查分片归属，还会根据 Attachment 中的原始 Key 进行过滤。只有本地确实有 Client 订阅了该原始 Key 时才执行转发，实现真正的“按需流量分配”。

## 3. PoC 验证流程
1. **启动 Router**: 充当 Backbone 中心。
2. **启动 Gateway A & B**: 
    - 观察 `Liveliness` 触发的成员更新。
    - 记录 A 和 B 各自计算出的“初始负责分片集”。
    - **观察控制台输出**: 确认分片在两个节点间是否分配均匀。
3. **模拟 Client 订阅**: 
    - 发送 `gateway/announcement/c1`，包含 10 个跨度较大的 Topic。
    - 观察 Gateway 如何将这些 Topic 归类到 Shard。
    - 观察只有“分片 Owner”的网关记录了该兴趣。
4. **动态再平衡与容错验证**: 
    - **节点退出**: 杀死 Gateway A，观察 Gateway B 是否自动接管了原本属于 A 的分片，记录从节点失效到完成接管的“静默期”时长。
    - **节点加入**: 重新启动 Gateway A，观察部分分片是否从 B 重新迁移回 A。
5. **精确匹配验证**:
    - 模拟分片 `shard/p1` 中混杂了多个原始 Topic 的数据，验证 Gateway 是否仅转发了 Client c1 声明过的那个 Topic。

## 4. 关键评估指标 (KPI)
- **收敛率**: 验证 46M Topic 是否能准确收敛到 10k Shard 空间。
- **分配均匀度**: 集群节点间分片分配的标准差。
- **再平衡速度**: 成员变更到哈希重新计算完成的延迟。

## 5. 暂不涉及范围
- 实际的消息发布 (Publish) 与转发 (Forwarding)。
- 存储插件 (Storage Plugin) 的集成。
- 复杂的 ACL 安全认证。