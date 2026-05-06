use zenoh::Wait;
use zenoh::sample::SampleKind;
use sha2::{Sha256, Digest};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use async_std::task;
use std::time::Duration;

const SHARD_COUNT: usize = 10000;

struct GatewayState {
    nodes: BTreeSet<String>,
    my_id: String,
    // Original Topic -> Set of Local Client IDs
    local_interests: HashMap<String, BTreeSet<String>>,
    owned_shards_cache: usize,
}

impl GatewayState {
    fn new(my_id: String) -> Self {
        let mut nodes = BTreeSet::new();
        nodes.insert(my_id.clone()); // 初始时候选人必须包含自己
        Self {
            nodes,
            my_id,
            local_interests: HashMap::new(),
            owned_shards_cache: 0,
        }
    }

    // Rendezvous Hashing: 判定自己是否是某个分片的 Owner (Internal logic)
    fn is_owner(&self, shard_id: &str) -> bool {
        if self.nodes.is_empty() { return true; }

        let mut best_node: Option<&String> = None;
        let mut max_hash: Option<[u8; 32]> = None;

        for node in &self.nodes {
            let mut hasher = Sha256::new();
            hasher.update(node.as_bytes());
            hasher.update(shard_id.as_bytes());
            let h: [u8; 32] = hasher.finalize().into();

            if max_hash.is_none() || h > *max_hash.as_ref().unwrap() {
                max_hash = Some(h);
                best_node = Some(node);
            }
        }
        best_node.map(|n| n == &self.my_id).unwrap_or(false)
    }

    // 重新计算当前节点负责的分片总数，仅在成员变化时调用
    fn refresh_load_stats(&mut self) {
        let mut count = 0;
        for i in 0..SHARD_COUNT {
            let shard = format!("shard/p{:04}", i);
            if self.is_owner(&shard) {
                count += 1;
            }
        }
        self.owned_shards_cache = count;
        println!("[{}] Shard ownership recalculated. Now owning {}/{} shards.", 
            self.my_id, self.owned_shards_cache, SHARD_COUNT);
    }

    // ShardMapper: Map Original Topic to Shard ID (shard/p0000 - shard/p9999)
    fn get_shard_id(topic: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(topic.as_bytes());
        let result = hasher.finalize();
        let mut b = [0u8; 8];
        b.copy_from_slice(&result[24..32]);
        let val = u64::from_be_bytes(b);
        format!("shard/p{:04}", val % SHARD_COUNT as u64)
    }
}

#[async_std::main]
async fn main() {
    let my_id = std::env::args().nth(1).unwrap_or_else(|| "gw-1".to_string());
    let cluster_expr = "gateway/cluster/**";
    let announcement_expr = "gateway/announcement/*";

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let state = Arc::new(Mutex::new(GatewayState::new(my_id.clone())));

    // 在初始化阶段先计算一次初始负载
    state.lock().unwrap().refresh_load_stats();

    // 1. 注册存活令牌 (Liveliness)
    let token_key = format!("gateway/cluster/{}", my_id);
    let token = session.liveliness().declare_token(&token_key).await.unwrap();
    let _token_handle = Arc::new(token);

    // 给网络拓扑发现留出一点预热时间
    task::sleep(Duration::from_millis(100)).await;

    // 2. 监听集群成员变化
    let member_state = state.clone();
    let _sub_liveliness = session
        .liveliness()
        .declare_subscriber(cluster_expr)
        .callback(move |sample| {
            let mut s = member_state.lock().unwrap();
            let key_str = sample.key_expr().as_str();
            let node_id = key_str.strip_prefix("gateway/cluster/").unwrap_or(key_str).to_string();
            let changed = match sample.kind() {
                SampleKind::Put => s.nodes.insert(node_id),
                SampleKind::Delete => s.nodes.remove(&node_id),
            };
            if changed {
                println!("Cluster changed! Current nodes: {:?}", s.nodes);
                s.refresh_load_stats();
            }
        })
        .await.unwrap();

    // 2b. 主动查询当前存活节点 (同步历史状态)
    let replies = session.liveliness().get(cluster_expr).await.unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let mut s = state.lock().unwrap();
            let key_str = sample.key_expr().as_str();
            let node_id = key_str.strip_prefix("gateway/cluster/").unwrap_or(key_str).to_string();
            if s.nodes.insert(node_id) {
                s.refresh_load_stats();
            }
        }
    }

    // 3. 兴趣管理 (Interest Management)
    let interest_state = state.clone();
    let _sub_announcement = session.declare_subscriber(announcement_expr)
        .callback(move |sample| {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/announcement/").unwrap_or("unknown");
            let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
            
            let mut s = interest_state.lock().unwrap();
            if sample.kind() == SampleKind::Put {
                for topic in payload.split(',') {
                    let t = topic.trim();
                    if !t.is_empty() {
                        let shard = GatewayState::get_shard_id(t);
                        println!("[{}] Local Interest: {} -> {}", s.my_id, t, shard);
                        s.local_interests.entry(t.to_string()).or_default().insert(client_id.to_string());
                    }
                }
            }
        })
        .await.unwrap();

    // 3c. 提供查询接口 (Queryable)，允许其他网关同步已有的兴趣
    let query_state = state.clone();
    let _queryable = session.declare_queryable(announcement_expr)
        .callback(move |query| {
            let s = query_state.lock().unwrap();
            // 为简化 PoC，我们将所有兴趣打包发送。
            // 实际场景应根据 query.key_expr() 过滤
            let all_topics = s.local_interests.keys().cloned().collect::<Vec<_>>().join(",");
            if !all_topics.is_empty() {
                let _ = query.reply(query.key_expr(), all_topics).wait();
            }
        })
        .await.unwrap();

    // 3b. 主动查询当前已存在的公告 (同步历史状态)
    let replies = session.get(announcement_expr).await.unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/announcement/").unwrap_or("unknown");
            let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
            
            let mut s = state.lock().unwrap();
            for topic in payload.split(',') {
                let t = topic.trim();
                if !t.is_empty() {
                    let shard = GatewayState::get_shard_id(t);
                    if !s.local_interests.contains_key(t) {
                        println!("[{}] Initial Interest: {} -> {}", s.my_id, t, shard);
                    }
                    s.local_interests.entry(t.to_string()).or_default().insert(client_id.to_string());
                }
            }
        }
    }

    // 4. 订阅分片数据流 (Backbone)
    let forward_state = state.clone();
    let _sub_shard = session.declare_subscriber("shard/*")
        .callback(move |sample| {
            let shard_id = sample.key_expr().as_str();
            let s = forward_state.lock().unwrap();

            // 执行哈希决策
            if s.is_owner(shard_id) {
                // B. 精确过滤 (Interest Refinement)
                let original_key = sample.attachment()
                    .map(|a| String::from_utf8_lossy(&a.to_bytes()).to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                if s.local_interests.contains_key(&original_key) {
                    println!("[{}] (MATCH) Shard: {} -> Original: {}", s.my_id, shard_id, original_key);
                }
            }
        })
        .await.unwrap();

    // 5. 定时负载统计 (Shard Distribution Stats)
    let stats_state = state.clone();
    task::spawn(async move {
        loop {
            task::sleep(Duration::from_secs(5)).await;
            let s = stats_state.lock().unwrap();
            
            // 计算当前节点真正“负责”的 Topic 数量
            let active_topics = s.local_interests.keys()
                .filter(|t| s.is_owner(&GatewayState::get_shard_id(t)))
                .count();

            println!("\n--- Load Stats [{}] ---", s.my_id);
            println!("Cluster Size: {}", s.nodes.len());
            println!("Nodes List: {:?}", s.nodes);
            println!("Owned Shards: {}/{}", s.owned_shards_cache, SHARD_COUNT);
            println!("Total Known Interests: {}", s.local_interests.len());
            println!("Active Handled Topics: {}", active_topics);
            println!("------------------------\n");
        }
    });

    println!("Gateway {} is running. Press Ctrl+C to stop.", my_id);
    async_std::task::sleep(std::time::Duration::from_secs(3600)).await;
}
