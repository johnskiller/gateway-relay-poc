use sha2::{Sha256, Digest};
use zenoh::sample::SampleKind;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tokio::task; // Replaced with tokio's task module
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
        nodes.insert(my_id.clone()); // Initial candidates must include self
        Self {
            nodes,
            my_id,
            local_interests: HashMap::new(),
            owned_shards_cache: 0,
        }
    }

    // Rendezvous Hashing: Determines if this node is the owner of a shard (Internal logic)
    fn is_owner(&self, shard_id: &str) -> bool {
        if self.nodes.is_empty() { return true; }

        let mut best_node: Option<&String> = None;
        let mut max_hash: Option<[u8; 32]> = None;

        for node in &self.nodes {
            let mut hasher = Sha256::new();
            // Use a separator to avoid string concatenation ambiguity and ensure more uniform mixing
            hasher.update(node.as_bytes());
            hasher.update(b"|");
            hasher.update(shard_id.as_bytes());
            let h: [u8; 32] = hasher.finalize().into();

            if max_hash.is_none() || h > *max_hash.as_ref().unwrap() {
                max_hash = Some(h);
                best_node = Some(node);
            }
        }
        best_node.map(|n| n == &self.my_id).unwrap_or(false)
    }

    // Recalculates the total number of shards this node is responsible for, only called on member changes
    fn refresh_load_stats(&mut self) {
        let mut count = 0;
        // Pre-allocate buffer to reduce memory allocation overhead
        let mut shard_name = String::with_capacity(12); 
        for i in 0..SHARD_COUNT {
            shard_name.clear();
            use std::fmt::Write;
            write!(shard_name, "shard/p{:04}", i).unwrap();
            if self.is_owner(&shard_name) {
                count += 1;
            }
        }
        self.owned_shards_cache = count;
        println!("[{}] Shard ownership recalculated. Now owning {}/{} shards.", 
            self.my_id, self.owned_shards_cache, SHARD_COUNT);
    }

    // ShardMapper: Maps Original Topic to Shard ID (shard/p0000 - shard/p9999)
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

#[tokio::main] // Replaced with tokio's main macro
async fn main() {
    let my_id = std::env::args().nth(1).unwrap_or_else(|| "gw-1".to_string());
    let cluster_expr = "gateway/cluster/**";
    let announcement_expr = "gateway/announcement/*";

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let state = Arc::new(Mutex::new(GatewayState::new(my_id.clone())));

    // Calculate initial load during initialization phase
    state.lock().unwrap().refresh_load_stats();

    // 1. Listen for cluster member changes (Declare subscriber first to ensure no events are missed)
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

    // 2. Register liveliness token (Broadcast presence after our listener is ready)
    let token_key = format!("gateway/cluster/{}", my_id);
    let token = session.liveliness().declare_token(&token_key).await.unwrap();
    let _token_handle = Arc::new(token);

    // 3. Actively query for currently alive nodes (synchronize historical state)
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

    // 3. Interest Management
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
                        println!("[{}] Local Interest Registered: {} (Topic) -> {} (Shard)", s.my_id, t, shard);
                        s.local_interests.entry(t.to_string()).or_default().insert(client_id.to_string());
                    }
                }
            } else if sample.kind() == SampleKind::Delete {
                let local_id = s.my_id.clone();
                println!("[{}] Cleaning up interests for client: {}", local_id, client_id); 
                // Iterate over all topics, remove this Client ID
                s.local_interests.retain(|topic, clients| {
                    clients.remove(client_id);
                    if clients.is_empty() {
                        println!("[{}] No more clients interested in {}, removing topic.", local_id, topic);
                        return false; // Remove this Topic Key
                    }
                    true
                });
            }
        })
        .await.unwrap();

    // 3c. Provide queryable interface, allowing other gateways to synchronize existing interests
    let query_state = state.clone();
    let queryable = session.declare_queryable(announcement_expr).await.unwrap();
    tokio::spawn(async move { // Replaced with tokio's spawn
        while let Ok(query) = queryable.recv_async().await {
            let all_topics = {
                let s = query_state.lock().unwrap();
                s.local_interests.keys().cloned().collect::<Vec<_>>().join(",")
            };
            
            if !all_topics.is_empty() {
                // Now can use .await here instead of .wait()
                let _ = query.reply(query.key_expr(), all_topics).await;
            }
        }
    });

    // 3b. Actively query for existing announcements (synchronize historical state)
    let replies = session.get(announcement_expr).await.unwrap();
    println!("[{}] Querying for existing announcements on '{}'", my_id, announcement_expr);
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/announcement/").unwrap_or("unknown");
            let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
            
            let mut s = state.lock().unwrap();
            for topic in payload.split(',') {
                println!("[{}] Found existing announcement: client_id={}, topic={}", my_id, client_id, topic); // Debug log
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

    // 4. Subscribe to shard data stream (Backbone)
    let forward_state = state.clone();
    let _sub_shard = session.declare_subscriber("shard/*")
        .callback(move |sample| {
            let shard_id = sample.key_expr().as_str();
            let s = forward_state.lock().unwrap();

            // Execute hash decision
            if s.is_owner(shard_id) {
                // B. Precise filtering (Interest Refinement)
                let original_key = sample.attachment()
                    .map(|a| String::from_utf8_lossy(&a.to_bytes()).to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                if s.local_interests.contains_key(&original_key) {
                    println!("[{}] (MATCH) Shard: {} -> Original: {}", s.my_id, shard_id, original_key);
                }
            }
        })
        .await.unwrap();

    // 5. Timed Load Statistics (Shard Distribution Stats)
    let stats_state = state.clone();
    task::spawn(async move {
        loop { // Replaced with tokio's sleep
            tokio::time::sleep(Duration::from_secs(5)).await;
            let s = stats_state.lock().unwrap();
            
            println!("\n--- Load Stats [{}] ---", s.my_id);
            println!("Cluster Size: {}", s.nodes.len());
            println!("Nodes List: {:?}", s.nodes);
            println!("Owned Shards: {}/{}", s.owned_shards_cache, SHARD_COUNT);
            println!("Total Known Interests: {}", s.local_interests.len()); // Total unique topics known

            // Calculate current active topics and their distribution
            let mut active_shards = BTreeSet::new();
            let mut active_topics_count = 0;
            let mut active_details = Vec::new();

            for topic in s.local_interests.keys() {
                let shard = GatewayState::get_shard_id(topic);
                if s.is_owner(&shard) {
                    active_topics_count += 1;
                    active_shards.insert(shard.clone());
                    active_details.push(format!("{} ({})", topic, shard));
                }
            }
            println!("Active Handled: {} Topics across {} Shards", active_topics_count, active_shards.len());
            if !active_details.is_empty() {
                println!("Active Details: [{}]", active_details.join(", "));
            }
 
            println!("------------------------\n");
        }
    });

    println!("Gateway {} is running. Press Ctrl+C to stop.", my_id);
    tokio::time::sleep(Duration::from_secs(3600)).await; // Replaced with tokio's sleep
}
