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
    let consumer_liveliness_expr = "gateway/consumer/**";

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

    // 4. Interest Management (Scheme E: Liveliness + Pull)
    let interest_state = state.clone();
    let interest_session = session.clone();
    
    // Listen for Consumer lifecycle via Liveliness
    let _sub_consumer = session
        .liveliness()
        .declare_subscriber(consumer_liveliness_expr)
        .callback(move |sample| {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/consumer/").unwrap_or(key).to_string();
            
            if sample.kind() == SampleKind::Put {
                // When a consumer is detected, PULL its interests
                let state_clone = interest_state.clone();
                let sess_clone = interest_session.clone();
                let cid = client_id.clone();
                tokio::spawn(async move {
                    let interest_query = format!("gateway/interest/{}", cid);
                    if let Ok(replies) = sess_clone.get(&interest_query).await {
                        while let Ok(reply) = replies.recv_async().await {
                            if let Ok(sample) = reply.result() {
                                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
                                let mut s = state_clone.lock().unwrap();
                                for topic in payload.split(',') {
                                    let t = topic.trim();
                                    if !t.is_empty() {
                                        let shard = GatewayState::get_shard_id(t);
                                        s.local_interests.entry(t.to_string()).or_default().insert(cid.clone());
                                        println!("[{}] Pulled Interest: {} -> {}", s.my_id, t, shard);
                                    }
                                }
                            }
                        }
                    }
                });
            } else if sample.kind() == SampleKind::Delete {
                let mut s = interest_state.lock().unwrap();
                println!("[{}] Cleaning up interests for client: {}", s.my_id, client_id); 
                // Iterate over all topics, remove this Client ID
                s.local_interests.retain(|topic, clients| {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        return false; // Remove this Topic Key
                    }
                    true
                });
            }
        })
        .await.unwrap();

    // 4b. Synchronize existing consumers on startup
    let replies = session.liveliness().get(consumer_liveliness_expr).await.unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/consumer/").unwrap_or(key).to_string();
            // Trigger the same pull logic for existing nodes
            // (In a real implementation, you'd refactor the pull logic into a function)
            let state_clone = state.clone();
            let sess_clone = session.clone();
            tokio::spawn(async move {
                let interest_query = format!("gateway/interest/{}", client_id);
                if let Ok(replies) = sess_clone.get(&interest_query).await {
                    while let Ok(reply) = replies.recv_async().await {
                        if let Ok(sample) = reply.result() {
                            let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
                            let mut s = state_clone.lock().unwrap();
                            for topic in payload.split(',') {
                                let t = topic.trim();
                                if !t.is_empty() {
                                    s.local_interests.entry(t.to_string()).or_default().insert(client_id.clone());
                                }
                            }
                        }
                    }
                }
            });
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
