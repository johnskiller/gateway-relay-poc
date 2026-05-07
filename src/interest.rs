use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use crate::hashing;
use crate::cluster::ClusterState;

/// Combined gateway state: cluster membership + interest tracking.
/// Both are protected by a single Mutex for simplicity in the PoC stage.
pub struct GatewayState {
    pub cluster: ClusterState,
    // Original Topic -> Set of Local Client IDs
    pub local_interests: HashMap<String, BTreeSet<String>>,
    // Track consumers whose interests have been/are being pulled (dedup)
    pub pulling_consumers: HashSet<String>,
}

impl GatewayState {
    pub fn new(my_id: String) -> Self {
        Self {
            cluster: ClusterState::new(my_id),
            local_interests: HashMap::new(),
            pulling_consumers: HashSet::new(),
        }
    }

    pub fn my_id(&self) -> &str {
        self.cluster.my_id()
    }

    /// Register interests for a consumer (from a pulled topic list).
    pub fn register_interests(&mut self, client_id: &str, topics: &str) {
        for topic in topics.split(',') {
            let t = topic.trim();
            if !t.is_empty() {
                let shard = hashing::get_shard_id(t);
                self.local_interests.entry(t.to_string()).or_default().insert(client_id.to_string());
                println!("[{}] Pulled Interest: {} -> {}", self.my_id(), t, shard);
            }
        }
    }

    /// Clean up all interests for a consumer that went offline.
    pub fn cleanup_interests(&mut self, client_id: &str) {
        println!("[{}] Cleaning up interests for client: {}", self.my_id(), client_id);
        // Remove from pulling_consumers so a re-appear will trigger a fresh pull
        self.pulling_consumers.remove(client_id);
        // Iterate over all topics, remove this Client ID
        self.local_interests.retain(|_topic, clients| {
            clients.remove(client_id);
            if clients.is_empty() {
                return false; // Remove this Topic Key
            }
            true
        });
    }

    /// Check if a consumer's interests should be pulled (dedup guard).
    /// Returns true if this is a new consumer that hasn't been pulled yet.
    pub fn mark_pulling(&mut self, client_id: &str) -> bool {
        self.pulling_consumers.insert(client_id.to_string())
    }
}

/// Pull a consumer's interest list via Queryable and register into local_interests.
/// Dedup is handled by the caller (checking `mark_pulling()` before spawning).
pub async fn pull_consumer_interests(
    client_id: String,
    session: zenoh::Session,
    state: Arc<Mutex<GatewayState>>,
) {
    let interest_query = format!("gateway/interest/{}", client_id);
    if let Ok(replies) = session.get(&interest_query).await {
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
                let mut s = state.lock().unwrap();
                s.register_interests(&client_id, &payload);
            }
        }
    }
}
