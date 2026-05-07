use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use crate::hashing;
use crate::cluster::ClusterState;

/// Combined gateway state: cluster membership + three-layer interest index.
/// All fields are protected by a single Mutex for simplicity in the PoC stage.
///
/// Three-layer index structure:
/// - client_topics:    ClientID → Set<Topic>   — O(M) cleanup on consumer offline
/// - topic_subscribers: Topic → Set<ClientID>   — O(1) forwarding filter
/// - shard_topics:     ShardID → Set<Topic>     — O(1) shard interest check for dynamic subscribe/unsubscribe
pub struct GatewayState {
    pub cluster: ClusterState,

    /// Layer 1: ClientID → Set<Topic>
    /// Used for O(M) cleanup when a consumer goes offline (M = number of topics for that client)
    pub client_topics: HashMap<String, BTreeSet<String>>,

    /// Layer 2: Topic → Set<ClientID>
    /// Used for O(1) forwarding filter: does this topic have any local subscribers?
    pub topic_subscribers: HashMap<String, BTreeSet<String>>,

    /// Layer 3: ShardID → Set<Topic>
    /// Used for O(1) shard interest check: should we subscribe to this shard?
    pub shard_topics: HashMap<String, BTreeSet<String>>,

    /// Track consumers whose interests have been/are being pulled (dedup guard)
    pub pulling_consumers: HashSet<String>,

    /// Shards currently subscribed to on the upstream session.
    /// Used to avoid redundant subscribe/unsubscribe calls.
    pub subscribed_shards: BTreeSet<String>,
}

impl GatewayState {
    pub fn new(my_id: String) -> Self {
        Self {
            cluster: ClusterState::new(my_id),
            client_topics: HashMap::new(),
            topic_subscribers: HashMap::new(),
            shard_topics: HashMap::new(),
            pulling_consumers: HashSet::new(),
            subscribed_shards: BTreeSet::new(),
        }
    }

    pub fn my_id(&self) -> &str {
        self.cluster.my_id()
    }

    /// Register interests for a consumer (from a pulled topic list).
    /// Maintains all three index layers atomically.
    pub fn register_interests(&mut self, client_id: &str, topics: &str) {
        for topic in topics.split(',') {
            let t = topic.trim();
            if !t.is_empty() {
                // Layer 1: client_topics — track which topics this client cares about
                let is_new = self.client_topics
                    .entry(client_id.to_string())
                    .or_default()
                    .insert(t.to_string());

                if is_new {
                    // Layer 2: topic_subscribers — track which clients care about this topic
                    self.topic_subscribers
                        .entry(t.to_string())
                        .or_default()
                        .insert(client_id.to_string());

                    // Layer 3: shard_topics — track which topics map to this shard
                    let shard = hashing::get_shard_id(t);
                    self.shard_topics
                        .entry(shard.clone())
                        .or_default()
                        .insert(t.to_string());

                    println!("[{}] Pulled Interest: {} -> {} (client: {})", self.my_id(), t, shard, client_id);
                }
            }
        }
    }

    /// Clean up all interests for a consumer that went offline.
    /// Maintains all three index layers atomically.
    pub fn cleanup_interests(&mut self, client_id: &str) {
        println!("[{}] Cleaning up interests for client: {}", self.my_id(), client_id);
        // Remove from pulling_consumers so a re-appear will trigger a fresh pull
        self.pulling_consumers.remove(client_id);

        // Layer 1: remove client entry, get their topics for cleanup
        if let Some(topics) = self.client_topics.remove(client_id) {
            for topic in topics {
                // Layer 2: remove client from topic_subscribers
                if let Some(subscribers) = self.topic_subscribers.get_mut(&topic) {
                    subscribers.remove(client_id);
                    if subscribers.is_empty() {
                        // No more subscribers for this topic — remove from Layer 2
                        self.topic_subscribers.remove(&topic);

                        // Layer 3: remove topic from shard_topics
                        let shard = hashing::get_shard_id(&topic);
                        if let Some(shard_topic_set) = self.shard_topics.get_mut(&shard) {
                            shard_topic_set.remove(&topic);
                            if shard_topic_set.is_empty() {
                                // No more topics in this shard — remove from Layer 3
                                self.shard_topics.remove(&shard);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Check if a consumer's interests should be pulled (dedup guard).
    /// Returns true if this is a new consumer that hasn't been pulled yet.
    pub fn mark_pulling(&mut self, client_id: &str) -> bool {
        self.pulling_consumers.insert(client_id.to_string())
    }

    /// Check if a topic has any local subscribers (O(1) forwarding filter).
    pub fn has_subscribers(&self, topic: &str) -> bool {
        self.topic_subscribers.get(topic).map_or(false, |s| !s.is_empty())
    }

    /// Get the set of shard IDs that have active interests AND are owned by this node.
    /// Used for dynamic shard subscription management.
    pub fn get_active_shards(&self) -> BTreeSet<String> {
        self.shard_topics.keys()
            .filter(|shard| self.cluster.is_owner(shard))
            .cloned()
            .collect()
    }

    /// Calculates which shards need to be subscribed or unsubscribed based on 
    /// current cluster ownership and local mesh interests.
    /// Returns (to_subscribe, to_unsubscribe) lists.
    pub fn compute_subscription_diff(&mut self) -> (Vec<String>, Vec<String>) {
        let mut desired_shards = BTreeSet::new();
        
        // A shard is desired ONLY if we are the owner AND someone in the local mesh wants it
        for (shard_id, topics) in &self.shard_topics {
            if !topics.is_empty() && self.cluster.is_owner(shard_id) {
                desired_shards.insert(shard_id.clone());
            }
        }

        let to_subscribe = desired_shards.difference(&self.subscribed_shards).cloned().collect();
        let to_unsubscribe = self.subscribed_shards.difference(&desired_shards).cloned().collect();

        // Update internal cache to match the new intended state
        self.subscribed_shards = desired_shards;

        (to_subscribe, to_unsubscribe)
    }
}

/// Pull a consumer's interest list via Queryable and register into the three-layer index.
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
