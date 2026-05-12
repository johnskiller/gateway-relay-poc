use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use crate::hashing;
use crate::cluster::ClusterState;
use zenoh::sample::SampleKind;

/// Combined gateway state: cluster membership + three-layer interest index + active subscribers.
/// All fields are protected by a single Mutex for simplicity in the PoC stage.
///
/// Three-layer index structure:
/// - client_topics:    ClientID → Set<Topic>   — O(M) cleanup on consumer offline
/// - topic_subscribers: Topic → Set<ClientID>   — O(1) forwarding filter
/// - shard_topics:     ShardID → Set<Topic>     — O(1) shard interest check for dynamic subscribe/unsubscribe
///
/// Active subscriber handles are co-located with the interest state to ensure
/// logical intent (which shards we want) and physical reality (which subscribers exist)
/// are always consistent — no risk of the two drifting apart.
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

    /// Active upstream Zenoh subscribers, keyed by shard ID.
    /// The HashMap keys serve the same role as the old `subscribed_shards: BTreeSet<String>`,
    /// while the values hold the actual subscriber handles for proper undeclaration.
    /// This eliminates the need for a separate `Arc<Mutex<HashMap<...>>>` in main.rs.
    pub active_subscribers: HashMap<String, zenoh::pubsub::Subscriber<()>>,
}

impl GatewayState {
    pub fn new(my_id: String) -> Self {
        Self {
            cluster: ClusterState::new(my_id),
            client_topics: HashMap::new(),
            topic_subscribers: HashMap::new(),
            shard_topics: HashMap::new(),
            pulling_consumers: HashSet::new(),
            active_subscribers: HashMap::new(),
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
    /// Note: does NOT mutate `active_subscribers`; the caller is responsible for
    /// calling `take_subscribers_for_undeclare` and `insert_subscriber` after
    /// the actual Zenoh operations succeed.
    pub fn compute_subscription_diff(&self) -> (Vec<String>, Vec<String>) {
        let mut desired_shards = BTreeSet::new();

        // A shard is desired ONLY if we are the owner AND someone in the local mesh wants it
        for (shard_id, topics) in &self.shard_topics {
            if !topics.is_empty() && self.cluster.is_owner(shard_id) {
                desired_shards.insert(shard_id.clone());
            }
        }

        let current_shards: BTreeSet<String> = self.active_subscribers.keys().cloned().collect();
        let to_subscribe = desired_shards.difference(&current_shards).cloned().collect();
        let to_unsubscribe = current_shards.difference(&desired_shards).cloned().collect();

        (to_subscribe, to_unsubscribe)
    }

    /// Remove and return subscriber handles for the given shards.
    /// Used to extract handles for explicit async undeclaration outside the mutex lock.
    pub fn take_subscribers_for_undeclare(&mut self, shards: &[String]) -> Vec<zenoh::pubsub::Subscriber<()>> {
        shards.iter()
            .filter_map(|shard| self.active_subscribers.remove(shard))
            .collect()
    }

    /// Insert a newly declared subscriber handle for a shard.
    pub fn insert_subscriber(&mut self, shard: String, sub: zenoh::pubsub::Subscriber<()>) {
        self.active_subscribers.insert(shard, sub);
    }

    /// Atomic operation: compute subscription diff + extract subscriber handles in one step
    /// Eliminates TOCTOU race: diff and take operations complete under the same lock
    pub fn compute_diff_and_take_undeclare(&mut self)
        -> (Vec<String>, Vec<zenoh::pubsub::Subscriber<()>>)
    {
        let mut desired_shards = BTreeSet::new();

        // A shard is desired ONLY if we are the owner AND someone in the local mesh wants it
        for (shard_id, topics) in &self.shard_topics {
            if !topics.is_empty() && self.cluster.is_owner(shard_id) {
                desired_shards.insert(shard_id.clone());
            }
        }

        let current_shards: BTreeSet<String> = self.active_subscribers.keys().cloned().collect();
        let to_subscribe: Vec<String> = desired_shards.difference(&current_shards).cloned().collect();
        let to_unsubscribe: Vec<String> = current_shards.difference(&desired_shards).cloned().collect::<Vec<_>>();

        let to_undeclare: Vec<zenoh::pubsub::Subscriber<()>> = to_unsubscribe.iter()
            .filter_map(|shard| self.active_subscribers.remove(shard))
            .collect();

        (to_subscribe, to_undeclare)
    }

    /// Atomic operation: add/remove node + refresh stats, return snapshot for lock-free printing
    /// Caller doesn't need to manually manage multi-step locking
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

    /// Snapshot: clone required data, release lock before printing
    pub fn stats_snapshot(&self) -> StatsSnapshot {
        // Calculate active shard count (deduplicated)
        let active_shards: BTreeSet<String> = self.topic_subscribers.keys()
            .filter(|topic| {
                let shard = hashing::get_shard_id(topic);
                self.cluster.is_owner(&shard)
            })
            .map(|topic| hashing::get_shard_id(topic))
            .collect();
        
        StatsSnapshot {
            my_id: self.my_id().to_string(),
            cluster_size: self.cluster.nodes().len(),
            nodes: self.cluster.nodes().clone(),
            owned_shards: self.cluster.owned_shards_cache(),
            total_interests: self.topic_subscribers.len(),
            active_shards: active_shards.len(),
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

/// Statistics snapshot structure for lock-free printing
pub struct StatsSnapshot {
    pub my_id: String,
    pub cluster_size: usize,
    pub nodes: BTreeSet<String>,
    pub owned_shards: usize,
    pub total_interests: usize,
    pub active_shards: usize,
    pub active_details: Vec<String>,
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
