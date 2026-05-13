use std::collections::HashMap;
use zenoh::pubsub::Subscriber;

/// Manages active upstream Zenoh subscribers, keyed by shard ID.
///
/// This module isolates subscription handle management from the interest index,
/// making the GatewayState more focused on interest tracking and making
/// subscription operations easier to test and reason about.
pub struct SubscriptionManager {
    /// Active upstream Zenoh subscribers, keyed by shard ID.
    /// The HashMap keys serve as the set of currently subscribed shards.
    active_subscribers: HashMap<String, Subscriber<()>>,
}

impl SubscriptionManager {
    /// Create a new empty SubscriptionManager.
    pub fn new() -> Self {
        Self {
            active_subscribers: HashMap::new(),
        }
    }

    /// Get the set of currently subscribed shard IDs.
    pub fn current_shards(&self) -> std::collections::BTreeSet<String> {
        self.active_subscribers.keys().cloned().collect()
    }

    /// Remove and return subscriber handles for the given shards.
    /// Used to extract handles for explicit async undeclaration outside the mutex lock.
    pub fn take_for_undeclare(&mut self, shards: &[String]) -> Vec<Subscriber<()>> {
        shards.iter()
            .filter_map(|shard| self.active_subscribers.remove(shard))
            .collect()
    }

    /// Insert a newly declared subscriber handle for a shard.
    pub fn insert(&mut self, shard: String, sub: Subscriber<()>) {
        self.active_subscribers.insert(shard, sub);
    }

    /// Remove a subscriber handle for a shard without returning it.
    /// Useful for cleanup when the subscriber is already being handled elsewhere.
    pub fn remove(&mut self, shard: &str) -> bool {
        self.active_subscribers.remove(shard).is_some()
    }

    /// Check if a shard is currently subscribed.
    pub fn is_subscribed(&self, shard: &str) -> bool {
        self.active_subscribers.contains_key(shard)
    }

    /// Get the number of currently active subscribers.
    pub fn count(&self) -> usize {
        self.active_subscribers.len()
    }

    /// Clear all subscribers (useful for testing or shutdown).
    pub fn clear(&mut self) {
        self.active_subscribers.clear();
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}
