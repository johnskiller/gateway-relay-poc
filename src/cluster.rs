use std::collections::BTreeSet;
use crate::hashing::{self, SHARD_COUNT};

/// Cluster membership state: tracks live gateway nodes and shard ownership.
pub struct ClusterState {
    nodes: BTreeSet<String>,
    my_id: String,
    owned_shards_cache: usize,
}

impl ClusterState {
    pub fn new(my_id: String) -> Self {
        let mut nodes = BTreeSet::new();
        nodes.insert(my_id.clone()); // Initial candidates must include self
        Self {
            nodes,
            my_id,
            owned_shards_cache: 0,
        }
    }

    pub fn my_id(&self) -> &str {
        &self.my_id
    }

    pub fn nodes(&self) -> &BTreeSet<String> {
        &self.nodes
    }

    pub fn owned_shards_cache(&self) -> usize {
        self.owned_shards_cache
    }

    /// Rendezvous Hashing: determines if this node is the owner of a shard.
    pub fn is_owner(&self, shard_id: &str) -> bool {
        hashing::is_owner(&self.nodes, &self.my_id, shard_id)
    }

    /// Add a node to the cluster. Returns true if it was newly inserted.
    pub fn add_node(&mut self, node_id: String) -> bool {
        self.nodes.insert(node_id)
    }

    /// Remove a node from the cluster. Returns true if it was present.
    pub fn remove_node(&mut self, node_id: &str) -> bool {
        self.nodes.remove(node_id)
    }

    /// Recalculates the total number of shards this node is responsible for.
    /// Only called on member changes.
    pub fn refresh_load_stats(&mut self) {
        let mut count = 0;
        let mut shard_name_buf = String::with_capacity(12);
        for i in 0..SHARD_COUNT {
            shard_name_buf.clear();
            use std::fmt::Write;
            write!(shard_name_buf, "shard/p{:04}", i).unwrap();
            if self.is_owner(&shard_name_buf) {
                count += 1;
            }
        }
        self.owned_shards_cache = count;
        println!("[{}] Shard ownership recalculated. Now owning {}/{} shards.",
            self.my_id, self.owned_shards_cache, SHARD_COUNT);
    }
}
