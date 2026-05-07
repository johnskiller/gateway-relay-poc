use sha2::{Sha256, Digest};
use std::collections::BTreeSet;

pub const SHARD_COUNT: usize = 10000;

/// Format shard index to shard key expression (e.g., "shard/p0042")
pub fn shard_name(index: usize) -> String {
    format!("shard/p{:04}", index)
}

/// Rendezvous Hashing: determine if `my_id` is the owner of `shard_id`
/// among the given set of candidate nodes.
pub fn is_owner(nodes: &BTreeSet<String>, my_id: &str, shard_id: &str) -> bool {
    if nodes.is_empty() {
        return true;
    }

    let mut best_node: Option<&String> = None;
    let mut max_hash: Option<[u8; 32]> = None;

    for node in nodes {
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
    best_node.map(|n| n.as_str() == my_id).unwrap_or(false)
}

/// ShardMapper: maps an original topic to its shard ID (shard/p0000 - shard/p9999)
pub fn get_shard_id(topic: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(topic.as_bytes());
    let result = hasher.finalize();
    let mut b = [0u8; 8];
    b.copy_from_slice(&result[24..32]);
    let val = u64::from_be_bytes(b);
    shard_name(val as usize % SHARD_COUNT)
}
