use zenoh::sample::SampleKind;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tokio::task;
use std::time::Duration;

use zenoh_gateway_poc::hashing;
use zenoh_gateway_poc::interest::{self, GatewayState};

#[tokio::main]
async fn main() {
    let my_id = std::env::args().nth(1).unwrap_or_else(|| "gw-1".to_string());
    let cluster_expr = "gateway/cluster/**";
    let consumer_liveliness_expr = "gateway/consumer/**";

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let state = Arc::new(Mutex::new(GatewayState::new(my_id.clone())));

    // Calculate initial load during initialization phase
    state.lock().unwrap().cluster.refresh_load_stats();

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
                SampleKind::Put => s.cluster.add_node(node_id),
                SampleKind::Delete => s.cluster.remove_node(&node_id),
            };
            if changed {
                println!("Cluster changed! Current nodes: {:?}", s.cluster.nodes());
                s.cluster.refresh_load_stats();
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
            if s.cluster.add_node(node_id) {
                s.cluster.refresh_load_stats();
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
                // Dedup: skip if this consumer's interests are already being pulled or have been pulled
                {
                    let mut s = interest_state.lock().unwrap();
                    if !s.mark_pulling(&client_id) {
                        println!("[{}] Skip duplicate pull for consumer: {}", s.my_id(), client_id);
                        return;
                    }
                }
                let state_clone = interest_state.clone();
                let sess_clone = interest_session.clone();
                let cid = client_id.clone();
                tokio::spawn(async move {
                    interest::pull_consumer_interests(cid, sess_clone, state_clone).await;
                });
            } else if sample.kind() == SampleKind::Delete {
                let mut s = interest_state.lock().unwrap();
                s.cleanup_interests(&client_id);
            }
        })
        .await.unwrap();

    // 4b. Synchronize existing consumers on startup
    let replies = session.liveliness().get(consumer_liveliness_expr).await.unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/consumer/").unwrap_or(key).to_string();
            // Dedup: skip if already handled by the Liveliness callback above
            {
                let mut s = state.lock().unwrap();
                if !s.mark_pulling(&client_id) {
                    continue;
                }
            }
            let state_clone = state.clone();
            let sess_clone = session.clone();
            let cid = client_id.clone();
            tokio::spawn(async move {
                interest::pull_consumer_interests(cid, sess_clone, state_clone).await;
            });
        }
    }

    // 5. Subscribe to shard data stream (Backbone)
    let forward_state = state.clone();
    let _sub_shard = session.declare_subscriber("shard/*")
        .callback(move |sample| {
            let shard_id = sample.key_expr().as_str();
            let s = forward_state.lock().unwrap();

            // Execute hash decision
            if s.cluster.is_owner(shard_id) {
                // B. Precise filtering (Interest Refinement)
                let original_key = sample.attachment()
                    .map(|a| String::from_utf8_lossy(&a.to_bytes()).to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                if s.local_interests.contains_key(&original_key) {
                    println!("[{}] (MATCH) Shard: {} -> Original: {}", s.my_id(), shard_id, original_key);
                }
            }
        })
        .await.unwrap();

    // 6. Timed Load Statistics (Shard Distribution Stats)
    let stats_state = state.clone();
    task::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let s = stats_state.lock().unwrap();
            
            println!("\n--- Load Stats [{}] ---", s.my_id());
            println!("Cluster Size: {}", s.cluster.nodes().len());
            println!("Nodes List: {:?}", s.cluster.nodes());
            println!("Owned Shards: {}/{}", s.cluster.owned_shards_cache(), hashing::SHARD_COUNT);
            println!("Total Known Interests: {}", s.local_interests.len());

            // Calculate current active topics and their distribution
            let mut active_shards = BTreeSet::new();
            let mut active_topics_count = 0;
            let mut active_details = Vec::new();

            for topic in s.local_interests.keys() {
                let shard = hashing::get_shard_id(topic);
                if s.cluster.is_owner(&shard) {
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
    tokio::time::sleep(Duration::from_secs(3600)).await;
}
