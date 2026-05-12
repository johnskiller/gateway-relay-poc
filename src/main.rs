use zenoh::sample::{Sample, SampleKind};
use std::sync::{Arc, Mutex};
use tokio::task;
use std::time::Duration;

use zenoh_gateway_poc::hashing;
use zenoh_gateway_poc::interest::{self, GatewayState};

/// Synchronizes upstream Zenoh subscribers with the internal GatewayState.
/// This ensures we only pull data for shards we own AND have local interest for.
///
/// Subscriber handles are stored inside `GatewayState.active_subscribers`,
/// so there is no need for a separate `Arc<Mutex<HashMap<...>>>` parameter.
///
/// Uses atomic operations to eliminate TOCTOU race conditions:
/// - compute_diff_and_take_undeclare() computes diff AND extracts handles in one lock
/// - insert_subscriber() is called after async operations complete
async fn sync_shard_subscriptions(
    state_arc: Arc<Mutex<GatewayState>>,
    upstream: zenoh::Session,
    downstream: zenoh::Session,
) {
    // Atomic operation: compute diff + extract subscriber handles in one step
    // Eliminates TOCTOU race: diff and take operations complete under the same lock
    let (to_sub, to_undeclare) = {
        let mut s = state_arc.lock().unwrap();
        s.compute_diff_and_take_undeclare()
    };

    if to_sub.is_empty() && to_undeclare.is_empty() { return; }

    // Unsubscribe: async undeclaration outside the lock
    for sub in to_undeclare {
        println!("[{}] Dynamic Unsubscribe: {}", upstream.zid(), sub.key_expr());
        let _ = sub.undeclare().await;
    }

    // Subscribe: async — declare subscriber first (no lock), then lock briefly to insert
    for shard in to_sub {
        println!("[{}] Dynamic Subscribe: {}", upstream.zid(), shard);
        let ds = downstream.clone();
        let s_arc = state_arc.clone();
        let sub = upstream.declare_subscriber(&shard)
            .callback(move |sample: Sample| {
                if let Some(attr) = sample.attachment() {
                    let okey = String::from_utf8_lossy(&attr.to_bytes()).to_string();
                    let has_interest = s_arc.lock().unwrap().topic_subscribers.contains_key(&okey);
                    if has_interest {
                        let payload = sample.payload().clone();
                        let ds_inner = ds.clone();
                        tokio::spawn(async move {
                            // P0: True Message Forwarding to local mesh
                            let _ = ds_inner.put(okey, payload).await;
                        });
                    }
                }
            })
            .await.unwrap();
        // Insert subscriber after async operations complete outside the lock
        state_arc.lock().unwrap().insert_subscriber(shard, sub);
    }
}

#[tokio::main]
async fn main() {
    let my_id = std::env::args().nth(1).unwrap_or_else(|| "gw-1".to_string());
    let cluster_expr = "gateway/cluster/**";
    let consumer_liveliness_expr = "gateway/consumer/**";

    // P0: Dual Session Architecture for Network Isolation
    let upstream = zenoh::open(zenoh::Config::default()).await.unwrap();
    let downstream = zenoh::open(zenoh::Config::default()).await.unwrap();

    let state = Arc::new(Mutex::new(GatewayState::new(my_id.clone())));

    // Calculate initial load during initialization phase
    state.lock().unwrap().cluster.refresh_load_stats();

    // 1. Listen for cluster member changes on DOWNSTREAM mesh
    let member_state = state.clone();
    let up_clone = upstream.clone();
    let ds_clone = downstream.clone();

    let _sub_liveliness = downstream
        .liveliness()
        .declare_subscriber(cluster_expr)
        .callback(move |sample| {
            // Atomic operation: one lock for add_node/remove_node + refresh stats
            let (changed, nodes) = {
                let mut s = member_state.lock().unwrap();
                let key_str = sample.key_expr().as_str();
                let node_id = key_str.strip_prefix("gateway/cluster/").unwrap_or(key_str).to_string();
                s.handle_cluster_change(node_id, sample.kind())
            };

            if changed {
                // Print outside lock to avoid blocking other callbacks
                println!("Cluster changed (node: {})! Current nodes: {:?}", nodes[0], nodes);
                let s_sync = member_state.clone();
                let up_sync = up_clone.clone();
                let ds_sync = ds_clone.clone();
                tokio::spawn(async move {
                    sync_shard_subscriptions(s_sync, up_sync, ds_sync).await;
                });
            }
        })
        .await.unwrap();

    // 2. Register liveliness token on DOWNSTREAM
    let token_key = format!("gateway/cluster/{}", my_id);
    let token = downstream.liveliness().declare_token(&token_key).await.unwrap();
    let _token_handle = Arc::new(token);

    // 3. Actively query for currently alive nodes
    let replies = downstream.liveliness().get(cluster_expr).await.unwrap();
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
    let interest_up = upstream.clone();
    let interest_ds = downstream.clone();
    
    // Listen for Consumer lifecycle on DOWNSTREAM
    let _sub_consumer = downstream
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
                let sess_clone = interest_ds.clone();
                let up_sync = interest_up.clone();
                let ds_sync = interest_ds.clone();
                let cid = client_id;
                tokio::spawn(async move {
                    interest::pull_consumer_interests(cid, sess_clone, state_clone.clone()).await;
                    sync_shard_subscriptions(state_clone, up_sync, ds_sync).await;
                });
            } else if sample.kind() == SampleKind::Delete {
                {
                    let mut s = interest_state.lock().unwrap();
                    s.cleanup_interests(&client_id);
                }
                let s_sync = interest_state.clone();
                let up_sync = interest_up.clone();
                let ds_sync = interest_ds.clone();
                tokio::spawn(async move {
                    sync_shard_subscriptions(s_sync, up_sync, ds_sync).await;
                });
            }
        })
        .await.unwrap();

    // 4b. Synchronize existing consumers
    let replies = downstream.liveliness().get(consumer_liveliness_expr).await.unwrap();
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
            let up_sync = upstream.clone();
            let ds_sync = downstream.clone();
            let cid = client_id;
            tokio::spawn(async move {
                interest::pull_consumer_interests(cid, ds_sync.clone(), state_clone.clone()).await;
                sync_shard_subscriptions(state_clone, up_sync, ds_sync).await;
            });
        }
    }

    // 6. Timed Load Statistics (Shard Distribution Stats)
    // Use snapshot mode to avoid long lock holding and prevent blocking other callbacks
    let stats_state = state.clone();
    task::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let snapshot = stats_state.lock().unwrap().stats_snapshot();
            
            println!("\n--- Load Stats [{}] ---", snapshot.my_id);
            println!("Cluster Size: {}", snapshot.cluster_size);
            println!("Nodes List: {:?}", snapshot.nodes);
            println!("Owned Shards: {}/{}", snapshot.owned_shards, hashing::SHARD_COUNT);
            println!("Total Known Interests: {}", snapshot.total_interests);

            println!("Active Handled: {} Topics across {} Shards",
                snapshot.active_details.len(), snapshot.active_shards);
            if !snapshot.active_details.is_empty() {
                let details = snapshot.active_details.join(", ");
                println!("Active Details: [{}]", details);
            }
  
            println!("------------------------\n");
        }
    });

    println!("Gateway {} is running. Press Ctrl+C to stop.", my_id);
    tokio::time::sleep(Duration::from_secs(3600)).await;
}
