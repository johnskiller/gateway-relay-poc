use zenoh::sample::{Sample, SampleKind};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tokio::task;
use std::time::Duration;

use zenoh_gateway_poc::hashing;
use zenoh_gateway_poc::interest::{self, GatewayState};

/// Synchronizes upstream Zenoh subscribers with the internal GatewayState.
/// This ensures we only pull data for shards we own AND have local interest for.
async fn sync_shard_subscriptions(
    state_arc: Arc<Mutex<GatewayState>>,
    upstream: zenoh::Session,
    downstream: zenoh::Session,
    subs_arc: Arc<Mutex<HashMap<String, zenoh::pubsub::Subscriber<()>>>>,
) {
    let (to_sub, to_unsub) = {
        let mut s = state_arc.lock().unwrap();
        s.compute_subscription_diff()
    };

    if to_sub.is_empty() && to_unsub.is_empty() { return; }

    // Unsubscribe: synchronous — lock briefly, remove, drop lock before any .await
    {
        let mut subs = subs_arc.lock().unwrap();
        for shard in &to_unsub {
            println!("[{}] Dynamic Unsubscribe: {}", upstream.zid(), shard);
            subs.remove(shard);
        }
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
        subs_arc.lock().unwrap().insert(shard, sub);
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
    let shard_subs = Arc::new(Mutex::new(HashMap::new()));

    // Calculate initial load during initialization phase
    state.lock().unwrap().cluster.refresh_load_stats();

    // 1. Listen for cluster member changes on DOWNSTREAM mesh
    let member_state = state.clone();
    let up_clone = upstream.clone();
    let ds_clone = downstream.clone();
    let subs_clone = shard_subs.clone();

    let _sub_liveliness = downstream
        .liveliness()
        .declare_subscriber(cluster_expr)
        .callback(move |sample| {
            let (changed, node_id) = {
                let mut s = member_state.lock().unwrap();
                let key_str = sample.key_expr().as_str();
                let node_id = key_str.strip_prefix("gateway/cluster/").unwrap_or(key_str).to_string();
                let changed = match sample.kind() {
                    SampleKind::Put => s.cluster.add_node(node_id.clone()),
                    SampleKind::Delete => s.cluster.remove_node(&node_id),
                };
                (changed, node_id)
            };

            if changed {
                {
                    let mut s = member_state.lock().unwrap();
                    println!("Cluster changed (node: {})! Current nodes: {:?}", node_id, s.cluster.nodes());
                    s.cluster.refresh_load_stats();
                }
                let s_sync = member_state.clone();
                let up_sync = up_clone.clone();
                let ds_sync = ds_clone.clone();
                let subs_sync = subs_clone.clone();
                tokio::spawn(async move {
                    sync_shard_subscriptions(s_sync, up_sync, ds_sync, subs_sync).await;
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
    let interest_subs = shard_subs.clone();
    
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
                let subs_sync = interest_subs.clone();
                let cid = client_id;
                tokio::spawn(async move {
                    interest::pull_consumer_interests(cid, sess_clone, state_clone.clone()).await;
                    sync_shard_subscriptions(state_clone, up_sync, ds_sync, subs_sync).await;
                });
            } else if sample.kind() == SampleKind::Delete {
                {
                    let mut s = interest_state.lock().unwrap();
                    s.cleanup_interests(&client_id);
                }
                let s_sync = interest_state.clone();
                let up_sync = interest_up.clone();
                let ds_sync = interest_ds.clone();
                let subs_sync = interest_subs.clone();
                tokio::spawn(async move {
                    sync_shard_subscriptions(s_sync, up_sync, ds_sync, subs_sync).await;
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
            let subs_sync = shard_subs.clone();
            let cid = client_id;
            tokio::spawn(async move {
                interest::pull_consumer_interests(cid, ds_sync.clone(), state_clone.clone()).await;
                sync_shard_subscriptions(state_clone, up_sync, ds_sync, subs_sync).await;
            });
        }
    }

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
            println!("Total Known Interests: {}", s.topic_subscribers.len());

            // Calculate current active topics and their distribution
            let mut active_shards = BTreeSet::new();
            let mut active_topics_count = 0;
            let mut active_details = Vec::new();

            for topic in s.topic_subscribers.keys() {
                let shard = hashing::get_shard_id(topic);
                if s.cluster.is_owner(&shard) {
                    active_topics_count += 1;
                    active_shards.insert(shard.clone());
                    active_details.push(format!("{} ({})", topic, shard));
                }
            }
            println!("Active Handled: {} Topics across {} Shards", active_topics_count, active_shards.len());
            if !active_details.is_empty() {
                let details = active_details.join(", ");
                println!("Active Details: [{}]", details);
            }
 
            println!("------------------------\n");
        }
    });

    println!("Gateway {} is running. Press Ctrl+C to stop.", my_id);
    tokio::time::sleep(Duration::from_secs(3600)).await;
}
