use std::sync::{Arc, Mutex};
use tokio::task;
use std::time::Duration;

use zenoh_gateway_poc::hashing;
use zenoh_gateway_poc::interest::GatewayState;
use zenoh_gateway_poc::event_handlers;
use zenoh_gateway_poc::config::GatewayConfig;
use zenoh_gateway_poc::metrics::MetricsCollector;

#[tokio::main]
async fn main() {
    // Parse CLI args: gateway [id] [--config <path>]
    let args: Vec<String> = std::env::args().collect();
    let mut cli_id: Option<String> = None;
    let mut config_path: Option<&str> = None;

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            config_path = Some(&args[i + 1]);
            i += 2;
        } else if cli_id.is_none() {
            cli_id = Some(args[i].clone());
            i += 1;
        } else {
            i += 1;
        }
    }

    // Load configuration
    let config = GatewayConfig::load(config_path);
    let my_id = config.resolve_id(cli_id.as_deref());

    println!("Gateway ID: {}", my_id);
    println!("Upstream connect: {:?}", config.upstream.connect);
    println!("Downstream connect: {:?}", config.downstream.connect);

    let cluster_expr = config.cluster_expr.clone();
    let consumer_liveliness_expr = config.consumer_liveliness_expr.clone();
    let stats_interval = Duration::from_secs(config.stats_interval_secs);

    // P0: Dual Session Architecture for Network Isolation
    let upstream_config = GatewayConfig::to_zenoh_config(&config.upstream);
    let downstream_config = GatewayConfig::to_zenoh_config(&config.downstream);

    let upstream = zenoh::open(upstream_config).await.unwrap();
    let downstream = zenoh::open(downstream_config).await.unwrap();

    let state = Arc::new(Mutex::new(GatewayState::new(my_id.clone())));

    // Create metrics collectors
    let forwarding_metrics = Arc::new(MetricsCollector::new());
    let ingress_metrics = Arc::new(MetricsCollector::new());

    // Calculate initial load during initialization phase
    state.lock().unwrap().cluster.refresh_load_stats();

    // 1. Listen for cluster member changes on DOWNSTREAM mesh
    let member_state = state.clone();
    let up_clone = upstream.clone();
    let ds_clone = downstream.clone();
    let member_fwd_metrics = forwarding_metrics.clone();
    let member_ing_metrics = ingress_metrics.clone();

    let _sub_liveliness = downstream
        .liveliness()
        .declare_subscriber(&cluster_expr)
        .callback(move |sample| {
            let key_str = sample.key_expr().as_str();
            let node_id = key_str.strip_prefix("gateway/cluster/").unwrap_or(key_str).to_string();
            // Clone before async move to satisfy Fn closure requirements
            let s_sync = member_state.clone();
            let up_sync = up_clone.clone();
            let ds_sync = ds_clone.clone();
            let fwd_m = member_fwd_metrics.clone();
            let ing_m = member_ing_metrics.clone();
            tokio::spawn(async move {
                event_handlers::on_cluster_change(s_sync, up_sync, ds_sync, node_id, sample.kind(), fwd_m, ing_m).await;
            });
        })
        .await.unwrap();

    // 2. Register liveliness token on DOWNSTREAM
    let token_key = format!("gateway/cluster/{}", my_id);
    let token = downstream.liveliness().declare_token(&token_key).await.unwrap();
    let _token_handle = Arc::new(token);

    // 3. Actively query for currently alive nodes
    let replies = downstream.liveliness().get(&cluster_expr).await.unwrap();
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
    let interest_fwd_metrics = forwarding_metrics.clone();
    let interest_ing_metrics = ingress_metrics.clone();

    // Listen for Consumer lifecycle on DOWNSTREAM
    let _sub_consumer = downstream
        .liveliness()
        .declare_subscriber(&consumer_liveliness_expr)
        .callback(move |sample| {
            let key = sample.key_expr().as_str();
            let client_id = key.strip_prefix("gateway/consumer/").unwrap_or(key).to_string();
            // Clone before async move to satisfy Fn closure requirements
            let s_sync = interest_state.clone();
            let up_sync = interest_up.clone();
            let ds_sync = interest_ds.clone();
            let fwd_m = interest_fwd_metrics.clone();
            let ing_m = interest_ing_metrics.clone();
            tokio::spawn(async move {
                event_handlers::on_consumer_change(s_sync, up_sync, ds_sync, client_id, sample.kind(), fwd_m, ing_m).await;
            });
        })
        .await.unwrap();

    // 4b. Synchronize existing consumers
    let replies = downstream.liveliness().get(&consumer_liveliness_expr).await.unwrap();
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
            let fwd_m = forwarding_metrics.clone();
            let ing_m = ingress_metrics.clone();
            tokio::spawn(async move {
                zenoh_gateway_poc::discovery::pull_consumer_interests(cid, ds_sync.clone(), state_clone.clone()).await;
                event_handlers::sync_shard_subscriptions(state_clone, up_sync, ds_sync, fwd_m, ing_m).await;
            });
        }
    }

    // 6. Timed Load Statistics (Shard Distribution Stats)
    // Use snapshot mode to avoid long lock holding and prevent blocking other callbacks
    let stats_state = state.clone();
    let stats_forwarding = forwarding_metrics.clone();
    let stats_ingress = ingress_metrics.clone();
    task::spawn(async move {
        loop {
            tokio::time::sleep(stats_interval).await;
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

            // Print metrics
            if let Some(snap) = stats_forwarding.snapshot_and_reset() {
                println!("Messages Forwarded: {}", snap.msg_count);
                println!("{}", snap.format_latency_line("Forwarding Latency"));
            }
            if let Some(snap) = stats_ingress.snapshot_and_reset() {
                println!("Messages Received (Ingress): {}", snap.msg_count);
                println!("{}", snap.format_latency_line("Ingress Latency (send→recv)"));
            }

            println!("------------------------\n");
        }
    });

    println!("Gateway {} is running. Press Ctrl+C to stop.", my_id);
    tokio::time::sleep(Duration::from_secs(3600)).await;
}
