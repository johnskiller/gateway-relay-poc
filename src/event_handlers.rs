use std::sync::{Arc, Mutex};
use zenoh::Session;
use zenoh::sample::SampleKind;
use crate::interest::GatewayState;
use crate::forwarding::ForwardingHandler;
use crate::metrics::MetricsCollector;
use crate::discovery;

/// Handle cluster member changes.
///
/// This function encapsulates the logic for processing cluster membership updates:
/// - Adds or removes nodes based on the sample kind
/// - Refreshes load statistics
/// - Triggers shard subscription synchronization if the cluster changed
///
/// # Arguments
/// * `state` - Shared state containing cluster membership and subscription management
/// * `upstream` - Upstream Zenoh session for declaring subscribers
/// * `downstream` - Downstream Zenoh session for publishing
/// * `node_id` - The node ID that changed
/// * `kind` - The sample kind (Put = add, Delete = remove)
/// * `forwarding_metrics` - Metrics collector for forwarding latency
/// * `ingress_metrics` - Metrics collector for ingress latency
pub async fn on_cluster_change(
    state: Arc<Mutex<GatewayState>>,
    upstream: Session,
    downstream: Session,
    node_id: String,
    kind: SampleKind,
    forwarding_metrics: Arc<MetricsCollector>,
    ingress_metrics: Arc<MetricsCollector>,
) {
    // Atomic operation: one lock for add_node/remove_node + refresh stats
    let (changed, nodes) = {
        let mut s = state.lock().unwrap();
        let changed = match kind {
            SampleKind::Put => s.cluster.add_node(node_id.clone()),
            SampleKind::Delete => s.cluster.remove_node(&node_id),
        };
        if changed {
            s.cluster.refresh_load_stats();
        }
        let nodes: Vec<String> = s.cluster.nodes().iter().cloned().collect();
        (changed, nodes)
    };

    if changed {
        // Print outside lock to avoid blocking other callbacks
        println!("Cluster changed (node: {})! Current nodes: {:?}", nodes[0], nodes);
        let s_sync = state.clone();
        let up_sync = upstream.clone();
        let ds_sync = downstream.clone();
        let fwd_m = forwarding_metrics.clone();
        let ing_m = ingress_metrics.clone();
        tokio::spawn(async move {
            sync_shard_subscriptions(s_sync, up_sync, ds_sync, fwd_m, ing_m).await;
        });
    }
}

/// Handle consumer lifecycle events (online/offline).
///
/// This function encapsulates the logic for processing consumer events:
/// - For online (Put): Marks consumer as pulling, pulls interests, triggers subscription sync
/// - For offline (Delete): Cleans up interests, triggers subscription sync
///
/// # Arguments
/// * `state` - Shared state containing interest index and subscription management
/// * `upstream` - Upstream Zenoh session for declaring subscribers
/// * `downstream` - Downstream Zenoh session for publishing
/// * `client_id` - The consumer's client ID
/// * `kind` - The sample kind (Put = online, Delete = offline)
/// * `forwarding_metrics` - Metrics collector for forwarding latency
/// * `ingress_metrics` - Metrics collector for ingress latency
pub async fn on_consumer_change(
    state: Arc<Mutex<GatewayState>>,
    upstream: Session,
    downstream: Session,
    client_id: String,
    kind: SampleKind,
    forwarding_metrics: Arc<MetricsCollector>,
    ingress_metrics: Arc<MetricsCollector>,
) {
    if kind == SampleKind::Put {
        // Dedup: skip if this consumer's interests are already being pulled or have been pulled
        {
            let mut s = state.lock().unwrap();
            if !s.mark_pulling(&client_id) {
                println!("[{}] Skip duplicate pull for consumer: {}", s.my_id(), client_id);
                return;
            }
        }
        let state_clone = state.clone();
        let up_sync = upstream.clone();
        let ds_sync = downstream.clone();
        let cid = client_id;
        let fwd_m = forwarding_metrics.clone();
        let ing_m = ingress_metrics.clone();
        tokio::spawn(async move {
            discovery::pull_consumer_interests(cid, ds_sync.clone(), state_clone.clone()).await;
            sync_shard_subscriptions(state_clone, up_sync, ds_sync, fwd_m, ing_m).await;
        });
    } else if kind == SampleKind::Delete {
        {
            let mut s = state.lock().unwrap();
            s.cleanup_interests(&client_id);
        }
        let s_sync = state.clone();
        let up_sync = upstream.clone();
        let ds_sync = downstream.clone();
        let fwd_m = forwarding_metrics.clone();
        let ing_m = ingress_metrics.clone();
        tokio::spawn(async move {
            sync_shard_subscriptions(s_sync, up_sync, ds_sync, fwd_m, ing_m).await;
        });
    }
}

/// Synchronizes upstream Zenoh subscribers with the internal GatewayState.
/// This ensures we only pull data for shards we own AND have local interest for.
///
/// Subscriber handles are stored inside `GatewayState.subscription_manager`,
/// so there is no need for a separate `Arc<Mutex<HashMap<...>>>` parameter.
///
/// Uses atomic operations to eliminate TOCTOU race conditions:
/// - compute_diff_and_take_undeclare() computes diff AND extracts handles in one lock
/// - insert_subscriber() is called after async operations complete
pub async fn sync_shard_subscriptions(
    state_arc: Arc<Mutex<GatewayState>>,
    upstream: Session,
    downstream: Session,
    forwarding_metrics: Arc<MetricsCollector>,
    ingress_metrics: Arc<MetricsCollector>,
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
    // Use ForwardingHandler for the data plane forwarding logic
    let handler = ForwardingHandler::new(state_arc.clone(), downstream.clone(), forwarding_metrics, ingress_metrics);
    for shard in to_sub {
        println!("[{}] Dynamic Subscribe: {}", upstream.zid(), shard);
        let h = handler.clone();
        let sub = upstream.declare_subscriber(&shard)
            .callback(move |sample| {
                h.on_sample(sample);
            })
            .await.unwrap();
        // Insert subscriber after async operations complete outside the lock
        state_arc.lock().unwrap().insert_subscriber(shard, sub);
    }
}
