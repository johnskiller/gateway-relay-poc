use std::sync::{Arc, Mutex};
use zenoh::Session;
use zenoh::sample::Sample;
use crate::interest::GatewayState;
use crate::metrics::{MetricsCollector, now_ns, decode_producer_attachment, encode_forward_attachment};

/// Handles message forwarding from upstream to downstream based on interest filtering.
///
/// This module isolates the forwarding logic from the subscription management,
/// making it easier to test and maintain. The handler checks if a topic has
/// local subscribers before forwarding messages.
///
/// # Latency Measurement
/// - **Ingress Latency**: Time from Producer send to Gateway receive (via attachment send_ts)
/// - **Forwarding Latency**: Time from Gateway receive to downstream put completion
///
/// # Attachment Protocol
/// - Producer → Gateway: `[topic_key_bytes][0x00][8 bytes send_ts_ns BE]`
/// - Gateway → Consumer: `[8 bytes send_ts_ns BE]` (topic is already in the key expression)
#[derive(Clone)]
pub struct ForwardingHandler {
    state: Arc<Mutex<GatewayState>>,
    downstream: Session,
    forwarding_metrics: Arc<MetricsCollector>,
    ingress_metrics: Arc<MetricsCollector>,
}

impl ForwardingHandler {
    /// Create a new ForwardingHandler.
    pub fn new(
        state: Arc<Mutex<GatewayState>>,
        downstream: Session,
        forwarding_metrics: Arc<MetricsCollector>,
        ingress_metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            state,
            downstream,
            forwarding_metrics,
            ingress_metrics,
        }
    }

    /// Handle a sample from upstream and forward if there are local subscribers.
    ///
    /// This method is called by the subscriber callback when messages arrive from upstream.
    /// It checks if the topic has local subscribers using the interest index, and if so,
    /// forwards the message to the downstream mesh.
    ///
    /// # Arguments
    /// * `sample` - The incoming sample from upstream
    pub fn on_sample(&self, sample: Sample) {
        let recv_ts = now_ns();

        if let Some(attr) = sample.attachment() {
            let attachment_bytes = attr.to_bytes();
            let (okey, send_ts) = match decode_producer_attachment(&attachment_bytes) {
                Some(result) => result,
                None => {
                    // Fallback: treat entire attachment as topic key (backward compat)
                    let okey = String::from_utf8_lossy(&attachment_bytes).to_string();
                    (okey, 0)
                }
            };

            let has_interest = self.state.lock().unwrap().topic_subscribers.contains_key(&okey);

            // Record ingress latency (send_ts → recv_ts)
            if send_ts > 0 {
                let ingress_latency = recv_ts.saturating_sub(send_ts);
                self.ingress_metrics.record_message();
                self.ingress_metrics.record_latency(ingress_latency);
            }

            if has_interest {
                let payload = sample.payload().clone();
                let ds = self.downstream.clone();
                let fwd_metrics = self.forwarding_metrics.clone();
                let forward_attachment = encode_forward_attachment(send_ts);

                tokio::spawn(async move {
                    // Forward message to downstream mesh with send_ts in attachment
                    let _ = ds.put(&*okey, payload)
                        .attachment(&forward_attachment)
                        .await;

                    // Record forwarding latency (recv_ts → forward complete)
                    let forward_ts = now_ns();
                    let forwarding_latency = forward_ts.saturating_sub(recv_ts);
                    fwd_metrics.record_message();
                    fwd_metrics.record_latency(forwarding_latency);
                });
            }
        }
    }
}
