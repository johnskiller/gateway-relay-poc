use std::sync::{Arc, Mutex};
use zenoh::Session;
use zenoh::sample::Sample;
use crate::interest::GatewayState;

/// Handles message forwarding from upstream to downstream based on interest filtering.
///
/// This module isolates the forwarding logic from the subscription management,
/// making it easier to test and maintain. The handler checks if a topic has
/// local subscribers before forwarding messages.
///
/// # Arguments
/// * `state` - Shared state containing the interest index
/// * `downstream` - Downstream Zenoh session for publishing
#[derive(Clone)]
pub struct ForwardingHandler {
    state: Arc<Mutex<GatewayState>>,
    downstream: Session,
}

impl ForwardingHandler {
    /// Create a new ForwardingHandler.
    pub fn new(state: Arc<Mutex<GatewayState>>, downstream: Session) -> Self {
        Self {
            state,
            downstream,
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
        if let Some(attr) = sample.attachment() {
            let okey = String::from_utf8_lossy(&attr.to_bytes()).to_string();
            let has_interest = self.state.lock().unwrap().topic_subscribers.contains_key(&okey);
            if has_interest {
                let payload = sample.payload().clone();
                let ds = self.downstream.clone();
                tokio::spawn(async move {
                    // Forward message to downstream mesh
                    let _ = ds.put(okey, payload).await;
                });
            }
        }
    }
}

