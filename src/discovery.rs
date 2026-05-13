use std::sync::{Arc, Mutex};
use zenoh::Session;
use crate::interest::GatewayState;

/// Pull a consumer's interest list via Queryable and register into the three-layer index.
///
/// This function handles the discovery of consumer interests through Zenoh's Queryable mechanism.
/// The consumer declares a Queryable at "gateway/interest/{client_id}", and this gateway queries it.
///
/// # Arguments
/// * `client_id` - The consumer's client ID
/// * `session` - The Zenoh session to use for querying
/// * `state` - Shared state containing the interest index
///
/// # Note
/// Dedup is handled by the caller (checking `mark_pulling()` before spawning).
pub async fn pull_consumer_interests(
    client_id: String,
    session: Session,
    state: Arc<Mutex<GatewayState>>,
) {
    let interest_query = format!("gateway/interest/{}", client_id);
    if let Ok(replies) = session.get(&interest_query).await {
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
                let mut s = state.lock().unwrap();
                s.register_interests(&client_id, &payload);
            }
        }
    }
}
