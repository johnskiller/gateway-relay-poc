use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Thread-safe metrics collector for latency and throughput measurement.
///
/// Records message counts and latency samples, then produces snapshots
/// with percentile statistics on demand.
pub struct MetricsCollector {
    msg_count: AtomicU64,
    latency_samples: Mutex<Vec<u64>>,
}

/// A point-in-time snapshot of metrics statistics.
pub struct MetricsSnapshot {
    pub msg_count: u64,
    pub latency_min_ns: u64,
    pub latency_max_ns: u64,
    pub latency_avg_ns: u64,
    pub latency_p50_ns: u64,
    pub latency_p90_ns: u64,
    pub latency_p99_ns: u64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            msg_count: AtomicU64::new(0),
            latency_samples: Mutex::new(Vec::with_capacity(1024)),
        }
    }

    /// Increment the message counter by 1.
    pub fn record_message(&self) {
        self.msg_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current message count (for conditional logging).
    pub fn msg_count(&self) -> u64 {
        self.msg_count.load(Ordering::Relaxed)
    }

    /// Record a latency sample in nanoseconds.
    pub fn record_latency(&self, latency_ns: u64) {
        if let Ok(mut samples) = self.latency_samples.lock() {
            samples.push(latency_ns);
        }
    }

    /// Take a snapshot of current metrics and reset all counters.
    /// Returns None if no messages were recorded.
    pub fn snapshot_and_reset(&self) -> Option<MetricsSnapshot> {
        let msg_count = self.msg_count.swap(0, Ordering::Relaxed);
        let samples = if let Ok(mut guard) = self.latency_samples.lock() {
            std::mem::take(&mut *guard)
        } else {
            Vec::new()
        };

        if msg_count == 0 && samples.is_empty() {
            return None;
        }

        let latency_stats = if samples.is_empty() {
            // No latency samples, just report count
            MetricsSnapshot {
                msg_count,
                latency_min_ns: 0,
                latency_max_ns: 0,
                latency_avg_ns: 0,
                latency_p50_ns: 0,
                latency_p90_ns: 0,
                latency_p99_ns: 0,
            }
        } else {
            let mut sorted = samples;
            sorted.sort_unstable();

            let min = sorted[0];
            let max = sorted[sorted.len() - 1];
            let sum: u64 = sorted.iter().sum();
            let avg = sum / sorted.len() as u64;

            let p50 = percentile(&sorted, 50);
            let p90 = percentile(&sorted, 90);
            let p99 = percentile(&sorted, 99);

            MetricsSnapshot {
                msg_count,
                latency_min_ns: min,
                latency_max_ns: max,
                latency_avg_ns: avg,
                latency_p50_ns: p50,
                latency_p90_ns: p90,
                latency_p99_ns: p99,
            }
        };

        Some(latency_stats)
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate the percentile value from a sorted slice.
/// Uses nearest-rank method.
fn percentile(sorted: &[u64], p: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p as usize * sorted.len()) + 99) / 100;
    let idx = idx.min(sorted.len()) - 1;
    sorted[idx]
}

impl MetricsSnapshot {
    /// Format latency value: use microseconds for values < 1ms, milliseconds otherwise.
    fn format_latency(ns: u64) -> String {
        if ns < 1_000 {
            format!("{}ns", ns)
        } else if ns < 1_000_000 {
            format!("{:.0}us", ns as f64 / 1_000.0)
        } else {
            format!("{:.2}ms", ns as f64 / 1_000_000.0)
        }
    }

    /// Format a latency line for display.
    pub fn format_latency_line(&self, label: &str) -> String {
        format!(
            "{}: min={} max={} avg={} p50={} p90={} p99={}",
            label,
            Self::format_latency(self.latency_min_ns),
            Self::format_latency(self.latency_max_ns),
            Self::format_latency(self.latency_avg_ns),
            Self::format_latency(self.latency_p50_ns),
            Self::format_latency(self.latency_p90_ns),
            Self::format_latency(self.latency_p99_ns),
        )
    }
}

// === Utility functions for timestamp handling ===

/// Get current time as nanoseconds since Unix epoch.
/// Used for embedding send timestamps in message attachments.
pub fn now_ns() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Encode a send timestamp as 8 bytes big-endian.
pub fn encode_send_ts(ts_ns: u64) -> [u8; 8] {
    ts_ns.to_be_bytes()
}

/// Decode a send timestamp from 8 bytes big-endian.
pub fn decode_send_ts(bytes: &[u8]) -> Option<u64> {
    if bytes.len() >= 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[0..8]);
        Some(u64::from_be_bytes(arr))
    } else {
        None
    }
}

/// Encode attachment for Producer → Gateway: [topic_key_bytes][0x00][8 bytes send_ts_ns BE]
pub fn encode_producer_attachment(topic: &str, send_ts_ns: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(topic.len() + 1 + 8);
    buf.extend_from_slice(topic.as_bytes());
    buf.push(0x00);
    buf.extend_from_slice(&encode_send_ts(send_ts_ns));
    buf
}

/// Decode producer attachment: returns (topic_key, send_ts_ns)
pub fn decode_producer_attachment(attachment: &[u8]) -> Option<(String, u64)> {
    // Find the 0x00 separator
    let sep_pos = attachment.iter().position(|&b| b == 0x00)?;
    let topic = String::from_utf8_lossy(&attachment[0..sep_pos]).to_string();
    let ts_bytes = &attachment[sep_pos + 1..];
    let send_ts = decode_send_ts(ts_bytes)?;
    Some((topic, send_ts))
}

/// Encode attachment for Gateway → Consumer: [8 bytes send_ts_ns BE]
pub fn encode_forward_attachment(send_ts_ns: u64) -> Vec<u8> {
    encode_send_ts(send_ts_ns).to_vec()
}

/// Decode forward attachment: returns send_ts_ns
pub fn decode_forward_attachment(attachment: &[u8]) -> Option<u64> {
    decode_send_ts(attachment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_producer_attachment() {
        let ts = now_ns();
        let attachment = encode_producer_attachment("tenant/dataset-42", ts);
        let (topic, decoded_ts) = decode_producer_attachment(&attachment).unwrap();
        assert_eq!(topic, "tenant/dataset-42");
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_encode_decode_forward_attachment() {
        let ts = now_ns();
        let attachment = encode_forward_attachment(ts);
        let decoded_ts = decode_forward_attachment(&attachment).unwrap();
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_metrics_snapshot_and_reset() {
        let collector = MetricsCollector::new();
        collector.record_message();
        collector.record_message();
        collector.record_latency(1000);
        collector.record_latency(2000);
        collector.record_latency(3000);

        let snapshot = collector.snapshot_and_reset().unwrap();
        assert_eq!(snapshot.msg_count, 2);
        assert_eq!(snapshot.latency_min_ns, 1000);
        assert_eq!(snapshot.latency_max_ns, 3000);

        // After reset, should return None
        assert!(collector.snapshot_and_reset().is_none());
    }

    #[test]
    fn test_percentile() {
        let data = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        assert_eq!(percentile(&data, 50), 50);
        assert_eq!(percentile(&data, 90), 90);
        assert_eq!(percentile(&data, 99), 100);
    }
}
