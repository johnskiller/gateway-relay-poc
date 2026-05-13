use std::env;
use std::sync::Arc;
use std::time::Duration;
use zenoh_gateway_poc::metrics::{MetricsCollector, now_ns, decode_forward_attachment};

/// Performance test Consumer with configurable topic count and E2E latency measurement.
///
/// Usage: consumer-perf <client-id> <topic-count> [options]
///   Options:
///     --prefix <prefix>        Topic prefix (default: perf/topic)
///     --zenoh-endpoint <addr>  Zenoh connect endpoint (default: tcp/localhost:7447)
///
/// The consumer auto-generates topics as `{prefix}-{0..N-1}`, subscribes to each,
/// and measures E2E latency by extracting send_ts from Zenoh attachments.
///
/// It also provides:
/// - Queryable for interest discovery (gateway pulls topic list)
/// - Liveliness token for presence detection (gateway auto-discovers)

fn parse_args() -> (String, usize, String, String) {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: consumer-perf <client-id> <topic-count> [options]");
        eprintln!("Options:");
        eprintln!("  --prefix <prefix>        Topic prefix (default: perf/topic)");
        eprintln!("  --zenoh-endpoint <addr>  Zenoh endpoint (default: tcp/localhost:7447)");
        eprintln!();
        eprintln!("Example: consumer-perf client-01 2000 --prefix perf/topic --zenoh-endpoint tcp/zenoh-downstream:7447");
        std::process::exit(1);
    }

    let client_id = args[1].clone();
    let topic_count: usize = args[2].parse().unwrap_or_else(|_| {
        eprintln!("Error: topic-count must be a positive integer");
        std::process::exit(1);
    });

    let mut prefix = "perf/topic".to_string();
    let mut zenoh_endpoint = "tcp/localhost:7447".to_string();

    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--prefix" if i + 1 < args.len() => {
                prefix = args[i + 1].clone();
                i += 2;
            }
            "--zenoh-endpoint" if i + 1 < args.len() => {
                zenoh_endpoint = args[i + 1].clone();
                i += 2;
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                i += 1;
            }
        }
    }

    (client_id, topic_count, prefix, zenoh_endpoint)
}

#[tokio::main]
async fn main() {
    let (client_id, topic_count, prefix, zenoh_endpoint) = parse_args();

    // Generate topic list: {prefix}-0, {prefix}-1, ..., {prefix}-(N-1)
    let topics: Vec<String> = (0..topic_count)
        .map(|i| format!("{}-{}", prefix, i))
        .collect();

    // Configure zenoh session
    let mut config = zenoh::Config::default();
    if let Err(e) = config.insert_json5("connect", &format!("{{\"endpoints\": [\"{}\"]}}", zenoh_endpoint)) {
        eprintln!("Warning: failed to set zenoh endpoint: {}", e);
    }

    let session = zenoh::open(config).await.unwrap();

    println!("=== Consumer-Perf ===");
    println!("Client ID: {}", client_id);
    println!("Topics: {} (prefix: {})", topic_count, prefix);
    println!("Zenoh endpoint: {}", zenoh_endpoint);
    println!("=====================\n");

    // Metrics collector for E2E latency
    let metrics = Arc::new(MetricsCollector::new());

    // 1. Subscribe to each topic to receive forwarded messages from gateway
    let mut _subscribers = Vec::new();
    for topic in &topics {
        let topic_clone = topic.clone();
        let cid = client_id.clone();
        let m = metrics.clone();
        let sub = session.declare_subscriber(topic.as_str())
            .callback(move |sample| {
                // Extract send_ts from attachment for E2E latency measurement
                if let Some(attr) = sample.attachment() {
                    if let Some(send_ts) = decode_forward_attachment(&attr.to_bytes()) {
                        let e2e_ns = now_ns().saturating_sub(send_ts);
                        m.record_message();
                        m.record_latency(e2e_ns);
                        // Only log every 1000th message to reduce I/O overhead
                        if m.msg_count() % 1000 == 0 {
                            println!("[{}] Received on '{}' (e2e: {}us)", cid, topic_clone, e2e_ns / 1000);
                        }
                        return;
                    }
                }

                // Fallback: no timestamp in attachment
                m.record_message();
            })
            .await.unwrap();
        _subscribers.push(sub);
    }

    // 2. Provide Queryable for interests (The "Pull" part)
    let interest_key = format!("gateway/interest/{}", &client_id);
    let client_id_for_callback = client_id.clone();
    let topics_str = topics.join(",");
    let topics_for_callback = topics_str.clone();
    let _queryable = session.declare_queryable(&interest_key)
        .callback(move |query| {
            let q = query.clone();
            let cid = client_id_for_callback.clone();
            let ts = topics_for_callback.clone();
            tokio::spawn(async move {
                println!("[{}] Replying to interest query with {} topics", cid, ts.split(',').count());
                let _ = q.reply(q.key_expr(), ts).await;
            });
        })
        .await.unwrap();

    // 3. Declare Liveliness Token (The "Discovery" part)
    let liveliness_key = format!("gateway/consumer/{}", &client_id);
    let _token = session.liveliness().declare_token(&liveliness_key).await.unwrap();

    println!("[{}] Online. Interests: {} topics", client_id, topic_count);
    println!("Liveliness: {}, Interest Path: {}", liveliness_key, interest_key);
    println!("Subscribed to {} topics. Measuring E2E latency...\n", topic_count);

    // Periodic metrics reporting
    let metrics_report = metrics.clone();
    let cid = client_id.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Some(snap) = metrics_report.snapshot_and_reset() {
                println!("\n--- Consumer Metrics [{}] ---", cid);
                println!("Messages Received: {}", snap.msg_count);
                println!("{}", snap.format_latency_line("E2E Latency"));
                println!("------------------------\n");
            }
        }
    });

    // Keep running until Ctrl+C
    let (tx, rx) = futures::channel::oneshot::channel();
    let mut tx = Some(tx);
    ctrlc::set_handler(move || {
        if let Some(t) = tx.take() {
            let _ = t.send(());
        }
    }).expect("Failed to set Ctrl-C handler");

    let _ = rx.await;

    println!("\n[{}] Process exiting. Liveliness token will be automatically released.", client_id);
}
