use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use futures::StreamExt;
use zenoh_gateway_poc::hashing;
use zenoh_gateway_poc::metrics::{encode_producer_attachment, now_ns};

/// Performance test Producer with configurable topic count and send rate.
///
/// Usage: producer-perf <topic-count> <msgs-per-sec> [options]
///   Options:
///     --prefix <prefix>        Topic prefix (default: perf/topic)
///     --payload-size <bytes>   Payload size in bytes (default: 1024 = 1KB)
///     --workers <N>            Concurrent send workers (default: CPU cores)
///     --zenoh-endpoint <addr>  Zenoh connect endpoint (default: tcp/localhost:7447)

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn parse_args() -> (usize, u64, String, usize, usize, String) {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: producer-perf <topic-count> <msgs-per-sec> [options]");
        eprintln!("Options:");
        eprintln!("  --prefix <prefix>        Topic prefix (default: perf/topic)");
        eprintln!("  --payload-size <bytes>   Payload size (default: 1024)");
        eprintln!("  --workers <N>            Concurrent workers (default: CPU cores)");
        eprintln!("  --zenoh-endpoint <addr>  Zenoh endpoint (default: tcp/localhost:7447)");
        eprintln!();
        eprintln!("Example: producer-perf 2000 5000 --prefix perf/topic --payload-size 1024");
        std::process::exit(1);
    }

    let topic_count: usize = args[1].parse().unwrap_or_else(|_| {
        eprintln!("Error: topic-count must be a positive integer");
        std::process::exit(1);
    });
    let msgs_per_sec: u64 = args[2].parse().unwrap_or_else(|_| {
        eprintln!("Error: msgs-per-sec must be a positive integer");
        std::process::exit(1);
    });

    let mut prefix = "perf/topic".to_string();
    let mut payload_size: usize = 1024;
    let mut workers = num_cpus();
    let mut zenoh_endpoint = "tcp/localhost:7447".to_string();

    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--prefix" if i + 1 < args.len() => {
                prefix = args[i + 1].clone();
                i += 2;
            }
            "--payload-size" if i + 1 < args.len() => {
                payload_size = args[i + 1].parse().unwrap_or(1024);
                i += 2;
            }
            "--workers" if i + 1 < args.len() => {
                workers = args[i + 1].parse().unwrap_or(num_cpus());
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

    (topic_count, msgs_per_sec, prefix, payload_size, workers, zenoh_endpoint)
}

#[tokio::main]
async fn main() {
    let (topic_count, msgs_per_sec, prefix, payload_size, workers, zenoh_endpoint) = parse_args();

    // Generate topic list
    let topics: Vec<String> = (0..topic_count)
        .map(|i| format!("{}-{}", prefix, i))
        .collect();

    // Configure zenoh session
    let mut config = zenoh::Config::default();
    if let Err(e) = config.insert_json5("connect", &format!("{{\"endpoints\": [\"{}\"]}}", zenoh_endpoint)) {
        eprintln!("Warning: failed to set zenoh endpoint: {}", e);
    }

    let session = zenoh::open(config).await.unwrap();

    // Print shard distribution
    let mut shard_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for topic in &topics {
        let shard = hashing::get_shard_id(topic);
        *shard_counts.entry(shard).or_insert(0) += 1;
    }

    println!("=== Producer-Perf ===");
    println!("Topics: {} (prefix: {})", topic_count, prefix);
    println!("Target rate: {} msg/s", msgs_per_sec);
    println!("Payload size: {} bytes", payload_size);
    println!("Workers: {}", workers);
    println!("Zenoh endpoint: {}", zenoh_endpoint);
    println!("Topics mapped to {} distinct shards", shard_counts.len());
    println!("=====================\n");

    // Pre-compute shard IDs for each topic
    let shard_ids: Vec<String> = topics.iter().map(|t| hashing::get_shard_id(t)).collect();

    // Pre-allocate payload buffer
    let payload = vec![0u8; payload_size];

    // Message counter for stats
    let msg_counter = Arc::new(AtomicU64::new(0));
    let start_time = std::time::Instant::now();

    // Stats reporting task
    let stats_counter = msg_counter.clone();
    tokio::spawn(async move {
        let mut last_count: u64 = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let current = stats_counter.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed().as_secs_f64();
            let rate = (current - last_count) as f64 / 5.0;
            println!("[Stats] Sent: {} total, {:.0} msg/s, elapsed: {:.1}s",
                     current, rate, elapsed);
            last_count = current;
        }
    });

    // Calculate interval between messages
    let interval_duration = if msgs_per_sec > 0 {
        Duration::from_nanos(1_000_000_000 / msgs_per_sec)
    } else {
        Duration::from_secs(1) // Fallback: 1 msg/s
    };

    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Main send loop with bounded concurrency
    IntervalStream::new(interval)
        .enumerate()
        .for_each_concurrent(workers, |(i, _)| {
            let session = session.clone();
            let topics = topics.clone();
            let shard_ids = shard_ids.clone();
            let payload = payload.clone();
            let msg_counter = msg_counter.clone();

            async move {
                let topic_index = i % topic_count;
                let topic = &topics[topic_index];
                let shard_id = &shard_ids[topic_index];

                let send_ts = now_ns();
                let attachment = encode_producer_attachment(topic, send_ts);

                let result = session.put(&**shard_id, &payload[..])
                    .attachment(&attachment)
                    .await;

                if result.is_err() {
                    eprintln!("Error sending to shard {} (topic: {})", shard_id, topic);
                }

                msg_counter.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await;
}
