use std::env;
use std::time::Duration;
use zenoh_gateway_poc::hashing;
use zenoh_gateway_poc::metrics::{encode_producer_attachment, now_ns};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: producer-sim <file-path> [interval-ms]");
        eprintln!("Example: producer-sim topics.txt 500");
        std::process::exit(1);
    }

    let file_path = &args[1];
    let interval_ms: u64 = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    // Read topic list from file, one topic per line
    let topics: Vec<String> = match std::fs::read_to_string(file_path) {
        Ok(content) => content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(|s| s.to_string())
            .collect(),
        Err(e) => {
            eprintln!("Failed to read file {}: {}", file_path, e);
            std::process::exit(1);
        }
    };

    if topics.is_empty() {
        eprintln!("Error: No valid topics found in file {}", file_path);
        std::process::exit(1);
    }

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Print shard distribution for verification
    let mut shard_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for topic in &topics {
        let shard = hashing::get_shard_id(topic);
        *shard_counts.entry(shard).or_insert(0) += 1;
    }
    println!("Producer started. {} topics, interval {}ms", topics.len(), interval_ms);
    println!("Topics mapped to {} distinct shards via SHA256 hashing:", shard_counts.len());
    for (shard, count) in &shard_counts {
        println!("  {} -> {} topics", shard, count);
    }

    loop {
        for topic in &topics {
            let shard_id = hashing::get_shard_id(topic);
            let send_ts = now_ns();
            let payload = format!("data-{}", topic);
            let attachment = encode_producer_attachment(topic, send_ts);

            println!("Sending to {} (Original Key: {})", shard_id, topic);

            session.put(&*shard_id, payload.as_bytes())
                .attachment(&attachment)
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
    }
}
