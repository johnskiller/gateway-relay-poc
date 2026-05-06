use std::env;
use tokio::task; // Replaced with tokio's task module
use std::time::Duration;

#[tokio::main] // Replaced with tokio's main macro
async fn main() {
    // 解析命令行参数: consumer-sim <client-id> <file-path>
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: consumer-sim <client-id> <file-path>");
        eprintln!("Example: consumer-sim client-01 topics.txt");
        std::process::exit(1);
    }

    let client_id = &args[1];
    let file_path = &args[2];

    // 从文件中读取 topic 列表，每行一个
    // Read topic list from file, one topic per line
    let topics = match std::fs::read_to_string(file_path) {
        Ok(content) => content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(","),
        Err(e) => {
            eprintln!("Failed to read file {}: {}", file_path, e);
            std::process::exit(1);
        }
    };

    if topics.is_empty() {
        eprintln!("Error: No valid topics found in file {}", file_path);
        std::process::exit(1);
    }

    let announcement_key = format!("gateway/announcement/{}", client_id);

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    // Warm-up: wait 500ms to ensure Zenoh network subscriptions have propagated
    tokio::time::sleep(Duration::from_millis(500)).await; // Replaced with tokio's sleep

    println!("[{}] Announcing interests: [{}] -> Path: {}", client_id, topics, announcement_key);
    
    // Publish announcement
    session.put(&announcement_key, topics).await.unwrap();

    println!("Announcement published. Press Ctrl+C to trigger cleanup logic...");

    // Listen for Ctrl+C to enable automatic cleanup
    let (tx, rx) = futures::channel::oneshot::channel();
    let mut tx = Some(tx);
    ctrlc::set_handler(move || {
        if let Some(t) = tx.take() {
            let _ = t.send(());
        }
    }).expect("Failed to set Ctrl-C handler");

    // Wait for signal
    let _ = rx.await;

    println!("\n[{}] Executing offline cleanup: {}", client_id, announcement_key);
    session.delete(&announcement_key).await.unwrap();
    println!("Cleanup complete, process exiting.");
}