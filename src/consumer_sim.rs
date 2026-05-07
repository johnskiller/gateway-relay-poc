use std::env;

#[tokio::main]
async fn main() {
    // 解析命令行参数: consumer-sim <client-id> <file-path>
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: consumer-sim <client-id> <file-path>");
        eprintln!("Example: consumer-sim client-01 topics.txt");
        std::process::exit(1);
    }

    let client_id = args[1].clone();
    let file_path = &args[2];

    // 从文件中读取 topic 列表，每行一个
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

    let interest_key = format!("gateway/interest/{}", &client_id);
    let liveliness_key = format!("gateway/consumer/{}", &client_id);

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // 1. Subscribe to each topic to receive forwarded messages from gateway
    //    (The actual message reception part — previously missing)
    let mut _subscribers = Vec::new();
    for topic in &topics {
        let topic_clone = topic.clone();
        let cid = client_id.clone();
        let sub = session.declare_subscriber(topic.as_str())
            .callback(move |sample| {
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
                println!("[{}] Received on '{}': {}", cid, topic_clone, payload);
            })
            .await.unwrap();
        _subscribers.push(sub);
    }

    // 2. Provide Queryable for interests (The "Pull" part)
    let client_id_for_callback = client_id.clone();
    let topics_str = topics.join(",");
    let topics_for_callback = topics_str.clone();
    let _queryable = session.declare_queryable(&interest_key)
        .callback(move |query| {
            let q = query.clone();
            let cid = client_id_for_callback.clone();
            let ts = topics_for_callback.clone();
            tokio::spawn(async move {
                println!("[{}] Replying to interest query for key: {} with topics: {}", cid, q.key_expr(), ts);
                let _ = q.reply(q.key_expr(), ts).await;
            });
        })
        .await.unwrap();

    // 3. Declare Liveliness Token (The "Discovery" part)
    let _token = session.liveliness().declare_token(&liveliness_key).await.unwrap();

    println!("[{}] Online. Interests: [{}]", client_id, topics_str);
    println!("Liveliness: {}, Interest Path: {}", liveliness_key, interest_key);
    println!("Subscribed to {} topics. Press Ctrl+C to trigger cleanup logic...", topics.len());

    // Listen for Ctrl+C to enable automatic cleanup
    let (tx, rx) = futures::channel::oneshot::channel();
    let mut tx = Some(tx);
    ctrlc::set_handler(move || {
        if let Some(t) = tx.take() {
            let _ = t.send(());
        }
    }).expect("Failed to set Ctrl-C handler");

    // Wait for signal — keep subscribers alive until exit
    let _ = rx.await;

    println!("\n[{}] Process exiting. Liveliness token will be automatically released.", client_id);
}