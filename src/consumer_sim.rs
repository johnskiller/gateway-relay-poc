use std::env;
use async_std::task;
use std::time::Duration;

#[async_std::main]
async fn main() {
    // 解析命令行参数: consumer-sim <client-id> <topic1> <topic2> ...
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("用法: consumer-sim <client-id> <topic1> [topic2] ...");
        eprintln!("示例: consumer-sim client-01 tenant/sensor/01 tenant/sensor/02");
        std::process::exit(1);
    }

    let client_id = &args[1];
    let topics = args[2..].join(",");
    let announcement_key = format!("gateway/announcement/{}", client_id);

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // 预热：等待 500ms 确保 Zenoh 网络中的订阅关系已完成传播
    task::sleep(Duration::from_millis(500)).await;

    println!("[{}] 正在公告兴趣: [{}] -> 路径: {}", client_id, topics, announcement_key);
    
    // 发布公告
    session.put(&announcement_key, topics).await.unwrap();

    println!("公告已发布。按 Ctrl+C 退出（保持 Session 活跃以观察效果）。");
    task::sleep(Duration::from_secs(3600)).await;
}