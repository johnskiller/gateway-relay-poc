use std::env;
use async_std::task;
use std::time::Duration;

#[async_std::main]
async fn main() {
    // 解析命令行参数: consumer-sim <client-id> <file-path>
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("用法: consumer-sim <client-id> <file-path>");
        eprintln!("示例: consumer-sim client-01 topics.txt");
        std::process::exit(1);
    }

    let client_id = &args[1];
    let file_path = &args[2];

    // 从文件中读取 topic 列表，每行一个
    let topics = match std::fs::read_to_string(file_path) {
        Ok(content) => content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join(","),
        Err(e) => {
            eprintln!("无法读取文件 {}: {}", file_path, e);
            std::process::exit(1);
        }
    };

    if topics.is_empty() {
        eprintln!("错误: 文件 {} 中没有有效的 topic", file_path);
        std::process::exit(1);
    }

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