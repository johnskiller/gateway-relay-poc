use std::env;
use tokio::task; // 替换为 tokio 的 task 模块
use std::time::Duration;

#[tokio::main] // 替换为 tokio 的 main 宏
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
    tokio::time::sleep(Duration::from_millis(500)).await; // 替换为 tokio 的 sleep

    println!("[{}] 正在公告兴趣: [{}] -> 路径: {}", client_id, topics, announcement_key);
    
    // 发布公告
    session.put(&announcement_key, topics).await.unwrap();

    println!("公告已发布。按 Ctrl+C 退出并将自动执行清理逻辑...");

    // 监听 Ctrl+C 以实现自动清理
    let (tx, rx) = futures::channel::oneshot::channel();
    let mut tx = Some(tx);
    ctrlc::set_handler(move || {
        if let Some(t) = tx.take() {
            let _ = t.send(());
        }
    }).expect("设置 Ctrl-C 处理器失败");

    // 等待信号
    let _ = rx.await;

    println!("\n[{}] 正在执行下线清理: {}", client_id, announcement_key);
    session.delete(&announcement_key).await.unwrap();
    println!("清理完成，进程退出。");
}