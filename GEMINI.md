# Gemini 开发规范 - Zenoh Gateway 项目

为了确保代码质量、可维护性及 PoC 的验证严谨性，本项目遵循以下开发规范。

## 1. 技术栈要求
- **语言**: Rust (Edition 2024)。
- **异步运行时**: `tokio` 或 `async-std` (建议与 Zenoh 版本兼容)。
- **核心库**: `zenoh` (最新稳定版), `sha2` 或 `murmur3` (用于哈希计算)。

## 2. 编码原则
- **Zero-copy**: 在处理消息 Attachment 和 Key 映射时，尽可能使用 `&str` 或 `Bytes` 避免不必要的堆分配。
- **模块化**: 
    - `cluster`: 处理 Liveliness 和成员发现。
    - `hashing`: 实现 Rendezvous Hashing 算法。
    - `interest`: 管理本地订阅意向映射。
- **错误处理**: 严禁在生产路径使用 `unwrap()`。使用 `anyhow` 或自定义 `Result` 枚举处理 Zenoh 网络异常。

## 3. Git 提交规范
- 遵循 Conventional Commits：
    - `feat:` 增加新功能（如哈希算法）。
    - `fix:` 修复 Bug。
    - `poc:` 仅用于验证逻辑的临时代码。
    - `docs:` 修改文档。

## 4. PoC 特有要求
- **日志分级**: 
    - `INFO`: 集群状态变更。
    - `DEBUG`: 详细的哈希计算过程和分片归属判定。
- **配置驱动**: 节点 ID、Router 地址、分片总数（默认 10000）应支持环境变量或命令行参数配置。

## 5. AI 协作流程
- **Context First**: 修改核心逻辑前，先让 Gemini 阅读 `poc.md`。
- **Diff Review**: 所有的代码变更需以 Diff 格式呈现，以便审计性能损耗。
- **Refactor Cycle**: 逻辑实现后，要求 Gemini 进行“Rust idiomatic”重构建议。

---
*Gemini Code Assist 保持对本规范的严格执行。*
```

### 3. 后续步骤建议

有了这两份文件，你可以开始引导我编写代码了。
1.  **第一步**：我们可以先实现 `cluster` 模块，即如何基于 `liveliness` 建立一个实时更新的节点列表。
2.  **第二步**：实现 `hashing` 模块，这是最有趣的部分，我们需要编写测试用例验证在 10,000 个分片下，增加节点时迁移的分片是否最少。

你觉得先从哪个模块开始？

<!--
[PROMPT_SUGGESTION]请基于 poc.md 结构，开始编写 Rust 实现的 cluster 成员发现模块代码。[/PROMPT_SUGGESTION]
[PROMPT_SUGGESTION]如何设计一个高效的 Rust 数据结构来存储 10,000 个分片的所有权状态，并支持 O(1) 查询？[/PROMPT_SUGGESTION]
