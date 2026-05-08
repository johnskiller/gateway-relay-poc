# Zenoh Gateway PoC

[Zenoh](https://zenoh.io/) ベースの分散ゲートウェイ PoC。千万級 Topic の負荷下において、**Source Sharding** + **Rendezvous Hashing** によりステートレス・自己組織型の負荷分散を実現することを検証する。

## コア設計

- **デュアルセッション**：Upstream（Backbone）と Downstream（Local Mesh）のネットワーク分離
- **Liveliness + Pull**：Consumer は Liveliness Token で存在を通知、Gateway が能動的にインタレストリストをプル
- **三層インデックス**：`client_topics` / `topic_subscribers` / `shard_topics` を原子的に維持、O(1) 転送判定
- **Rendezvous Hashing**：無調整の確定的シャード所有権割り当て、ノード変更時の移行を最小化

## クイックスタート

### 前提条件

- Rust 1.85+（edition 2024）
- Zenoh Router（[インストールガイド](https://zenoh.io/docs/getting-started/installation/)）

### ビルド

```bash
cargo build --release
```

### 起動手順

**1. Zenoh Router 起動（Backbone + Local Mesh）**

```bash
# 端末 1：Backbone Router
zenohd

# 端末 2：Local Mesh Router（ネットワーク分離が必要な場合、異なるポートを設定）
zenohd --config router.json5
```

**2. Gateway インスタンス起動**

```bash
# 端末 3：Gateway A
cargo run --bin gateway -- gw-a

# 端末 4：Gateway B
cargo run --bin gateway -- gw-b
```

コンソール出力で両 Gateway が互相に発見し、シャードが均等に分配されていることを確認。

**3. Consumer シミュレータ起動**

```bash
# 端末 5
cargo run --bin consumer-sim -- c1 topics.txt
```

Gateway ログに `Pulled Interest` と `Dynamic Subscribe` が表示されることを確認。

**4. Producer シミュレータ起動**

```bash
# 端末 6
cargo run --bin producer-sim -- topics.txt 500
```

Consumer 端末で転送されたメッセージが受信できることを確認。

### フォールトトレランス検証

- **Gateway 障害**：Gateway A を停止し、Gateway B の `Active Handled Topics` が増加するのを観察（A のシャードを自動引き継ぎ）。
- **Consumer 障害**：Consumer を停止（Ctrl+C）し、Gateway ログの `Cleaning up interests` を観察。

## モジュール構造

```
src/
├── hashing.rs          # SHA256 シャードマッピング + Rendezvous Hashing 所有権判定
├── cluster.rs          # クラスタメンバーシップ + シャード負荷統計
├── interest.rs         # 三層インデックス + サブスクリプションハンドル管理 + Consumer インタレストプル
├── lib.rs              # ライブラリモジュール宣言
├── main.rs             # Gateway メイン：イベント駆動オーケストレーション + 転送コールバック + 統計ループ
├── consumer_sim.rs     # Consumer シミュレータ：サブスクライブ + Queryable + Liveliness
└── producer_sim.rs     # Producer シミュレータ：シャード送信 + Attachment
```

## 主要データフロー

### Consumer インタレスト登録

```
Consumer                    Gateway                         GatewayState
   │                          │                                │
   │── Liveliness Put ───────▶│                                │
   │                          │── mark_pulling() ─────────────▶│
   │                          │── get(interest/c1) ──────────▶│ Consumer
   │◀── Queryable Reply ─────│                                │ (pull)
   │                          │── register_interests() ───────▶│
   │                          │── sync_shard_subscriptions() ─▶│
   │                          │    declare_subscriber(shard)   │
```

### メッセージ転送

```
Producer          Gateway (Upstream)        Gateway (Downstream)      Consumer
   │                    │                          │                     │
   │── put(shard/p0042) │                          │                     │
   │   attachment=topic │                          │                     │
   │▶──────────────────▶│                          │                     │
   │                    │ check topic_subscribers  │                     │
   │                    │── put(topic, payload) ──▶│                     │
   │                    │                          │── subscriber cb ───▶│
```

## 設定

### コマンドライン引数

| コンポーネント | 引数 | 説明 |
|--------------|------|------|
| `gateway` | `--id <node_id>` | Gateway ノードの一意識別子（デフォルト：`gw-1`） |
| `consumer-sim` | `<client-id> <file-path>` | Consumer ID + Topic リストファイルパス |
| `producer-sim` | `<file-path> [interval-ms]` | Topic リストファイルパス + 送信間隔（デフォルト：500ms） |

### Zenoh キースペース

| キー表現 | 用途 |
|---------|------|
| `gateway/cluster/<node_id>` | Gateway ノード Liveliness Token |
| `gateway/consumer/<client_id>` | Consumer Liveliness Token |
| `gateway/interest/<client_id>` | Consumer Queryable（インタレストリストプル） |
| `shard/p0000` ~ `shard/p9999` | シャードデータチャネル |

## ドキュメント

| ドキュメント | 説明 |
|------------|------|
| [poc_ja.md](poc_ja.md) | PoC 設計ドキュメント（日本語） |
| [analyze_ja.md](analyze.md) | コード分析レポート |

## 依存関係

| Crate | バージョン | 用途 |
|-------|----------|------|
| `zenoh` | 1.8.0 | Pub/Sub + Queryable + Liveliness |
| `tokio` | 1 | 非同期ランタイム |
| `sha2` | 0.11.0 | SHA256 ハッシュ（シャードマッピング + Rendezvous Hashing） |
| `futures` | 0.3 | 非同期ユーティリティ（oneshot channel） |
| `ctrlc` | 3.4 | Consumer のグレースフルシャットダウン |

## ライセンス

Private / Internal Use Only
