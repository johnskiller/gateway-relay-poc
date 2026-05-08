# Zenoh Gateway PoC 設計ドキュメント - Rendezvous Hashing 検証

## 1. 背景と目標
本 PoC は、千万級 Topic の負荷下において、**Source Sharding** によりトラフィックを 10,000 シャードに収束させ、**Rendezvous Hashing** を用いて Gateway クラスタでステートレス・自己組織型の負荷分散を実現することを検証する。

## 2. コア機能設計

### 2.1 ノードアイデンティティとクラスタ検出 (Node ID & Liveliness)
- 各インスタンスは起動時にコマンドライン引数 `--id` で一意識別子を指定する。
- Zenoh `Liveliness Token` メカニズムを使用：
    - パス：`gateway/cluster/<node_id>`
    - 動作：起動時に `declare_liveliness`、同時に `gateway/cluster/**` をリッスン。
    - 产出：各ノードがリアルタイムに更新される `BTreeSet<NodeID>` を維持。
- 起動時に `liveliness.get()` を能動的にクエリし、既存ノードを同期。先に起動したメンバーの見落としを防ぐ。

### 2.2 デュアルセッションアーキテクチャ (Dual Session Architecture)
ネットワーク分離（Backbone と Local Mesh の分離）を実現するため、Gateway はデュアルセッション設計を採用：

| セッション | 接続先 | 責務 |
|-----------|--------|------|
| **Upstream** | Backbone Router | Subscriber を宣言してシャードデータを購読、Producer からのメッセージを受信 |
| **Downstream** | Local Mesh Router | Liveliness イベントのリッスン、自身の Liveliness Token 宣言、ローカル Consumer へのメッセージ転送 |

このアーキテクチャにより、Backbone トラフィックがローカルネットワークに漏洩せず、ローカル Consumer の検出トラフィックが Backbone を妨害しないことを保証する。

### 2.3 インタレスト感知メカニズム (Interest Management — Liveliness + Pull)
**Liveliness + Pull** 方式を採用。Zenoh プリミティブのみで実装し、外部ストレージプラグインは不要：

- **Consumer 側の動作**：
    1. 関心のある各 Topic に対して `session.declare_subscriber()` を呼び出し、真のサブスクリプションを登録（Gateway から転送されるメッセージを受信）。
    2. Queryable を宣言：パス `gateway/interest/<client_id>`、返信内容はカンマ区切りの Topic リスト。
    3. Liveliness Token を宣言：パス `gateway/consumer/<client_id>`、自身のオンライン状態を通知。

- **Gateway 側の動作**：
    1. `gateway/consumer/**` の Liveliness イベントをリッスン。
    2. **Consumer オンライン**（`SampleKind::Put`）：
        - 重複排除チェック：`mark_pulling(client_id)` で重複プルを防止。
        - 非同期プル：`pull_consumer_interests()` が `session.get("gateway/interest/<client_id>")` で Consumer の Queryable にクエリし、Topic リストを取得。
        - インタレスト登録：プルした Topic リストを三層インデックスに書き込み。
        - サブスクリプション同期：`sync_shard_subscriptions()` がインデックスの変更に基づいて上流サブスクリプションを調整。
    3. **Consumer オフライン**（`SampleKind::Delete`）：
        - インデックスクリーンアップ：`cleanup_interests()` が三層インデックスから当該 Consumer の全レコードを削除。
        - サブスクリプション同期：`sync_shard_subscriptions()` が不要になった上流サブスクリプションを解放。
    4. 起動時に `liveliness.get("gateway/consumer/**")` を能動的にクエリし、既存 Consumer を同期。

### 2.4 三層インデックス構造 (Three-Layer Interest Index)
`GatewayState` は三層インデックスを維持し、各操作の時間計算量を最適化：

| 層 | データ構造 | 用途 | 操作計算量 |
|----|-----------|------|-----------|
| Layer 1 | `client_topics: HashMap<ClientID, BTreeSet<Topic>>` | Consumer オフライン時の O(M) クリーンアップ（M = 当該 Consumer の Topic 数） | O(M) 削除 |
| Layer 2 | `topic_subscribers: HashMap<Topic, BTreeSet<ClientID>>` | 転送時の O(1) ローカルサブスクライバ有無判定 | O(1) 照会 |
| Layer 3 | `shard_topics: HashMap<ShardID, BTreeSet<Topic>>` | 動的サブスクリプション時の O(1) シャードインタレスト判定 | O(1) 照会 |

さらに2つの補助フィールド：
- `pulling_consumers: HashSet<ClientID>` — 重複排除ガード。同一 Consumer の重複プルを防止。
- `active_subscribers: HashMap<ShardID, Subscriber>` — アクティブな上流サブスクリプションハンドル。keys が現在サブスクライブ済みシャード集合。

三層インデックスは `register_interests()` と `cleanup_interests()` で原子的に維持され、一貫性を保証する。

### 2.5 シャードマッピングと所有権裁定 (Sharding & Rendezvous Hashing)
- **Topic からシャードへのマッピング**：
    - `ShardMapper(topic) → SHA256(topic) % 10000` → `shard/p0000` ~ `shard/p9999`（4桁ゼロ埋め）を生成。
- **所有権アルゴリズム (Rendezvous Hashing)**：
    - 各アクティブシャード $S$ について、全生存ノード $N_i$ のスコアを計算：$Score = SHA256(N_i + "|" + S)$。
    - 最高 $Score$ を持つノードが当該シャードの **Owner** と判定される。
    - Gateway は `is_owner(shard_id)` を満たすシャードのみ上流サブスクリプションをアクティブ化。

### 2.6 動的サブスクリプション管理 (Dynamic Shard Subscription)
`sync_shard_subscriptions()` はコアオーケストレーション関数であり、三層インデックスの「論理的意図」と上流 Zenoh の「物理的サブスクリプション」の整合性を維持する：

1. **Diff 計算**：`compute_subscription_diff()` が `desired_shards`（ローカルインタレストあり + 自ノード Owner）と `current_shards`（`active_subscribers` の keys）を比較し、`to_subscribe` / `to_unsubscribe` リストを出力。
2. **サブスクリプション解除**：`take_subscribers_for_undeclare()` が `active_subscribers` からハンドルを取り出し、ロック外で `sub.undeclare().await` を実行。
3. **新規サブスクリプション**：各新シャードに対して `upstream.declare_subscriber(shard)` でコールバックを登録し、成功後 `insert_subscriber()` でハンドルを格納。

### 2.7 メッセージ転送 (Message Forwarding)
完全なデータプレーン転送チェーンを実装済み：

- **Producer → Gateway**：Producer はシャードキー（例：`shard/p0042`）にメッセージを送信。元の Topic は Zenoh Attachment で携帯。
- **Gateway 転送ロジック**（`declare_subscriber` のコールバック内）：
    1. `sample.attachment()` から元の Topic Key（`okey`）を抽出。
    2. `topic_subscribers.contains_key(&okey)` でローカルサブスクライバの有無を判定。
    3. ある場合、`downstream.put(okey, payload)` でローカル Mesh に転送。
- **Gateway → Consumer**：Consumer は起動時に `declare_subscriber(okey)` で元の Topic をサブスクライブ済み。メッセージを直接受信。

### 2.8 シャード負荷統計 (Shard Distribution Stats)
- **監視次元**：クラスタサイズ、ノードリスト、自ノード担当シャード総数、アクティブインタレスト数、アクティブ Topic 詳細。
- **実装方式**：5秒タイマーで現在の負荷スナップショットをコンソールに出力。
- **発展方向**：将来的に HTTP サービスまたは Zenoh Queryable で JSON 形式のステータスデータを提供し、可視化 Dashboard に接続。

## 3. PoC 検証フロー
1. **Router 起動**：Backbone センターとして機能。
2. **Gateway A & B 起動**：
    - `Liveliness` によるメンバ更新を観察。
    - A と B がそれぞれ計算した「初期担当シャードセット」を記録。
    - **コンソール出力の観察**：シャードが2ノード間で均等に分配されているか確認。
3. **Consumer サブスクリプションのシミュレーション**：`cargo run --bin consumer-sim -- c1 topics.txt`
    - Gateway ログの `Pulled Interest` 出力を観察。
    - `Dynamic Subscribe` 出力を観察し、シャードサブスクリプションが確立されたことを確認。
4. **Producer 送信のシミュレーション**：`cargo run --bin producer-sim -- topics.txt 500`
    - Consumer 側で転送されたメッセージを受信できたか確認。
    - Attachment 内の元の Topic が正しくマッチしているか検証。
5. **動的リバランスとフォールトトレランス検証**：
    - A を停止し、B の `Active Handled Topics` が増加するのを観察。
    - Consumer を停止し、Gateway ログの `Cleaning up interests` を観察。
6. **精密マッチング検証**：
    - シャード `shard/p0001` に複数の元 Topic のデータが混在する状況をシミュレートし、Gateway が Consumer が宣言した Topic のみ転送することを検証。

## 4. 主要評価指標 (KPI)
- **収束率**：46M Topic が 10k シャード空間に正確に収束できるか検証。
- **分配均等度**：クラスタノード間のシャード分配の標準偏差。
- **リバランス速度**：メンバ変更からハッシュ再計算完了までのレイテンシ。
- **転送精度**：ローカルにサブスクライバがいる Topic のみ転送し、冗長トラフィックなし。

## 5. モジュール構造

```
src/
├── hashing.rs          # 純粋計算：SHA256 シャーディング + Rendezvous Hashing
├── cluster.rs          # クラスタメンバーシップ + シャード所有権
├── interest.rs         # 三層インデックス + サブスクリプションハンドル + プルロジック
├── lib.rs              # モジュール宣言
├── main.rs             # イベント駆動オーケストレーション + 転送コールバック + 統計ループ
├── consumer_sim.rs     # Consumer シミュレータ
└── producer_sim.rs     # Producer シミュレータ
```

## 6. 対象外スコープ
- ストレージプラグイン (Storage Plugin) の統合（現在は Liveliness + Queryable Pull で代替）。
- 複雑な ACL セキュリティ認証。
- メッセージの永続化と再送。
- 多段カスケード（Gateway ネスト）。
