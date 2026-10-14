# Design decision: durability & write model（记录 2026-10-14 讨论）

> 状态：accepted（方向性决策；具体实现按触发条件分批）
> 参与上下文：SCV 实测反馈修复（P1–P4）、MCP HTTP 并发修复、write-after-seal 惰性重开、业界调研（RDBMS / 图引擎 / rqlite）

## 1. 背景：数据等级变了

Vectra 最初的持久化赌注是"图数据可从上游再生，磁盘只做快照恢复，不做增量 WAL"（`internal/store` 包注释 + README scope）。这个假设在两类使用方式下不再成立：

- **SCV**：structure.json → loader → Vectra。JSON 只是中间产物，导入后即弃——**图是查找能力的唯一载体**，导入后的图就是事实副本；
- **corpit**：agent 关系**直接写入** Vectra，没有上游——**Vectra 是 system of record**。

写入形态因此分两类，匹配不同的持久化机制：

| 写入形态 | 来源 | 持久化机制 |
|---|---|---|
| 批量导入（可重跑） | SCV | 导入完成做一次快照（现有 seal + SaveFile，store v3） |
| 增量写入（不可再生） | corpit | **提交级持久 → 日志** |

## 2. 为什么"seal 节流"撑不住事实源（定量）

不用日志、靠定期 seal 存快照，持久化频率撞 O(V+E) 墙（`clone_bench_test.go`，M5 Pro 实测）：

| 图规模 | 一次 seal（clone+Seal） | 持久化频率上限 |
|---|---|---|
| 100K/1M | ~354ms / 455MB | ~3/s（CPU 全耗在克隆） |
| 1M/10M | ~5.7s / 4.1GB | <1/6s，且随图变大恶化 |

日志追加是 O(batch)、与图规模无关——这是 RDBMS log-then-checkpoint 的结构性理由，不是实现技巧。

## 3. 决策

1. **现在（零新代码）**：corpit 增量写走**单节点 vectra-server** 的 `/v1/batch` / `/v1/txn`——raft 日志即 WAL（提交即持久、重启重放、VCTX 快照截断）；raft n=1 完整工作，且保留平滑扩到 3 节点的路径。写多优化 = 组提交（`feat/txn-group-commit`，即 PG `commit_delay` / MySQL binlog group commit 的同构物，单节点同样受益）。
2. **触发条件到了再做**：核心引擎内嵌 WAL——仅当出现"必须进程内嵌入、禁止 sidecar"的硬约束。设计草图：复用 server 的 gob Batch 框架（Batch 上提到 core 避免双 changeset 漂移）+ store 的 CRC 纪律 + 快照时旋转 + fsync 策略旋钮（每批/组提交/间隔）。**这要修订 README 的"no incremental WAL"赌注——是有意识的决策修订，不是补丁。**
3. **否决**：seal 间隔持久做事实源（有界丢失窗口 + 频率天花板）；Redis 式 fork-COW（Go runtime 禁止裸 fork 后运行 Go 代码，不可移植）。

## 4. MCP 定位

- 嵌入式（Go MCP 直嵌引擎）：写路径持久性 = 最后一次 seal——保留给"派生数据 + 读多"场景（SCV 快照服务化即此类）；
- 持久写入口：server 的 HTTP API（python/ts MCP 本就是 HTTP 包装；Go MCP 加 HTTP 后端模式列为 follow-up，三语对齐）。

## 5. write-after-seal（已实现，`mcp/server.go`）

seal 发布不可变视图后，首个新写触发 `live = view.Clone()`（惰性重开，携带索引声明）；查询持续服务旧视图直到下次 seal——与 server 模块相同的有界过期读语义。这是低频写下家族 (a)（clone-and-swap，ArcSwap 甜点区："几分钟更新一次的快照"）的正确形态。

## 6. 业界佐证（要点）

- **RDBMS**（PG/InnoDB/Oracle/SQL Server/SQLite）：读写冲突一律 MVCC——行粒度版本链 + undo/版本存储 + 后台摊销回收（VACUUM/purge/checkpoint）；从不整库克隆，从不让读等写。SQL Server 2005 前的纯锁模型被整个行业抛弃。写多场景三板斧：组提交（fsync 摊销）、降低写放大（PG HOT、InnoDB change buffer）、回收后台化。
- **图/嵌入式引擎**（Memgraph/Kùzu/DuckDB/LSM）：成熟后收敛到 delta/overlay 叠不可变基底；Memgraph 早期用整份拷贝后为省内存换 delta，又为批量导入提供反向剥 MVCC 的模式——双向证据。
- **rqlite**：raft 层零克隆，并发读写全交给 SQLite WAL——"嵌入成熟引擎"路线，Vectra 的零依赖不可变快照设计刻意不取此路。
- **RCU/Left-Right/ArcSwap**：Vectra 的"原子指针发布不可变快照"即此族——适用于小体量、读多、低频整体替换。

## 7. 后续映射

| RDBMS 概念 | Vectra 对应物 | 状态 |
|---|---|---|
| WAL + 组提交 | raft 日志 + TxnGroup（feat/txn-group-commit） | Task 0 闸门触发，重诊进行中（spec §4.1） |
| VACUUM/purge/checkpoint | fsm.publishOnce（SealInterval 节流） | 已有 |
| 行粒度版本链 | delta-CSR / overlay | README not-in-scope，触发条件：seal 频率或图规模顶到 O(V+E) 墙 |
| 低频写轻量路径 | MCP 惰性重开 | 已实现 |

## 8. 版本与升级注意

- store v3（索引声明持久化）：旧二进制读不了 v3 文件——与 0.4.0 VCTX 双向不兼容同口径，README 已注明；
- corpit 部署基线：单节点 vectra-server，数据目录即持久层；扩容路径 = 加节点（raft 自动），无需改写路径。
