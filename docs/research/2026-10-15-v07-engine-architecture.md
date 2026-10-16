# v0.7 引擎架构调研：多引擎 vs 单引擎增量演进（①交互式写 ②聚合 ③嵌入式 WAL）

**日期**：2026-10-15
**性质**：架构方向决策输入（brainstorming 阶段产出，尚未定稿 spec）
**前置**：[docs/design/2026-10-14-durability-write-model.md](../design/2026-10-14-durability-write-model.md)（团队已有决策：embedded WAL 触发条件、delta-CSR 路线映射）
**调研方法**：4 路并行（多引擎/HTAP 先例、动态图数据结构、嵌入式 WAL 与聚合实现、产品视角），官方文档与顶会论文为准，事实附来源。

---

## TL;DR

1. **MySQL 式多引擎：不推荐。** 你举的例子恰好是反面教材——MEMORY 引擎表级锁、无持久化、官方自己承认"繁忙服务器上不一定比 InnoDB 快"，内部用途已被 TempTable 替代；MySQL 二十年最大的教训是 InnoDB 一统、其余引擎碎片化。存活的 HTAP 双引擎（TiFlash/Oracle IM/SingleStore）全是多团队大厂产品，同步链路是它们的固有税。
2. **正确路线是"单引擎、两个区域、一条合并路径"**：sealed CSR = 不可变冷区（已有），补一个可变 **delta overlay** + 后台 merge（把 Seal 从阻塞动作变成后台合并）。这正是 Umbra（小页热区/大页冷区）、SingleStore（rowstore/columnstore 单表）、SQL Server（delta store + tuple mover）、Memgraph（delta 链）的共同形态——LSMGraph（SIGMOD）用该架构同时拿到 36x 更新与 24x 分析加速。
3. **图库领域没有任何一家走双引擎**；与 Vectra 最同源的 Kùzu（列存+CSR+CoW MVCC，单引擎）已归档（Apple 收编），嵌入式图库生态出现真空——这是 Vectra 的定位机会，别用架构复杂度把它挥霍掉。
4. **节奏建议**：v0.7 = ②聚合 + ③WAL，v0.8 = ①delta。三个一起吞会同时动"怎么写/怎么存/怎么查"三条路径，1-2 人 review 带宽盖不住；且 ③ 先行能锁定逻辑日志格式，v0.8 的 delta 提交直接记在它上面。

---

## 1. 多引擎问题的正面回答

### 1.1 MySQL 经验教训（证据）

| 维度 | 事实 | 来源 |
|---|---|---|
| 分层 | server 层拥有 parser/optimizer；引擎经 handler API 提供存储/索引/**事务/崩溃恢复**——功能随引擎碎裂 | [MySQL 官方](https://dev.mysql.com/doc/refman/8.0/en/pluggable-storage-overview.html) |
| MEMORY 引擎 | **表级锁**、无事务/MVCC、hash 索引只利等值、定长行、**不持久**（重启丢数据）；官方原话"繁忙服务器上不一定比 InnoDB 快"（buffer pool 本身就是内存引擎） | [8.4 官方文档](https://dev.mysql.com/doc/refman/8.4/en/memory-storage-engine.html) |
| MEMORY 现状 | 未正式废弃，但 8.0 起内部临时表已换 TempTable——作为"用户可选内存引擎"名存实亡 | [Internal Temp Tables](https://dev.mysql.com/doc/refman/8.3/en/internal-temporary-tables.html) |
| 碎片化代价 | 跨引擎 JOIN 不保证原子、参照完整性在引擎边界失效、优化器把引擎当黑盒 | [Alternative Engines](https://dev.mysql.com/doc/refman/8.2/en/storage-engines.html) |
| 收敛结局 | 5.7 起系统表强制 InnoDB；"单一全功能引擎击败碎片矩阵" | — |

### 1.2 HTAP 双引擎的量化代价

HTAP survey（[arXiv 2404.15670](https://arxiv.org/html/2404.15670v1)）核心权衡：**data freshness ↔ performance isolation**。
- 单引擎统一（HyPer 系）：微秒级新鲜度，混合负载吞吐上限损失 ~40%；
- 分离双引擎（TiDB+TiFlash）：隔离好（OLTP 仅降 ~10%），但列存副本靠 Raft learner 异步回放，应用延迟可达 ~1000ms，且数据双份、建议分节点部署。

**双引擎的存活样本全是用团队规模买隔离的**：TiFlash 要 Raft learner + 读时一致性校验 RPC；Oracle IM 是 SGA 内双格式 + 每 IMCU 事务 journal；SingleStore 靠后台 flusher + 常驻同步索引。这些同步链路是它们的核心工程开销，不是实现瑕疵。

### 1.3 单引擎统一路线（推荐依据）

- **Umbra**（[CIDR 2020](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf)）：单引擎，**变长页**——小页放热/可变数据（低延迟交互写），大页放批量/分析数据（高效扫描、可溢写）。"一个引擎、内部按冷热分存储形态 + 自动升降级"，不是两个引擎。
- **图库全员单引擎**：Neo4j（store+page cache+tx log，GDS 的第二格式只是临时投影）、Memgraph（delta 链 MVCC，analytics 模式关 delta 换速度——同一引擎切模式）、Kùzu（列存+CSR+CoW MVCC）、FalkorDB（GraphBLAS 只读）。
- **嵌入式成功先例全是单引擎**：SQLite（存储引擎不可插拔，唯一的可插拔层是 VFS；流产的 SQLite4 草案恰包含可插拔 KV，被放弃）、DuckDB（扩展只覆盖 I/O/文件格式/函数，不覆盖存储引擎）。业界的"多引擎"实践是**组合嵌入式库**（TiKV=Raft+RocksDB、MyRocks、MariaDB+DuckDB 插件），不是库内双引擎。

### 1.4 对"OLTP/OLAP 两个引擎"的澄清

Vectra 的负载不是经典 OLTP/OLAP 对立：图遍历（点查/多跳）与图分析（聚合/全图）**都吃 CSR 的读性能**。真正的分轴是**"交互式小写入" vs "封印批量读"**——SingleStore 单表内 rowstore 热区 + columnstore 冷区的形态证明：这不是两个引擎，是一张表的两个区域 + 一条合并路径。Vectra 的 Seal 模型已经是"冷区 + 合并器"，缺的只是热区。

## 2. 推荐架构：sealed CSR 基线 + delta overlay + 后台 merge

### 2.1 动态图结构的先例排序（风险×收益）

| 排序 | 方案 | 结论 | 证据 |
|---|---|---|---|
| 1（v0.8 采用） | **delta 层 + 阈值触发后台 merge** | 风险最小收益最直接；现有 Seal/CSR/Tarjan **一行不改**地成为基线与 merge 器 | LSMGraph（[arXiv 2411.06392](https://arxiv.org/html/2411.06392v1)）：内存 MemGraph（低度顶点分段数组+高度跳表）+ 多层 CSR + **顶点感知 compaction**——更新 36x 于 LiveGraph、分析 24x、空间 45%；Lucene segment（新写进不可变段、查询 union、后台合并）；**Dgraph（Go 同语言先例）**：mutation 先写 WAL 再应用内存 posting list，64 分片 map 存脏数据 |
| 2 | Seal 输出改 gap-CSR/PCSR | merge 的加速器，读代价仅 ~1.05x | PCSR（[ALENEX 2021](https://itshelenxu.github.io/files/papers/ppcsr-alenex-21.pdf)）、BP-CSR（批更新+静态算法仅 1.05x 慢于 CSR） |
| 3 | 度数感知分层（Terrace 式） | v0.9+ 再考虑 | [Terrace, SIGMOD 2021](https://dl.acm.org/doi/10.1145/3448016.3457313) |
| 4 | 事务型容器（Stinger/Teseo/Sortledton） | **不做**：链表块读性能劣于 CSR（BYO 基准） | [BYO, PVLDB 2024](https://www.vldb.org/pvldb/vol17/p2307-wheatman.pdf) |
| 5 | 增量 SCC | **不做**：Tarjan 全量 O(V+E) 极快且已有，merge 时重算；理论下界 O(m^{3/2}) 挡路 | [Bender et al.](https://arxiv.org/abs/1112.0784) |

### 2.2 落到 Vectra 代码上的形状（草案，待 spec 细化）

```
写入: 单写者 → changeset{upsert/delete} → ③WAL append（fsync 旋钮）
                                    → 应用到 delta（adjacency map + tombstones + pending 索引段）
读:   atomic.Pointer 发布的 view 世代 = sealed CSR（不可变，无锁遍历）
                                 ∪ delta（读小结构；RWMutex 或不可变世代）
合并: delta 达阈值（边数/字节）→ 后台 goroutine 用现有 Seal 管线重建 CSR
                                 → Left-Right 式翻转发布新世代（"新 CSR + 空 delta"）→ 截断 WAL 基线
恢复: Load 快照 → WAL 顺序重放（幂等 upsert/delete）→ CRC 损坏即停（torn tail）
```

- Go 并发无需 C/Rust 的 epoch 回收：GC + `atomic.Pointer` 世代发布即可（MCP 的 view 原子指针已是这个模式的小号版）。
- 属性索引增量照 Lucene：pending 段 + 查询 union + merge 归并。
- 摊销保证：global rebuilding 理论（Overmars 1983）——delta 阈值 B 使 O(V+E) 重建摊销为每写 O((V+E)/B)。

## 3. ②聚合与 ③WAL 的最小设计（v0.7 候选）

### ③ 嵌入式 WAL（逻辑日志）
- **格式**：段文件（预分配，回卷复用带 salt 递增拒陈旧帧）+ 记录帧 `[len(56bit)+padding 位][CRC32C][type][payload]`（etcd wal.go 同构：[wal.go](https://github.com/etcd-io/etcd/blob/main/server/storage/wal/wal.go)）；payload = 现有 raft changeset 的 gob 编码——**与复制流一份数据两用**（团队设计文档 §3.2 已有此草图）。
- **Checkpoint**：WAL 达阈值（对标 SQLite 1000 页 ≈4MB）或 Close() 时，用现有 `SaveFile`（temp+fsync+rename）写新快照 + 原子 manifest（快照名+段号+已含 LSN）→ 回卷 WAL。
- **恢复**：`Load` 快照 → 顺序解码 → 经现有 Upsert/Delete 重放 → 首个 CRC/长度错误停（Redis AOF/etcd torn-tail 同款容忍）。
- **持久性旋钮**：`SyncMode{Always, Interval, Off}`（SQLite synchronous / Redis appendfsync / Badger SyncWrites 三家同构）；Interval 档用组提交（RocksDB naive group commit 原样照搬）。
- 先例背书：**Redis AOF = "逻辑命令日志 + RDB checkpoint" 的完整同构**；LevelDB/RocksDB 的 WAL 载荷本身就是 WriteBatch 逻辑操作。

### ② Cypher 聚合 v1
- **函数集**：`count(*)` / `count(x)` / `count(DISTINCT x)` / `sum` / `avg` / `min` / `max` / `collect`（+DISTINCT）。percentile/stDev 留 v2。
- **语义照抄 Neo4j**：隐式分组（RETURN 非聚合项 = 分组键）；null 规则（count(*) 计行、sum 空集=0、avg/min/max=null、collect 忽略 null）；无分组键零输入仍输出一行。
- **实现落点**：`internal/cypher/project.go` 的 project() 是 materialize→ORDER→LIMIT 三段式，聚合就是在最前面插一个 pipeline breaker：`map[组键]*accumulator` 单遍构建（组键 = 类型标签+规范字节序，复用 cmpVals 全序），每组出一行后走现有 ORDER BY/LIMIT。**LIMIT 不能短路聚合**（DuckDB 语义）。`maxGroups` 护栏防 OOM。
- v1 不加 GROUP BY 子句与 WITH；文档注明未来对齐 Cypher 25 显式 GROUP BY。

## 4. 产品与节奏（PM 视角）

| 短板 | 嵌入式用户 | server 用户 | MCP/agent 用户 |
|---|---|---|---|
| ① 交互写 | **corpit 阻断项**（实测 354ms@100K、5.7s@1M 的 seal 墙） | 高价值非阻断（有界过期读已有） | 无感 |
| ② 聚合 | nice-to-have | nice-to-have | **接近阻断**（agent 高频算子，现在只能数行） |
| ③ WAL | 企业采用的一票否决项（"重启全量重建"过不了耐用性评审）；scv 批导入不需要 | 不需要（raft 日志即 WAL） | 无感 |

**节奏建议（拆分）**：v0.7 = ②+③（一个动查询层、一个动持久层，都不动读路径核心，回归面可二分）；v0.8 = ①（存储核心重构 + 新基准，独占周期）。corpit 在 v0.7 周期用单节点 server + SealInterval 调优缓冲（需与 corpit 确认 seal 频率预期与图规模曲线）。
**发布主题句（v0.7）**："查询会算总账，重启不丢数据——Cypher 聚合 + 嵌入式 WAL 崩溃恢复。"

若坚持三个都进 v0.7：技术上可行（②独立，③①共享 changeset），代价是回归面三线交织、发布风险回到 0.4/0.5 时代的不兼容阴影，且 ① 的基准验收（delta 命中率、merge 抖动、读尾延迟）需要独立周期才能做扎实。

---

## 5. 待用户决策

1. **架构路线**：B（单引擎 + delta overlay）——四路调研一致推荐；A（多引擎）是否有调研未覆盖的理由？
2. **版本切分**：v0.7=②③ + v0.8=①（推荐）vs 三个全进 v0.7？
3. 决策后进入 brainstorming 收尾 → spec（`docs/superpowers/specs/`）→ plan。

## 6. 来源索引（主要）

多引擎/HTAP：[MySQL pluggable overview](https://dev.mysql.com/doc/refman/8.0/en/pluggable-storage-overview.html) · [MEMORY engine](https://dev.mysql.com/doc/refman/8.4/en/memory-storage-engine.html) · [TiFlash overview](https://docs.pingcap.com/tidb/stable/tiflash-overview) · [SingleStore Universal Storage](https://www.singlestore.com/blog/memsql-singlestore-then-there-was-one) · [Oracle IM](https://docs.oracle.com/en/database/oracle/oracle-database/21/inmem/in-memory-column-store-architecture.html) · [HTAP survey](https://arxiv.org/html/2404.15670v1) · [Umbra CIDR20](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf) · [HyPer ICDE11](https://dl.acm.org/doi/10.1109/ICDE.2011.5767867) · [SQLite 架构](https://sqlite.org/zipvfs/doc/trunk/www/howitworks.wiki) · [MariaDB DuckDB engine](https://mariadb.com/docs/server/server-usage/storage-engines/duckdb-storage-engine)

动态图：[LSMGraph](https://arxiv.org/html/2411.06392v1) · [Terrace SIGMOD21](https://dl.acm.org/doi/10.1145/3448016.3457313) · [PCSR ALENEX21](https://itshelenxu.github.io/files/papers/ppcsr-alenex-21.pdf) · [BP-CSR HPEC24](https://brianwheatman.com/papers/batch_pcsr.pdf) · [BYO PVLDB24](https://www.vldb.org/pvldb/vol17/p2307-wheatman.pdf) · [LiveGraph PVLDB13](https://dl.acm.org/doi/10.14778/3384345.3384351) · [GraphOne FAST19](https://www.usenix.org/conference/fast19/presentation/kumar) · [Dgraph mutation 设计](https://docs.dgraph.io/design-concepts/transaction-mutation-concept) · [Lucene NRT](https://www.elastic.co/docs/manage-data/data-store/near-real-time-search) · [HyPer MVCC SIGMOD15](https://dl.acm.org/doi/10.1145/2723372.2749436)

WAL/聚合：[SQLite WAL](https://www.sqlite.org/wal.html) · [RocksDB WAL format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format) · [WAL performance](https://github.com/facebook/rocksdb/wiki/WAL-Performance) · [LMDB](http://www.lmdb.tech/doc/index.html) · [etcd wal.go](https://github.com/etcd-io/etcd/blob/main/server/storage/wal/wal.go) · [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) · [Neo4j aggregating](https://neo4j.com/docs/cypher-manual/current/functions/aggregating/) · [DuckDB hash agg](https://duckdb.org/2022/03/07/aggregate-hashtable.html) · [DuckDB external agg](https://duckdb.org/2024/03/29/external-aggregation.html)
