# Vectra 轻量性与广泛采用能力评估

**日期**：2026-10-29
**性质**：本地代码取证 + 外部市场调研
**基准**：main@36e9d11（v0.13.1）。体积/LOC 为本机实测（arm64，darwin）。
**问题**：① 作为图数据库，Vectra 是否已经足够轻量？② 是否具备让开发者广泛使用的能力？
**方法**：本地一路并行盘点（依赖/LOC/二进制/Cypher 覆盖/运维面/bench 证据）+ 外部调研（Kùzu 归档事件、后 Kùzu 嵌入式图库格局、DuckDB 图扩展、图数据库采用门槛）。

---

## TL;DR

1. **轻量性：达标，且在当前市场处于最轻一档。** 核心模块零第三方依赖（无 go.sum）、无 CGO、全部生产代码 22,047 行、`vectra-server` 13.7 MB（`-s -w` 后 9.3 MB）、单文件快照 + WAL sidecar、embedded / server(raft) / MCP 三级形态共用一个引擎。代价同样清晰：in-memory 模型，1M 向量层常驻 6.6 GB、merge 期 2x live set——轻量成立的前提是"图装得进内存"（README 定位 100K–10M 边）。
2. **广泛采用：尚不具备。** 引擎与数据面已经过硬（索引/FTS/向量/WAL/事务/复制/鉴权/MCP 三语言齐全，20+ 份基准报告），但四类缺口卡在"外部开发者第一次试用"之前：Cypher 完全只读（CREATE/MERGE/SET/DELETE 零实现）、没有正式客户端 SDK（TS/Python 只有 MCP 工具包装）、查询面薄（无 shell/UI/`$params`/`IS NULL`/`CREATE INDEX`）、发布工程缺失（无 Dockerfile/CI/examples，bench 文献大半中文，17 天仓库 0 star）。
3. **市场窗口真实存在但有时效。** Kùzu 2025-10-10 被 Apple acqui-hire 后归档，"嵌入式图数据库"出现空位；其最重要真实用例（数据管道临时图、agent 记忆层）与 Vectra 形态高度重合。新一波竞品（LadybugDB 等）已在 6 个月内成形。**纯 Go 生态位目前是空的**（Cayley 2020 起停更、CozoDB 2024-12 起休眠）——Vectra 是当前唯一活跃的纯 Go、零依赖、Cypher+向量+FTS 嵌入式属性图库。
4. **补齐顺序建议**：P0 = Cypher 写子句（v0.14 既定）+ 正式 Python/TS SDK；P1 = 发布工程包（Dockerfile/CI/英文 bench 索引/examples/`go get` 冒烟）+ 最小 CLI REPL；P2 = TLS/backup 端点/唯一约束。差异化主张应是"纯 Go / 零依赖 / agent 原生（MCP）/ CJK FTS"，不必跟进 Kùzu 系的列存分析型性能。

---

## 1. 轻量性评估（结论：达标）

### 1.1 本地实测

| 维度 | 实测值 | 证据 |
|---|---|---|
| 核心模块依赖 | **0 个第三方 require**（无 go.sum） | go.mod（仅 `go 1.27.2`） |
| server 模块依赖 | hashicorp/raft 一族（raft v1.8.0、raft-boltdb/v2、go-metrics） | server/go.mod |
| mcp 模块依赖 | modelcontextprotocol/go-sdk v1.8.0 | mcp/go.mod |
| CGO | 无（全仓无 `import "C"`） | 代理 grep |
| 生产代码量 | **22,047 行**（非测试）：root 5,983 / internal 11,080（cypher 3,592、vector 1,860、wal 1,130、engine 990、delta 825…）/ server 3,997 / mcp 987 | 本机 find+wc |
| 二进制体积 | vectra-server **13.7 MB**（`-s -w` **9.3 MB**）、vectra-mcp 13.5 MB；嵌入式形态 = 0（进程内） | 本机 go build |
| 磁盘布局 | 单文件快照（VCTR v7）+ `<path>.wal/` sidecar + `<base>.vec` 森林缓存；无外部服务 | internal/store/store.go:1-32、internal/wal/wal.go:247 |
| 冷启动（1M 层） | forest-load open 8.73 s、**ready 0 s**（红线 10 min） | docs/benchmarks/2026-10-23-vector-1m.md |
| 形态 | embedded（`vectra.New/Open`）→ vectra-server（raft+REST+auth）→ vectra-mcp（stdio/HTTP，Go/TS/Python）三级渐进，同一引擎 | README、server/cmd、mcp/cmd |

对照组的量级感：Kùzu 是 5,231 commits 的 C++ 列存引擎（归档时）；Neo4j/Memgraph 是 JVM/C++ 服务端；DuckDB 生态走扩展加载。Vectra 的"轻"不是砍功能换来 的——事务（OCC/SI/SER）、复制（raft）、鉴权、三类索引、WAL/checkpoint/异步压实都在。

### 1.2 轻量的边界（诚实标注）

- **内存模型**：图常驻内存，磁盘只做恢复。1M 向量层 graph-alone 6.59–6.61 GB（2.14x 原始字节），merge 期瞬时 2x live set，README:72 建议 ≥16 GB。10M 边定位之外不做承诺。
- 这与 Kùzu（列存、可超内存做分析扫描）是根本分野：Vectra 轻在"没有 buffer manager / 页缓存 / 服务端进程"，代价是不做磁盘换页。对依赖图、agent 记忆、管道内临时图这些"装得进内存"的负载是正确取舍。
- 工具链：go directive 1.27.2（当前代工具链；GOTOOLCHAIN=auto 会自动下载，但把支持面钉在很新的小版本上）。

---

## 2. 外部市场：窗口真实存在，正在被填补

### 2.1 Kùzu 事件与时间线

- **2025-10-10**：kuzudb/kuzu 仓库无预警归档（GitHub 页面现存 "This repository was archived by the owner on Oct 10, 2025. It is now read-only."）。其后欧委会备案证实 Apple acqui-hire 了团队。
- Kùzu 归档时的仓库描述——"Embedded property graph database built for speed. **Vector search and full-text search built in. Implements Cypher**"——与 Vectra v0.13 的功能三件套逐词同构。这不是巧合：嵌入式 + Cypher + 向量 + FTS 已被 Kùzu 教育成此类产品的标准形态。
- Kùzu 被验证过的两大用例：**数据管道临时图**（Bauplan 用 in-memory 模式做 DAG 校验，规划提速 20x）与 **agent/LLM 记忆层**（Cognee 等）——后者正是 Vectra MCP + 向量车道瞄准的场景。
- gdotv 的判断：Kùzu 把"embedded + 列存 + Cypher + 单文件可移植"从 nice-to-have 抬成了行业底线，并把可持续治理/许可姿态变成继任者的必答题。

### 2.2 后 Kùzu 的嵌入式图库格局（2026-05 gdotv 盘点 + 补充）

| 项目 | 与 Kùzu 关系 | 特点 | 状态 |
|---|---|---|---|
| **LadybugDB** | Kùzu 的 C++ 社区 fork，gdotv 背书 | 延续 Cypher 方言/列存/FTS+向量；已加 multi-label、DuckDB/Arrow/Parquet attach；提供 Kùzu 迁移工具 | 活跃（动量最大） |
| Lance Graph | 精神继承（ex-Kùzu 成员在 LanceDB） | Lance 列存上的图负载 | 活跃 |
| Raphtory | 早于 Kùzu，同嵌入式理念 | 时间图一等公民 | 成熟、活跃 |
| TuringDB | 同理念 | 版本化一等公民 | 活跃 |
| FalkorDBLite | 同理念 | 每库多图、Python 优先 | 活跃 |
| RyuGraph / HelixDB / GraphLite | 归档后新入场 | — | 初期 |

另有两大分流：**DuckDB 生态的 DuckPGQ**（CWI，SQL:2023 SQL/PGQ）与 **DuckGQL**（ISO GQL 扩展）——关系阵营在把图查询吸进嵌入式 OLAP；以及 ApolloGraph/ArcadeDB/PuppyGraph 等发布 Kùzu 迁移指南抢用户。

### 2.3 纯 Go 生态位：空缺（Vectra 的差异化所在）

- **Cayley**（Google 出身的 Go 图库）：2020 年起无实质开发，DB-Engines 标 inactive；本质是 RDF + 外置 KV 后端。
- **CozoDB**（Rust，Datalog， relational-graph-vector）：2024-12 起休眠，最新 release 停在 2023-12。
- 即：**"纯 Go、零依赖、属性图 + Cypher 子集 + 向量 + FTS + 嵌入式"这个组合，目前市场没有第二个活跃项目。** 新一波竞品全是 C++/Rust/Python 系。Go 开发者想要"go get 一个包就有的图引擎"，可选集为空——这正是 SQLite/DuckDB 式采用曲线的起点位。

### 2.4 采用门槛的共性结论（外部调研汇总）

1. 技能门槛：Cypher/GQL 对关系背景团队是再培训成本（Quora/SO 讨论的共同主题）。
2. 运维负担：多养一个 DBMS 的持有成本是最大劝退项——嵌入式+单文件模型正对此痛点（SQLite/DuckDB playbook）。
3. 生态成熟度：IDE 工具（gdotv 类）、可视化、驱动质量、社区。Kùzu 靠 C/Python/Node/Java/Rust/Go/**WASM** 绑定起家——绑定覆盖面直接决定试用漏斗。

---

## 3. 广泛采用能力清单

### 3.1 已具备（证据在内）

| 能力 | 状态 | 证据 |
|---|---|---|
| 图引擎 CRUD/遍历 | AddNode/AddEdge/UpsertEdge/DeleteNode(级联)/BFS/Reachable(SCC)/ShortestPath/Cycles | vectra.go、query.go、traverse.go |
| Cypher 读子集 | `[OPTIONAL] MATCH`×N + WITH/DISTINCT/UNWIND/SKIP/LIMIT + 聚合 + CONTAINS bigram 下推 | internal/cypher/ast.go:30-101 |
| 索引 | typed（string/number/bool + 范围）、FTS bigram（CJK 友好）、HNSW 向量（REST CRUD） | IndexProperty/IndexFullText/CreateVectorIndex |
| 持久化 | WAL 三模式、轻量 checkpoint、异步压实（停顿 11.47s→190ms） | internal/wal、vectra.go:2649 |
| 服务端 | raft 复制 + OCC 事务（SI/SER、409 协议）+ read-your-writes await + bearer auth | server/api.go、fsm.go、auth.go |
| MCP | Go（进程内嵌引擎）/ TS / Python 三实现，13 个工具 | mcp/ |
| 可信度材料 | 20+ bench 报告、10 spec/15 plan、双语 README、Keep-a-Changelog、race 全绿、LDBC | docs/ |
| 许可/仓库 | MIT、PUBLIC | LICENSE、gh repo view |

### 3.2 缺口（按对"广泛使用"的杀伤力排序）

1. **Cypher 完全只读**。`internal/cypher` 全目录 grep `CREATE/MERGE/SET/DETACH/REMOVE` 零命中；非 MATCH 开头直接报 "expected MATCH"（parser.go:92）。无 `CREATE INDEX` DDL（索引声明只有 Go API，REST 连属性/FTS 索引都管不了）。后果：非 Go 开发者没有查询级写路径，REST 只有 `/v1/batch` 这种结构化写入——Kùzu/LadybugDB 用户的第一条语句就是 `CREATE`。**v0.14 计划内（gap-fill 顺序⑤），但从采用角度看它是 P0。**
2. **没有正式客户端 SDK**。TS/Python 交付物是 MCP 工具包装，`client.py` 只覆盖 batch/stats/nodes/neighbors/query/path/reachable/cycles/cypher/vector——不含 `/v1/txn`、`/v1/await`、删除、索引声明。无 Node/Java/Rust 绑定、无 WASM、无 PyPI/npm 包。对比 Kùzu 的 7 语言绑定，这是试用漏斗的第一道断层。
3. **查询面薄**。无 CLI REPL、无 Web UI、无 `$params`、无算术/`IS NULL`/`RETURN DISTINCT`/`WITH *`（README:121-127 自列）。外部工具链（gdotv 类 IDE、可视化）无处接入。
4. **发布工程**。无 Dockerfile、无 CI 配置、无 examples/ 目录（只有 godoc Example 函数）、bench 文献大半中文（国际开发者不可读）、多模块 tag 策略未验证（server/mcp 子模块的 `go get` 路径）、仓库 17 天 0 star 0 fork。
5. **运维面**。无 TLS（raft 链路明文，README 已文档化为前提）、无 backup 端点/CLI（"快照即备份"成立，但要在进程内调 SaveFile）、嵌入式单写者纪律（裸并发写 = data race，README:68）与 SQLite"任意并发"心智不同，需要文档化引导到 Batch。

### 3.3 判定

把"广泛使用"拆成漏斗：**能不能跑起来**（✅ 三形态+单二进制）→ **能不能写入**（⚠️ 仅 Go API/REST batch，无 Cypher 写）→ **能不能从常用语言用**（❌ 无 SDK）→ **能不能被工具生态接入**（❌ 无 shell/驱动标准）→ **敢不敢信**（⚠️ 引擎证据充分，但项目年龄/英文材料/CI 缺失）。当前卡在第二、三级。

---

## 4. 结论与建议

1. **问题①：足够轻量了吗——是**，且有数字背书（0 依赖 / 22K 行 / 9.3 MB / 无 CGO / 单文件 + 三形态）。轻量是卖点，不必再瘦。
2. **问题②：能广泛使用了吗——还不能**，但缺口全部是外围工程，不触及引擎内核。路线与 gap-fill 既定顺序一致，只需把两件事的优先级提到"对外发布的门槛"高度：
   - **P0a：Cypher 写子句**（v0.14 既定：CREATE/SET/DELETE/DETACH DELETE → MERGE）+ `CREATE INDEX` DDL；
   - **P0b：Python/TS 正式 REST SDK**（补 txn/await/删除/索引面，发 PyPI/npm）。
3. **P1：发布工程包**——Dockerfile、CI（含 race 套件）、英文 bench 摘要页、examples/、`go get` 与子模块 tag 冒烟、README 增"SQLite 式并发纪律"引导；**P1：`vectra-cli` Cypher REPL**（最小人类入口）。
4. **P2**：TLS / backup 端点 / 唯一约束（gap-fill ⑥）。
5. **窗口判断**：纯 Go 位空缺 + Kùzu 用户迁移潮 + agentic 记忆叙事 = 真实入口，但 LadybugDB 动量在前、DuckDB 系在侧翼。差异化主张应是**纯 Go / 零依赖 / agent 原生（MCP 三语言）/ CJK FTS / in-memory 极低延迟**；不要跟进 Kùzu 系的列存分析型赛道（那是它们的强项，也是它们重的原因）。

---

## 来源

- Kùzu 仓库归档状态：<https://github.com/kuzudb/kuzu>（"archived by the owner on Oct 10, 2025"）
- Kuzu's Legacy and the New Wave of Embedded Graph Databases（gdotv，2026）： <https://gdotv.com/blog/kuzu-legacy-embedded-graph-database-landscape>
- The Verge：Apple 收购 Kùzu：<https://www.theverge.com/tech/877360/apple-quietly-acquired-the-graph-database-company-kuzu-late-last-year>
- LadybugDB：<https://ladybugdb.com>
- DuckPGQ（SQL:2023 in DuckDB）：<https://duckpgq.notion.site/b8ac652667964f958bfada1c3e53f1bb>
- gdotv Weekly Edge：Kùzu forks / DuckDB goes graph：<https://gdotv.com/blog/weekly-edge-kuzu-forks-duckdb-graph-cypher-24-october-2025>
- Cayley 维护状态（DB-Engines 对比）：<https://gdb-engines.com/compare/google-cayley-vs-helixdb>
- 前置本仓调研：docs/research/2026-10-14-graph-db-capability-survey.md（市场动荡与"不押注外部图库"裁定）、docs/research/2026-10-26-gap-fill-feasibility.md（五缺口与立项顺序）
