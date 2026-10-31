# Vectra

[English](README.md) | **简体中文**

[![CI](https://github.com/ProjAnvil/Vectra/actions/workflows/ci.yml/badge.svg)](https://github.com/ProjAnvil/Vectra/actions/workflows/ci.yml)

面向**企业依赖图谱**（服务调用图、数据血缘、权限继承，10 万～1000 万边）的 Go 嵌入式内存图引擎。在这个量级，大图数据库背负的机制（buffer manager、磁盘页缓存、WAL 事务）全是纯成本。Vectra 押注相反的方向：图永远装得进内存，读免锁，整张图就是一份原子文件。

v0.2 起，同一引擎可升级为**多副本基础服务**：独立的 `server` 模块用 hashicorp/raft（leader 选举、写复制）加 HTTP API 包住引擎——rqlite 模式的图版。并且原生说 **MCP**（Go / Python / TypeScript 三语实现），AI agent 可以直接查询你的依赖图谱。

## 五个赌注

| 赌注 | 收益 |
|---|---|
| 全内存 + 无指针 CSR（`[]uint32` 平坦数组） | GC 成本与边数无关；并发读免锁——任何世代，无论是否 Seal |
| 层同步并行 BFS（morsel 驱动 + 原子 Or 位图去重） | 1000 万边上比朴素 map 邻接快 3.3 倍 |
| 写入侧 SCC 压缩（Tarjan）——查询面对 DAG | 可达性每位图一比特；`Cycles()` 免费诊断循环依赖 |
| 单文件快照 + CRC32（v3 格式） | 原子发布/备份；服务模式蓝绿热替换；属性索引声明随快照往返 |
| 不可变微层世代（v0.7） | 持续写 + 零锁读；每笔已应用写入立即可见 |

## 快速上手（嵌入式）

```go
import "github.com/projanvil/vectra"

g := vectra.New()
g.AddNode("gateway", "Service", map[string]any{"env": "prod"})
g.AddEdge("gateway", "auth", "CALLS", nil)
g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2}) // 同键 (src,dst,label) 替换属性
g.DeleteNode("ledger")                                               // 删点并级联删边

// 批量导入：用 g.Batch(func(g *vectra.Graph) { ... }) —— 每批一个微层；裸逐 op 写也会被
// 吸收进一个打开的窗口（在下次查询、边界或容量上限时物化），批量导入不再强制 Batch——
// 但 WAL 下 Batch 仍是最快形态（每边界一组帧，而非每写一帧）

// 链式遍历（Seal 与否都能查——写入立即可见）
res, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()

// 可达性（SCC 压缩快路径）
reach, _ := g.Reachable("gateway")

// 双向 BFS 最短路径
path, found, _ := g.ShortestPath("gateway", "kafka")

// 循环依赖发现（节点 + 环内边，带 label/props）
for _, c := range g.Cycles() { ... }

// Cypher 写子句（v0.14）：CREATE/MERGE/SET/DELETE + 索引 DDL
cw, _ := g.Cypher(`CREATE (p:Service {key:'payments'})-[:CALLS {w:2}]->(o:Service {key:'orders'})`)
_ = cw.Summary // {CreatedNodes: 2, CreatedEdges: 1, ...}——写语句带计数器

// 原子单文件快照
g.SaveFile("deps.vg")
```

快照说明：v3 格式会持久化 `IndexProperty` 声明，`vectra.Load` 恢复图的同时恢复查询索引（重载后类型化查找立即可用；v2/v1 旧文件仍可读但不含索引声明）。属性索引是**类型化的**——每个 key 三个有序桶（string / number / bool）：`LookupByProperty`（字符串等值）、`LookupByNumber` / `LookupByNumberRange(min, max *float64)`（数值等值 / 范围）、`LookupByBool`。数值在索引里统一归一化为 `float64`：Go API 写入的 `int` 与 JSON 快照读回的同值落进同一桶——"整数读回变 float64" 的老坑在索引查找上不复存在（`int64` 超过 2^53 有精度损失，JSON 数值模型的固有限制）。

## 持续写模型：微层世代（v0.7）

v0.7 溶解了密封快照门。每次写入（或 `Batch`）把变更物化为一个不可变**微层（micro-layer）**，经单个原子指针发布新世代 `{base, layers}`——读者永不加锁、永不阻塞，无论写者在做什么。后台合并器把 `base ∪ layers` 折叠成新的不可变 base（累计层边数达到 max(6.4万, 基座边数) 触发后台合并——折叠间隔与基座同量级，6.4万为小图下限；2048 层帽作为兜底触发 write stall，写者停顿等折叠落地），`Seal()`/`Compact()` 则显式执行该折叠，挂 WAL 时再追加 checkpoint。Seal 不再冻结任何东西：Seal 之后的写只是开启新世代；`Compact` 与 `Seal` 语义等价（spec §3.1 的"深度压缩重排"已被 merge 本身吸收）。二者都是写路径方法。

**可见性分族：**

| 查询族 | 可见性 |
|---|---|
| 点查族（`NodeByKey` / `NodeByID` / `EdgeByID` / 计数 / `EdgesOf` / `LookupBy*`） | 立即——三源联读（base ∪ layers，墓碑遮蔽） |
| 遍历族（`BFS` / `ShortestPath` / `Cypher` / `Query` / `NeighborsOf`） | 立即——base 与微层记录合并行流 |
| `Reachable` / SCC 相关统计 | 计数立即；超节点（SCC 压缩）加速只在合并后的 base 上生效，微层期间的查询退回点级 BFS |
| `Cycles` | 先折叠未合并层——强一致答案，唯一走写路径的查询 |

**ID 口径：**节点/边 ID 在世代内稳定（删除消耗号段）；merge 时稠密重编号，因此跨 `Seal`/`Compact`/后台折叠 ID 可能变化——与 v0.6 的跨 merge 契约一致，未变。

**写者纪律：**零锁引擎假设单写者 goroutine。直接模式（自己调 `AddNode`/`Batch`/...）下就是你任意一个单一 goroutine——并发裸写是数据竞争且会交错 WAL 帧。未分组的逐 op 写累积进**吸收窗口**（一次 flush 物化一个微层——下次查询、边界或容量上限触发——而非每写一层），批量导入不再强制 `Batch`；WAL 下 `Batch`（或包装层 changeset）仍是最快形态：每边界一组帧而非每写一帧。结构化形态是单写者包装层（`internal/writer.Attach` → `Handle.Submit`）：有界队列、Badger 式组提交（每抽干批一组帧一次 fsync）、带层顶停顿的后台合并、以及 WAL 自上次 checkpoint ≥64 MiB 时的容量触发 checkpoint。附加期间，非 release 构建会以 `ErrNotWriter` 拒绝裸写与非写者 goroutine 的 `Seal`/`Compact`/`Cycles`；release 构建编译掉该检查，依赖本契约。

**写前日志（可选）：**`Open(path, Options{WALMode, WALDir})` 开启持久会话——整帧 JSON changeset，`ModeAlways` 下每个 `Batch` 边界一次 fsync（`ModeInterval` 只在显式边界 fsync；零值 `ModeOff` 恰为 v0.6 快照行为）。checkpoint——`Seal`/`Compact`/`Close` 时或包装层的 ≥64 MiB 容量触发——写水位快照 `<db>-<lsn>.db`、原子翻 manifest、重置日志；恢复 = 载入水位快照 + 重放其上的帧（撕裂尾在首个坏帧处停止；checkpoint 失败保留 WAL 供下次尝试）。磁盘上这是快照文件旁的 `wal` 旁路目录：旧版本二进制读新目录会无害忽略；v0.7 面对没有 wal 目录的旧目录则直接加载快照——v3 快照格式本身不变。

**内存：**merge 在旧世代成为垃圾前先分配新 base，1M/10M 档的峰值 RSS 接近 4.1 GB 活集的 2 倍（瞬时 9–11 GB；spec §9 R11）。merge 密集的 1M 档负载请预算 ≥16 GB 内存，或启用包装层的 `GCTune`（merge 前后把 GOGC 降到 50）。

**⚠️ 升级到 0.5.0。** 存储快照改以 v3 格式写入。不兼容是单向的：v1/v2 旧文件仍可读，但 0.5 之前的二进制读不了 v3 文件——跨版本交换 `.vg` 快照的嵌入式与 MCP 用户需要两端都在 0.5+。raft 集群新旧混布策略不变（本就不支持）。

## 并发纪律（SQLite 式）

嵌入式遵循 SQLite 规则：**数据目录由一个进程独占。**进程内所有 goroutine 共享同一个 `*Vectra` 句柄——读者免锁（只与原子世代指针竞争，即[上文的微层模型](#持续写模型微层世代v07)），写者单写。不存在跨进程锁：两个进程打开同一数据目录会互相破坏对方的 WAL 帧与快照，正如两个 `sqlite3` 写者写同一个文件。多进程或远程访问正是服务模式的用途——每个数据目录一个属主进程，前面挡上 raft 复制与 HTTP API：

| 访问形态 | 用法 |
|---|---|
| 单进程多 goroutine | 共享同一个 `*Vectra`（或其 `internal/writer` 句柄）——任意多并发读者 + 一个写者 |
| 多进程 / 远程客户端 / 需要认证 | 数据目录交给 `vectra-server`；其余一切走 `/v1/*` |

## 向量检索（v0.9, Phase 1）

对节点属性声明向量索引，即可做近似 top-k 查询（嵌入式 HNSW，纯 Go 标准库、零新增依赖——完整设计见
[docs/superpowers/specs/2026-10-19-vector-search-design.md](docs/superpowers/specs/2026-10-19-vector-search-design.md)）：

```go
g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "embedding", Dim: 768, Metric: "cosine"})
g.AddNode("thought-1", "Memory", map[string]any{"embedding": someVec}) // []float32 属性
hits, _ := g.QueryVector("mem", queryVec, vectra.QueryVectorOptions{K: 10, Label: "Memory"})
```

已提交的向量立即可查（overlay 的精确扫描兜底）；后台打包在不阻塞写的前提下构建段森林（写者摄入从不等待建图）。结果为近似值，并列时不保证顺序。声明经 WAL 与 store v4 持久化；恢复自底向上重建，overlay 作为精确回退。MCP 服务器以 `vector_search` 工具暴露（query_vector 或 similar_to，k ≤ 1000）。门 2b 数据：
[docs/benchmarks/2026-10-20-vector-phase1-gate2b.md](docs/benchmarks/2026-10-20-vector-phase1-gate2b.md)。

## 全文属性检索（v0.12）

类型化索引回答等值与范围；全文声明回答字符串属性上的**子串**查找——中英混排同样适用：

```go
g.IndexFullText("desc")                            // 声明（挂起至下次 Seal/merge）
hits, ok := g.LookupByContains("desc", "支付网关")  // 大小写不敏感子串，按 NodeID 升序
```

语义：`LookupByContains` **大小写不敏感**（值与检索词都折叠为小写），按相邻**二元组（bigram）**工作——Seal 时把声明 key 的各值切分为相邻两 rune 的 gram（"图数据库" → 图数 数据 据库；m 个 rune 的值去重后产出 m-1 个 gram），查询时交集检索词的 gram 倒排表，再对每个候选做真实的 `Contains` 复核——bigram 索引只是预过滤，gram 碰撞不会变成假命中。声明契约与 `IndexProperty` 一致：挂起至下次 merge（此前挂起 key 走全扫回答）、幂等、记 WAL、随 store v7 快照持久化；同一 key 可以**同时**被类型化索引与全文索引。未声明的 key 与空检索词返回 `(nil, false)`；短于两个 rune 的检索词走已文档化的全扫路径。交叉引用：Cypher 的 `CONTAINS` 语义保持大小写敏感不变，但在已声明全文索引的 key 上，其锚点扫描现在会借用同一批 bigram 桶做预过滤（大小写不敏感的超集收窄候选；最终仍由逐行的 `CONTAINS` 复核裁决）——声明过的 key 让 `WHERE n.k CONTAINS '...'` 不再全表扫描。

**成本（按需开启）。** 只有声明的 key 才建桶，且每次 Seal/merge/clone 都重建。预算口径 ~24-30 B 每 (gram, id)：100 万节点 × 100 字符值 ≈ 3-5 GB。发布门（`BenchmarkSealFTS100K`：10 万点 / 100 万边、单 key、~32 rune 中英混排值）实测带 FTS 的 clone+seal 周期为无 FTS 线的 1.69 倍（+162 MB/op，0.34 s/op）。MCP 侧暴露为 `index_fulltext` / `lookup_contains`（Go 服务端）。

## Cypher 子集查询

Seal 后的图可回答 openCypher/GQL 风格的模式查询——DSL 直接编译到现有 CSR 遍历原语上（`bfs` / `neighbors` / `lookup_property` 就是它的物理算子）：

```go
res, _ := g.Cypher(`MATCH (b {type:'controller'})-[:CALLS*1..2]->(s)
                    WHERE s.env = 'prod' AND s.w >= 3
                    RETURN b.key, s ORDER BY s.w DESC LIMIT 10`)
// res.Columns = ["b.key", "s"]; res.Rows = [{"b.key": "gw", "s": {key,label,props,...}}, ...]
```

语法（v0.13 子句管线）：子句按任意顺序串联——`[OPTIONAL] MATCH pattern[, pattern...] [WHERE expr]`（可重复）、`WITH [DISTINCT] items [ORDER BY item [ASC|DESC]...] [SKIP n] [LIMIT n] [WHERE expr]`、`UNWIND list AS alias`——以 `RETURN *|items [ORDER BY item [ASC|DESC]...] [SKIP n] [LIMIT n]` 收尾；节点 `(var:Label {prop:literal})`，边 `-[var:REL|REL*n..m]->` / `<-[...]-` / `--`（无方向）；WHERE 支持 `= <> < <= > >=`、`STARTS/ENDS WITH`、`CONTAINS`、`IN [...]`、`AND/OR/NOT`、伪属性 `var.key / var.label / var.id`，以及定长单跳上的边属性谓词（`WHERE e.weight > 3`——并行边各绑定一行）。

写子句与 DDL（v0.14）：`CREATE pattern[, pattern...]`、`SET var.prop = expr, ...`（`null` 右值删除该属性）、`[DETACH] DELETE var[, var...]`、`MERGE pattern [ON CREATE SET items] [ON MATCH SET items]`，以及独立成句的 `CREATE [FULLTEXT] INDEX ON (n.prop)`；v0.16 追加独立成句的唯一约束 DDL（见下文）。写模式里的属性值是**表达式**（`{key: k, weight: 2}`——批量导入形态），不同于读模式的字面量：

```go
g.Cypher(`CREATE (s:Service {key:'payments', env:'prod'}) RETURN s`) // 带键创建（RETURN 可省）
g.Cypher(`UNWIND ['a','b','c'] AS k CREATE (n:Service {key:k})`)    // 批量：每行展开各建一个
g.Cypher(`MATCH (s {key:'payments'}) SET s.env = 'staging', s.tmp = null`)
g.Cypher(`MATCH (s {key:'payments'}) DETACH DELETE s`)              // 节点 + 关联边
g.Cypher(`MERGE (s:Service {key:'payments'}) ON CREATE SET s.fresh = true ON MATCH SET s.seen = true`)
g.Cypher(`CREATE INDEX ON (n.env)`)                                 // 类型化
g.Cypher(`CREATE FULLTEXT INDEX ON (n.desc)`)                       // 全文
g.Cypher(`CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`) // 唯一约束（v0.16，独立成句）
g.Cypher(`DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`)   // ……及其删除形式
```

语句形态：可选的读前缀（`MATCH` / `WITH` / `UNWIND`）逐行喂给写子句（从左到右），可选的尾部 `RETURN` 投影写后状态（被删绑定渲染 null）。本版本不允许写子句之后再出现读子句。写语句的结果带 `summary`——`createdNodes / createdEdges / deletedNodes / deletedEdges / propertiesSet / indexesCreated / constraintsCreated`。

语义（刻意设计并已文档化）：变长边采用**节点对语义**——目标的最短距离落在跳数窗口内即匹配（`[:CALLS*1..2]` = 按最短路径 1..2 跳；环不会制造更长的匹配），中间节点不施加目标模式过滤，变长段不绑定边变量。行按具名绑定 + 终点去重；WHERE 采用 null 传播逻辑（属性缺失永不满足，`NOT` 救不回来）。锚定在模式允许时走类型化属性索引（`{type:'controller'}`），否则全扫。中间行数上限 1,000,000（超限报错并提示收窄模式或加 `LIMIT`）。

子句管线与作用域：子句逐级变换同一个行集。重复 `MATCH`（v0.13 新增）对不相交变量做笛卡尔积、对共享变量做连接；阶段是严格的——输入为零行的阶段输出零行。`OPTIONAL MATCH` 在整个子句（含逗号模式与其 WHERE）无匹配时保留输入行，该子句的模式变量读作 null。`WITH` 开启新作用域：只有被投影的名字存活——裸变量按原名或别名透传其实体绑定，`var.prop` 与聚合项必须 `AS` 且产出标量——尾部可选的 `WHERE` 在投影之后过滤。`UNWIND` 保留当前作用域，并把别名声明为逐元素绑定的标量。

差异与限制（刻意设计）：`UNWIND` 扇出与所有中间阶段同受 1,000,000 行预算约束；`OPTIONAL MATCH` 不跨基行去重（重复输入保持重复）；关键字可作别名（`WITH a AS match`）；首次声明于 `OPTIONAL MATCH` 的模式变量是可空变量——在后续非 OPTIONAL 的 `MATCH` 中用作模式变量会被校验期报错，并给出两种改法（把模式折进该 `OPTIONAL MATCH`，或把后续 `MATCH` 标记为 OPTIONAL；语法中没有 `IS [NOT] NULL`）；`RETURN *` 仍仅限退化单 `MATCH` 查询；`WITH *`、`range()`、`RETURN DISTINCT` 与通用表达式项暂缓（聚合 `count/sum/avg/min/max/collect`——含 count/collect 内的 `DISTINCT`——在 RETURN 与 WITH 项中可用）。

写路径身份与差异（v0.14，刻意设计）：写模式 props 里的 `key` 是节点的逻辑键——作为身份提取，绝不存成属性；无键 CREATE 取确定性的引擎生成键 `_c<id>`（`_c` 前缀保留），对已存在键的 CREATE 是 `AddNode` 式 upsert（label 与 props 整体替换）。`MERGE` 仅按键匹配节点——模式里的其余 props 只在建时生效，命中绝不回写（`ON MATCH SET` 才是更新路径）——边按 `(src, dst, label)` 三元组匹配（已存在的边绑定整个模式并抑制端点创建）。对边变量做 `SET`/`DELETE` 寻址的是整个三元组：一次 SET 写入每条并行边，一次 DELETE 全部删除（两条并行的三元组上一次 `DELETE r` 计 `deletedEdges: 2`）；`DETACH DELETE` 删节点时，级联杀掉的关联边同样计入 `deletedEdges`；`DETACH DELETE a, r` 中若 a 的级联已杀掉 r，r 是静默 no-op。重绑定解析存活并行中 ID 最小的一条，因此多边图上 RETURN 的边标量可能渲染出另一条并行边未被触碰的属性。错误采用 first-error-stops、已应用写入保留（与 `Batch` 同契约——不回滚）；对仍有边的节点做普通 `DELETE` 在任何写入落地前即失败。`CREATE INDEX ON (n.prop)` 声明的是按属性键全局（GLOBAL）的索引——带 label 的写法被拒绝——声明挂起至下次 Seal/merge（幂等；每个成功应用的语句计一次 `indexesCreated`）。写语句运行在单写者路径上：嵌入式即 `AddNode`/`UpsertEdge` 的调用约定，服务模式即每语句一次 raft `Apply`。

**唯一约束（v0.16）。** `CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE` 在一个 `(label, prop)` 对上声明属性值唯一约束；`DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE` 将其删除。两者都是独立成句的语句（不得伴随其他子句），只有 REQUIRE 形式——没有命名形式：`(label, prop)` 对即约束的身份。Go API 一一对应：`g.DeclareUnique("Person", "email")`、`g.DropUnique("Person", "email")`、`g.UniqueConstraints()`（排序副本）。声明先校验存量数据（Neo4j 社区版规则）：若该 `(label, prop)` 对已存在重复的字符串值，声明即告失败——错误匹配 `ErrUniqueViolation` 并列出至多十个重复的 值/键 对（有序——每个 raft 副本完全一致），约束**不会**注册。重复声明报 `ErrConstraintExists`；删除未声明的约束报 `ErrConstraintNotFound`；两种声明在 `g.Batch` 内部都会被拒绝（请在窗口外声明——与 `CreateVectorIndex` 同先例，因为校验扫描需要完整存活视图）。此后执法挂在仅有的两个节点变更根入口（`AddNode`/`DeleteNode`——其余表面都汇入它们）：会让同 label 的两个存活节点在该 prop 上持有同一字符串值的写入，在写方法入口即被拒绝，返回 `*UniqueViolationError`（可经 `errors.Is` 匹配 `ErrUniqueViolation`；报出 label、prop、值与两个键），且该操作零副作用；删除节点立即释放其占有的值（同批次内即可复用），覆盖写先释放旧值再注册新值。只有**字符串**属性值参与约束——非字符串值不可见（既不冲突也不报错；这是与 Neo4j 的刻意差异，后者对错误类型直接报错）。声明扫描为每约束一次 O(存活节点数)——与 Seal 时的全文索引构建同一成本档。

**部分应用按表面区分。** Go `Batch` 契约保持 first-error-stops：违规之前的写入持久且已发布，违规操作零副作用，其后的操作不再执行——不回滚。服务端上，`/v1/batch` 与 `/v1/txn` 在节点 upsert 违反唯一约束时**现在会中止**——这是刻意的行为变更：该路径此前不可能因节点写入失败（失败的 upsert 被静默跳过——返回 200 但节点缺失）。中止是确定性的（约束状态由日志推导，每个副本在同一个操作上同样拒绝——沿用向量 shape 校验先例），请求应答 **400 `UNIQUE_VIOLATION`**；`/v1/cypher` 对违规写语句给出同样的 400 面（绝不是 400 `PARSE`，也不是 500）。违规之前的条目已经应用——first-error-stops、不回滚，与其他写错误同一契约。声明经 WAL 持久化（整表帧、回放即整体替换——被删除的约束不会复活）并写入 **store v8** 快照；v7 及更早的文件原样打开（零约束）并在下次 compact 升到 v8，而 v8 文件无法被 0.16 之前的二进制读取——与历次存储版本递增同款的单向 tripwire（规划降级前请先读 CHANGELOG 的 0.16.0 说明）。

其他接入面：server `POST /v1/cypher` 对语句分类——读仍从本地 published 快照应答、逐字节不变（`{"query": "..."}` → `{columns, rows, count}`），写与 `/v1/batch` 一样经 raft 复制并返回 `{columns, rows, summary}`（即上述计数键，绝无 `count`）；两种路径的解析错都是 400 `PARSE`。三语 MCP 的 `cypher` 工具（默认 limit 1000，`total` 报真实总数）因写子句标注为非只读；Go 服务端在嵌入引擎上进程内执行写语句并透出同一 summary 对象（Python/TS 代理原样透传）。

## 服务模式（可选模块）

所有嵌入式图产品（Kuzu、CozoDB、SQLite）都不带复制；所有 Raft 产品都是 server。Vectra 沿用 rqlite 先例——复制包在引擎外，不进核心：

```
写入: 客户端 →POST /v1/batch→ leader ─raft→ 多数派提交 → FSM 确定性回放
发布器（每节点）: RLock 克隆 → 锁外 Seal → 原子指针交换
查询: GET /v1/* 与 POST /v1/query → 本地 published 快照（免锁，永不中断）
cypher: POST /v1/cypher 先分类——读 → 本地 published 快照，保持不变；
        写 → raft（leader 的 FSM 确定性重执行该语句）
```

```bash
cd server && go build -o vectra-server ./cmd/vectra-server
./vectra-server -id n1 -data /data/n1 -http :8080 -raft :9000     # 首节点
./vectra-server -id n2 -data /data/n2 -join http://n1:8080         # 加入集群
```

一致性语义：写线性一致（raft 提交即返回）——upsert 加逻辑键删除（删点级联删边）；读有界过期（≤ SealInterval，小图即时）。批量与事务写入中边的身份是 `(srcKey, dstKey, label)` 三元组——upsert 折叠平行边、props 后写覆盖；边条目设 `"allowParallel": true` 则改为追加真正的平行边。Cypher 写语句走同一条路：`POST /v1/cypher` 对每条语句分类（读保持本地、逐字节不变），写与 batch 完全一样经 raft 提交，响应为 `{columns, rows, summary}`，计数器（`createdNodes / createdEdges / deletedNodes / deletedEdges / propertiesSet / indexesCreated / constraintsCreated`）来自 APPLY 结果——绝无读路径的 `count` 键。唯一约束违规会在每个副本上确定性地中止整个 batch/txn/cypher 写 apply，并应答 400 `UNIQUE_VIOLATION`（完整契约见 Cypher 一节——包括它取代的 0.16 前静默跳过路径）。防脑裂：设了 `Join` 的节点绝不引导。三节点进程内混沌测试（leader 宕机切换、日志回放追平、重发布期间查询零失败）在 `-race` 下进 CI。

**Bearer 令牌认证（v0.12，可选）。** `-auth-token-file <文件>` 启用静态 bearer 认证：文件每行一个令牌——多行全部接受，这也是轮换方式（先把新令牌追加到所有节点，滚动重启，再删旧令牌）。空行与 CRLF 被容忍；行内含空白、文件不可读或为空都会中止启动——认证绝不静默退化成敞开的数据库。此后除 `GET /v1/health`（留给负载均衡探活）外，所有路由都要求 `Authorization: Bearer <令牌>`（SHA-256 摘要、常数时间比较）。加入节点的 `-join` 请求自带令牌——单一信任域，所有节点共享同一份令牌文件。**边界：**HTTP 认证只保护 HTTP；不启用 TLS（默认）时，节点间 raft 信道是明文无认证，请把 `-raft` 绑到私有网卡，别让集群流量经过不受信网络——或启用下文的内置 TLS（API 走 HTTPS + 可选 mTLS、节点间 X.509 双向 TLS），令牌就无需明文过网。

**Docker。** 仓库根目录的 [Dockerfile](Dockerfile) 构建单节点镜像——多阶段（`golang:1.27-alpine` 构建 → `FROM scratch` 运行时），成品镜像里只有静态二进制：

```bash
docker build -t vectra-server .        # 在仓库根执行（构建上下文横跨全部三个模块）
docker run -d -p 8080:8080 -v vectra-data:/data vectra-server
```

容器以非 root（uid 65534）运行，暴露 8080（HTTP）与 9000（raft），并声明 `VOLUME /data`——`/data` 在镜像内已归属运行时用户，Docker 挂载卷经 copy-up 继承其可写属主。默认 CMD 即上面的首节点形态（`-id n1 -data /data -http :8080 -raft :9000`）；二进制是 ENTRYPOINT，换参数直接跟在镜像名后面。存活探活：`GET /v1/health`——刻意不设 `HEALTHCHECK`（scratch 运行时没有 shell 可跑探针），请从外部探测（bearer 认证下该路由保持开放）。

**TLS（v0.16，可选；两个独立平面，仅标准库）。** HTTP API 平面：`-http-cert` + `-http-key` 启用 HTTPS（成对全有或全无——只给其中一个会中止启动）；`-http-client-ca <文件>` 追加客户端证书 mTLS——没有该 CA 签发证书的客户端在 TLS 握手阶段即被拒绝，任何路由都还没跑——且它要求 cert/key 成对，因此没有服务端 TLS 却设 client CA 同样中止启动（那只可能是配错的 mTLS）。raft 平面：`-node-cert` + `-node-key` + `-node-ca-cert` 为节点间提供 X.509 **双向** TLS——每个节点出示由共享 CA 签发的叶子证书，并以此 CA 校验所有对端（最低 TLS 1.2）；三件套全有或全无，缺任何一个都会中止启动并报出缺失的标志。两个平面相互独立——HTTPS 配明文 raft（或反过来）是合法配置；完全不设 TLS 标志时，服务端与纯 HTTP、纯 TCP 的旧行为逐字节一致。证书/密钥/CA 文件不可读或格式错误都会中止启动并指名文件；TLS 绝不静默回退明文。轮换即重启轮换：换好文件后重启所有节点——没有热加载。

客户端都不带 CA 参数；各技术栈经平台机制信任自签/私有 CA：

| 客户端 | 信任私有 CA 的方式 |
|---|---|
| Go 二进制：`vectra-cli`、`vectra-mcp` | 环境变量 `SSL_CERT_FILE=<ca.pem>`（Unix） |
| [sdk/ts](sdk/ts/README.md) 与 TS 版 MCP（[mcp/ts](mcp/ts)） | `NODE_EXTRA_CA_CERTS=<ca.pem>` |
| [sdk/python](sdk/python/README.md) | `SSL_CERT_FILE` 或 `SSL_CERT_DIR` |

经 TLS 加入集群：把加入节点的 `-join` 指向首节点的 `https://` URL，并**在加入节点启动前**就在其环境里设好 `SSL_CERT_FILE`——Go 在首次 TLS 使用时缓存根证书池，进程中途修改不会生效。加入节点自身若要跑 raft mTLS，须在 `-join` 前备齐三件套。有一个坑要知道：join 处理器跑在 HTTP 平面上，看不到 raft future，因此出示**错误**节点 TLS 材料的加入节点仍可能让 `AddVoter` 提交成功——集群从此持有一个永远不可达的 voter。别留下这种僵尸：3 个 voter 的集群里，一个永久不可达 voter 再加一次故障就会卡死写入（无多数派）。join 失败后，先清理死 voter 再加新节点——本版本没有移除 voter 的端点，支持的路径是修好加入节点的 TLS 材料（改正 cert/key/CA）、以**同一 node ID** 重新 join：重复的 `AddVoter` 是幂等的，会把既有 voter 条目重新指向可用地址。

**备份与恢复（v0.16）。** 在 **leader** 上 `GET /v1/backup` 以 tar.gz 流式返回该节点已物化的 raft 快照——分块传输（`Content-Disposition: attachment; filename="vectra-backup-<snapshot-id>.tar.gz"`），全程不在内存缓冲：

```bash
curl -f http://n1:8080/v1/backup -o b.tar.gz        # 启用 -auth-token-file 时加 -H 'Authorization: Bearer <token>'
```

follower 返回 503 `NOT_LEADER`，并以 `X-Vectra-Leader` 头指向 leader（与所有写路由一样，跟着它重试）。归档恰好三个条目——`meta.json`（节点身份）、`snapshots/<id>/meta.json`、`snapshots/<id>/state.bin`（FileSnapshotStore 那一对；`<id>` 即 `<term>-<index>-<msec>`）。两个已知的 500：从未提交过写入的 leader 磁盘上没有可流的快照（`snapshot: nothing new to snapshot`——先写一笔，再备份）；同一窗口内的第三个并发备份请求可能偶发 500——快照存储只保留两份快照，回收会与流的打开竞态——干净的 500，绝不会产出损坏的归档，因此请串行化备份拉取（或直接重试）。

恢复 = 解包到空目录 + 以**同一 node ID** 启动：raft 找到快照，FSM 恢复图（类型化索引、FTS 键、向量索引声明全部随行往返），节点以全新 term 服务：

```bash
mkdir /data/n1-restored && tar -xzf b.tar.gz -C /data/n1-restored
./vectra-server -id n1 -data /data/n1-restored -http :8080 -raft :9000
```

**恢复出的备份绝不能与原集群同时运行**——它带着同一 node ID 与空 raft 日志（全新 term），两者永远无法并成一个集群；恢复只用于灾难恢复或拉起新集群，不能用来扇出。

## vectra-cli（Cypher REPL）

`make build` 同时产出 `bin/vectra-cli`（源码：`server/cmd/vectra-cli`）——服务端 `POST /v1/cypher` 端点的轻客户端，面向运行中集群的即席查询：

| 标志 | 环境变量 | 默认值 | 含义 |
|---|---|---|---|
| `-url` | `VECTRA_URL` | `http://127.0.0.1:8080` | server 基地址 |
| `-token` | `VECTRA_TOKEN` | 空 | bearer 令牌，以 `Authorization` 头发送（服务端启用 `-auth-token-file` 时必设） |
| `-c` | — | — | 单次执行：跑一条语句、渲染、退出 |
| `-timeout` | — | `30s` | 单请求超时 |
| `-history` | — | `$HOME/.vectra_history` | 历史文件；空路径关闭持久化 |

```bash
./bin/vectra-cli                                                    # 对本地 server 开 REPL
VECTRA_URL=http://n1:8080 VECTRA_TOKEN=s3cr3t ./bin/vectra-cli     # 经环境变量给认证令牌
./bin/vectra-cli -c 'MATCH (n:Service) RETURN n.key LIMIT 5'       # 单次执行，不开 REPL
```

语句以 `;` 结尾、可跨多行（提示符 `> `，续行 `... `）。读语句渲染对齐表格加一行 `(N rows)`；Cypher 写语句渲染变更摘要——`Summary: created N nodes, ...`——语句带 `RETURN` 时同样渲染表格；错误打印为 `error: <code>: <message>`，循环继续。行编辑是 readline 风格（按 rune 编辑、方向键召回历史——这给 `server` 模块新增的唯一依赖是 `golang.org/x/term`），历史文件跨会话持久；管道 stdin 直接执行、无提示符。`:help` 列出 REPL 命令；`:q`、`:quit`、`:exit`、`quit`、`exit` 退出（仅在空缓冲时识别）。认证令牌走标志或环境变量，绝不进语句：

```
$ ./bin/vectra-cli
> MATCH (s:Service {env:'prod'}) RETURN s.key LIMIT 2;
s.key
gateway
payments
(2 rows)
> CREATE (n:Service {key:'billing', env:'prod'});
Summary: created 1 nodes
> :q
```

## 事务

v0.4 起服务端在同一条 raft 日志上支持**交互式乐观事务**——基于每键版本表（各键最后被改的 raft index）的 OCC。会话期间不持任何锁；验证只是 `fsm.Apply` 内的确定性查表，每个副本必然得出相同的提交/冲突结论：

```
读:     任意查询端点 → 取响应头 X-Vectra-Snapshot（publishedSeq）作为 token
写:     POST /v1/txn {nodes, edges, edgeDeletes, nodeDeletes,
                     txn: {snapshot: <token>, ser: false, readNodes: [...], readEdges: [...]}}
成功:   200 {committed: true, index, nodesApplied, edgesApplied, ...}
冲突:   409 {committed: false, conflicts: [...], conflictTotal, hint}
```

**隔离级别。** 默认快照隔离（SI）：写集验证、先提交者赢——token 之后任何被写键被改过则整批拒绝，盲写同样参与验证（强于 badger/FDB 默认档，后者的盲写会溜过去）。`"ser": true` 追加读集验证：记录在 `readNodes`/`readEdges` 里的键在 token 之后被改同样拒绝（防幻读 / 写偏序）。边写入同时提升两端点的版本；删点级联影响邻居邻接——这些都能被验证观察到。`txn` 字段必填（缺失返回 400 `VALIDATION`——自动提交请用 `/v1/batch`）；readNodes 与 readEdges 上限 10 万键（400 `TXN_READSET_TOO_LARGE`）。

**⚠️ Cypher 写入对 OCC 验证不可见（v0.14 已知限制）。** Cypher 写语句不推进每键版本表——写 summary 带的是计数器而非被写键集——因此 OCC/SI/SER 事务无法对经 Cypher 写过的键做冲突检测：`/v1/txn` 批次的读集或写集若与 Cypher 写过的键重叠，验证时会当作这些键从未变过。在引擎级 touched-key 追踪落地之前，不要在相同的键上混用交互式 `/v1/txn` 流量与 Cypher 写——让两者落在不相交的键集上，或把有争议的写入改走 `/v1/txn` 的 node/edge 条目。

**唯一约束违规在 apply 时中止，不在验证时（v0.16）。** OCC 验证比较的是版本而非值：`/v1/txn`（或 `/v1/batch`）批次里的节点 upsert 若会撞上已声明的唯一约束值，会先通过验证、随后在 `fsm.Apply` 确定性失败——每个副本得出同一结论——应答 **400 `UNIQUE_VIOLATION`**，而不是 409 冲突报告。违规之前的条目已经应用（first-error-stops；不回滚）。0.16 之前该路径会静默跳过失败的节点 upsert 并返回 200；约束完整契约见 Cypher 一节。

**读集覆盖**——`ser: true` 时应记录哪些键：

| 端点 | 记录 |
|---|---|
| `GET /v1/nodes/{key}` | `{key}` |
| `GET /v1/neighbors` | `{key}` + 返回的全部邻居键 |
| `POST /v1/query` | 起点键 + 到达的全部键 |
| `GET /v1/reachable` | `from` + 返回的全部键 |
| `/v1/path`、`/v1/stats`、`/v1/cycles` | 仅 SI——全图/依赖边界的读取无法做 SER 验证 |

**重试礼仪。** 收到 409：full-jitter 指数退避 `sleep = rand(0, min(500ms, 5ms·2^attempt))`（参考 [AWS Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)），重读取刷新 token，再提交。读己之写（v0.5）：`GET /v1/await?seq=<index>&timeoutMs=5000`（`timeoutMs` 默认 5s、上限 30s）——服务端长轮询，本副本发布覆盖到提交序号即返回 `{publishedSeq, reached}` 并附 `X-Vectra-Snapshot` 头；`reached:false` 则重发。轮询 `GET /v1/stats` 保留为 0.5 之前版本的兼容回退。S 档 / http / 16 worker / Apple M5 Pro：txnsi ~542 → ~846 提交/s（+56%），txnser ~556 → ~822（+48%），RYW 等待 11.1ms → ~3.1ms，p50 26.5 → ~18.3ms（docs/benchmarks/2026-10-14-ryw-await.md）。

**开销口径。** v0.4.0 以 fsm 级基准验收（`server/txn_bench_test.go` 的 `BenchmarkFSMAutoCommit` / `TxnSI` / `TxnSER16`——直接驱动 `fsm.Apply`，`Snapshot = ^uint64(0)` 使验证永不冲突）：AutoCommit **34.9µs** · TxnSI **28.6µs** · TxnSER16 **38.6µs**（AutoCommit/SER16 为 17 节点批次，TxnSI 为 1 节点批次；Apple M5 Pro）——16 键读集下 SI/SER 与自动提交差距 <25%（SER16 约 +11%）。vectra-bench 已内置 HTTP 级 `txnsi`/`txnser` 负载（`-mode http -workloads txnsi,txnser`）：S 档近线性扩展（1 → 16 worker：51 → 542 提交/s，SER 冲突 ≤ 4.3%）；大档位上遵守 RYW 礼仪的吞吐受发布周期支配（SealInterval + clone/Seal）——规模/间隔/并发的完整矩阵见 `docs/benchmarks/2026-10-13-oltp-txn.md`。

**⚠️ 升级到 0.4.0。** raft 快照新增 VCTX 尾帧（版本表）。不兼容是双向的：旧二进制读不了 0.4.0 快照，0.4.0 也拒绝 0.4.0 之前的快照（"legacy snapshot without VCTX rejected"）——不支持新旧混布。磁盘上有 0.4.0 之前 raft 快照的节点会带着该错误拒绝启动（启动时 raft 恢复盘上快照即失败），因此不存在旧快照状态的原地转换。恢复方式是重播种（re-seed）：停掉全部节点，按运维策略删除 raft 快照状态（或整个数据目录），重启并重新导入数据——之后 raft 的快照策略（SnapshotThreshold / SnapshotInterval）会自动拍出带 VCTX 的新快照。

## MCP（AI agent）

三个一等实现——[Go（嵌入引擎）](mcp)、[Python（FastMCP）](mcp/python)、[TypeScript](mcp/ts)——把一跳/多跳/最短路/可达性/循环依赖（含环内边）/Cypher 子集模式查询暴露为 MCP 工具，另提供 `inject_batch` 写入（支持 `allowParallel`；`lookup_property` 接受 string/number/bool 值）。v0.14 起 `cypher` 工具也接受写子句（CREATE/MERGE/SET/DELETE、CREATE INDEX）：标注为非只读，Go 服务端在嵌入引擎上进程内执行，写结果带 `summary` 计数器（`createdNodes / createdEdges / deletedNodes / deletedEdges / propertiesSet / indexesCreated / constraintsCreated`；读结果不含该键）。Go 服务端每笔已应用写入立即可查——无需 seal 往返；`seal` 是可选维护（merge + checkpoint），`-wal <dir>` 则运行可跨重启恢复的持久会话。v0.16 的约束 DDL 走同一个 `cypher` 工具。**运维提示：**经 `inject_batch` 注入的图可能带有历史重复数据——在这类图上声明唯一约束会在声明时失败并附重复清单（设计如此；声明会校验存量数据）。请**先**声明约束**再**注入，或先去重：

```bash
claude mcp add vectra -- /path/to/vectra-mcp
```

全部传输方式与宿主配置见 [mcp/README.md](mcp/README.md)。

## SDK（Python / TypeScript）

两个一等 REST 客户端——[sdk/python](sdk/python/README.md)（`vectra-graph`，仅标准库，Python 3.9+）与 [sdk/ts](sdk/ts/README.md)（`@projanvil/vectra`，原生 `fetch`，Node >= 18）——以**零运行时依赖**覆盖完整 `/v1/*` 面：图读取与遍历、Cypher 读+写（带变更摘要计数器）、两种 409 面皆为类型化冲突错误的 OCC 事务、读己之写 `await`、向量检索、向量索引 CRUD、索引 DDL。两者版本与服务端对齐在 0.14.0。

```
pip install vectra-graph
npm install @projanvil/vectra
```

两个包均在仓库内 `sdk/` 下；PyPI/npm 发布待所有者执行——在此之前可从检出安装（`pip install ./sdk/python` / `npm install ./sdk/ts`）。

Python 快速上手（连接 → 带摘要的 Cypher 写入 → 声明向量索引 → batch 写入向量 → 读回 → 检索）：

```python
from vectra_graph import Client

client = Client(base_url="http://127.0.0.1:8080")      # 或 $VECTRA_URL / $VECTRA_TOKEN
summary = client.cypher("CREATE (n:Tag {key:'t1', title:'hello'})")["summary"]
client.vector_create_index("docs", prop="emb", dim=3)  # 声明向量索引
client.batch(nodes=[{"key": "d1", "label": "Doc", "props": {"emb": [1.0, 0.0, 0.0]}}])
rows = client.cypher("MATCH (n:Doc) RETURN n.key, n.emb")["rows"]
hits = client.vector_search("docs", similar_to="d1", k=5)["hits"]
```

TypeScript 快速上手：

```js
import { VectraClient } from "@projanvil/vectra";

const vectra = new VectraClient({ baseUrl: "http://127.0.0.1:8080" });
const write = await vectra.cypher("CREATE (n:Tag {key:'t1', title:'hello'})");  // write.summary.createdNodes === 1
await vectra.vectorCreateIndex("docs", "emb", 3);  // 声明向量索引
await vectra.batch({ nodes: [{ key: "d1", label: "Doc", props: { emb: [1.0, 0.0, 0.0] } }] });
const rows = (await vectra.cypher("MATCH (n:Doc) RETURN n.key, n.emb")).rows;
const hits = (await vectra.vectorSearch("docs", { similarTo: "d1", k: 5 })).hits;
```

注意这里的分工：Cypher 写入在标量属性上演示摘要计数器，而向量经由 `batch`/`txn` 的节点属性写入——Cypher 子集没有列表字面量（CREATE 属性表和 SET 都只接受标量）。

## 示例

[examples/](examples/README.md) 用三种语言跑同一个场景——一张小型"项目知识图谱"：[Go 直连嵌入引擎](examples/go-embedded)（无需 server；在临时数据目录开持久 WAL 会话），以及 [Python](examples/python) / [TypeScript](examples/typescript) SDK 示例（对着运行中的 server）。每个变体都批量 upsert `Person`/`Project` 节点与 `WORKS_ON` 边、声明 `FULLTEXT` 索引并用 CJK 名字查询、跑一条带 `RETURN` 的 Cypher 写加一条 `MATCH ... RETURN` 读、最后回答一次 top-1 向量检索。

## 基准（幂律图；硬件以各报告为准——见英文摘要）

英文摘要与报告索引：[docs/benchmarks/README.md](docs/benchmarks/README.md)——头条结果及其来源报告与所在机器。

| 负载 | 1千/1万 | 10万/100万 | 100万/1000万（设计上界） |
|---|---|---|---|
| 一跳 p50 | 0.5µs | 4.4µs | 36µs |
| 3 跳 p50 | 7.7µs | 763µs | 7.1ms |
| 全图 BFS p50 | 16µs | 3.8ms | 70ms |
| 最短路径 p50 | 3.2µs | 101µs | 1.1ms |
| 注入（引擎 / raft+HTTP） | — | 950 万 / 140 万边每秒 | 940 万 / 130 万边每秒 |

v0.7 验收报告——微层世代 + WAL 对拍 v0.6.0 基线（遍历回归、WAL Off 导入线、merge 进行中查询尾延迟、崩溃恢复）：[docs/benchmarks/2026-10-15-delta-wal.md](docs/benchmarks/2026-10-15-delta-wal.md)（Apple M5 Pro）。

本地复现：`make bench [SIZE=medium] [MODE=http]`，数据目录与报告运行结束即删。

正确性按 LDBC 方法验证：与独立参考实现做属性对账、真实数据集外部 ground truth（SNAP email-Eu-core：203 个 SCC / 最大 803，对外发布的数字）、混沌与退化图测试、确定性检查。覆盖率：根包 92.3% · csr/dag 100% · store 84.0% · engine+traverse 98.8% · server 87.5% · mcp 85.6%。

## 仓库布局

```
vectra.go            门面：写入、Batch、Seal/Compact、快照、类型化属性索引
changeset.go         Changeset 词表（WAL 帧、server raft 批）
options.go           Open 选项：WALMode / WALDir
merge.go             后台合并协议：CaptureMerge / PublishMerge / LayerPressure
view.go              三源行流（base CSR + 微层记录）
cypher.go            门面：Cypher 子集入口（Graph.Cypher——读与写）
internal/csr         CSR 邻接（无指针 uint32 数组）
internal/dag         Tarjan SCC、凝聚图、拓扑层级
internal/engine      密封快照 + 访问器 + Seal 流水线
internal/delta       不可变微层构建器与层记录
internal/wal         嵌入式逻辑 WAL（JSON 帧、水位 checkpoint）
internal/writer      单写者包装层：队列、组提交、后台合并器
internal/cypher      Cypher 子集词法、语法、AST、行式执行器 + 写子句执行器
internal/traverse    算法：层同步 BFS、可达性、双向最短路、循环发现
internal/store       快照编解码（v3：索引声明区 + CRC32 尾校验，v1/v2 读兼容）
server/              独立模块：raft 复制 + HTTP API + 热替换发布器
                     ├─ cmd/vectra-server   服务二进制
                     ├─ cmd/vectra-bench    压测工具
                     └─ cmd/vectra-cli      面向运行中 server 的 Cypher REPL
mcp/                 独立模块：MCP 服务
                     ├─ cmd/vectra-mcp      Go（嵌入引擎，stdio + HTTP，快照或 WAL 会话）
                     ├─ python/             FastMCP（包装 HTTP API）
                     └─ ts/                 TypeScript（包装 HTTP API）
sdk/                 一方 SDK：python/（vectra-graph）+ ts/（@projanvil/vectra）
examples/            可运行示例：go-embedded（嵌入引擎）、python + typescript（SDK）
testdata/            真实数据集夹具（SNAP email-Eu-core，含引用）
docs/benchmarks/     压测设计与结果（README.md 为英文摘要）
docs/RELEASING.md    三模块发布指南（附 tag + go get 验证）
scripts/             工具脚本（goget-smoke.sh——发布标签冒烟）
.github/workflows/   CI（ci.yml）：三模块 vet/test、根模块 race、Cypher 分配门、SDK 套件、示例
```

核心模块**零第三方依赖**（无 go.sum）。`server` 模块引入 hashicorp/raft v1.8.0 + raft-boltdb/v2，以及 CLI 行编辑所需的 `golang.org/x/term`；`mcp` 模块引入 MCP 官方 Go SDK。部署什么引入什么。

## 基准与门禁命令

标准门禁：`go test . -count=1 && go test ./bench/ -count=1 && go vet .`（`bench/` 验收项按环境门控——例如 `VECTRA_RESTART=1`——未设则跳过）。race 门禁按包执行并**显式钉住超时**：`internal/vector` 在 `-race` 下实测约 1100 秒（结果干净），超过 Go 默认 600 秒，裸 `go test -race` 形态会假超时。各处统一用同一形态：

```
go test -race ./internal/vector/ -count=1 -timeout 40m
go test -race ./internal/writer/ -count=1 -timeout 40m   # 同一形态；远低于上限即可完成
go test -race .                -count=1 -timeout 40m     # 同一形态；远低于上限即可完成
```

## 明确不做

在线写跨进 server 模块——raft 复制仍是克隆/发布模型（merge 会重编号 ID，副本回放必须确定性，v0.7 微层引擎仅限嵌入式）；超 1000 万边的磁盘溢出；核心内置完整 GQL 解析器；分片。原"在线写 / delta-CSR"这条排除项已解决：v0.7 以微层世代形态落地，且没有放弃两个核心赌注（内存 CSR、免锁读）。路线图不变：k-hop 预计算。

## 许可证

[MIT](LICENSE)
