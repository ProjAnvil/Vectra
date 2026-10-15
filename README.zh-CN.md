# Vectra

[English](README.md) | **简体中文**

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

// 链式遍历（Seal 与否都能查——写入立即可见）
res, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()

// 可达性（SCC 压缩快路径）
reach, _ := g.Reachable("gateway")

// 双向 BFS 最短路径
path, found, _ := g.ShortestPath("gateway", "kafka")

// 循环依赖发现（节点 + 环内边，带 label/props）
for _, c := range g.Cycles() { ... }

// 原子单文件快照
g.SaveFile("deps.vg")
```

快照说明：v3 格式会持久化 `IndexProperty` 声明，`vectra.Load` 恢复图的同时恢复查询索引（重载后类型化查找立即可用；v2/v1 旧文件仍可读但不含索引声明）。属性索引是**类型化的**——每个 key 三个有序桶（string / number / bool）：`LookupByProperty`（字符串等值）、`LookupByNumber` / `LookupByNumberRange(min, max *float64)`（数值等值 / 范围）、`LookupByBool`。数值在索引里统一归一化为 `float64`：Go API 写入的 `int` 与 JSON 快照读回的同值落进同一桶——"整数读回变 float64" 的老坑在索引查找上不复存在（`int64` 超过 2^53 有精度损失，JSON 数值模型的固有限制）。

## 持续写模型：微层世代（v0.7）

v0.7 溶解了密封快照门。每次写入（或 `Batch`）把变更物化为一个不可变**微层（micro-layer）**，经单个原子指针发布新世代 `{base, layers}`——读者永不加锁、永不阻塞，无论写者在做什么。后台合并器把 `base ∪ layers` 折叠成新的不可变 base（64 层或层内累计 6.4 万条边记录触发；层数触顶时写者短暂停顿等折叠落地），`Seal()`/`Compact()` 则显式执行该折叠，挂 WAL 时再追加 checkpoint。Seal 不再冻结任何东西：Seal 之后的写只是开启新世代；`Compact` 与 `Seal` 语义等价（spec §3.1 的"深度压缩重排"已被 merge 本身吸收）。二者都是写路径方法。

**可见性分族：**

| 查询族 | 可见性 |
|---|---|
| 点查族（`NodeByKey` / `NodeByID` / `EdgeByID` / 计数 / `EdgesOf` / `LookupBy*`） | 立即——三源联读（base ∪ layers，墓碑遮蔽） |
| 遍历族（`BFS` / `ShortestPath` / `Cypher` / `Query` / `NeighborsOf`） | 立即——base 与微层记录合并行流 |
| `Reachable` / SCC 相关统计 | 计数立即；超节点（SCC 压缩）加速只在合并后的 base 上生效，微层期间的查询退回点级 BFS |
| `Cycles` | 先折叠未合并层——强一致答案，唯一走写路径的查询 |

**ID 口径：**节点/边 ID 在世代内稳定（删除消耗号段）；merge 时稠密重编号，因此跨 `Seal`/`Compact`/后台折叠 ID 可能变化——与 v0.6 的跨 merge 契约一致，未变。

**写者纪律：**零锁引擎假设单写者 goroutine。直接模式（自己调 `AddNode`/`Batch`/...）下就是你任意一个单一 goroutine——并发裸写是数据竞争且会交错 WAL 帧。批量导入请用 `Batch` 分组（或包装层 changeset）：**未分组的单写各自物化一个微层**——语义正确，但百万边量级下慢约一个数量级（见验收报告）。结构化形态是单写者包装层（`internal/writer.Attach` → `Handle.Submit`）：有界队列、Badger 式组提交（每抽干批一组帧一次 fsync）、带层顶停顿的后台合并、以及 WAL 自上次 checkpoint ≥4 MiB 时的容量触发 checkpoint。附加期间，非 release 构建会以 `ErrNotWriter` 拒绝裸写与非写者 goroutine 的 `Seal`/`Compact`/`Cycles`；release 构建编译掉该检查，依赖本契约。

**写前日志（可选）：**`Open(path, Options{WALMode, WALDir})` 开启持久会话——整帧 JSON changeset，`ModeAlways` 下每个 `Batch` 边界一次 fsync（`ModeInterval` 只在显式边界 fsync；零值 `ModeOff` 恰为 v0.6 快照行为）。checkpoint——`Seal`/`Compact`/`Close` 时或包装层的 ≥4 MiB 容量触发——写水位快照 `<db>-<lsn>.db`、原子翻 manifest、重置日志；恢复 = 载入水位快照 + 重放其上的帧（撕裂尾在首个坏帧处停止；checkpoint 失败保留 WAL 供下次尝试）。磁盘上这是快照文件旁的 `wal` 旁路目录：旧版本二进制读新目录会无害忽略；v0.7 面对没有 wal 目录的旧目录则直接加载快照——v3 快照格式本身不变。

**内存：**merge 在旧世代成为垃圾前先分配新 base，1M/10M 档的峰值 RSS 接近 4.1 GB 活集的 2 倍（瞬时 9–11 GB；spec §9 R11）。merge 密集的 1M 档负载请预算 ≥16 GB 内存，或启用包装层的 `GCTune`（merge 前后把 GOGC 降到 50）。

**⚠️ 升级到 0.5.0。** 存储快照改以 v3 格式写入。不兼容是单向的：v1/v2 旧文件仍可读，但 0.5 之前的二进制读不了 v3 文件——跨版本交换 `.vg` 快照的嵌入式与 MCP 用户需要两端都在 0.5+。raft 集群新旧混布策略不变（本就不支持）。

## Cypher 子集查询

Seal 后的图可回答 openCypher/GQL 风格的模式查询——DSL 直接编译到现有 CSR 遍历原语上（`bfs` / `neighbors` / `lookup_property` 就是它的物理算子）：

```go
res, _ := g.Cypher(`MATCH (b {type:'controller'})-[:CALLS*1..2]->(s)
                    WHERE s.env = 'prod' AND s.w >= 3
                    RETURN b.key, s ORDER BY s.w DESC LIMIT 10`)
// res.Columns = ["b.key", "s"]; res.Rows = [{"b.key": "gw", "s": {key,label,props,...}}, ...]
```

语法：`MATCH pattern[, pattern...] [WHERE expr] RETURN *|items [ORDER BY item [ASC|DESC]...] [LIMIT n]`；节点 `(var:Label {prop:literal})`，边 `-[var:REL|REL*n..m]->` / `<-[...]-` / `--`（无方向）；WHERE 支持 `= <> < <= > >=`、`STARTS/ENDS WITH`、`CONTAINS`、`IN [...]`、`AND/OR/NOT`、伪属性 `var.key / var.label / var.id`，以及定长单跳上的边属性谓词（`WHERE e.weight > 3`——并行边各绑定一行）。

语义（刻意设计并已文档化）：变长边采用**节点对语义**——目标的最短距离落在跳数窗口内即匹配（`[:CALLS*1..2]` = 按最短路径 1..2 跳；环不会制造更长的匹配），中间节点不施加目标模式过滤，变长段不绑定边变量。行按具名绑定 + 终点去重；WHERE 采用 null 传播逻辑（属性缺失永不满足，`NOT` 救不回来）。锚定在模式允许时走类型化属性索引（`{type:'controller'}`），否则全扫。中间行数上限 1,000,000（超限报错并提示收窄模式或加 `LIMIT`）。

其他接入面：server `POST /v1/cypher`（`{"query": "..."}` → `{columns, rows, count}`；解析错 400 `PARSE`）、三语 MCP 的 `cypher` 工具（默认 limit 1000，`total` 报真实总数）。

## 服务模式（可选模块）

所有嵌入式图产品（Kuzu、CozoDB、SQLite）都不带复制；所有 Raft 产品都是 server。Vectra 沿用 rqlite 先例——复制包在引擎外，不进核心：

```
写入: 客户端 →POST /v1/batch→ leader ─raft→ 多数派提交 → FSM 确定性回放
发布器（每节点）: RLock 克隆 → 锁外 Seal → 原子指针交换
查询: GET /v1/* 与 POST /v1/query、/v1/cypher → 本地 published 快照（免锁，永不中断）
```

```bash
cd server && go build -o vectra-server ./cmd/vectra-server
./vectra-server -id n1 -data /data/n1 -http :8080 -raft :9000     # 首节点
./vectra-server -id n2 -data /data/n2 -join http://n1:8080         # 加入集群
```

一致性语义：写线性一致（raft 提交即返回）——upsert 加逻辑键删除（删点级联删边）；读有界过期（≤ SealInterval，小图即时）。批量与事务写入中边的身份是 `(srcKey, dstKey, label)` 三元组——upsert 折叠平行边、props 后写覆盖；边条目设 `"allowParallel": true` 则改为追加真正的平行边。防脑裂：设了 `Join` 的节点绝不引导。三节点进程内混沌测试（leader 宕机切换、日志回放追平、重发布期间查询零失败）在 `-race` 下进 CI。

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

三个一等实现——[Go（嵌入引擎）](mcp)、[Python（FastMCP）](mcp/python)、[TypeScript](mcp/ts)——把一跳/多跳/最短路/可达性/循环依赖（含环内边）/Cypher 子集模式查询暴露为 MCP 工具，另提供 `inject_batch` 写入（支持 `allowParallel`；`lookup_property` 接受 string/number/bool 值）。Go 服务端每笔已应用写入立即可查——无需 seal 往返；`seal` 是可选维护（merge + checkpoint），`-wal <dir>` 则运行可跨重启恢复的持久会话：

```bash
claude mcp add vectra -- /path/to/vectra-mcp
```

全部传输方式与宿主配置见 [mcp/README.md](mcp/README.md)。

## 基准（M1 Pro，幂律图，完整报告见 [docs/benchmarks](docs/benchmarks)）

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
cypher.go            门面：Cypher 子集查询入口（Graph.Cypher）
internal/csr         CSR 邻接（无指针 uint32 数组）
internal/dag         Tarjan SCC、凝聚图、拓扑层级
internal/engine      密封快照 + 访问器 + Seal 流水线
internal/delta       不可变微层构建器与层记录
internal/wal         嵌入式逻辑 WAL（JSON 帧、水位 checkpoint）
internal/writer      单写者包装层：队列、组提交、后台合并器
internal/cypher      Cypher 子集词法、语法、AST、行式执行器
internal/traverse    算法：层同步 BFS、可达性、双向最短路、循环发现
internal/store       快照编解码（v3：索引声明区 + CRC32 尾校验，v1/v2 读兼容）
server/              独立模块：raft 复制 + HTTP API + 热替换发布器
                     ├─ cmd/vectra-server   服务二进制
                     ├─ cmd/vectra-bench    压测工具
mcp/                 独立模块：MCP 服务
                     ├─ cmd/vectra-mcp      Go（嵌入引擎，stdio + HTTP，快照或 WAL 会话）
                     ├─ python/             FastMCP（包装 HTTP API）
                     └─ ts/                 TypeScript（包装 HTTP API）
testdata/            真实数据集夹具（SNAP email-Eu-core，含引用）
docs/benchmarks/     压测设计与结果
```

核心模块**零第三方依赖**（无 go.sum）。`server` 模块引入 hashicorp/raft v1.8.0 + raft-boltdb/v2；`mcp` 模块引入 MCP 官方 Go SDK。部署什么引入什么。

## 明确不做

在线写跨进 server 模块——raft 复制仍是克隆/发布模型（merge 会重编号 ID，副本回放必须确定性，v0.7 微层引擎仅限嵌入式）；超 1000 万边的磁盘溢出；核心内置完整 GQL 解析器；分片。原"在线写 / delta-CSR"这条排除项已解决：v0.7 以微层世代形态落地，且没有放弃两个核心赌注（内存 CSR、免锁读）。路线图不变：k-hop 预计算。

## 许可证

[MIT](LICENSE)
