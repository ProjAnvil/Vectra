# Vectra

[English](README.md) | **简体中文**

面向**企业依赖图谱**（服务调用图、数据血缘、权限继承，10 万～1000 万边）的 Go 嵌入式内存图引擎。在这个量级，大图数据库背负的机制（buffer manager、磁盘页缓存、WAL 事务）全是纯成本。Vectra 押注相反的方向：图永远装得进内存，读免锁，整张图就是一份原子文件。

v0.2 起，同一引擎可升级为**多副本基础服务**：独立的 `server` 模块用 hashicorp/raft（leader 选举、写复制）加 HTTP API 包住引擎——rqlite 模式的图版。并且原生说 **MCP**（Go / Python / TypeScript 三语实现），AI agent 可以直接查询你的依赖图谱。

## 四个赌注

| 赌注 | 收益 |
|---|---|
| 全内存 + 无指针 CSR（`[]uint32` 平坦数组） | GC 成本与边数无关；`Seal()` 后并发读免锁 |
| 层同步并行 BFS（morsel 驱动 + 原子 Or 位图去重） | 1000 万边上比朴素 map 邻接快 3.3 倍 |
| 写入侧 SCC 压缩（Tarjan）——查询面对 DAG | 可达性每位图一比特；`Cycles()` 免费诊断循环依赖 |
| 单文件快照 + CRC32（v2 格式） | 原子发布/备份；服务模式蓝绿热替换 |

## 快速上手（嵌入式）

```go
import "github.com/projanvil/vectra"

g := vectra.New()
g.AddNode("gateway", "Service", map[string]any{"env": "prod"})
g.AddEdge("gateway", "auth", "CALLS", nil)
g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2}) // 同键 (src,dst,label) 替换属性
g.DeleteNode("ledger")                                               // 删点并级联删边
g.Seal() // 此后不可变；并发读免锁

// 链式遍历
res, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()

// 可达性（SCC 压缩快路径）
reach, _ := g.Reachable("gateway")

// 双向 BFS 最短路径
path, found, _ := g.ShortestPath("gateway", "kafka")

// 循环依赖发现
for _, c := range g.Cycles() { ... }

// 原子单文件快照
g.SaveFile("deps.vg")
```

## 服务模式（可选模块）

所有嵌入式图产品（Kuzu、CozoDB、SQLite）都不带复制；所有 Raft 产品都是 server。Vectra 沿用 rqlite 先例——复制包在引擎外，不进核心：

```
写入: 客户端 →POST /v1/batch→ leader ─raft→ 多数派提交 → FSM 确定性回放
发布器（每节点）: RLock 克隆 → 锁外 Seal → 原子指针交换
查询: GET /v1/* → 本地 published 快照（免锁，永不中断）
```

```bash
cd server && go build -o vectra-server ./cmd/vectra-server
./vectra-server -id n1 -data /data/n1 -http :8080 -raft :9000     # 首节点
./vectra-server -id n2 -data /data/n2 -join http://n1:8080         # 加入集群
```

一致性语义：写线性一致（raft 提交即返回）——upsert 加逻辑键删除（删点级联删边）；读有界过期（≤ SealInterval，小图即时）。防脑裂：设了 `Join` 的节点绝不引导。三节点进程内混沌测试（leader 宕机切换、日志回放追平、重发布期间查询零失败）在 `-race` 下进 CI。

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

**重试礼仪。** 收到 409：full-jitter 指数退避 `sleep = rand(0, min(500ms, 5ms·2^attempt))`（参考 [AWS Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)），重读取刷新 token，再提交。读己之写：轮询 `GET /v1/stats` 直到 `publishedSeq >= 提交响应里的 index`。

**开销口径。** v0.4.0 以 fsm 级基准验收（`server/txn_bench_test.go` 的 `BenchmarkFSMAutoCommit` / `TxnSI` / `TxnSER16`——直接驱动 `fsm.Apply`，`Snapshot = ^uint64(0)` 使验证永不冲突）：AutoCommit **34.9µs** · TxnSI **28.6µs** · TxnSER16 **38.6µs**（AutoCommit/SER16 为 17 节点批次，TxnSI 为 1 节点批次；Apple M5 Pro）——16 键读集下 SI/SER 与自动提交差距 <25%（SER16 约 +11%）。vectra-bench 的 txnsi/txnser HTTP 负载为后续工作。

**⚠️ 升级到 0.4.0。** raft 快照新增 VCTX 尾帧（版本表）。不兼容是双向的：旧二进制读不了 0.4.0 快照，0.4.0 也拒绝 0.4.0 之前的快照（"legacy snapshot without VCTX rejected"）——不支持新旧混布。磁盘上有 0.4.0 之前 raft 快照的节点会带着该错误拒绝启动（启动时 raft 恢复盘上快照即失败），因此不存在旧快照状态的原地转换。恢复方式是重播种（re-seed）：停掉全部节点，按运维策略删除 raft 快照状态（或整个数据目录），重启并重新导入数据——之后 raft 的快照策略（SnapshotThreshold / SnapshotInterval）会自动拍出带 VCTX 的新快照。

## MCP（AI agent）

三个一等实现——[Go（嵌入引擎）](mcp)、[Python（FastMCP）](mcp/python)、[TypeScript](mcp/ts)——把一跳/多跳/最短路/可达性/循环依赖暴露为 MCP 工具：

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

本地复现：`make bench [SIZE=medium] [MODE=http]`，数据目录与报告运行结束即删。

正确性按 LDBC 方法验证：与独立参考实现做属性对账、真实数据集外部 ground truth（SNAP email-Eu-core：203 个 SCC / 最大 803，对外发布的数字）、混沌与退化图测试、确定性检查。覆盖率：根包 92.2% · csr/dag 100% · store 84.3% · engine+traverse 98.7% · server 86.8% · mcp 81.5%。

## 仓库布局

```
vectra.go            门面：写入、Seal、快照、属性索引
internal/csr         CSR 邻接（无指针 uint32 数组）
internal/dag         Tarjan SCC、凝聚图、拓扑层级
internal/engine      密封快照 + 访问器 + Seal 流水线
internal/traverse    算法：层同步 BFS、可达性、双向最短路、循环发现
internal/store       快照编解码（v2：CRC32 尾校验，v1 读兼容）
server/              独立模块：raft 复制 + HTTP API + 热替换发布器
                     ├─ cmd/vectra-server   服务二进制
                     ├─ cmd/vectra-bench    压测工具
mcp/                 独立模块：MCP 服务
                     ├─ cmd/vectra-mcp      Go（嵌入引擎，stdio + HTTP）
                     ├─ python/             FastMCP（包装 HTTP API）
                     └─ ts/                 TypeScript（包装 HTTP API）
testdata/            真实数据集夹具（SNAP email-Eu-core，含引用）
docs/benchmarks/     压测设计与结果
```

核心模块**零第三方依赖**（无 go.sum）。`server` 模块引入 hashicorp/raft v1.8.0 + raft-boltdb/v2；`mcp` 模块引入 MCP 官方 Go SDK。部署什么引入什么。

## 明确不做

在线写 / delta-CSR（会推翻两个核心赌注）、超 1000 万边的磁盘溢出、核心内置完整 GQL 解析器、分片。路线图：k-hop 预计算、极简查询 DSL、属性类型化索引。

## 许可证

[MIT](LICENSE)
