# Vectra

嵌入式内存图引擎，为 Go 而生。面向**企业依赖图谱**（服务调用图、数据血缘、权限继承）——这个量级（10 万～1000 万条边）下，现有图引擎为大图做的过度设计（buffer manager、磁盘页缓存、WAL 事务）全是纯成本。

```go
g := vectra.New()
g.AddNode("gateway", "Service", nil)
g.AddEdge("gateway", "auth", "CALLS", nil)
g.Seal() // 构建查询结构，此后不可变、并发读免锁

// 链式遍历
res, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()

// 可达性（SCC 压缩快路径）
reach, _ := g.Reachable("gateway")

// 双向 BFS 最短路径
path, found, _ := g.ShortestPath("gateway", "kafka")

// 循环依赖发现（SCC 副产品）
for _, c := range g.Cycles() { ... }

// 单文件快照
g.SaveFile("deps.vg")
g2, _ := vectra.LoadFile("deps.vg")
```

## 设计：四个赌注

**1. 小图全内存。** 图永远小于单机内存（企业依赖图谱恒成立）。整个 CSR 常驻内存，磁盘只做崩溃恢复（单文件快照整图重写）。无 buffer manager，无缓存替换。SQLite 靠"整页读进内存"这种在小规模成立的赌注赢下全世界，Vectra 的赌注相同。

**2. 无指针 CSR。** 邻接关系是两个平坦 `[]uint32`（偏移 + 目标），边属性（edge ID、标签）是平行数组。Go GC 按指针追踪存活对象——无指针数据结构的扫描成本与边数无关，百万级遍历期间 GC 停顿趋近于零。这是 Java 系图库（Neo4j 的 off-heap 实体池）用巨大复杂度换的东西，Go 里免费。

**3. 层同步并行 BFS。** 每层前沿是扁平数组，切块分给 goroutine（morsel 驱动），去重用跨 worker 原子 Or 位图（`atomic.OrUint32`，无 CAS 重试）。Kuzu 的多核并行模型，goroutine 版本的实现成本只有一小部分。

**4. 写入侧 SCC 压缩。** Seal 时跑 Tarjan，把强连通分量压成超级节点——**查询面对的永远是 DAG**：
- 可达性走超节点路径，visited 每位对应一个分量，零环检测；
- "循环依赖阻断"的防御机制变成 `Cycles()` 诊断能力——SCC 成员表就是循环依赖清单。

## 基准（M1 Pro，18 核，1M 节点 / 10M 边随机图，10 轮均值）

| 查询 | 耗时 | vs 朴素 map 邻接 BFS (145ms) |
|---|---|---|
| 全图 BFS（串行 CSR） | 102ms | 1.4x |
| 全图 BFS（并行 CSR） | 44ms | 3.3x |
| 全图可达性（SCC 超节点） | 15ms | 9.7x |
| 双向最短路径（单对，64 对均值） | ~0.42ms | — |
| Seal（建图 + CSR + Tarjan + 凝聚） | 4.6s | — |

```
go test -bench 1M -benchtime 5x ./
```

细节：BFS 结果深度用平坦 `[]uint32` 而非 map——基准实测 map 插入会吞掉 CSR 的全部访存优势（两者曾同为 ~115ms）。

## API 一览

| 方法 | 说明 |
|---|---|
| `New()` / `AddNode` / `AddEdge` / `Seal` | 批量导入 → 不可变化身 |
| `BFS(starts, opts...)` | 多源层同步 BFS：`WithMaxDepth` / `WithLabels` / `WithDirection` / `WithParallelThreshold` |
| `Query().From(...).Out(labels...).MaxDepth(n).Run()` | 链式遍历构造器 |
| `Reachable(key, opts...)` / `ReachableIDs` | 可达集（无过滤走 SCC 超节点快路径，有标签过滤走严格节点级） |
| `ShortestPath(from, to)` | 双向 BFS 最短路径，返回节点与边序列 |
| `Cycles()` | 循环依赖清单（大小>1 的 SCC + 自环），按规模降序 |
| `NeighborsOf(key, dir, labels...)` | 一跳邻居 |
| `SaveFile` / `LoadFile` | 单文件二进制快照，加载自动 Seal |
| `Stats()` | 节点/边/分量/环上节点数 |

错误哨兵：`ErrSealed`（Seal 后写入）、`ErrNotSealed`（Seal 前查询）、`ErrNodeNotFound`。

## v1 边界（明确不做）

- **并发写事务**。定调批量导入 + 只读服务，与依赖图索引的实际更新模式吻合。Seal 前是单写者。
- **WAL / 增量持久化**。快照是整图重写；图大时保存成本 = 序列化全图。
- **查询 DSL / 服务模式**。Go API 优先；gRPC/HTTP 壳与类 Cypher 极简语法留待 v2（CozoDB 已验证同一引擎可双模式）。
- **因子化 / WCOJ**（Kuzu 的学术深水区）。多跳模式匹配靠 CSR + 批量 BFS + 双向搜索打 80% 场景；CSR 的边标签列存已按 slot 平行数组排列，为后续预留。

## 路线图

1. 热点 k-hop 预计算（高频起点的 2-hop 可达集交集）
2. 文本查询（`MATCH 起点 -[边*1..3]-> 终点 WHERE ...` 的 3-4 个固定模式）
3. 服务模式（gRPC/HTTP 壳，文件锁防多进程）
4. 写入侧增量：批量追加后后台重 Seal

## 测试体系（对标 LDBC 方法论）

测试不是凑覆盖率，四层各有明确的业界对标：

**1. 属性测试 × 独立参考实现**（对标 LDBC Graphalytics 的 reference output 验证）。测试内置一套与生产代码零共享的朴素实现（map 邻接 BFS、可达闭包暴力 SCC），在五种拓扑形态（稀疏/稠密随机图、分层 DAG、环簇）上逐点对账：全源 BFS 深度向量（串行+并行）、可达集、150 对最短路（含路径合法性）、SCC 划分。

**2. 真实数据集外部 ground-truth**（对标 LDBC 审计的外部验证）。`testdata/email-Eu-core.txt.gz`（SNAP 真实邮件图，1005 节点/25571 边）断言 SNAP/SuiteSparse 独立发布的统计值：203 个 SCC、最大 SCC 803 节点、803 个成员两两互达（全量 803 次 BFS）。这些数字不是我们自己算的。

**3. 鲁棒性**（对标 Graphalytics robustness 轴）。8 goroutine 混跑全部只读 API 过 race 检测（"Seal 后免锁并发读"的声明由此背书）；退化形态表驱动（空图/孤立点/完全图/星/多边/自环海）；百万节点深链压迭代 Tarjan 的显式调用栈；同种子两次构建全部查询逐位一致。

**4. 工作负载基准**（对标 SNB Interactive + parameter curation）。真实图上预固定查询参数（高扇出起点、已验证可达对）：2/3 跳邻域 17/36µs、点对最短路 2.3µs、可达性 6.6µs、16 源批量 BFS 122µs、快照冷加载 172 MB/s。

```
go test ./...                     # 引擎全部测试
go test -race ./...               # 并发声明验证
go test -short ./...              # 跳过百万节点压力项
go test -bench 'SNB|1M' ./...     # 工作负载 + 微基准
cd server && go test -race ./...  # 三节点混沌集群（选主/宕机/日志回放/查询不中断）
```

## 服务模式（可选模块 `vectra/server`）

竞品调研的规律：所有嵌入式图产品（Kuzu/CozoDB/SQLite）都没有内置复制，Raft 只活在 server 产品里。Vectra 沿用 rqlite/DQLite 先例——**复制包在引擎外面**：

```
写入: 客户端 →POST /v1/batch→ leader ─raft 复制→ 多数派提交
                                        ↓ FSM.Apply（确定性回放到 live 可写图）
      发布器（每节点本地）: RLock 克隆 → 锁外 Seal → atomic.Pointer 交换
查询: GET /v1/* → 本地 published 快照（免锁并发读，永不中断）
```

```go
// 节点 1（首个，自动引导）
n, _ := server.New(server.Config{NodeID: "n1", HTTPAddr: ":8080",
    RaftAddr: ":9000", DataDir: "/data/vectra"})
http.ListenAndServe(":8080", n.Handler())

// 节点 2/3（Join 指向任一既有节点的 HTTP 地址）
n, _ = server.New(server.Config{NodeID: "n2", ..., Join: "http://n1:8080"})
```

**一致性语义**：写线性一致（raft 提交即返回）；读有界过期（本地快照，新鲜度 ≤ SealInterval，默认 2s；小图每 batch 立即发布）。**就绪门**：重启恢复期间（日志回放 + 首次发布完成前）查询返回 503 NOT_READY。

**运维约束（防脑裂）**：`Join` 非空的节点绝不 bootstrap；两个空 Join 的新数据目录各自引导会形成两个独立集群——与 etcd initial-cluster 同类的运维错误，扩容永远走 Join。缩容（成员移除）v2 不支持，走重建集群。`{DataDir}/meta.json` 锁定节点身份，目录复用会报 `ErrDirMismatch`。

**API**：`POST /v1/batch`（写）、`POST /v1/join`、`GET /v1/{health,stats,nodes/{key},neighbors,reachable,path,cycles}`、`POST /v1/query`、`GET /debug/vars`（expvar：applied/seals/leader_changes 等指标）。错误体 `{"error":{"code","message"}}`；非 leader 写入 503 + `Retry-After` + `X-Vectra-Leader` 提示。

**依赖声明**：核心模块零第三方依赖（go.sum 不存在）；server 模块引入 hashicorp/raft v1.8.0 + raft-boltdb/v2 v2.4.2（传递依赖含已归档的 boltdb/bolt v1.3.1——Consul/Nomad 生产同款路径，可接受并在此记录）。

## 布局

```
vectra.go            门面：Graph 写入、Seal、快照、属性索引
traverse.go          门面：BFS/可达性/最短路/Cycles 的选项与类型翻译
query.go             链式查询构造器（From/Out/In/MaxDepth/Where）
internal/csr         CSR 邻接（无指针 uint32 平坦数组）
internal/dag         Tarjan SCC、凝聚图、拓扑层级
internal/engine      密封快照 + 访问器 + Seal 流水线 + 属性索引
internal/traverse    算法：层同步 BFS、超节点可达性、双向最短路、Cycles
internal/store       快照编解码（v2：CRC32 尾校验，v1 读兼容）
server/              独立 module：raft 复制 + HTTP API + 热替换发布
testdata/           真实数据集夹具（SNAP email-Eu-core，84KB，含引用与统计推导）
docs/specs/         v2 spec（含竞品与 Raft 选型调研结论、子代理审计记录）
```

**覆盖率**（`go test -cover`）：根包 89.8% · csr 100% · dag 100% · store 84.3% · engine+traverse 94.6%（跨包）· server 83.5%。store/server 的剩余缺口集中在需要故障注入基建的防御分支（权限瞬态错误、发布单调守卫的丢弃路径等），属有意取舍。

测试含三组等价性/属性验证：并行 BFS ≡ 串行 BFS（20k 节点随机图 + race 检测）、超节点可达性 ≡ 节点级 BFS、双向最短路径 ≡ 单向 BFS 距离（200 对随机点，含路径连续性与边存在性）。
