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

## 基准（M1 Pro，18 核，1M 节点 / 10M 边随机图）

| 查询 | 耗时 | vs 朴素 map 邻接 BFS (104ms) |
|---|---|---|
| 全图 BFS（串行 CSR） | 57ms | 1.8x |
| 全图 BFS（并行 CSR） | 41ms | 2.6x |
| 全图可达性（SCC 超节点） | 14ms | 7.3x |
| 双向最短路径（单对，64 对均值） | ~0.42ms | — |

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

## 布局

```
vectra.go      Graph：写入、Seal、查找、快照入口
traverse.go    BFS（串行/并行）、Reachable、ShortestPath、Cycles
query.go       链式查询构造器
pool.go        visited 位图缓冲池
internal/csr   CSR 邻接（无指针 uint32 平坦数组）
internal/dag   Tarjan SCC、凝聚图、拓扑层级
internal/store 二进制快照编解码
```

测试含三组等价性/属性验证：并行 BFS ≡ 串行 BFS（20k 节点随机图 + race 检测）、超节点可达性 ≡ 节点级 BFS、双向最短路径 ≡ 单向 BFS 距离（200 对随机点，含路径连续性与边存在性）。
