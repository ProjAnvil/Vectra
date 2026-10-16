# Vectra 压测模块设计（vectra-bench）

## 规模分级（依据 LDBC）

LDBC SNB 以 SF（CSV GiB）分级：SF0.1=32.8万节点/150万边、SF1=320万/1730万；
Graphalytics 用 s(V,E)=log10(V+E) 分 T-shirt 档。Vectra 定位"10万~1000万边"，
取对齐的三档：

| 档位 | 节点 | 边 | log10(V+E) | Graphalytics 对应 |
|---|---|---|---|---|
| S | 1,000 | 10,000 | 4.0 | S |
| M | 100,000 | 1,000,000 | 6.0 | M |
| L | 1,000,000 | 10,000,000 | 7.0 | L（Vectra 设计上界） |

## 驱动选型：纯 Go（否决 Python 注入）

- 压测客户端必须显著快于被测系统，否则测量的是客户端而非引擎。
  业界共识（k6 vs Locust 对比）：Go/goroutine 单机吞吐数倍于
  Python/gevent；Python 客户端 p95 在 5k+ QPS 后自身劣化。
- 单工具链：进 CI、零环境依赖、种子确定性可复现。
- Python 的优势（生态、脚本生成）对纯压测无意义。
- 双模式都由 Go 驱动：`engine`（进程内函数调用，测引擎）与
  `http`（起 server 节点走完整 HTTP+raft 路径，测服务形态）。

## 图形态

均匀随机图（ER）的遍历特性不代表真实依赖图。生成器采用
**幂律度分布**（Barabási–Albert 优先连接 + 前向时间边方向），
天然产生少数高扇出"热点服务"——正是多跳查询的困难样例。
确定性种子，同参数同图。

## 工作负载（对齐 SNB Interactive 查询形态 + parameter curation）

| 负载 | 形态 | Vectra API |
|---|---|---|
| 注入 | 数据注入吞吐 | AddNode/AddEdge→Seal（http: POST /v1/batch） |
| W1 one-hop | 一跳邻域 | NeighborsOf / GET /v1/neighbors |
| W2 two-hop | 2 跳邻域计数（CR3 形态） | BFS MaxDepth=2 / POST /v1/query |
| W3 three-hop | 3 跳邻域计数（CR9 形态） | BFS MaxDepth=3 |
| Wall | 全图可达 | BFS 不限深 |
| Wpath | 点对最短路（CR14） | ShortestPath / GET /v1/path |
| Wreach | 可达集（爆炸半径） | Reachable / GET /v1/reachable |
| Wmixed | 读写混合（engine 专属） | 见下 |

查询参数预采样（LDBC parameter curation）：起点按出度加权（模拟
"网关/核心服务"被高频查询），点对只保留可达对，每档固定种子。

## Wmixed：读写混合负载

业界基准（SNB Interactive ~20% 更新、FalkorDB 82/18、Memgraph mixed）
都以读写混合为核心负载；对 Vectra 它还命中特有风险——持续写入下
微层栈增长对查询尾部的影响（v0.7 A2 曾见的稳态尾部）。

- 形态：`workers` 个读 goroutine 全速轮转 W1/W2/W3/Wpath，加
  **一个**步调写 goroutine（引擎写入本就串行，单写者是诚实形态）。
- 写操作：`-writebatch`（默认 50）条边的 Batch 变更集——一半
  UpsertEdge 改属性，一半在既有节点间加新边（拓扑扰动，不失效
  策展参数）。
- 写速率 `-writerate`（默认 200 批/s，0 关闭）：写供给是显式旋钮，
  读尾部因此可归因到具体的持续写速率（ pacing 精度上限 ~500/s）。
- 输出：读/写两侧各自的 ops/QPS/p50/p90/p99/max。

## 指标

注入：节点/s、边/s、Seal 耗时。查询：ops、p50/p90/p99/max 延迟、
QPS（并发 worker 池，预热一轮丢弃——LDBC 惯例）。另：

- **TEPS**（GAP/Graph500 归一化）：Wall 的遍历边速率 ≈ QPS ×
  每操作平均扫描出边数（策展期对至多 16 个起点预算一次无界 BFS）。
- **内存与 GC**：阶段边界（注入后/读负载后/混合后）的 heap
  alloc/heap sys、GC 次数与暂停增量、GC CPU 占比
  （runtime.ReadMemStats；嵌入式 Go 引擎的内存故事在 GC，
  不在进程 RSS）。
- **冷启动**（engine 专属）：SaveFile→LoadFile 往返，报恢复
  边/s 与 MiB/s（服务形态的对应指标是 A7 的 WAL 重放）。
- **pprof 归因**：`-profile <dir>` 时每负载落 cpu profile +
  heap/mutex 快照；归因运行不与干净运行对比（mutex 采样全程开启）。

## 数据保留

生成数据落 `benchdata/`（gitignore）或 `--datadir`（默认系统临时目录），
运行结束自动删除（`--keep` 保留调试）；仓库只保留
`docs/benchmarks/20xx-xx-xx-*.md` 结果。
