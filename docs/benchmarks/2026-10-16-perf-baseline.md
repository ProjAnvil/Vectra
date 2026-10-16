# Vectra 性能基线与瓶颈报告(engine 模式)

Date: 2026-10-16 · Commit: `ee256e3` (v0.7.1)
环境: Apple M5 Pro (18 核) / 64 GB / APFS / Go 1.27.2 darwin/arm64
矩阵: S(1K/10K) · M(100K/1M) · L(1M/10M) × workers{1, 8, 16} × engine 模式,seed=42,M-w8 重复 3 次(偏差 ±1%)
工具: `make bench`(vectra-bench,本轮新增 mixed 负载、内存/GC 采样、TEPS、冷启动、pprof 归因,见 `docs/benchmarks/DESIGN.md`)

## 结论(TL;DR)

1. **纯读路径健康且随核扩展**。M 档 8 并发:neighbors 376K QPS、bfs2 60K、path 40K、bfsall 1.8K(p50 3.96ms);16 并发 bfsall 提升 2.1×(TEPS 1.09G→2.31G edges/s)。L 档 bfsall TEPS 721M edges/s(8 并发),w1→w8 并行加速 4.6×。
2. **最重瓶颈 B1:持续写入下读路径崩塌**。仅 ~165 写批/秒(每批 50 边)时,M 档读 p50 从 ~92µs 抬到 109ms(**~27×**),p99 达 1.4-1.7**秒**;L 档读 p50 89ms。归因:每查询的视图构建对**全部活跃微层**做逐节点解析(`engineViewOf`→`nodeWinner`,字符串 map 查找),成本 = O(活跃层数),无直接模式的后台合并兜底。写路径本身无恙(p50 150µs)。
3. **瓶颈 B2:读路径分配密集 → GC 占用 10-15%**。整程累计分配 1.24TB;热点为 `NeighborsOf` 结果片与 `ShortestPath` 访问集。heap sys 从注入后的 0.7GiB(M)膨胀到 1.1-1.2GiB。
4. **瓶颈 B3(轻)**:未批注式注入速率随规模衰减 8.6M(S)→2.0M(L) edges/s,仍高于 A4 底线 1M/s。
5. 冷启动(快照恢复)2.1-3.0M edges/s;L 档 464MiB 快照 4.6-4.8s 恢复。L 档 Seal(一次性折叠)4.4s。

## 方法论(与业界基准的对齐与偏差)

| 维度 | 业界参照 | 本报告做法 | 偏差声明 |
|---|---|---|---|
| 查询参数 | LDBC SNB parameter curation | 出度加权起点 + 可达对,固定种子预采样 | 一致 |
| 延迟指标 | LDBC/FalkorDB: p50/p90/p99 + QPS | 一致;worker 池并发,预热一轮丢弃 | 一致 |
| 读写混合 | SNB Interactive ~20% 更新;FalkorDB 82/18 | 新增 Wmixed:8 读全速 + 1 写定速旋钮(-writerate) | 写速率是显式旋钮而非比例;单写者匹配引擎串行写模型 |
| 遍历吞吐 | GAP/Graph500 TEPS | bfsall TEPS ≈ QPS × 每操作平均扫描出边 | **近似**:均值吞吐,非 64 次搜索的调和平均;分子为有向出边扫描数(与 out-BFS 语义一致),非无向分量边数 |
| 内存 | 厂商报 RSS | runtime 堆/GC 指标(嵌入式 Go 引擎的内存故事在 GC) | 未采进程 RSS |
| 线程梯度 | 1/2/4/8/16/32 | 1/8(/16 仅 M) | 缩减 |
| 数据集 | SNAP Pokec(事实标准) | BA 幂律合成图(确定性种子) | 无真实社交图;形态对齐(幂律、热点) |

不在本轮范围:http 模式(工具已支持,后续)、GAP 分析内核(PageRank/CC——OLTP 定位,bench 侧实现只测到 API 开销)、WAL-on 混合(A5 已覆盖 WAL 吞吐)。Go 生态无成熟 SNB/GAP harness(已调研),自建 Go 驱动成立。

## 纯读结果(engine,10s/负载,预热丢弃)

| 负载 | 指标 | S-w1 | S-w8 | M-w1 | M-w8(3 次中位) | M-w16 | L-w1 | L-w8 |
|---|---|---|---|---|---|---|---|---|
| neighbors | QPS | 1.97M | 3.95M | 244K | 376K | 333K | 54K | 46K |
| | p50 / p99 | 333ns / 2.0µs | 541ns / 5.2µs | 2.1µs / 26µs | 4.0µs / 250µs | 7.6µs / 728µs | 8.4µs / 303µs | 31.5µs / 1.33ms |
| bfs2 | QPS | 540K | 846K | 18K | 60K | 50K | 2.3K | 6.9K |
| | p50 / p99 | 1.4µs / 7.8µs | 2.5µs / 116µs | 54µs / 112µs | 92µs / 720µs | 147µs / 2.3ms | 426µs / 789µs | 771µs / 5.5ms |
| bfs3 | QPS | 183K | 369K | 1.7K | 8.8K | 11.8K | 298 | 1.1K |
| | p50 / p99 | 4.8µs / 19µs | 8.0µs / 349µs | 635µs / 1.1ms | 746µs / 3.6ms | 908µs / 7.9ms | 3.3ms / 7.2ms | 6.8ms / 19.1ms |
| bfsall | QPS | 84K | 202K | 237 | 1,845 | 3,910 | 26 | 122 |
| | p50 / p99 | 11.2µs / 28µs | 17.6µs / 647µs | 4.2ms / 4.7ms | 3.96ms / 13.2ms | 3.2ms / 16.4ms | 37.5ms / 44.6ms | 64.9ms / 91.8ms |
| path | QPS | 749K | 738K | 15K | 40K | 39K | 1.4K | 3.5K |
| | p50 / p99 | 1.0µs / 4.6µs | 2.6µs / 169µs | 46µs / 396µs | 95µs / 1.4ms | 184µs / 3.1ms | 418µs / 2.9ms | 1.0ms / 11.7ms |
| reach | QPS | 116K | 291K | 1.4K | 7.0K | 9.6K | 141 | 721 |
| | p50 / p99 | 7.5µs / 35µs | 11.8µs / 443µs | 686µs / 1.1ms | 1.0ms / 3.1ms | 1.3ms / 9.6ms | 6.9ms / 10.6ms | 10.5ms / 20.2ms |

- M-w8 三次重复:bfsall QPS 1845/1871/1832(±1%),p99 12.2-13.2ms——数据稳定。
- bfsall TEPS:**S-w8 1.21G · M-w8 1.09G · M-w16 2.31G · L-w8 721M · L-w1 157M** edges/s(L 档均值扫描 593 万出边/查询;w1→w8 加速 4.6×,parallel BFS 扩展性好)。
- 注入(未批注 AddEdge,吸收窗口内):S 8.6M / M 2.1-3.3M / L 1.6-2.0M edges/s。Seal(一次性折叠):M 267-398ms,L 4.35-4.40s。
- 参数策展:0.35-0.69s(M)→ 5.4-5.8s(L,含 TEPS 采样的 16 次无界 BFS;策展侧成本,非引擎)。

## 读写混合(Wmixed:8/16 读全速 + 1 写定速 200 批/s,批 50 边;实际达成 136-167 批/s)

| 档-并发 | mixed-read QPS | read p50 | read p99 | read max | write p50 | write p99 |
|---|---|---|---|---|---|---|
| S-w1 | 644 | 39.1µs | 17.4ms | 48.6ms | 94µs | 411µs |
| S-w8 | 364 | 17.1ms | 71.4ms | 102ms | 202µs | 5.9ms |
| M-w1 | 5 | 93.5ms | 1.11s | 1.11s | 120µs | 268µs |
| M-w8(3 次) | 28-31 | 109-130ms | 1.42-1.72s | ~1.45-1.74s | 151-159µs | 301-393µs |
| M-w16 | 52 | 144ms | 1.61s | 1.67s | 170µs | 1.7ms |
| L-w1 | 4 | 89.1ms | 2.86s | 2.86s | 147µs | 270µs |
| L-w8 | 24 | 148.9ms | 2.80s | 2.98s | 186µs | 380µs |

**崩塌签名**:每查询出现 ~50-100ms 的固定开销,与查询族无关(M 档 p50 92µs 的 bfs2 与 p50 4ms 的 bfsall 同时被抬到 ~110-130ms)。10s × ~165 批/s ≈ 1,650 个活跃微层(每 Batch 发布一层);S 档层小故仅 +28µs,L 档底图大故相对倍率低(2.4×),**绝对开销同量级**——成本 ∝ 活跃层数,与查询工作量无关。

## 内存与 GC(阶段边界)

| 档-并发 | 注入后 alloc/sys | 读负载后 alloc/sys | 读阶段 GC 次数/暂停 | 读阶段 GC CPU% | 混合后 alloc |
|---|---|---|---|---|---|
| S-w8 | 6.0M / 11.6M | 28M / **1.02G** | 6,756 / 2.39s | 2.9% | 91M |
| M-w8 | 546M / 731M | 351M / **1.08G** | 3,126 / 629ms | **10.5%** | 422M |
| M-w16 | 598M / 651M | 523M / **1.21G** | 2,899 / 981ms | **14.9%** | 656M |
| L-w8 | 4.81G / 6.42G | 5.68G / 9.04G | 357 / 94ms | **14.7%** | 4.95G |

L 档驻留 ~4.8-5.7GiB(10M 边,约 500B/边含属性/标签)。GC 暂停本身短(均值 <0.3ms),代价在 CPU 占用与 heap sys 膨胀。

## 冷启动(SaveFile→LoadFile)

| 档 | 快照 | 恢复 | 速率 |
|---|---|---|---|
| S | 1.6MiB | 8.9-11.4ms | 2.8-3.6M edges/s |
| M | 49.0MiB | 334-430ms | 2.4-3.0M edges/s(113-142 MiB/s) |
| L | 463.6-463.9MiB | 4.64-4.81s | 2.1-2.2M edges/s(~100 MiB/s) |

## 瓶颈归因(pprof,M-w8 归因运行,数字与无画像运行一致)

### B1(关键):每查询视图构建扫描全部活跃微层

mixed CPU 画像:`Graph.engineViewOf → genView.engineView → nodeWinner` 占 **86.1%**,其中 `runtime.mapaccess2_faststr` 69.4% + `delta.(*Layer).LookupNode` 66.3%(cum)、`internal/runtime/maps.probeSeq.next` flat 34.8%。BFS(43.1%)与 ShortestPath(21.9%)的耗时几乎全部来自该路径。

对照:纯读(sealed,层栈为空)bfsall 的 70.7% flat 在 `traverse.expandLayerParallel.func1`——真正的图遍历,健康。

机制:direct 模式每个 `Batch` 发布一个微层(设计如此);查询入口 `rowsOf/engineViewOf` 需要合并 base+全部活跃层,`nodeWinner` 逐节点按"最新层优先"做**字符串键** map 查找。活跃层数无上界、无直接模式后台合并(writer 包装器才有),层栈随持续写入线性增长,读成本随之线性增长。

当日缓解(不改代码):持续写场景使用 writer 包装器(后台合并 + 写停顿),或周期性 `Seal`。修复方向(v0.8 候选):direct 模式 Batch 流的吸收窗口扩展/层栈上界自动折叠;`nodeWinner` 的整数 NodeID 索引替代字符串键查找。

### B2(高):读路径分配密集 → GC 10-15%

整程累计分配 **1.24TB**(累计 heap 画像,含全部负载)。热点:`NeighborsOf` 343GB、`traverse.ShortestPath` 379GB(含 filledNoDepth 265GB)、`Reachable` 106GB、`expandLayerSerial` 82GB。对照纯读画像:neighbors 负载 41.6% CPU 在 `runtime.lock`(mheap 分配路径),26.4% 在 GC 后台标记。

方向:结果集/前置队列复用或池化;visited 结构预估容量;`filledNoDepth` 的分配消除。

### B3(轻):未批注注入随规模衰减

8.6M(S)→ 2.0M(L) edges/s。仍高于 A4 底线(1M/s)。吸收窗口折叠成本随 pending 集增大。低优先。

## 对标已发布数字(方向性参考,硬件/协议/数据集均不同,不可直接比)

| 系统(来源) | 规模 | 关键数字 | Vectra 对应(本报告) |
|---|---|---|---|
| CozoDB(自测,M1 16GB) | medium 1.63M/30.6M | 导入 150-190K edges/s;点查 ~100K QPS;PageRank(32M)~30s | 导入 1.6-3.3M edges/s(8-20×);点查 46-376K QPS(同量级) |
| FalkorDB(2024 博客) | Pokec,8c/16t | 混合 82/18:p50 55ms / p99 136ms | 混合(165 写/s):M 档 read p50 109ms / p99 1.4s(**差距主因是 B1**) |
| Neo4j(AIMultiple) | ~400K/800K | 峰值 ~1.0K QPS @16 线程;k-hop 14-28ms | M-w16:bfs2 50K QPS、p50 147µs(嵌入式进程内 vs 服务端,量级差预期内) |
| Memgraph(2022) | Pokec,12 客户端 | 32K QPS 1-hop;1-hop 1.09ms | neighbors M-w8:376K QPS、p50 4µs |

参照来源:CozoDB v0.3 性能页、FalkorDB 2024 基准博客、AIMultiple 2026 图数据库基准、Memgraph 2022 对比博客(链接见仓库外调研记录)。

## 复现

```bash
make bench SIZE=small                        # S 档全套(含 mixed/冷启动/内存)
make bench SIZE=medium WORKERS=8 DURATION=10s BENCH_EXTRA='-warmup 1s'
make bench SIZE=large  WORKERS=8 DURATION=10s BENCH_EXTRA='-warmup 1s'
make bench SIZE=medium BENCH_EXTRA='-profile benchdata/profile -warmup 1s'   # 归因运行
go tool pprof -top -cum benchdata/profile/mixed.cpu.pprof
```

## 建议进入 v0.8 backlog(按优先级)

1. **B1**:direct 模式持续写入的层栈治理(自动折叠/吸收扩展)——当前混合负载读 p99 秒级,是发布叙事的最大短板。
2. **B2**:读路径分配削减(GC CPU 10-15% → <5% 目标)。
3. **B3**:注入 fold 成本随规模的衰减(低优先)。
4. 基准套件后续:http 模式对照、WAL-on 混合、真实数据集(Pokec)选项、M 档以外也做重复采样。
