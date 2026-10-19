# Vector 构建吞吐优化报告

- 日期：2026-10-19（深夜会话）
- 分支：`perf/vector-build-throughput`（自 main 2cccdd7）
- 环境：Apple M5 Pro（18 核 / 64GB），单线程构建，纯标量 Go，零第三方依赖与 cgo 约束不变
- 目标：100k×768 单线程 build ≤ 15 分钟（约 110 向量/s）
- 方法：先测量后设计——每项优化均有 profile 证据，每项独立提交，每项过回归门

## 结论

| 指标 | 优化前（main） | 优化后（分支头） | 变化 |
|---|---|---|---|
| 10k×768 构建 | 2m07s（约 80/s） | 23.1s（433/s） | **5.5x** |
| 100k×768 构建 | 1h40m33s（约 17/s） | **19m04s（87.4/s）** | **5.3x** |
| 10k 构建分配量 | 2.77GB | 507MB | 5.5x↓ |
| 768 维 Cosine 内核 | 577 ns | 92 ns | 6.3x |
| recall@10（100k，分布内，ef 32/64/128） | 1.0000 ×3 | 1.0000 ×3 | 门 ✓ |
| 查询 p99（100k，同 efs） | 1.93 / 3.08 / 6.01 ms | **0.80 / 1.24 / 2.37 ms** | 门 ✓（快 58-61%） |
| GC 占用（100k 构建） | 未测 | 0%（gctrace） | — |

**15 分钟目标未单靠纯代码优化达成**（19m04s，差 1.27x）。剩余差距与可选项见下——三个方向级选项（efc 配方 / 4 路合并 / 汇编内核）任一均可补足。

## 热点演化（CPU profile，flat 占比）

| 函数 | 优化前（10k） | 优化后（10k） | 优化后（50k） | 优化后（100k） |
|---|---|---|---|---|
| Distance / dotProduct | 88.5% | 75.8%（dotProduct） | 81.1% | 82.7% |
| selectNeighbors（cum） | 63.6% | 46.0%（85% 来自 pruneTo） | 56.3% | 55.6% |
| searchLayer（cum） | 18.7% | 27.9% | 24.8% | 27.5% |
| mergeSegments（cum，写放大） | 64.2% | 63.9% | 78.0% | 81.7% |
| GC / runtime | ~6% | ~4% | ~4% | ~5% |

构建规模序列（分支头）：10k 23.1s（等效插入 1,142/s）→ 50k 6m26s（596/s）→ 100k 19m04s（488/s）。名义速率与等效速率都随规模单调劣化：一半来自 tier 写放大随 n 增长，一半来自段变大后 300MB 级工作集的随机访问延迟（10k 时可缓存）。

内核提速后，瓶颈从「内核慢」转为「距离调用次数 × 写放大」：两把刀都还在。

## 优化项

### 1. `81465a2` Distance 内核 8 路累加器展开

- 证据：10k profile 中 Distance flat 88.5%。Go 编译器不自动向量化 float 循环；单累加器串行依赖 FADD 延迟（约 4 cycle/元素）。
- 改动：dotProduct 用 8 条独立累加链（768 维 577→92ns）；L2 用 4 条 float64 链；顺带修掉台账遗留缺陷「L2 白算一遍 dot」（distance.go 原 41-44 行）。
- 求和顺序变化引入 ulp 级差异，远低于 ANN 边界（recall 门验证）。
- 效果：10k 构建 118s→28s；`BenchmarkDistance` 进基准（768 维热形状）。
- 附带测试：`TestDistanceLongVectors`（8/15/16/…/768 维对照 float64 参考，覆盖展开主体与所有尾部余数）。

### 2. `05f4f36` 构建路径分配清理

- 证据：10k 构建分配画像 2.77GB 垃圾（索引本体仅 ~30MB）。热点：heap 接口装箱 0.57GB、selectNeighbors/pruneTo 一次性切片 1.3GB、双重 Normalize 0.14GB。100k 写放大 5.59x 下外推 ~180GB GC 压力。
- 改动（全部行为保持）：
  - 手写数组堆替换 `container/heap`（cand 经 `any` 装箱每次 Push 分配）；
  - 堆与选邻 scratch 缓冲挂在 HNSW 上复用（Phase 0 单线程契约；Phase 1 并发化时必须改为每调用状态）；
  - `insertPrepared`：Pack/merge/compact 路径跳过防御性二次拷贝；
  - Normalize：一次除法 + 乘法替换逐元素除法。
- 效果：分配 2.77GB→507MB（5.5x↓）；10k 构建再 -8~9%；100k GC 占用 0%。
- 注意：手写堆的并列距离序与 container/heap 不同，精确并列的图不与旧实现逐位一致；同种子确定性不变（TestHNSWDeterminism 守护）。

### 负结果（同样按证据裁决）

- **向量 arena 布局：已实现、已测、已回退。** 动机是 100k 工作集 300MB 的随机 3KB 读散落（10k→100k 每插入代价劣化 2.3x）。实测 10k -3%、100k 边际贡献 ≤0（21m26s，含本项与项 2；项 2 单独预期 ~20.6min）。结论：访问是延迟受限的图随机模式，地址密度不改变延迟链。代码不保留，判决记录于此。
- **heap 装箱的 CPU 嫌疑不成立**（mallocgc 仅 0.57s/118s），但其分配量成立，随项 2 一并消除。

## 写放大（结构性上限）

tier 级联 pair-wise 重建的精确模拟（按 Pack 级联逐 pack 计算）：

| n | 等效插入数 | 放大 |
|---|---|---|
| 10k | 26,384 | 2.64x |
| 100k | 558,752 | **5.59x** |
| 1M | 8,192,576 | 8.19x |

即 100k 构建时间的 83.6%（100k profile cum）花在合并重建上。纯代码优化不动这一层；这是剩余差距的主要来源。

## 剩余差距与选项（方向级，需裁决）

优化后 100k 单线程 19m04s。距 15 分钟差 1.27x。按投入/收益排序：

1. **efC 配方 200→100**（不改代码，改构建配方）：10k 实测 -23% 构建，recall 三档不降（0.9980/1.0000/1.0000，与 efc=200 完全一致）。外推 100k ≈14.7 分钟，压线达标。hnswlib 生态的标准吞吐旋钮。采用则需一次 100k 复测确认。
2. **tier 4 路合并**（spec §4 修订）：写放大 5.59x→2.97x（-47% 总功），外推 100k ≈10 分钟达标。与 spec 表中「层 i 容量 = 2048·4^i」及代码里未使用的 `tierFactor=4` 吻合（更像补全容量意图），但与同表「层内 2 个合并为 1 个」矛盾，需改 spec 并评估搜索侧段数变多的 p99 影响。
3. **手写 arm64/amd64 汇编内核**（weaviate distancer 先例，Go 汇编非 cgo、不破零依赖）：dotProduct 占 82.7%，FMLA 内核预期 ~4x，外推 100k ≈7-8 分钟达标，查询同步受益。代价是每架构一份 .s + 纯 Go 回退路径的长期维护。
4. **并行建段/fold 流水线**（本 prompt 加分项，Phase 1 folder 设计范畴）：持续写下多 pack 重叠构建；大合并链仍串行，预期 1.5-2.5x，且涉及并发契约（epoch visited 单线程约束）。

单项 2 或 3 达标且有裕量，单项 1 压线；1+2 组合 ≈7.8 分钟且不动内核代码。

## 开源对照（调研摘要）

- [hnswlib](https://github.com/nmslib/hnswlib)：SIMD 距离内核（SSE/AVX 编译开关）+ ef_construction 作为构建速度/质量旋钮 + OpenMP 并行插入。我们对应的纯 Go 手段：多累加链展开（已做）、efc 配方（选项 1）、并行（选项 4）。
- [Weaviate](https://docs.weaviate.io)：纯 Go + 手写汇编距离内核（AVX2/FMA）+ 不可移植架构回退纯 Go——选项 3 的直接先例。
- Pinecone/Zilliz 等资料确认 efConstruction 是公认建图速度杠杆；arXiv 2412.01940（Down with the Hierarchy）讨论层级本身可简化，未采纳。

## 回归门

- `go test ./internal/vector/` 全绿（TestRecallSmoke、TestStateNoPhantomRetention、TestHNSWDeterminism 等全量，多次运行）。
- 100k recall-ef 曲线：ef 32/64/128 全 1.0000，不劣于优化前（基线亦全 1.0000）。
- 查询 p99 优化后反而提升 58-61%（0.80/1.24/2.37ms 对 1.93/3.08/6.01ms；内核同时加速查询路径）；门为「不劣化超过 10%」，远超。
- 零依赖：go.mod 无新增 require；无 cgo。
- 多样性启发式未动（closest-first 禁令保持）。

## 复现

- 内核微基准：`go test ./internal/vector/ -bench BenchmarkDistance -benchtime 2s`
- 构建速率/门：`VECTRA_BUILD_BENCH=1 VECTRA_BUILD_N=<n> VECTRA_BUILD_SWEEP=1 go test ./internal/vector/ -run TestBuildThroughput -v -timeout 0`（协议与 runCell/spike 报告同口径：clustered seed 1000+n、buildState 16/200/999、扰动查询 seed 777）。
- 全矩阵长期入口仍是 `TestSpikeReport`（`VECTRA_SPIKE_FULL=1`）。
- 运行间方差约 ±10%（同机热状态），本文单次读数未做多次平均。
