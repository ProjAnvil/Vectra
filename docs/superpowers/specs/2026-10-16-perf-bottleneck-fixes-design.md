# 性能瓶颈修复设计(B1/B2/B3)

Date: 2026-10-16 · 依据: `docs/benchmarks/2026-10-16-perf-baseline.md`(矩阵数据 + pprof 归因)
范围: 引擎核心(direct 模式)的三个已定位瓶颈。不改变公共 API 语义;零锁内核不变量(A8)不变。

> **v4 修订(2026-10-17,实施中发现)**:初稿 F2 的"层数阈值整栈折叠 +
> base/8 体量 Seal"被 A3 验收门证伪(整栈重折在持续写入下二次方;
> 读触发 flush 内联 O(base) Seal 吹爆写后读 p99)。F2 定稿为三段尺寸
> 分层(F/T/tail、写路径专属触发、tail 体量门槛、max(64K,base) 体量线、
> wrapper 跳过);F3(增量视图)从"不做"改为**实施**(层栈变浅不解决
> 记录体量的每发布全量重建),以发布时预构建 + 域分离偷取扩展实现。
> 详见 `docs/benchmarks/2026-10-17-perf-fixes-retest.md` 的修订记录。

## 背景与根因(来自基线报告)

- **B1(关键)**: 持续写入下读崩塌。M 档 165 写批/秒 → 读 p50 92µs→109ms,p99 1.4-1.7s。
  机制: 每发布一层产生新代;首次查询重建 `engineView`(成本 ∝ 未合并记录 × 每端点 O(层数) 字符串查找)。重建 50-100ms > 写间隔 6ms → 逐代缓存永不命中 → 每查询全量重建。pprof: 86% CPU 在 `engineViewOf→nodeWinner`,`mapaccess2_faststr` 69%。wrapper 模式(MaxLayers=2048)存在同款隐患。
- **B2(高)**: 读路径分配 1.24TB/程,GC CPU 10-15%。三大分配点:
  `NeighborsOf` 每调用 `make([]bool, NodeCount())`(M 档 100KB);
  `ShortestPath` 每调用 `make` 两个全图 parent 数组(800KB);
  `BFS` 每调用 `filledNoDepth(NodeCount())`(400KB)。
- **B3(轻)**: 未批注注入 8.6M(S)→2.0M(L) edges/s,仍高于 A4 底线 1M/s。

## B1 设计: direct 模式层栈治理

### F1. `delta.FoldLayers(layers []*Layer) *Layer`

把一段层(同基座)折叠为一层,语义等价:

- 节点: 逐层重放,键的新胜者(含复活)胜出;记录保留**原写入者 ID**。
- 边: 逐层重放;键的墓碑清除该键更早层的存活记录(含平行边),幸存记录保留原 ID 与原相对顺序。
- 墓碑集: **并集全保留**(过保留安全: 对基座不存在的键是无害空杀;折叠时更早记录已被真实清除)。
- labels 与 indexKeys: 并集,保序。
- 结果层携带 `BuildWithIDs` 的全部 ID 结构(nodeIDs/edgeIDs);edgeIDIdx 保持惰性。

不变量: 对任意基座 B 与层栈 L,`view{B, [Fold(L)]}` 与 `view{B, L}` 在所有观察面(邻接行、点查、计数、ID 解析、Cypher、遍历)上不可区分。以随机差分测试对拍钉死。

### F2. 发布路径自动折叠(direct 模式)

`publishLayer` 末尾(写者路径,零新锁):

- 层数触发: `len(layers) ≥ AutoCompactLayers`(默认 32,0 关闭)→ `FoldLayers(全部层)`,以 `{base, [folded]}` 原子换出视图。
- 体量触发: 折叠后若折叠层边数 ≥ `max(AutoSealEdges, base.EdgeCount/AutoSealFactor)`(默认 64K、factor 8)→ 执行与公共 `Seal()` 相同的全折叠(含 WAL checkpoint 行为),界住长跑的未合并体量。

成本模型: M 档混合负载,折叠每 32 批 ≈ 200ms 一次,成本 O(待折记录 ~1600) ≈ 数十 µs,摊销 <0.1%;Seal 每 ~15s 一次 ≈ 300ms,摊销 ~2%。读侧: 重建成本 ≤ O(32 层记录, resolve 深度 ≤32) ≈ 亚毫秒 ≪ 6ms 写间隔,逐代缓存恢复命中。

安全性:

- 折叠是纯内存视图变换。层在发布时已入 WAL;崩溃重放按原始帧重建,折叠等价性保证可见状态一致。
- 镜像(g.nodes/g.edges)与吸收窗口注册表不受影响(折叠不改可见状态)。
- wrapper 模式: 折叠使层栈变浅;`PublishMerge` 的陈旧检测(`len(cur.layers) < len(c.layers)`)已兼容栈收缩(丢弃该次合并,下轮重试)。
- WAL-on: 体量触发走 `Seal()` 既有管线(checkpoint),无新交互。

### F3. 不做的事(YAGNI 记录)

- 增量 engine.View 扩展(跨代复用 sources): 可将重建成本降到 O(新增层),但需改 engine 包与 memo 失效协议;自动折叠已把重建成本压到亚毫秒,收益不抵复杂度。若未来写速率再提一个量级,重新评估。
- `nodeWinner` 整数索引化: 折叠后扫描深度 ≤32,收益变小;不动。

## B2 设计: 读路径分配削减

- **N1**: `NeighborsOf` 去掉全图 `seen`。行内去重: 邻居数 ≤ 64 用结果切片线性查重;更大用 `visitedPool` 位图(traverse 已有 sync.Pool 位图池,internal/traverse 不在 A8 门内)。
- **S1**: `ShortestPath` parent 数组改池化。编码 `parent[v] = u+1`,0 = 无 —— 池的清零语义即初始化,删除 noParent 填充循环。重构路径解码减一。visited 已池化,不动。
- **L1**: `BFS` 的 `Result.depth` 改惰性: 遍历期只写 `Layers`;`Depth()/DepthRaw()` 首次调用时由 Layers 物化(一次性,语义不变)。`filledNoDepth` 逐调用分配消失。`Count()` 由 Layers 求和,不触发物化。
- 预期: 三项合计消除 ~70% 读路径分配(GC CPU 10-15% → 目标 <5%),公共 API 行为不变。

## B3 设计: 注入衰减

先归因再动手: 对 L 档纯注入跑 CPU profile。候选机制: 镜像 map(g.nodes/g.edges 字符串键)在 1M/10M 条目下的缓存未命中与扩容。若确认为镜像成本: 本轮做零风险缓解(注入窗口期间的 map 预热/容量提示,若有效);结构性重构(镜像分片/整数键)记入 backlog。底线: 任何改动不使各档注入速率回退。

## 验收线(以 2026-10-16 基线为对照,M-w8, writerate 200)

| 线 | 基线 | 目标 |
|---|---|---|
| mixed-read p50 | 109ms | ≤ 2× bfs3 纯读 p50(≈1.5ms) |
| mixed-read p99 | 1.42s | ≤ 50ms |
| 读负载 GC CPU | 10.5% | ≤ 5% |
| 各档纯读 QPS/p50 | — | 不回退 >5% |
| 各档注入速率 | — | 不回退 |
| A8(引擎零锁) | PASS | PASS(不变) |
| 全量测试 + race | PASS | PASS |

## 测试计划

1. `delta` 折叠金标: 随机层栈(含墓碑/复活/平行边/折叠再折叠)× 随机基座,折叠前后全观察面对拍(邻接、点查、计数、ID、遍历、Cypher)。
2. 自动折叠触发: 层数/体量阈值单测(含 0 关闭);折叠后写入继续正确(注册表/吸收窗口不受扰)。
3. B2 等价: 惰性 depth、parent 编码、NeighborsOf 去重 —— 既有遍历/查询测试全过 + 新增边界用例 + `testing.AllocsPerRun` 上限断言。
4. 基准复跑: 同矩阵、同种子,M-w8 ×3;对照验收线写复测报告。
