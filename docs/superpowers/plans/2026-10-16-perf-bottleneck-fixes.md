# 性能瓶颈修复实施计划(B1/B2/B3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复基线报告(2026-10-16)定位的三个瓶颈:B1 持续写入下读崩塌、B2 读路径分配密集、B3 注入随规模衰减。

**Architecture:** B1 = `delta.FoldLayers`(层栈→单层,语义等价)+ 发布路径自动折叠(层数阈值)+ 体量自动 Seal(复用 forceMerge);全部写者路径、不可变结构 + 原子换视图、零新锁。B2 = 三个分配点的外科修除(全图 seen/parent 数组/depth 数组)。B3 = 先归因后决策。

**Tech Stack:** Go 1.27,标准库;vectra 根包 + internal/delta + internal/traverse。

**Spec:** `docs/superpowers/specs/2026-10-16-perf-bottleneck-fixes-design.md`

## Global Constraints

- 不改变公共 API 语义(BFSResult.Depth/Reached/Count/DepthsMap、NeighborsOf、ShortestPath 返回行为不变)。
- A8 不变量:internal/engine、internal/delta、internal/wal 不得引入 `sync`(import 或 Mutex);internal/traverse 已有 sync.Pool,不受限。
- 所有既有测试(`make test` 三模块 + `bench/` 验收门)必须全过;race 版本全过。
- 验收线(spec):M-w8 mixed-read p50 ≤ 1.5ms、p99 ≤ 50ms;读负载 GC CPU ≤ 5%;纯读与注入不回退 >5%。
- 提交信息英文,逐任务提交。

---

### Task 1: `delta.FoldLayers` —— 层栈折叠原语

**Files:**
- Create: `internal/delta/fold.go`
- Test: `internal/delta/fold_test.go`

**Interfaces:**
- Produces: `func FoldLayers(layers []*Layer) *Layer` —— oldest-first 层栈 → 单层;nil 输入返回 nil;单层输入原样返回。结果层携带全部 ID 结构(nodeIDs/nodeIDIdx/edgeIDs),edgeIDIdx 保持惰性(零值)。

- [ ] **Step 1: 写失败测试(不变量 + 等价)**

```go
package delta

// 随机层栈:UpsertNode(含删除后复活)/ AddEdge 平行边 / UpsertEdgeFold /
// DeleteEdge / DeleteNode(基座键经由 nodeTomb 表达)——折叠后对拍:
// 1) 逐键节点胜者(rec 字段 + ID)与逐键边存活槽位(rec + ID 序列)一致
// 2) 墓碑集为输入并集的子集(允许折叠消除"被同键后写覆盖"的等价场景不存在——本实现全保留,故断言相等)
// 3) 折叠再折叠幂等
// 4) ID 保留:折叠层每条记录的 ID 等于原层栈该键最终胜者/幸存者的 ID
func TestFoldLayersEquivalence(t *testing.T) {
	// 构造:两个 Builder 层 + 一个墓碑层,覆盖:复活、平行边、跨层 DeleteEdge、
	// 同键跨层 AddEdge。手写固定用例(确定性),断言逐字段相等。
}
func TestFoldLayersRandomDifferential(t *testing.T) {
	// rand 新种子 × 200 轮:随机层栈;用与上面相同的重放语义在测试内
	// 独立计算期望(键→最终节点状态、键→幸存边槽序列),对拍 FoldLayers 输出。
}
func TestFoldLayersIdempotent(t *testing.T) { /* fold(fold(L)) == fold(L) 字段级 */ }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/delta/ -run TestFoldLayers -v`
Expected: FAIL(未定义 FoldLayers)

- [ ] **Step 3: 实现 fold.go**

```go
package delta

// FoldLayers folds one stack of micro-layers (oldest first, same base) into a
// single layer with identical composition semantics:
//   - node winner per key: newest live record wins, keeping its writer-assigned
//     ID; a key whose final state is dead contributes no record but its
//     tombstone survives (it must keep shadowing the base);
//   - edge survivors: a later layer's EdgeTombstone drops every older-layer
//     record of that key (parallels wholesale); surviving records keep their
//     IDs and relative per-key order; global emission order is the original
//     stack order restricted to survivors;
//   - tombstone sets are the full union (over-approximation is safe: a
//     tombstone on a key the base lacks kills nothing);
//   - labels and indexKeys: first-seen union.
// The input layers are not modified. A stack of ID-less layers folds to an
// ID-less layer. FoldLayers is pure: no locks, no shared state.
func FoldLayers(layers []*Layer) *Layer {
	if len(layers) == 0 {
		return nil
	}
	if len(layers) == 1 {
		return layers[0]
	}
	type nodeWin struct {
		rec  NodeRec
		id   uint32
		dead bool
	}
	type edgeSlot struct {
		rec    EdgeRec
		id     uint32
		live   bool // still surviving at the end
	}
	nodes := make(map[string]*nodeWin)
	var nodeOrder []string
	edges := make(map[EdgeKey][]*edgeSlot)
	var edgeFlat []*edgeSlot // global original order
	f := &Layer{
		nodeTomb: make(map[string]bool),
		edgeTomb: make(map[EdgeKey]bool),
	}
	for _, l := range layers {
		for k := range l.nodeTomb {
			f.nodeTomb[k] = true
			if w := nodes[k]; w != nil {
				w.dead = true
			}
		}
		for j, rec := range l.Nodes() {
			w := nodes[rec.Key]
			if w == nil {
				w = &nodeWin{}
				nodes[rec.Key] = w
				nodeOrder = append(nodeOrder, rec.Key)
			}
			w.rec, w.dead = rec, false
			if id, ok := l.LookupNodeID(rec.Key); ok {
				w.id = id
			}
		}
		for k := range l.EdgeTombstones() {
			f.edgeTomb[k] = true
			for _, s := range edges[k] {
				s.live = false
			}
		}
		for j, rec := range l.Edges() {
			s := &edgeSlot{rec: rec, live: true}
			if l.edgeIDs != nil {
				s.id = l.EdgeIDAt(j)
			}
			edges[delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}] = append(edges[...], s)
			edgeFlat = append(edgeFlat, s)
		}
		// labels/indexKeys first-seen union(见 Step 3 实现内 dedup 循环)
	}
	// 发射:节点按 nodeOrder 过滤 dead;边按 edgeFlat 过滤 live;
	// 构建 l.nodes/l.nodeIdx/l.nodeIDs/l.nodeIDIdx/l.edges/l.edgeIDs。
	// (完整代码在实现时按上述骨架落盘;edgeFlat 的 key 计算注意 EdgeKey 构造)
	return f
}
```

(实现时补全发射段与 labels/indexKeys 并集;函数放 fold.go,单测覆盖上述四类断言。)

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./internal/delta/ -v`
Expected: PASS(含既有 delta 测试)

- [ ] **Step 5: A8 门检查 + 提交**

```bash
grep -rn -E '"sync"|sync\.Mutex|sync\.RWMutex' internal/engine internal/delta internal/wal --include='*.go' | grep -v _test.go
git add internal/delta/fold.go internal/delta/fold_test.go
git commit -m "feat(delta): FoldLayers - semantics-preserving layer-stack compaction primitive"
```

---

### Task 2: direct 模式层数自动折叠

**Files:**
- Modify: `vectra.go`(Graph 字段 + New/Load/Open 初始化 + cutLayer/Batch 末尾挂钩)
- Test: `fold_auto_test.go`(根包,新建)

**Interfaces:**
- Consumes: `delta.FoldLayers`
- Produces: `func (g *Graph) SetAutoCompact(maxLayers int) *Graph`(链式;0 关闭;写前调用);默认值 32(常量 `defaultAutoCompactLayers`)。

- [ ] **Step 1: 写失败测试(开/关差分等价)**

```go
// 同一随机操作流(含 UpsertEdge 改属性、新边、DeleteEdge、DeleteNode、复活)
// 分批 Batch 到两个图:gOn=SetAutoCompact(4),gOff=SetAutoCompact(0)。
// 每批后对拍:NodeCount/EdgeCount;抽样键的 NodeByKey 字段;EdgesOf(排序后);
// 全起点 BFS 层集合(排序);Reachable 集合;ShortestPath 可达性与长度;
// LookupByProperty(建过索引时)。最终各 Seal 一次后再对拍。
func TestAutoCompactDifferentialEquivalence(t *testing.T) { /* 200 批随机流 */ }

// 触发单测:threshold=4,连发 6 个 Batch,断言 view 层数 ≤ 4(经 g.LayerPressure());
// 关闭时不折叠。折叠后继续写入与点查正确(端点注册表不受扰)。
func TestAutoCompactTriggerAndContinue(t *testing.T) { ... }

// WAL 重放安全:WALMode Always + SetAutoCompact(2) + 批量写 + 折叠发生 + Close;
// 重开 Open 后计数与关键查询 == 内存图。折叠是视图变换,WAL 帧未动。
func TestAutoCompactWALReplay(t *testing.T) { ... }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test . -run TestAutoCompact -v` — FAIL(SetAutoCompact 未定义)

- [ ] **Step 3: 实现**

- Graph 增字段 `autoCompactLayers int`;`New`/`Load`/`LoadFile`/`Open` 初始化为 `defaultAutoCompactLayers = 32`。
- `SetAutoCompact(n int) *Graph`:写者路径(写前调用;实现只设字段)。
- 新私有方法:

```go
// maybeAutoCompact folds the published layer stack down to one layer when it
// has reached the layer-count cap. Writer-path only (callers: cutLayer and
// Batch's end); pure view swap — mirrors, absorption-window registry and the
// builder are untouched (the fold does not change visible state). g.merging
// guards forceMerge's internal publishLayer path from recursing here.
func (g *Graph) maybeAutoCompact() {
	if g.autoCompactLayers <= 0 || g.merging {
		return
	}
	v := g.view.Load()
	if len(v.layers) < g.autoCompactLayers {
		return
	}
	folded := delta.FoldLayers(v.layers)
	if folded == nil {
		return
	}
	g.view.Store(&genView{
		base: v.base, baseKeys: v.baseKeys, layers: []*delta.Layer{folded},
		lsn: v.lsn, nodes: v.nodes, edges: v.edges,
	})
}
```

- `cutLayer()` 末尾(发布后)与 `g.Batch` 成功路径末尾各加 `g.maybeAutoCompact()`。
- `forceMerge()` 开头设 `g.merging = true`(defer 复位)防重入。
- viewCache 失效:折叠换出 genView 后,旧缓存 entry 的 gen 指针不等,自然 miss —— 无需显式清理(确认 engineViewOf 的判等逻辑,已是指针判等)。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test . -run TestAutoCompact -v && go test . -count=1` — PASS(全根包)

- [ ] **Step 5: 提交**

```bash
git add vectra.go fold_auto_test.go
git commit -m "feat(core): direct-mode auto-compaction bounds the unmerged layer stack (B1)"
```

---

### Task 3: 体量自动 Seal

**Files:**
- Modify: `vectra.go`(maybeAutoCompact 扩展 + `SetAutoSealEdges`)
- Test: `fold_auto_test.go`(追加)

**Interfaces:**
- Produces: `func (g *Graph) SetAutoSealEdges(minEdges int) *Graph`(0 关闭;默认 `defaultAutoSealEdges = 65536`);触发条件 `folded.EdgeCount() ≥ max(minEdges, base.EdgeCount()/8)`。

- [ ] **Step 1: 写失败测试**:小阈值(如 200)+ 阈值化写入 → 断言某时刻 `g.LayerPressure()` 归零(层栈被 Seal 清空)、计数不变、后续写入查询正确;`SetAutoSealEdges(0)` 不触发。
- [ ] **Step 2: 确认失败** — `go test . -run TestAutoSeal -v`
- [ ] **Step 3: 实现**:maybeAutoCompact 折叠后追加:

```go
	if folded.EdgeCount() >= max(g.autoSealEdges, v.base.EdgeCount()/autoSealFactor) && g.autoSealEdges > 0 {
		_ = g.forceMerge() // full fold into a fresh base; WAL checkpoint stays with explicit Seal (frames already persisted)
	}
```

(forceMerge 错误传播:发布路径已发布的层不丢失,记 debug 日志或直接忽略——实现时按 forceMerge 既有错误处理惯例,公共 Seal 才返回。)
- [ ] **Step 4: 全量根包测试通过 + 提交** — `feat(core): volume-proportional auto-seal bounds unmerged records (B1)`

---

### Task 4: B2-N1 `NeighborsOf` 去重分配消除

**Files:**
- Modify: `query.go`(NeighborsOf 两个分支)
- Test: `query_alloc_test.go`(新建)

- [ ] **Step 1: 失败测试**:密封图(300 节点/平均度 8)上 `testing.AllocsPerRun`(先预热一轮)断言 `NeighborsOf` ≤ (结果数 + 4) 次分配(现状:额外一次 NodeCount 大小 `[]bool`);同图行为等价(与旧实现输出对拍——用小图手写期望)。
- [ ] **Step 2: 确认失败** — 现状分配数超界。
- [ ] **Step 3: 实现**:删除 `seen := make([]bool, src.NodeCount())`;改为:

```go
	seenSmall := make(map[uint32]struct{}, 16) // 仅当行数 > 64 时才换位图
	// FastAdjacency 分支:for 循环内 len(tg) <= 64 用 out 线性查重(嵌套小循环,
	// 上界 64×结果数);len(tg) > 64 用 visitedPool 位图(traverse 导出
	// AcquireVisited/ReleaseVisited 或在 root 复制一份小池——采用导出,见 Interfaces)。
```

实现选择:internal/traverse 导出 `tr.AcquireVisited(words int) []uint32` / `tr.ReleaseVisited(b []uint32)`(包一层 visitedPool);两分支同改。
- [ ] **Step 4: 测试通过 + `go test . -count=1` + 提交** — `perf(query): NeighborsOf drops whole-graph dedup allocation (B2)`

---

### Task 5: B2-S1 `ShortestPath` parent 池化(+1 编码)

**Files:**
- Modify: `internal/traverse/traverse.go`(ShortestPath/expandMeeting/expandMeetingRows/reconstructCSR/reconstructRows)
- Test: `internal/traverse/shortest_alloc_test.go` 或并入既有测试文件

- [ ] **Step 1: 失败测试**:既有 ShortestPath 行为测试保持;新增 AllocsPerRun(小图)≤ 常数(如 10);新增路径正确性用例(从非零 ID 起点编码减一解码;双向相遇)。
- [ ] **Step 2: 确认失败**。
- [ ] **Step 3: 实现**:

```go
	// parent[v] = u+1;0 = 无父(池的清零即初始化,noParent 填充循环删除)
	fParent := parentPool.acquire(n) // 新池:uint32 数组,acquire 清零;与 visitedPool 同构
	bParent := parentPool.acquire(n)
	defer parentPool.release(fParent); defer parentPool.release(bParent)
	// expandMeeting*: parent[v] = u + 1
	// reconstruct*: for p := parent[meet]; p != 0; p = parent[p-1] { ... use p-1 ... }
```

- [ ] **Step 4: 测试通过 + 提交** — `perf(traverse): ShortestPath pools parent arrays, +1 encoding (B2)`

---

### Task 6: B2-L1 BFS 惰性 depth

**Files:**
- Modify: `internal/traverse/traverse.go`(BFS:去掉逐调用 filledNoDepth 与 depth 写入;Result 增惰性物化)
- Modify: `traverse.go`(根,`BFSResult`:depth 惰性 + Count 改由 Layers 求和)
- Test: 既有 BFS/遍历测试 + 新增 AllocsPerRun

- [ ] **Step 1: 失败测试**:`tr.BFS` 小图 AllocsPerRun 断言不含 NodeCount 大小分配(上限 ~层数+4);`BFSResult.Depth/Reached/DepthsMap/Count` 语义用既有测试(必须全过)+ 边界:未到达 id 的 Depth == noDepth、Count 与 Layers 一致。
- [ ] **Step 2: 确认失败**。
- [ ] **Step 3: 实现**:

```go
// traverse.Result:
//   depth 字段仅在 DepthMaterialized() 首次调用时物化(双层填充 + Layers 盖章),
//   之后缓存;DepthRaw() 保留但改为触发物化(facade 是唯一调用方)。
//   BFS 本体不再写 res.depth(删除 filledNoDepth 调用与逐层盖章循环;
//   起点检越界分支返回 &Result{} —— depth 为 nil,物化时按 n=src.NodeCount() 处理,
//   该分支改为返回 Result{Layers: nil} + 调用方处理,见实现)。
// 根 BFSResult:
//   type BFSResult struct { Layers [][]NodeID; ...; n int; depth []uint32; depthDone bool }
//   Count() = Σ len(Layers)(不物化)
//   Depth/Reached/DepthsMap → ensureDepth()(n 已在 facade 存入)
```

注意:traverse.Reachable / facade ReachableIDs 若用 Result.depth,一并适配(实现时 grep depth 用户:仅 facade)。Cycles/expandMeeting 不受影响。
- [ ] **Step 4: 全部测试 + race 根包 + 提交** — `perf(traverse): lazy BFS depth materialization (B2)`

---

### Task 7: B3 注入衰减归因与处置

**Files:**
- 无源码预设;产出:profile 归因 + (若有可行动项)对应小修;报告段落。

- [ ] **Step 1**: 对 L 档注入跑 CPU profile:

```bash
go test -run XXX -bench . -cpuprofile ... # 或临时 main:10M AddEdge 循环
go tool pprof -top -cum
```

- [ ] **Step 2**: 判据:若热点为 g.nodes/g.edges 镜像 map(字符串键、10M 条目缓存未命中)→ 记录为结构性成本(镜像分片/整数键入 backlog),不做大重构;若热点为可修的小项(lift 预算、重复查找等)→ 实现修复并验证不回退。
- [ ] **Step 3**: 结论写入复测报告(B3 段);若有修复,独立提交。

---

### Task 8: 全量验证 + 基准复跑 + 报告

- [ ] **Step 1**: `make test`(三模块)+ `go test -race ./...`(根)+ bench 验收门:

```bash
go test -run 'TestQueryLatencyDuringMerge|TestPointQueryAfterWrite|TestWALGroupCommitBatchCost|TestMergeAmortization|TestCrashRecoveryTimeToReady' -v ./bench/
```

- [ ] **Step 2**: 基准复跑(spec 验收线,同种子同矩阵关键格):

```bash
# server/
run medium 8 ×3;run medium 1;run medium 16;run small 8;run large 8;+ profile 运行归因
```

- [ ] **Step 3**: `docs/benchmarks/2026-10-16-perf-fixes-retest.md`:验收线对照表(修复前后)、残余差距、B3 结论;更新 DESIGN.md(自动折叠/自动 Seal 语义)。
- [ ] **Step 4**: 提交 + 更新自动记忆。

---

## Self-Review

1. **Spec 覆盖**:B1=F1(T1)/F2(T2,T3);B2=N1(T4)/S1(T5)/L1(T6);B3=T7;验收=T8。F3(不做的事)无需任务。✓
2. **占位符**:T1 Step 3 骨架含"实现时补全"——为诚实标注,核心语义(重放规则、ID 保留、发射顺序)已完整给出;其余任务代码完整。可接受。
3. **类型一致**:FoldLayers 签名在 T1/T2 一致;SetAutoCompact/SetAutoSealEdges 命名统一;traverse 导出名 AcquireVisited/ReleaseVisited 在 T4 前后一致。✓
