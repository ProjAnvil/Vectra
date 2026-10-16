# v0.7 实现计划 v2（plan 审计后修订）：delta 微层 + WAL + 聚合

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans。checkbox 跟踪。

**Goal / Architecture / Tech Stack**：同 v0.7 spec rev3（执行前必读：[spec](../specs/2026-10-15-v07-delta-wal-aggregation.md) §2.3 微层不变量、§3.1 结构与 ID/EdgeID 口径、§3.2 分族表、§4 Changeset 嵌入与 checkpoint 边界、§6 API 表、§7 验收线 A1-A8）。

## Global Constraints（v2 修订）

- core（root + internal/*，含新 delta/wal）零第三方依赖、零 goroutine、零 channel、零锁（`internal/writer` 为包装包，允许 goroutine/channel；A8 断言范围 = engine/delta/wal 无 sync.Mutex import）。
- **已发布数据永不就地修改**：writer 物化不可变微层经 atomic.Pointer 发布世代。
- 对外契约按 key 寻址；ID/EdgeID 同世代内稳定、跨 merge 可重排（EdgeID = base 边数棘轮 + 层偏移 + 层内序号）。
- WAL 帧 `[len(56bit)|pad][CRC32C Castagnoli][type u8][JSON(Changeset)]`；core `Changeset{Version,Nodes,Edges,EdgeDeletes,NodeDeletes}` 字段名与 server.Batch 现值逐字一致，server `Batch` 嵌入之（gob 打平，双向兼容旧日志）。
- 保留 `New()`；新增 `Open(path, Options)`。`Mode` 零值 = Off。查询分族语义严格按 spec §3.2。
- 每任务 TDD；全模块绿 + vet 后 commit（conventional，英文）；不变量：props replace-not-mutate、折叠/复活语义、engine.Snapshot 不可变。

---

### Task 1: Cypher 聚合（②，可立即开工，独立）

同 v0.7 计划 v1 的 Task 1 全文（Files/Interfaces/Steps 不变）：parser 聚合项 + `AggCall`、project.go 哈希聚合 breaker、测试清单（函数集/隐式分组/混用报错/`RETURN *`+聚合报错/null 规则/零输入一行/ORDER BY 聚合别名/maxGroups/LIMIT 不短路）。commit `feat(cypher): aggregation ...`。

### Task 2: Changeset 上提 + WAL（③）

**Files:**
- Create: `changeset.go`（root：Changeset/NodeUpsert/EdgeUpsert/EdgeDelete，json tag 全带，字段名=server 现值）、`internal/wal/wal.go` + `wal_test.go`、`options.go`（root：`Options{WALMode Mode; WALDir string}`，`Mode`：`ModeOff=0, ModeAlways, ModeInterval`）、`wal_integration_test.go`（root）
- Modify: `server/batch.go`（Batch 改为 `struct{ vectra.Changeset; Txn *TxnGuard }` 嵌入 + 编译适配）、`server/fsm.go`（decodeBatch 适配）、`server/api.go`（handleTxn 适配）、`vectra.go`（写方法级 op 记录器：WALOn 时逐写方法记录为 Changeset 条目，`g.Batch(fn)` 边界 flush 一帧一次 fsync；`Open(path, opts)` = loadGraph（不自动 Seal）→ wal.Replay 逐帧 apply → Seal 首代【T2 阶段沿用 Seal/genView 于 T3 引入】；`Close()`：WALOn 必 checkpoint——先 Seal 再 SaveFile + manifest + wal.Reset，失败返 err 不删 wal；Closed 后写返回 `ErrClosed`，查询继续合法）

**Steps:** 2.1 wal 单元测试（帧往返/CRC 截断/跨段/salt/幂等/Reset 拒旧帧）→ 2.2 集成测试（Open→写→Close→重开数据在；不 Close 弃用→重放至最后帧；WALOff=v0.6 行为；`Batch` N 写一帧断言；Mode 三档；**manifest 对账：重放首帧 lsn==manifest.lsn+1**）→ 2.3 实现 → 2.4 server 适配 + server 模块全绿（含 raft 日志 decode 兼容测试：旧格式帧可 decode）→ commit `feat(wal): embedded logical WAL + core changeset types (gob-compatible embed)`。

### Task 3a: delta.Layer（不动 Graph）

**Files:** Create `internal/delta/delta.go` + `delta_test.go`
**Interfaces:**
```go
type Layer struct { /* 不可变（物化后 Build() 冻结）*/ }
type Builder struct { /* writer 私有攒批 */ }
func NewBuilder() *Builder
func (b *Builder) UpsertNode/AddEdge/UpsertEdgeFold/DeleteEdge/DeleteNode  // 语义逐字对齐 vectra.go（折叠/复活/级联墓碑，key 寻址）
func (b *Builder) Build() *Layer          // 物化冻结
func (l *Layer) LookupNode(key) (Node, bool)  // 层内新节点/override；base 墓碑由 genView 组合
func (l *Layer) Records() (nodes []NodeRec, edges []EdgeRec, labels []string, indexKeys []string)
```
**Steps:** 3a.1 失败测试：**golden 等价性——先固化**（对同一操作序列，记录现未密封 Graph 的 compact() 输出为 golden，再断言 Builder/Records 逐字段一致）+ 折叠/复活/级联/遮蔽单元。3a.2 实现。3a.3 绿 + commit `feat(delta): immutable micro-layer builder`。

### Task 3b: Graph 换血（微层世代 + 门溶解 + 点查分族）

**Files:**
- Modify: `vectra.go`（`view atomic.Pointer[*genView]`；`genView{base, layers []*delta.Layer, lsn, edgeIDBase}`；写方法走 Builder→Build→世代发布（直接模式）或由 writer 包驱动；**base 实体写经 CSR 行扫描寻址打 key 墓碑**；Save/ExportSnapshot = merge-on-demand 导出；IndexProperty pending；`Seal()`=显式 merge+checkpoint；`Sealed()`=「无未 merge 微层」）、**`traverse.go`/`query.go`/`cypher.go`（root：g.snap/g.sealed 全部换 view.Load()——审计 P1-4）**
- Test: `delta_graph_test.go`、既有测试按 §6 表更新（13 处生产门 + ~15 条断言，5 个文件 + mcp TestQueryBeforeSealIsHelpfulError）

**Steps:** 3b.1 失败测试：写后点查族立即三源可见（NodeByKey/EdgeByID【EdgeID 棘轮】/EdgesOf/LookupBy*【pending 索引 + override 线扫兜底】）；删后不可见；**遍历族（BFS/ShortestPath/Cypher/Reachable/Cycles/NeighborsOf）T4 前一律只读 base（统一 TODO-Task4 标注）**；Close 后写=ErrClosed。3b.2 实现换血。3b.3 全量回归 + commit `feat(delta): micro-layer generations on Graph, sealed gates dissolved`。

### Task 4: View 三源遍历（① 之二）

**Files:** Create `internal/engine/view.go`；Modify `internal/traverse/traverse.go`（本地定义 `RowSource` 接口——**不 import engine**，保持层叠约定：`Rows(u uint32, dir int) []Row` + `NodeCount` 等；BFS/ShortestPath 增 View 入口并存保留 Snapshot 入口）、`internal/cypher/exec.go`（directions 走 View）、root 三文件接 View。
**Interfaces:**
```go
// engine
type DeltaSource interface { // root 适配 delta.Layer 注入
    NodeByKey(key string) (uint32, bool)      // 稠密 ID：base.NodeCount+层偏移+序号
    RowsFrom(id uint32, dir int) []AdjRow      // AdjRow{Target, EdgeID, LabelID uint32}
    TombbedEdge(key EdgeKey) bool; TombbedNode(key string) bool
}
type View struct { Base *Snapshot; Sources []DeltaSource }
func (v *View) Rows(u, dir) []AdjRow
func (v *View) FastAdjacency(dir int) (csr.Adjacency, bool) // layers 全空时 true——traverse 走现有 slice zip 快路径（A1 守护）
```
**Steps:** 4.1 失败测试（写后 BFS/ShortestPath/Cypher 立即见 delta 新边含跨 base-delta 边；墓碑不可达；Reachable 点级回退；Cycles delta 非空先 merge；-race 并行）。4.2 实现 + 快路径。4.3 绿 + commit `feat(view): three-source traversal`。

### Task 5: merge/stall + writer 包装 + Compact

**Files:** Create `internal/writer/writer.go` + `writer_test.go`、root `merge.go`（merge 管线：genView → union Records → engine.Seal → 发布 {newBase,nil}；`Compact()` 公共 API = merge + deep compaction 重排）
**Interfaces:**
```go
type Config struct { ChanCap int /*4096*/; MaxLayers int /*64*/; MergeEdges int /*64K*/; MergeDelay time.Duration /*测试注入*/; FailMerge bool /*测试注入*/ }
func Attach(g *vectra.Graph, cfg Config) *Handle
type Handle struct{ ... }
func (h *Handle) Submit(b vectra.Changeset) error        // 入队等 replyCh；ErrClosed
func (h *Handle) SubmitAsync(b vectra.Changeset) <-chan error
func (h *Handle) Close() error                           // drain → final merge+checkpoint
// stall：len(layers)≥MaxLayers 且 merger 在飞 → writer channel 等待；merger 失败退避重试、连续 3 次回退同步 merge；frozen 等价物=世代快照，保持可读
// 误用：Attach 后非 writer goroutine 裸写 → ErrNotWriter（debug 构建经 runtime.Stack 令牌断言）
```
**Steps:** 5.1 失败测试（-race：并发 Submit + 点查/遍历立见；stall 触发/解除【MergeDelay 注入】；merger 失败重试与同步回退【FailMerge 注入】；Close drain 无丢写；ErrNotWriter；层上限）。5.2 实现 + `debug.SetGCPercent` merge 期临时降 GC 选项（默认关，spec R11）。5.3 绿 + commit `feat(writer): single-writer wrapper, background merge, write-stall, Compact`。

### Task 6: 接入面 + 文档 + 基准

**Files:** Modify `mcp/server.go`（seal→merge+checkpoint 语义、删 write-after-seal Clone、Options 透传）、README×2、`server/` 编译残留；Create `docs/benchmarks/2026-10-15-delta-wal.md`。
**Steps:** 6.1 MCP 适配 + mcp 模块绿。6.2 基准 A1-A8（A2 需 merge-in-flight 采样 harness：MergeDelay 拉长窗口；A5/A6/A7 在指定基准机跑【M5 Pro，≥16GB】并标注环境、CI 跳过；A7 用 TestMain 子进程 re-exec 模拟 kill -9；A8 = engine/delta/wal 无 sync.Mutex import 的静态检查脚本）。6.3 README 双语（持续写语义、分族可见性表、Options/Batch/Close/Compact、ID 口径、内存峰值标注）+ 全模块 `-race` + D2 退路检查（A4/A2/A5）+ commit `feat: continuous-write delta + WAL + aggregation (v0.7)`。

## Self-Review（v2）

- 审计 P1：P1-1→微层设计（spec §2.3 + T3a/3b/T5）；P1-2→T5 Compact()；P1-3→T2 嵌入兼容 + fsm/api 清单；P1-4→T3b 三文件 + 全遍历族临时语义；P1-5→T4 FastAdjacency bypass + Rows 签名定死。P2-6→EdgeID 棘轮；P2-7→DeltaSource 导出 + traverse 本地接口；P2-8→A8 改写；P2-9→Sealed() 语义；P2-10→op 记录器；P2-11→MergeDelay/FailMerge；P2-12→manifest 对账；P2-13→基准环境标注；P2-14→checkpoint 边界。P3-15→删 Watch；P3-16→ModeOff=0；P3-17→保留 New()；P3-18→runtime.Stack 注明；P3-19→golden 先固化；P3-20→internal/writer 定死；P3-21→T2 阶段注释。✔
- 任务拆分：T3 拆 3a/3b，回滚粒度恢复。规模预算：T1 ~0.9K / T2 ~1.3K / T3a ~0.6K / T3b ~1.5K / T4 ~0.9K / T5 ~1K / T6 文档。✔
