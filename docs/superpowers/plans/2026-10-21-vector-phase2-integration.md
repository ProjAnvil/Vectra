# Vectra 向量 Phase 2 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 Phase 2 三项：WAL/store 二进制向量通道（v2 帧 + store v5 + 迁移）、
vectors.vec 森林序列化（消除重启重建窗口）、server REST 向量面 + wrapper 声明
通道 + TS/Python parity。

**Architecture:** 三条相对独立的纵切。P2-1 换帧/快照格式并带迁移式升级；P2-2
在 Seal 批次侧挂森林文件、Open 侧加载+尾部重放；P2-3 扩 query 面（clone 挂载
向量快照、raft 显式声明 ops、REST/客户端）。

**Tech Stack:** Go（零第三方依赖、无 cgo——硬约束）、TypeScript/Python MCP
客户端（既有，不新增依赖）。

**Spec:** `docs/superpowers/specs/2026-10-21-vector-phase2-design.md`（增补；
含 10 条裁定）。母 spec：`2026-10-19-vector-search-design.md`。执行者必须先读
增补的 §1-§3 与 §6 裁定清单。

## Global Constraints

- go.mod 零新增 require；无 cgo；`internal/vector` 不 import 仓库其他包。
- 每任务 TDD：先看测试失败。每任务完成后跑完整门：`go test ./...`（root/
  mcp/server 三模块）+ `go test -race ./internal/vector/ ./internal/writer/ .`
  的向量与并发测试。验收类（bench/）必须单独跑，不与全模块并行。
- 管道会掩盖退出码：提交前用显式 `&&` 链或检查 `echo $?`。
- 英文 commit message 与代码 comment。
- 门 2b 三线不劣化：吞吐 ≥20k/s、普通写 p50 比值 ≤110%、recall@10 ≥0.8x
  基线。
- 多样性启发式与两段式 pack 行为等价（既有 parity 测试守护，不许改参数绕
  过）。
- 版本发布（CHANGELOG + 三语言版本串 + annotated tag + gh release）等用户
  确认；发布前从干净 clone 验证 `go build ./... && go test ./...`。

## Review Focus

1. **float64 来源的向量**（快照重载、JSON 管道）必须走 v2 通道——检测按声明
   不按 Go 类型。测试：Task 3（`[]any(float64)` props 触发 v2 帧）。
2. **`$vec` 字面量冲突**：未声明属性上的 `{"$vec":[...]}` 用户字面量原样保
   留；已声明属性上越界引用 = 硬解码错误。测试：Task 1、Task 2。
3. **降级安全**：迁移后的库对旧读者在快照处硬失败（非静默截断 WAL）；全新
   库从第一个字节就有 v5 绊线。测试：Task 5。
4. **崩溃窗口**：快照写完、vectors.vec 未改名（半写）→ 丢弃走重建，结果正
   确。测试：Task 8。
5. **REST 错误面**：未知索引、维度不符、similar_to 节点无向量、k>100 限幅；
   follower 最终一致。测试：Task 13。
6. **混链重放**：同一 WAL 里 v1 帧与 v2 帧交错（迁移窗口产物）按序重放。测
   试：Task 2、Task 5。

---

## P2-1 WAL 二进制 section + store v5

### Task 1: internal/vecbin 值级 codec

**Files:**
- Create: `internal/vecbin/vecbin.go`
- Test: `internal/vecbin/vecbin_test.go`

**Interfaces:**
- Produces（后续任务依赖的精确签名）:

```go
package vecbin

// PropDecls maps a declared vector prop name to its dimension.
type PropDecls map[string]int

// NumericArray reports v's element count and true when v is []float32 or
// []any whose elements are all float64/float32/int (uniform numeric).
func NumericArray(v any) (dim int, ok bool)

// Hoist copies props, replacing declared vector-prop values that are
// numeric arrays with {"$vec": [off, dim]} refs; the floats append to
// *sec (off is the pre-call len(*sec)). Returns the copy and whether
// anything was hoisted. Input map is never mutated.
func Hoist(props map[string]any, decls PropDecls, sec *[]float32) (map[string]any, bool)

// Restore reverses Hoist: "$vec" refs on declared props resolve against
// sec (bounds-checked, error on violation); everything else passes
// through verbatim - including "$vec"-shaped literals on undeclared
// props.
func Restore(props map[string]any, decls PropDecls, sec []float32) (map[string]any, error)
```

- [ ] **Step 1: 写失败测试** `TestHoistRestoreRoundTrip`（[]float32 与
  []any(float64) 两种来源、多 prop 混合普通属性）、`TestRestoreRefOutOfBand`
  （越界 off/dim → error）、`TestRestoreLiteralDollarVecOnUndeclared`（未声
  明属性的字面量原样保留）、`TestNumericArrayRejectsMixed`（非均匀/非数值 →
  false）。
- [ ] **Step 2: 跑测试确认失败** `go test ./internal/vecbin/ && echo OK`（期
  望：包不存在编译失败）。
- [ ] **Step 3: 实现** 三个函数按 Interfaces 签名。float64→float32 收敛在
  Hoist 落 `sec` 时做（Restore 出来永远是 `[]float32`）。
- [ ] **Step 4: 跑测试通过** `go test ./internal/vecbin/ -count=1 && echo PASS`。
- [ ] **Step 5: Commit** `feat(vecbin): value codec hoisting declared vector props to a float32 section`

### Task 2: WAL v2 帧（类型 4）与混链重放

**Files:**
- Modify: `internal/wal/wal.go`（帧常量、`AppendHoisted`、`scanFrames`、
  `replayLSN` 解码分支）
- Test: `internal/wal/wal_v2_test.go`

**Interfaces:**
- Consumes: `vecbin.PropDecls/Hoist/Restore`（Task 1）。
- Produces:

```go
const frameChangesetV2 = 4

// AppendHoisted writes cs as a frameChangesetV2 frame when any node's
// declared prop hoists (vecbin.Hoist over Nodes), else a plain v1
// frameChangeset frame. The v2 payload envelope is self-describing:
// [4B "VVB2"][u16 nProps][u16 nameLen+name+u16 dim]*[u32 jsonLen]
// [json skeleton][raw float32 LE section].
func (w *WAL) AppendHoisted(cs Changeset, decls vecbin.PropDecls) (uint64, error)
```

- `Frame` 结构不变：解码时已 Restore 完毕，调用方无感。
- [ ] **Step 1: 写失败测试**：
  - `TestAppendHoistedV2RoundTrip`：向量 changeset → AppendHoisted →
    Replay → Changeset 与输入语义相等（向量 prop 为 []float32，其余 prop
    原样）。
  - `TestAppendHoistedFallsBackToV1`：无声明 prop 命中 → 落盘帧 type==1
    （读 Segment 文件字节断言 type 位，或暴露的测试钩子）。
  - `TestMixedV1V2ChainReplay`：v1 帧、v2 帧、v1 帧交错 → 按序重放全对。
  - `TestV2EnvelopeSelfDescribing`：帧内 hint 段记录被 hoist 的 prop 名
    与 dim；伪造越界引用 → Replay 返回 error。
- [ ] **Step 2: 确认失败** `go test ./internal/wal/ -run V2 -count=1; echo $?`。
- [ ] **Step 3: 实现**：帧常量；envelope 编解码（内联在 wal，不导出）；
  `scanFrames` 接受 type 4；`replayLSN` 的 payload 分支按 envelope 拆分后
  `vecbin.Restore` 每 node。hint 段只记实际 hoist 的 prop。
- [ ] **Step 4: 通过** `go test ./internal/wal/ -count=1 && echo PASS`（含
  既有全部测试，证明 v1 路径未破）。
- [ ] **Step 5: Commit** `feat(wal): frameChangesetV2 - self-describing binary vector section`

### Task 3: Graph 写路径选型（声明感知）

**Files:**
- Modify: `vectra.go`（batch 提交处 `wal.Append` 调用点 → 选
  `AppendHoisted`；decls 从 `vecReg()` 现算 `vecbin.PropDecls`）
- Test: `vectra_vector_bin_test.go`（根包）

**Interfaces:**
- Consumes: `AppendHoisted`（Task 2）。
- Produces: 行为——声明了向量索引的图，向量 changeset 落 v2 帧。
- [ ] **Step 1: 写失败测试**：
  - `TestVectorWriteProducesV2Frame`：Open+声明 → Batch 写向量节点 → 关
    闭 → 重开 → `QueryVector` 命中（磁盘 round-trip）且（可选强断言）WAL
    字节含 `"VVB2"`。
  - `TestFloat64SourcedVectorRidesV2`：props 用 `[]any{float64...}`（模拟
    JSON 管道）→ 重开后 QueryVector 命中（Review Focus #1）。
  - `TestPlainGraphStaysV1`：未声明索引的图写普通节点 → WAL 无 v2 帧。
- [ ] **Step 2: 确认失败**。
- [ ] **Step 3: 实现**：提交点选型；`vecbin.PropDecls` 构建helper
  `g.vecPropDecls()`（prop→dim，来自 `vecReg()`）。NormalizeVec 拦截位置
  不动。
- [ ] **Step 4: 通过** `go test . -count=1 && go test -race . -count=1 && echo PASS`。
- [ ] **Step 5: Commit** `feat(vectra): decl-aware v2 frame selection on the write path`

### Task 4: store v5（声明前置 + 二进制向量区）

**Files:**
- Modify: `internal/store/store.go`（`versionV5 = 5`；布局：头|声明|节点|
  边|indexKey|float32 区|CRC；Encode/Decode 走 vecbin）
- Test: `internal/store/store_v5_test.go`

**Interfaces:**
- Consumes: `vecbin`（Task 1）。
- Produces:

```go
// ProbeVersion reads only the header and reports the file's version.
func ProbeVersion(r io.Reader) (uint16, error)
```

- 布局约束（裁定 #2）：声明 section 前移到节点之前——解码节点时已知声明
  以解析 `$vec`。CRC 覆盖声明前置后的全部字节。
- [ ] **Step 1: 写失败测试**：
  - `TestV5RoundTripWithVectors`：带 VectorDecls + 向量 props 的 Data →
    Encode → Decode → 语义相等（向量 prop 为 []float32）。
  - `TestV5HoistsFloat64Props`（Review Focus #1：float64 来源同样进二进
    制区——Encode 前后体积断言，二进制区非空）。
  - `TestV4StillReadable`：手工构造 v4 布局字节（或 golden testdata）→
    Decode 成功。
  - `TestProbeVersion`：v4/v5 头识别；坏 magic → error。
  - `TestUnsupportedVersionFails`：version=6 → "unsupported version"。
- [ ] **Step 2: 确认失败**。
- [ ] **Step 3: 实现**。v5 Encode：先写声明 section，再节点（props 经
  Hoist，共享一个 `sec []float32`），尾部 float 区。
- [ ] **Step 4: 通过** `go test ./internal/store/ -count=1 && echo PASS`。
- [ ] **Step 5: Commit** `feat(store): v5 - declarations first, binary vector section`

### Task 5: Open 迁移 + 全新库绊线 + Seal 写 v5

**Files:**
- Modify: `vectra.go`（Open 迁移分支、Seal/checkpoint 的 SaveFile 走新
  Encode——Encode 内部已按 Data 版本落 v5；fresh 库初始 checkpoint）
- Test: `vectra_migrate_test.go`（根包）、`testdata/legacy-v4/`（golden）
- [ ] **Step 1: 生成 golden**：用当前 HEAD（v0.10，改码前！）跑一个小脚本
  或测试生成 `testdata/legacy-v4/`：v4 快照 + 含向量帧的 WAL（几十个节点即
  可）。提交 golden。此步必须在 Task 4 合并前完成——若已完成 P2-1 前置任务
  则从 git 历史的 v0.10 tag 检出生成。
- [ ] **Step 2: 写失败测试**：
  - `TestMigrateV4LibraryOnOpen`：golden 目录 → Open → 断言磁盘出现 v5 快
    照（ProbeVersion==5）→ 重开 → QueryVector 命中（Review Focus #3）。
  - `TestFreshLibraryHasV5Tripwire`：全新目录 Open → Close → 快照存在且
    v5（裁定 #9）。
  - `TestMigrationIdempotent`：连续两次 Open，第二次不再触发全量 seal 的
    可观察行为（快照 mtime/LSN 不变语义）。
  - `TestMigratedLibRejectedByV4Decode`：迁移产物快照 → 旧版
    Decode 路径（用 `ProbeVersion` 断言 + 版本分派测试）硬失败。
- [ ] **Step 3: 确认失败**。
- [ ] **Step 4: 实现**：Open 后置钩子——快照版本 <5 或库为全新 →
  `g.Seal()` 一次。注意 Seal 在 Open 末尾、写者附属前运行（无并发）。
- [ ] **Step 5: 通过** `go test . -count=1 && echo PASS`。
- [ ] **Step 6: Commit** `feat(vectra): migration-on-open to v5 with fresh-library tripwire`

### Task 6: P2-1 门（基准 + 回归）

- [ ] **Step 1: 全模块** `go test ./... -count=1 && go test -race ./internal/vector/ ./internal/writer/ . -count=1 && echo PASS`。
- [ ] **Step 2: 门 2b 单独跑** `VECTRA_GATE2B=1 go test ./bench/ -run TestVectorGate2B -v -timeout 4h`；三线不劣化；记录批边界 p50（对比 21ms 基线）。
- [ ] **Step 3: A.2 复测单独跑** `VECTRA_GATE_CP=1 go test ./bench/ -run TestVectorWrapperCheckpoint -v -count=1 -timeout 1h`；seal 次数应降至 ~1/2.4。
- [ ] **Step 4: 数据落档** `docs/benchmarks/2026-10-21-vector-phase2-p21.md`（数值 + 复现命令）。
- [ ] **Step 5: Commit** `bench(vector): P2-1 gates - boundary p50 and checkpoint cadence after binary frames`

---

## P2-2 vectors.vec 森林序列化

### Task 7: internal/vector State.Encode/Decode

**Files:**
- Create: `internal/vector/serialize.go`
- Test: `internal/vector/serialize_test.go`

**Interfaces:**
- Produces:

```go
// Encode writes s's full state: config (metric/dim/m/efc/seed), vid
// space (nextVID, vidOf), deadBits, overlay entries, and the tiered
// segment forest (per segment: tier, seq, HNSW ids/vecs/level/links/
// entry). Format: magic "VCS1" | fields... | CRC32 trailer. rng state
// and build scratch are not persisted (segments are immutable).
func (s *State) Encode(w io.Writer) error

// DecodeState restores what Encode wrote; bad magic/version/CRC = error.
func DecodeState(r io.Reader) (*State, error)
```

- 包内自洽：不 import 仓库其他包（spike 纪律）。
- [ ] **Step 1: 写失败测试**：
  - `TestStateEncodeDecodeIdentity`（裁定 #4 正确性线）：建 State（含已
    pack 的 segments + 半满 overlay tail + deadBits 非空）→ 记录一组查询
    的 hits → Encode → DecodeState → 新 State 同查询 hits **逐位相同**。
  - `TestDecodeRejectsCorruption`：翻转一字节 → error。
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（binary.LittleEndian 手写，
  不引依赖）→ **Step 4: 通过**
  `go test ./internal/vector/ -count=1 && go test -race ./internal/vector/ -run Encode -count=1 && echo PASS`。
- [ ] **Step 5: Commit** `feat(vector): State forest serialization with CRC`

### Task 8: Seal 写入 + Open 加载 + 尾部重放

**Files:**
- Modify: `vectra.go`（`checkpoint()`：快照+manifest 后写 `vectors.vec`，
  temp+fsync+rename；无索引时删除遗留文件）
- Modify: `vectra_vector.go`（加载分支：文件头 LSN==manifest LSN 且声明集
  合全同 → 挂载 State 并**跳过快照 props 摄入**（`vectra_vector.go:273` 的
  加载侧循环）；否则删文件走现有重建。WAL 尾部照常 `vecIntakeNode`。）
- Test: `vectra_forest_test.go`（根包）
- [ ] **Step 1: 写失败测试**：
  - `TestForestRestartIdentity`：建库+声明+写向量+drain 至收敛 → 记录
    QueryVector hits → Close → Open → hits 相同，且走的是加载路径——用
    `export_test.go` 暴露 `func (g *Graph) VecForestLoadedForTest() bool`
    断言为 true（重建路径为 false）。
  - `TestForestTailReplay`：森林 LSN=X 后 WAL 尾再写向量（含一次删除）→
    重开 → 与"全量重建"路径的 live 向量集合逐 key 相同（裁定 #3）。
  - `TestForestDiscardClasses`：LSN 错位（改 manifest）、CRC 坏、声明不一
    致（换 dim 重声明）三类 → 各自走重建且结果正确（Review Focus #4 半写
    场景含在内）。
- [ ] **Step 2: 确认失败** → **Step 3: 实现** → **Step 4: 通过**
  `go test . -count=1 && go test -race . -count=1 && echo PASS`。
- [ ] **Step 5: Commit** `feat(vectra): vectors.vec - forest persistence with watermark load gate`

### Task 9: P2-2 门

- [ ] **Step 1: 全模块 + race（同 Task 6 Step 1）**。
- [ ] **Step 2: 重启对比基准**：100k×768 建库收敛 → 关闭 → 计时 Open（加
  载路径）vs 删除 vectors.vec 后计时 Open（重建路径）。落
  `docs/benchmarks/2026-10-21-vector-phase2-p22.md`。
- [ ] **Step 3: 门 2b 单独跑**（不劣化）。
- [ ] **Step 4: Commit** `bench(vector): P2-2 gate - restart load vs rebuild at 100k`

---

## P2-3 REST + wrapper 声明 + TS/Python

### Task 10: Clone 挂载向量快照

**Files:**
- Modify: `vectra.go`（`Clone()`：复制向量注册表声明 + 各索引当前不可变
  `Snapshot` 头，只读挂载——裁定 #5）
- Test: `vectra_clone_vec_test.go`
- [ ] **Step 1: 写失败测试** `TestCloneCarriesVectorState`：源图建索引+写向
  量+收敛 → Clone → 两者 `QueryVector` hits 相同、`VectorIndexes()` 相
  等；clone 无写者（后续源图再写不影响 clone 结果快照）。
- [ ] **Step 2-4: 红 → 实现 → 绿**（`go test . -count=1 && go test -race . -count=1`）。
- [ ] **Step 5: Commit** `feat(vectra): Clone mounts read-only vector snapshots`

### Task 11: writer Handle 声明通道

**Files:**
- Modify: `internal/writer/writer.go`（请求 kind 扩展：声明/删除与
  changeset 同通道，writer goroutine 的 batch 内调用 Graph API——裁定 #8）
- Test: `internal/writer/writer_decl_test.go`
- [ ] **Step 1: 写失败测试**：
  - `TestDeclareViaHandle`：Attach → `h.DeclareVectorIndex(decl)` →
    `h.Submit`(向量 changeset) → drain → `QueryVector` 命中。
  - `TestDeclareDropOrdering`：声明 → 写入 → `h.DropVectorIndex` → 后续
    写入不再摄入（QueryVector 返回 ErrVectorIndexNotFound）。
  - `-race` 下并发 Submit 与 Declare 不死锁。
- [ ] **Step 2-4: 红 → 实现 → 绿**（`go test ./internal/writer/ -count=1
  && go test -race ./internal/writer/ -count=1`）。
- [ ] **Step 5: Commit** `feat(writer): Handle declare/drop vector index through the writer lane`

### Task 12: raft Batch 显式声明 ops

**Files:**
- Modify: `server/batch.go`（`Batch` 增 `VectorIndexOps`；gob add-only）、
  `server/fsm.go`（Apply 按序调 Create/Drop）
- Test: `server/batch_vec_test.go`
- [ ] **Step 1: 写失败测试**：
  - `TestVectorIndexOpsApply`：Batch{VectorIndexOps:[Create, 写入,
    Drop]} → fsm apply → 声明可见/删除生效。
  - `TestBatchGobLegacyRoundtrip` 扩展：新字段对旧 gob 字节双向兼容
    （nil ops = 无变化；旧编码无该字段 → nil）。
- [ ] **Step 2-4: 红 → 实现 → 绿**（`go test ./server/ -count=1`）。
- [ ] **Step 5: Commit** `feat(server): explicit vector index ops in raft Batch`

### Task 13: REST 端点

**Files:**
- Modify: `server/api.go`（四端点：`POST /v1/vector/search`、`POST /v1/
  vector/indexes`、`GET /v1/vector/indexes`、`DELETE /v1/vector/indexes/
  {name}`）
- Test: `server/api_vector_test.go`
- [ ] **Step 1: 写失败测试**（Review Focus #5）：leader 声明→写入→查询
  round-trip；follower 最终一致收敛（await 轮询）；未知索引 / 维度不符 /
  similar_to 无向量 / k=101 限幅 各错误面；DELETE 后查询 404/未知索引。
- [ ] **Step 2-4: 红 → 实现 → 绿**（`go test ./server/ -count=1`）。
- [ ] **Step 5: Commit** `feat(server): vector search and index REST endpoints`

### Task 14: TS/Python 客户端工具

**Files:**
- Modify: `mcp/ts/src/index.ts`、`mcp/python/vectra_mcp/*`、`mcp/README.md`
  （三语言工具对照表）
- Test: `mcp/ts`（vitest/tsc 既有管线）、`mcp/python`（pytest 既有管线）
- [ ] **Step 1: 写失败测试**：四工具（`vector_search`、`vector_index_
  create`、`vector_index_drop`、`vector_index_list`）的请求形状与响应映
  射；`vector_search` 描述含"近似"（approximate）字样。
- [ ] **Step 2-4: 红 → 实现 → 绿**（两语言各自测试命令）。
- [ ] **Step 5: Commit** `feat(mcp): vector tools for TS and Python clients`

### Task 15: 终审与发布准备

- [ ] **Step 1: 全模块 + race 全绿（显式 && 链）**。
- [ ] **Step 2: 门 2b + A.2 复测单独跑，数值与 Task 6/9 记录一致（无回
  弹）**。
- [ ] **Step 3: 全分支终审**（fresh reviewer subagent：安全性/并发/语义对
  spec；重点：迁移幂等、混链、clone 快照生命周期）。
- [ ] **Step 4: 干净 clone 验证** `git clone --no-hardlinks . /tmp/vectra-verify && cd /tmp/vectra-verify && go build ./... && go test ./... && echo VERIFIED`。
- [ ] **Step 5: CHANGELOG + 三语言版本串 bump（minor）+ 母 spec 偏差记录
  回写**。CHANGELOG 必含三条：迁移行为（Open 即 checkpoint 到 v5）、"迁
  移未完成的库禁止降级"、server 混合版本不支持 + 升级 runbook（同步进
  server README）。tag 与 gh release 等用户确认。

---

## 执行说明

- 任务 1-6 顺序执行（接口链）；7-9 在 6 之后；10-14 可与 7-9 并行（不同文
  件面）；15 收尾。
- Task 5 Step 1 的 golden 生成有先后依赖（必须用 v0.10 代码生成）——若执行
  时 P2-1 已部分合并，从 git tag v0.10.0 检出临时 worktree 生成。
- 每任务的"完整门"指 Global Constraints 第 2 条；本计划中的门命令均给出。
