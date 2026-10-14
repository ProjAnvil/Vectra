# v0.5.0: TxnGroup 组提交实现计划

> **状态：WITHDRAWN（2026-10-14）**——Task 0 闸门触发（logsPerBatch 均值 5.5）+ Task 0b 重诊（spec §4.2）：验收线在本硬件算术不可达，机制结构性冗余。Task 1-7 永不派发；本计划保留作历史记录。继任计划：`2026-10-14-ryw-await.md`。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** leader 侧 batcher 把并发 `/v1/txn` 攒成一条 raft 日志（多个子事务，一次 fsync），S 档 16w 提交 QPS ≥ v0.4.0 的 5×（≥2,700/s），单请求延迟无回归（p50 增量 <1ms）。

**Architecture:** Batch v4 新增 `Group []TxnItem`（一条日志多事务）；版本表值改 composite `(idx<<8)|slot`（VCTX fmtVer 2，v1 快照 ×256 迁移）；fsm.Apply 按槽位序逐 item 验证/应用（等价逐条提交）；新组件 `txnBatcher`（自适应窗口 2ms、256 项/4MB 双上限、有界队列 4096、per-slot future 路由）。单事务路径与 `/v1/batch` 行为不变。

**Tech Stack:** Go stdlib + 既有依赖。根模块与 internal/ 零改动。

**Spec:** `docs/superpowers/specs/2026-10-13-txn-group-commit.md`（rev2，已审计；执行者必须同时读）。当前基线：main = v0.4.0（`docs/benchmarks/2026-10-13-oltp-txn.md` 有对照数字）。

## Global Constraints

- 注释英文；测试 stdlib 断言；只在 `server/` 动代码（README/版本串除外）。
- 分支 `feat/txn-group-commit`；每任务红→绿→commit；`feat(server)/test(server)` 风格。
- 三模块 `go test -race -count=1 ./...` 全绿；server 覆盖率 ≥ 86.9% 不回退。
- **Task 0 是闸门**：若诊断结论与"批=1"假设不符（raft 侧批均值的实测已 >1.5 且 TPS 仍 ~500），停止实现，把诊断数据回写 spec 并升级给用户。
- 发布（merge/push/gh release v0.5.0）走确认门。
- 关键正确性红线（spec §3.2/§3.3）：slot 0 基 0..255；自排除条件 `(v>>8)==idx && (v&0xFF)==slot`；可见性 `(v>>8) > Snapshot`；`effectiveWriteSet` 必须逐 item 在循环内对已应用的 live 计算，禁止预计算。

## 设计决策

| 决策 | 选择 | 理由 |
|---|---|---|
| slot 编码 | 0 基，`(idx<<8)\|slot`，组 ≤256 项 | spec 审计 P0-1；单/组条目不同 index 无 slot 0 碰撞 |
| 全拒组 appliedIndex | 恒推进 | spec 审计 P1-4（RYW 轮询不悬挂）；v0.4 被拒单事务路径保持现状 |
| 组条目 Apply 返回 | `[]ApplyResult`（按槽位）；单事务仍返回 `ApplyResult` | node.Apply 的 switch 加一个分支 |
| 超大单 item（>4MB 编码） | 走 v0.4 单事务直发 | Group 下限 2 闭合；不拆批 |
| 派发 timeout | 队内 item 剩余 deadline 最小值，下限 1s | per-item 语义；入队时已超期者逐槽 ErrTimeout |
| 窗口实现 | 可注入 sleep/now 的 `clock` 小接口 | 测试确定性 |
| batcher 测试 | 依赖注入 dispatch func（不真调 raft） | 单元级可控 |

---

### Task 0: 诊断闸门（不写产品代码）

**Files:** Modify `server/cmd/vectra-bench/main.go`、`server/go.mod`（go-metrics 由 indirect 提升为直接依赖——已是依赖树内包）。Test 无（产物是数据）。

- [ ] **Step 1:** main.go http 模式下、`server.New` 之前安装 go-metrics inmem sink（`logsPerBatch` 样本经 hashicorp/go-metrics 全局实例上报，默认 sink 是黑洞，必须显式装 sink）：

```go
import gometrics "github.com/hashicorp/go-metrics"

sink := gometrics.NewInmemSink(10*time.Second, time.Minute)
_, _ = gometrics.NewGlobal(&gometrics.Config{EnableHostname: false, FilterDefault: true}, sink)
```

工作负载循环结束后（注入已完成的对照快照做法见 Step 3）：

```go
var n, sum float64
for _, im := range sink.Data() {
    if sv, ok := im.Samples["raft.boltdb.logsPerBatch"]; ok {
        n += float64(sv.Count); sum += sv.Sum
    }
}
if n > 0 {
    fmt.Fprintf(os.Stderr, "raft logsPerBatch (txnsi phase): mean=%.2f samples=%.0f\n", sum/n, n)
}
```

（注意：`n.bolt.Stats()` 返回 bbolt 页统计，**不含** logsPerBatch，不要走 expvar/bolt.Stats 路线——审计 P0-1。）
- [ ] **Step 2:** `cd server && go build ./... && go test ./...` 绿 → commit `test(bench): install go-metrics inmem sink; report raft logsPerBatch in http runs`
- [ ] **Step 3（诊断）:** 注入阶段与压测段分别取数：txnsi 工作负载开始前先 `sink.Data()` 清空一次不可行（inmem 不可清）——改为**两次快照做差**：负载循环前记录 (count₀,sum₀)，结束后取 (count₁,sum₁)，压测段均值 = (sum₁-sum₀)/(count₁-count₀)。跑 `make bench SIZE=small MODE=http WORKERS=16 WORKLOADS=txnsi DURATION=10s`，记录压测段 logsPerBatch 均值与 txnsi QPS。
- [ ] **Step 4:** 判定写入执行记录：压测段均值 ≈1 → 假设成立，继续 Task 1；均值 >1.5 且 TPS 仍 ~500 → **停止**，数据回写 spec §4 并 BLOCKED 上报（瓶颈在别处：FSM bolt fsync / 上游串行点）。

### Task 1: Batch v4 — TxnItem / Group（类型 + gob 兼容 + 互斥校验）

**Files:** Modify `server/batch.go`；Test `server/txnbatch_test.go`（新建，后续任务共用）。

**Interfaces:** Produces `TxnItem{Nodes []NodeUpsert; Edges []EdgeUpsert; EdgeDeletes []EdgeDelete; NodeDeletes []string; Txn *TxnGuard}`；`Batch.Group []TxnItem`；`batchVersion = 4`；`const maxGroupItems = 256, maxGroupBytes = 4 << 20`。

- [ ] **Step 1: 失败测试**

```go
package server

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func groupBatch() Batch {
	return Batch{Version: 4, Group: []TxnItem{
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 1}},
		{Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}, Txn: &TxnGuard{Snapshot: 1, SER: true, ReadNodes: []string{"a"}}},
	}}
}

func TestBatchGroupGobRoundtrip(t *testing.T) {
	data, err := encodeBatch(groupBatch())
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Group) != 2 || got.Group[1].Txn.SER != true || got.Group[1].Txn.ReadNodes[0] != "a" {
		t.Fatalf("group roundtrip = %+v", got.Group)
	}
}

// legacyBatchV3 mirrors the 0.4 field set: gob must skip Group.
type legacyBatchV3 struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
	Txn         *TxnGuard
}

func TestBatchGroupLegacyCompat(t *testing.T) {
	data, err := encodeBatch(groupBatch())
	if err != nil {
		t.Fatal(err)
	}
	var old legacyBatchV3
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&old); err != nil {
		t.Fatalf("legacy decode of group entry: %v", err)
	}
	if len(old.Nodes) != 0 || old.Txn != nil {
		t.Fatalf("legacy struct saw residue: %+v", old) // spec §7: 0.4 sees an all-empty batch
	}
}

func TestGroupEntryValidation(t *testing.T) {
	// Group and v0.4 fields are mutually exclusive; size limits enforced.
	mixed := Batch{Version: 4, Nodes: []NodeUpsert{{Key: "x", Label: "S"}}, Group: []TxnItem{{}}}
	if err := validateBatchShape(mixed); err == nil {
		t.Fatal("mixed single+group fields must be rejected")
	}
	big := Batch{Version: 4}
	for i := 0; i <= maxGroupItems; i++ {
		big.Group = append(big.Group, TxnItem{})
	}
	if err := validateBatchShape(big); err == nil {
		t.Fatalf("group of %d items must be rejected", len(big.Group))
	}
	if err := validateBatchShape(groupBatch()); err != nil {
		t.Fatalf("valid group rejected: %v", err)
	}
	// Empty group: gob omits zero-length slices so this is unreachable via the
	// wire (decodes to nil -> single-txn path); the guard is defense-in-depth.
	if err := validateBatchShape(Batch{Version: 4, Group: []TxnItem{}}); err == nil {
		t.Fatal("empty group must be rejected")
	}
}
```

- [ ] **Step 2:** `cd server && go test -run 'TestBatchGroup|TestGroupEntry' -v .` → FAIL（未定义）
- [ ] **Step 3:** batch.go：`batchVersion = 4`；TxnItem 类型（注释：slot = 0-based index in Group, see spec §3.2）；Batch 加 `Group []TxnItem`；新增

```go
// validateBatchShape rejects entries that mix v0.4 single-txn fields with
// Group, and enforces the group size cap. Called at decode boundaries.
func validateBatchShape(b Batch) error {
	hasSingle := len(b.Nodes) > 0 || len(b.Edges) > 0 || len(b.EdgeDeletes) > 0 || len(b.NodeDeletes) > 0 || b.Txn != nil
	if b.Group == nil {
		return nil
	}
	if hasSingle {
		return errors.New("batch: Group and single-txn fields are mutually exclusive")
	}
	if len(b.Group) < 1 || len(b.Group) > maxGroupItems {
		return fmt.Errorf("batch: group size %d out of range [1,%d]", len(b.Group), maxGroupItems)
	}
	return nil
}
```

（`errors`/`fmt` import；len==1 允许——伪造条目防御性接受，fsm 正常处理；batcher 永不产生 1。**调用点钉死（审计 P2-1）**：`validateBatchShape` 在 `fsm.Apply` 内 `decodeBatch` 成功之后、组分支之前调用，违规返回 error（所有副本确定性一致）；不放进 decodeBatch。HTTP 层构造不出 Group（handleTxn 请求结构无此字段），无 400 映射。）

- [ ] **Step 4:** 测试绿 + `go test ./...` 绿 → commit `feat(server): Batch v4 carries Group []TxnItem (one log entry, many transactions)`

### Task 2: composite 版本编码（fsm + VCTX v2 迁移）

**Files:** Modify `server/fsm.go`；Test `server/txnbatch_test.go` 追加。

**Interfaces:** Produces `func versionAt(idx uint64, slot int) uint64 { return idx<<8 | uint64(slot) }`；`validateTxn` 在**现有签名末尾**追加 `slot int`（现签名 `(tg, idx, wNodes, wEdges)` → `(tg, idx, wNodes, wEdges, slot)`，比较改 `(v>>8) > Snapshot && !(v>>8 == idx && (v&0xFF) == slot)`）；单事务路径 bump（nodeVer/edgeVer 两处循环）改用 `versionAt(l.Index, 0)`；VCTX fmtVer 1→2，`splitSnapshot` 读到 fmtVer 1 时对 NodeVer/EdgeVer 全量 `<<8` 迁移（AppliedIndex 不变）。

- [ ] **Step 1: 失败测试**（txnbatch_test.go 追加；`applyAt` 已存在于 txn_test.go——同包复用）

```go
func TestCompositeVersionVisibleAfterToken(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5) // bump a -> (5<<8)|0
	// Fresh token 5 sees it (visible); token 4 conflicts.
	r := applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "b", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, SER: true, ReadNodes: []string{"a"}}}, 6).(ApplyResult)
	if !r.Committed {
		t.Fatal("token 5 must see entry 5 (composite >>8 comparison)")
	}
	r = applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "b", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 4, SER: true, ReadNodes: []string{"a"}}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("token 4 must conflict with entry 5")
	}
}

func TestVCTXV1MigrationOnRestore(t *testing.T) {
	// Hand-write a genuine 0.4-era snapshot: fmtVer 1 with plain table values.
	// The applyAt only materializes the graph payload (its bump never enters
	// the snapshot — the verSnapshot below IS the 0.4-era fixture).
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 9)
	graph, _ := f.live.ExportSnapshot()
	vs := verSnapshot{NodeVer: map[string]uint64{"a": 9}, EdgeVer: map[edgeVerKey]uint64{}, AppliedIndex: 9}
	var gobBuf bytes.Buffer
	if err := gob.NewEncoder(&gobBuf).Encode(vs); err != nil {
		t.Fatal(err)
	}
	trailer := append([]byte("VCTX\x01\x00"), gobBuf.Bytes()...) // fmtVer 1, u16 LE
	raw := frameSnapshot(graph, trailer)                          // reuse existing test helper (txn_test.go)
	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	f2.mu.RLock()
	got := f2.nodeVer["a"]
	f2.mu.RUnlock()
	if want := versionAt(9, 0); got != want {
		t.Fatalf("migrated nodeVer[a] = %d, want %d (plain<<8)", got, want)
	}
	// Reference replica: replays the same prefix under 0.5 semantics.
	f3 := newFSM(time.Hour)
	applyAt(f3, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 9)
	f3.mu.RLock()
	got3 := f3.nodeVer["a"]
	f3.mu.RUnlock()
	if got3 != got {
		t.Fatalf("restore-migrated %d != replay-derived %d (cross-epoch divergence)", got, got3)
	}
}
```

（`frameSnapshot` helper 已存在于 txn_test.go——直接复用；`gob` import。）

另追加一步**既有测试口径更新**（审计 P1-1：composite 对决策级测试透明，但字面量表值断言会挂）：
- txn_test.go `TestFSMTxnReplayBumpIdempotent`：`got != 5` → `got != versionAt(5, 0)`（want 注释同步 `(5<<8)|0`）；
- txn_test.go `TestFSMSnapshotVCTXPersistRestore`：`got != 6` → `versionAt(6, 0)`；`got != 7` → `versionAt(7, 0)`；
- txn_test.go 坏输入表 `"unsupported VCTX version 2"` 用例：fmtVer 2 已合法，trailer 改 `[]byte("VCTX\x03\x00junk")` 并更名 `"unsupported VCTX version 3"`（保住该分支覆盖）。

- [ ] **Step 2:** `go test -run 'TestComposite|TestVCTXV1' -v .` → FAIL
- [ ] **Step 3:** 实现——fsm.go：`versionAt`；`validateTxn` 签名加 `slot int`（自排除条件如上）；Apply 单事务路径三处 bump（nodeVer/edgeVer）改 `versionAt(l.Index, 0)`；`Persist` 写 fmtVer 2；`splitSnapshot`：fmtVer 1 → `for k, v := range vs.NodeVer { vs.NodeVer[k] = v << 8 }`（edgeVer 同理；`v<<8 == v*256`，注释注明迁移语义）；fmtVer 2 直读；其余报错不变。
- [ ] **Step 4:** 全部 txn 测试 + `go test ./...` 绿 → commit `feat(server): composite versions (idx<<8|slot) — slot-aware self-exclusion, VCTX v2 with v1 ×256 migration`

### Task 3: fsm 组应用（Group 条目逐 item 验证/应用）

**Files:** Modify `server/fsm.go`（Apply 组分支）——**不动 node.go**（node.Apply 签名返回单个 ApplyResult，无法透传 slice；组结果由 Task 5 的 batcher 桥接消费，审计 P1-3）；Test `server/txnbatch_test.go` 追加。

**Interfaces:** Produces：fsm.Apply 对 `b.Group != nil` 条目返回 `[]ApplyResult`（槽位序；每项含 Committed/Index/Conflicts/ConflictTotal）；`applyGroupAt(f, b, idx)` 测试 helper；`validateBatchShape` 挂入 Apply（decode 后调用，违规返回 error）。

- [ ] **Step 1: 失败测试**（核心等价性 + 审计矩阵）

```go
func applyGroupAt(f *fsm, b Batch, idx uint64) []ApplyResult {
	data, err := encodeBatch(b)
	if err != nil {
		panic(err)
	}
	return f.Apply(&raft.Log{Data: data, Index: idx}).([]ApplyResult)
}

func TestGroupIntraConflictSameKey(t *testing.T) {
	f := newFSM(time.Hour)
	item := func(k string) TxnItem {
		return TxnItem{Nodes: []NodeUpsert{{Key: k, Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}}
	}
	rs := applyGroupAt(f, Batch{Version: 4, Group: []TxnItem{item("a"), item("a")}}, 5)
	if !rs[0].Committed || rs[1].Committed {
		t.Fatalf("same-key pair = [%v,%v], want [commit,reject] (sequential equivalence)", rs[0].Committed, rs[1].Committed)
	}
	// Replay: item decisions + final state identical; counts may drift legally.
	rs2 := applyGroupAt(f, Batch{Version: 4, Group: []TxnItem{item("a"), item("a")}}, 5)
	if rs2[0].Committed != true || rs2[1].Committed != false {
		t.Fatalf("replay decisions flipped: [%v,%v]", rs2[0].Committed, rs2[1].Committed)
	}
	f.mu.RLock()
	n := f.live.NodeCount()
	f.mu.RUnlock()
	if n != 1 {
		t.Fatalf("live nodes = %d, want 1", n)
	}
}

func TestGroupRejectedItemVisibleToLaterItems(t *testing.T) {
	f := newFSM(time.Hour)
	// item0 reads "a" (SER); item1 writes "a" (commits); item2 reads "a" SER -> must reject.
	g := []TxnItem{
		{Nodes: []NodeUpsert{{Key: "z", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0, SER: true, ReadNodes: []string{"a"}}},
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
		{Nodes: []NodeUpsert{{Key: "y", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0, SER: true, ReadNodes: []string{"a"}}},
	}
	rs := applyGroupAt(f, Batch{Version: 4, Group: g}, 5)
	if !rs[0].Committed || !rs[1].Committed || rs[2].Committed {
		t.Fatalf("decisions = %v/%v/%v, want C/C/R (item2 sees item1's bump)", rs[0].Committed, rs[1].Committed, rs[2].Committed)
	}
}

func TestGroupAllRejectedAdvancesAppliedIndex(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	before := f.metrics.AppliedBatches.Load()
	g := []TxnItem{
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
	}
	rs := applyGroupAt(f, Batch{Version: 4, Group: g}, 6)
	if rs[0].Committed || rs[1].Committed {
		t.Fatal("both items should conflict")
	}
	if got := f.appliedIndex.Load(); got != 6 {
		t.Fatalf("appliedIndex = %d, want 6 (advances on all-rejected groups)", got)
	}
	if got := f.metrics.TxnConflicts.Load(); got != 2 {
		t.Fatalf("TxnConflicts = %d, want 2", got)
	}
	if got := f.metrics.AppliedBatches.Load(); got != before {
		t.Fatalf("AppliedBatches advanced on all-rejected group (want unchanged %d)", before)
	}
	// A later txn with token 6 (which now reflects the group entry) commits.
	r := applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "fresh", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 6}}, 7).(ApplyResult)
	if !r.Committed {
		t.Fatal("token 6 must be usable after an all-rejected group")
	}
}

func TestGroupSlotBoundary256(t *testing.T) {
	f := newFSM(time.Hour)
	g := make([]TxnItem, maxGroupItems)
	for i := range g {
		g[i] = TxnItem{Nodes: []NodeUpsert{{Key: "k", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}}
	}
	rs := applyGroupAt(f, Batch{Version: 4, Group: g}, 5)
	committed := 0
	for _, r := range rs {
		if r.Committed {
			committed++
		}
	}
	if committed != 1 {
		t.Fatalf("committed = %d, want exactly 1 (first slot wins, no overflow)", committed)
	}
	// Replay must reproduce decisions (slot-255 item must not self-conflict).
	rs2 := applyGroupAt(f, Batch{Version: 4, Group: g}, 5)
	for i := range rs {
		if rs[i].Committed != rs2[i].Committed {
			t.Fatalf("replay flipped slot %d", i)
		}
	}
}

func TestGroupMixedSequenceWithSingleEntry(t *testing.T) {
	f := newFSM(time.Hour)
	// Single-txn entry @5 writes k (slot 0); group @6 has two same-key items.
	applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "k", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}}, 5)
	g := []TxnItem{
		{Nodes: []NodeUpsert{{Key: "k", Label: "S"}}, Txn: &TxnGuard{Snapshot: 5}}, // token 5 sees entry 5 -> commit
		{Nodes: []NodeUpsert{{Key: "k", Label: "S"}}, Txn: &TxnGuard{Snapshot: 5}}, // conflicts with slot 0
	}
	rs := applyGroupAt(f, Batch{Version: 4, Group: g}, 6)
	if !rs[0].Committed || rs[1].Committed {
		t.Fatalf("mixed sequence = [%v,%v], want [C,R]", rs[0].Committed, rs[1].Committed)
	}
	// A later entry with token 6 sees the group's committed slot-0 item.
	r := applyAt(f, Batch{Version: 4, Nodes: []NodeUpsert{{Key: "w", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 6, SER: true, ReadNodes: []string{"k"}}}, 7).(ApplyResult)
	if !r.Committed {
		t.Fatal("token 6 must see group entry 6")
	}
}

func TestGroupTxnNilItemAutoCommits(t *testing.T) {
	f := newFSM(time.Hour)
	rs := applyGroupAt(f, Batch{Version: 4, Group: []TxnItem{
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, // forged Txn=nil: auto-commit item
	}}, 5)
	if !rs[0].Committed {
		t.Fatal("nil-guard item auto-commits")
	}
}

// Cross-epoch A/B (audit P1-4): a v1-snapshot-restored replica and a
// replay-from-scratch replica must decide the SAME subsequent group entry
// identically, item by item. (Builds on TestVCTXV1MigrationOnRestore's
// fixtures: f2 restored from fmtVer-1, f3 replayed.)
func TestGroupCrossEpochEquivalence(t *testing.T) {
	build := func() (*fsm, *fsm) {
		f := newFSM(time.Hour)
		applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 9)
		graph, _ := f.live.ExportSnapshot()
		vs := verSnapshot{NodeVer: map[string]uint64{"a": 9}, EdgeVer: map[edgeVerKey]uint64{}, AppliedIndex: 9}
		var gb bytes.Buffer
		_ = gob.NewEncoder(&gb).Encode(vs)
		raw := frameSnapshot(graph, append([]byte("VCTX\x01\x00"), gb.Bytes()...))
		f2 := newFSM(time.Hour)
		if err := f2.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
			t.Fatal(err)
		}
		f3 := newFSM(time.Hour)
		applyAt(f3, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 9)
		return f2, f3
	}
	fA, fB := build()
	g := []TxnItem{
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 9}},
		{Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 9}},
	}
	rsA := applyGroupAt(fA, Batch{Version: 4, Group: g}, 10)
	rsB := applyGroupAt(fB, Batch{Version: 4, Group: g}, 10)
	for i := range rsA {
		if rsA[i].Committed != rsB[i].Committed {
			t.Fatalf("cross-epoch slot %d: %v vs %v", i, rsA[i].Committed, rsB[i].Committed)
		}
	}
	if !rsA[0].Committed || rsA[1].Committed {
		t.Fatalf("decisions = [%v,%v], want [C,R]", rsA[0].Committed, rsA[1].Committed)
	}
}
```

- [ ] **Step 2:** `go test -run 'TestGroup' -v .` → FAIL（fsm.Apply 对 Group 返回的是空 ApplyResult）
- [ ] **Step 3:** 实现——fsm.Apply：decode 后先 `validateBatchShape(b)`（违规返回 err）；`if b.Group != nil { …组分支… }`（组分支完整替换单事务路径：锁内逐 item `effectiveWriteSet(item)` → `validateTxn(item.Txn, l.Index, wNodes, wEdges, slot)`（item.Txn==nil 跳过验证）→ 拒绝记 results 继续 / 提交则重放 item 四循环 + bump `versionAt(l.Index, slot)`；锁内结尾恒 `appliedIndex.Store(l.Index)`，≥1 item 提交才 dirty/version++；锁外 metrics：≥1 提交才 AppliedBatches++/计数器按 item 聚合）；返回 `[]ApplyResult`。既有单事务路径（Group==nil）一行不改；node.go 不动（组结果由 Task 5 的 `applyEncoded` 桥接消费）。
- [ ] **Step 4:** `go test ./...` 全绿 → commit `feat(server): fsm applies Group entries slot-by-slot — sequential-entry equivalence, per-item results`

### Task 4: txnBatcher（攒批/上限/future 路由/兜底）

**Files:** Create `server/txnbatch.go`；Test `server/txnbatch_test.go` 追加。

**Interfaces:** Produces：

```go
// clock is injectable for deterministic batcher tests.
type clock interface{ Now() time.Time; Sleep(time.Duration) }

type txnItemReq struct {
	item     TxnItem
	deadline time.Time // zero = no per-item deadline
	fut      chan batcherResult
}
type batcherResult struct { res ApplyResult; err error }

// txnBatcher coalesces concurrent /v1/txn items into one raft entry.
// Counters feed the spec §3.5 expvars.
type txnBatcher struct {
	q        chan txnItemReq // bounded (qsize, default 4096)
	apply    func(b Batch, timeout time.Duration) (any, error) // injected bridge
	ck       clock
	maxWait  time.Duration // 2ms
	maxItems int           // 256
	maxBytes int           // 4MB
	groupDispatches atomic.Int64 // vectra_txn_group_batches
	groupItemsTotal atomic.Int64 // vectra_txn_group_size_total
	windowWaitNanos  atomic.Int64 // vectra_txn_window_wait_ns
	// stop/close/wg...
}
func newTxnBatcher(apply func(Batch, time.Duration) (any, error)) *txnBatcher
func newTxnBatcherWith(apply func(Batch, time.Duration) (any, error), ck clock, qsize int) *txnBatcher // tests
func (b *txnBatcher) submit(ctx context.Context, item TxnItem) (ApplyResult, error) // blocks on fut; oversized item -> direct single path
```

- [ ] **Step 1: 失败测试**。**同步屏障模式（审计 P1-5，所有聚合类用例统一用它）**：窗口只在 ≥2 个 item 已在队时才打开，因此屏障必须**阻塞注入的 apply func**——loop 在第一次派发里卡住，后续 item 得以入队，`close(release)` 放行后 loop 回来 drain 到它们：

```go
func newBlockedApply(release chan struct{}, entries *[]Batch, mu *sync.Mutex) func(Batch, time.Duration) (any, error) {
	return func(b Batch, _ time.Duration) (any, error) {
		<-release
		mu.Lock(); *entries = append(*entries, b); mu.Unlock()
		if b.Group == nil { // fake must mirror production shapes per entry kind
			return ApplyResult{Committed: true, Index: 1}, nil
		}
		rs := make([]ApplyResult, len(b.Group))
		for i := range rs { rs[i] = ApplyResult{Committed: true, Index: 2} }
		return rs, nil
	}
}
```

用例（`bn := newTxnBatcherWith(apply, &fakeClock{now: time.Now()}, 4096)`；`submitAsync` = 返回 fut 的非阻塞入口，同包测试可直接构造 `txnItemReq` 入队）：
- `TestBatcherSoloDispatchesImmediately`：1 个 submit → 恰一条 entry 且 `entries[0].Group == nil && entries[0].Txn != nil`（v0.4 单事务形状，审计 P1-5 附带修正）；
- `TestBatcherAggregatesWhenTwoPending`：blocked-apply 屏障 + filler 占住 loop + 两个真实 item 入队 → release → 恰一条 Group entry len==2、两 future 按槽位各得 Committed 结果；
- `TestBatcherCapItems`：屏障下塞 maxItems+1 个 → 首次派发恰 maxItems、余量下批；
- `TestBatcherCapBytes`：maxBytes 注入为小值 + 屏障 + 两个大 item → 首派发截断为 1 项（审计 P2-3）；
- `TestBatcherOversizedItemDirect`：单 item 编码 > maxBytes → 走 v0.4 直发形状；
- `TestBatcherQueueFull503`：qsize=1 + 屏障阻塞 → 第二个 submit 得 `errOverloaded`；
- `TestBatcherLeadershipLostMapsAllSlots`：apply 返回 `ErrNotLeader` → 全部 submit 得同错误；
- `TestBatcherExpiredDeadlineRejectedAtEnqueue`：ctx 已超期 → submit 返回 `ErrTimeout`，entries 无记录；
- `TestBatcherPanicDrains`：apply panic 一次 → 在队全部 future 得内部错误，batcher 存活（下一次 submit 正常）。

- [ ] **Step 2:** → FAIL（类型未定义）
- [ ] **Step 3:** 实现 txnbatch.go：单 goroutine loop（select on q / stop）；收集策略：收到第一个后非阻塞 drain 至 maxItems；`len(pending) == 1` → 单事务直发（v0.4 Batch{Txn}）；≥2 → 等待窗口（`ck.Sleep(min(maxWait, 至首个 deadline 剩余))` 期间继续 drain，用带 timeout 的 select 模拟）；编码尺寸超 maxBytes 时截断到当前并派发（超出部分留下一批）；派发 timeout = min(各 item 剩余 deadline, 10s)，下限 1s；结果 `[]ApplyResult` 按槽位投递、长度不符/类型不符视为内部错误全槽位报错；`defer recover()` 包裹派发循环，panic 时全队 future 报错后重启 loop；队列满/入队超期在 submit 侧快速失败。
- [ ] **Step 4:** 绿 + race（`go test -race -run TestBatcher -count=3 .`）→ commit `feat(server): txnBatcher — adaptive coalescing (2ms/256/4MB), per-slot futures, overload backpressure`

### Task 5: handleTxn 接入 batcher + HTTP 语义

**Files:** Modify `server/api.go`、`server/node.go`（Node 持有 batcher + 共享 `applyEncoded` 桥）；Test `server/txnbatch_test.go` 追加（HTTP 层，复用 testCluster）。

- [ ] **Step 0（node.go 桥，审计 P1-3）**：抽出 `applyEncoded` 供 node.Apply 与 batcher 共用，错误映射逐字复刻现有 fut.Error() 分支：

```go
// applyEncoded submits an encoded batch to raft and normalizes errors.
// Shared by Node.Apply and the txn batcher bridge.
func (n *Node) applyEncoded(data []byte, timeout time.Duration) (any, error) {
	fut := n.r.Apply(data, timeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, ErrNotLeader
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raft.ErrRaftShutdown) {
			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		return nil, err
	}
	return fut.Response(), nil
}
```

batcher 桥 = `func(b Batch, to time.Duration) (any, error) { data, err := encodeBatch(b); if err != nil { return nil, err }; return n.applyEncoded(data, to) }`；结果侧 `switch v := resp.(type) { case ApplyResult: (solo) case []ApplyResult: (group per-slot) }`（`[]ApplyResult` 与 `ApplyResult` 动态类型不同，type switch 安全）。

- [ ] **Step 1: 失败测试**：`TestTxnGroupOverHTTP`（3 节点集群；8 并发同 token 族提交（Task 6 的 16 并发混流覆盖 spec §5 的 16 并发格）；断言：≥1 个 200；Node 暴露 `TxnGroupEntries() int64`（读 batcher.groupDispatches）>0；最终 published stats 一致）；`TestTxnOverloaded`（batcher 队列容量注入为 1 + 阻塞 apply → 503 TXN_OVERLOADED）；`TestTxnGroup504RetrySafe`（派发超时后同请求体重提 → 409 或 200，不得 500）。
- [ ] **Step 2:** FAIL → **Step 3:** node.go：`Node` 增加 `txnQ *txnBatcher`（New 时以桥创建；Close 时先关 batcher）；api.go handleTxn：入队前 `if !n.IsLeader() { 503 + hint }`（复用现有分支）；上限校验后 `res, err := n.txnQ.submit(r.Context(), item)`；错误映射 `ErrNotLeader/ErrTimeout/errOverloaded`（503+hint / 504 / 503 TXN_OVERLOADED+Retry-After）；成功走现有 200/409 分支（响应不变）。`registerExpvar` 的 `expvarOnce` 块内 publish spec §3.5 三指标（`vectra_txn_group_batches`/`vectra_txn_group_size_total`/`vectra_txn_window_wait_ns`，读 batcher 计数器，沿用既有 expvar 模式——审计 P1-2）。
- [ ] **Step 4:** 绿 + `go test ./...` → commit `feat(server): /v1/txn routes through the batcher — group commit on the leader`

### Task 6: 集群确定性与混流

**Files:** Test `server/txnbatch_test.go` 追加。

- [ ] **Step 1:** `TestTxnGroupClusterDeterminism`（3 节点；leader 上混流：/v1/batch（slot-0 条目）+ 16 并发 /v1/txn（组条目）交替 30s 内缩至 ~5s；断言各副本 nodeVer 完全一致（含 composite 值）、live Stats 一致）；`TestTxnGroupFailover`（组提交进行中停 leader，新 leader 接着收 /v1/txn 正常 200/409）。
- [ ] **Step 2:** FAIL/直接绿按现状 → 修复 → **Step 3:** `go test -race ./...` 绿 → commit `test(server): cluster determinism under mixed slot-0/group entries and failover`

### Task 7: 基准验收 + 文档 + v0.5.0

**Files:** Create `server/txnbatch_bench_test.go`；Modify README×2、版本串 5-6 处（0.4.0→0.5.0，逐处核对）。

- [ ] **Step 1:** `BenchmarkFSMGroupApply64`（fsm 级：一次 Apply 64 item 组 vs 64 次单条，ns/item 对比）。
- [ ] **Step 2（验收）:** `make bench SIZE=small MODE=http WORKERS=16 WORKLOADS=txnsi,txnser DURATION=10s` 对照 v0.4.0（S/16w：txnsi 542.4、txnser 556.2）：**txnsi ≥ 2,700/s 为达标线**；记录 p50/p99；同配置跑 WORKERS=32 探上界。数字写入 `docs/benchmarks/2026-10-14-txn-group.md`（含原始表附录）。
- [ ] **Step 3:** README 双语 Transactions 节补组提交段（batcher 窗口/上限、503 TXN_OVERLOADED、混布约束升级为 0.5 停机全量 + token-unsafe 分歧措辞）；Coverage 行按实测更新。
- [ ] **Step 4:** 三模块 `-race -count=1` + vet + fmt + 覆盖率（server ≥86.9%）→ commit → **确认门**：merge/push/gh release v0.5.0 必须用户批准。

## Self-Review

- Spec 覆盖：§3.1（Task 1）、§3.2（Task 2）、§3.3（Task 3）、§3.4（Task 4/5）、§3.5（Task 4 计数器 + Task 5 expvar publish）、§4（Task 0 闸门，go-metrics inmem sink 方案）、§5 矩阵逐行落位（slot 边界/混合序/跨纪元 A/B 含组条目等价/全拒组+AppliedBatches+后续 token/Txn=nil/混流/504 重试/超大 item/panic 兜底/超期入队/4MB 截断/空 Group）、§6-§7（Task 7）✓
- 红线复述：slot 0 基；`(v>>8)` 可见性；自排除带 slot；effectiveWriteSet 逐 item 循环内计算；全拒组 appliedIndex 恒推进 ✓
- 类型一致性：`TxnItem`/`versionAt`/`[]ApplyResult`/`newTxnBatcherWith`/`applyEncoded` 各任务引用一致；`applyAt`/`frameSnapshot` 复用 txn_test.go 既有 helper；`validateTxn` 按现有签名 `(tg, idx, wNodes, wEdges)` 只追加尾参 `slot` ✓
- Plan 审计（agent）P0-1（Task 0 取数机制）、P1-1（既有字面断言 3 处 + fmtVer case）、P1-2（expvar 落位）、P1-3（node.Apply 死分支改 applyEncoded 桥）、P1-4（跨纪元组条目等价 + 测试注释修正）、P1-5（阻塞-apply 屏障模式 + fake 分形 + newTxnBatcherWith）已全部合入；P2 项（validateBatchShape 调用点、空 Group 用例、CapBytes、全拒组断言扩展、签名注记、8/16 并发等效注记、frameSnapshot 复用）一并处理 ✓
