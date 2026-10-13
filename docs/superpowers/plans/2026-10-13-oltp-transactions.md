# v0.4.0: 交互式 OLTP 事务层实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 server 模块实现交互式事务：读发布快照拿 token → 提交携带读写集的 Batch → fsm.Apply 内 OCC 后向验证（默认 SI 验写集 / 可选 SER 加验读集）→ 冲突确定性拒绝（HTTP 409）。

**Architecture:** 全部改动收敛在 server 模块：fsm 旁路 per-key 版本表（raft Index 口径）+ 有效写集枚举（含级联）+ VCTX 快照框架（表随快照持久化，拒绝旧格式）+ 原子束发布（图与 seq 一次 Load）+ `POST /v1/txn`。根模块与 engine 零改动，密封 CSR lock-free 读契约不变。

**Tech Stack:** Go stdlib + 既有依赖（hashicorp/raft v1.8.0）。根模块零三方依赖红线不变。

**Spec:** `docs/superpowers/specs/2026-10-13-oltp-transactions.md`（rev2，已过审计；执行者须同时读 spec）。

## Global Constraints

- 注释英文（`8fe04f3` 确立的约束）；测试 stdlib 断言风格（无第三方断言库）。
- 根模块（`/`）与 `internal/` 零改动；只在 `server/` 模块动代码（README/版本串除外）。
- 分支 `feat/oltp-txn`；每任务红→绿→commit；commit 消息对齐 git log 风格（`feat(server): …` / `test(server): …`）。
- 三模块 `go test -race ./...` 全绿；覆盖率：server ≥ 83.8%（现值）目标 ≥ 85%，根模块 92.2% 与 internal 各包不回退。
- 版本串 0.2.1 → 0.4.0，共 6 处：`mcp/server.go:23`、`mcp/ts/package.json:3`、`mcp/ts/src/index.ts:40`、`mcp/ts/package-lock.json`（2 处）、`mcp/python/pyproject.toml:3`（发布前核对 tag，git 历史有 0.3.0→0.2.1 回落）。
- 发布（merge main、push、gh release）走确认门：**执行到发布前必须停下等用户确认**。

## 设计决策

| 决策 | 选择 | 理由 |
|---|---|---|
| 版本口径 | raft `l.Index` | 跨重启/重放天然连续；与 VCTX 持久化对齐（spec §2.1） |
| bump 规则 | 有效写集无条件 bump（含 skipped 操作的请求键） | 只会多拒不少拒；免除 skip 判定与 bump 二选一的分歧行为（spec §2.1，审计 B.2） |
| 级联枚举 | `live.EdgesOf(key, DirOut)` + `DirIn`（无 DirBoth 常量），验证阶段一次性枚举、bump 复用 | 审计 P1-3；replay 前枚举保证两阶段一致 |
| 拒绝旧快照 | Restore 见无 VCTX 框架直接 error | 审计 P0-1：任何表状态分歧 = FSM 永久分歧 |
| token 发布 | `pubState{graph, seq}` 原子束 | 审计 P1-2：杜绝读侧 seq 偏新 |
| await 端点 | 不实现，README 文档化轮询 | spec §2.8 降级决议 |
| HTTP 负载基准 | v0.4.0 用 fsm 级 Go benchmark 验证开销；vectra-bench 的 txnsi/txnser 负载推迟 | 保持本版范围可控；spec §3 验收线由 fsm 级基准 + 自动提交回归对照覆盖 |
| metrics | `TxnCommits`/`TxnConflicts` 新增；被拒事务不计 AppliedBatches 等既有计数 | spec §2.4 / 审计 P2-6 |

---

### Task 1: Batch v3 事务载荷（类型 + gob 兼容）

**Files:**
- Modify: `server/batch.go`
- Test: `server/txn_test.go`（新建，后续任务继续追加）

**Interfaces:**
- Produces: `EdgeRef{Src, Dst, Label string}`；`TxnGuard{Snapshot uint64, SER bool, ReadNodes []string, ReadEdges []EdgeRef}`（json tag：`snapshot`/`ser`/`readNodes`/`readEdges`）；`Batch.Txn *TxnGuard`；`batchVersion = 3`。

- [ ] **Step 0:** `git checkout -b feat/oltp-txn`（全计划的工作分支，后续任务不再重复）

- [ ] **Step 1: 失败测试**（server/txn_test.go 新建）

```go
package server

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func txnBatch() Batch {
	return Batch{
		Version: 3,
		Nodes:   []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{
			Snapshot:  7,
			SER:       true,
			ReadNodes: []string{"a", "b"},
			ReadEdges: []EdgeRef{{Src: "a", Dst: "b", Label: "CALLS"}},
		},
	}
}

func TestBatchTxnGobRoundtrip(t *testing.T) {
	data, err := encodeBatch(txnBatch())
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn == nil {
		t.Fatal("Txn lost in roundtrip")
	}
	if got.Txn.Snapshot != 7 || !got.Txn.SER || len(got.Txn.ReadNodes) != 2 {
		t.Fatalf("TxnGuard = %+v", got.Txn)
	}
	if got.Txn.ReadEdges[0] != (EdgeRef{Src: "a", Dst: "b", Label: "CALLS"}) {
		t.Fatalf("ReadEdges = %+v", got.Txn.ReadEdges)
	}
}

// legacyBatch mirrors the pre-0.4.0 Batch field set: gob must skip the new
// Txn field when an old binary (or a test-local legacy struct) decodes.
type legacyBatch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
}

func TestBatchGobLegacyCompat(t *testing.T) {
	// New encoder -> legacy struct: unknown Txn field must be ignored.
	data, err := encodeBatch(txnBatch())
	if err != nil {
		t.Fatal(err)
	}
	var old legacyBatch
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&old); err != nil {
		t.Fatalf("legacy decode of new batch: %v", err)
	}
	if len(old.Nodes) != 1 || old.Nodes[0].Key != "a" {
		t.Fatalf("legacy struct = %+v", old)
	}

	// Legacy encoder -> new struct: Txn must stay nil (auto-commit).
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(legacyBatch{Version: 2, Nodes: []NodeUpsert{{Key: "x", Label: "L"}}}); err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn != nil {
		t.Fatalf("Txn = %+v, want nil for legacy payload", got.Txn)
	}
}
```

- [ ] **Step 2:** `cd server && go test -run 'TestBatchTxn|TestBatchGobLegacy' -v .` → FAIL（`TxnGuard`/`EdgeRef` 未定义，编译错误）
- [ ] **Step 3:** 实现——batch.go：`batchVersion = 3`；新增类型与字段（含演进纪律注释）：

```go
// EdgeRef addresses an edge by its logical key (mirrors EdgeDelete addressing).
type EdgeRef struct {
	Src   string
	Dst   string
	Label string
}

// TxnGuard carries an interactive transaction's read set and snapshot token.
// Snapshot is the publishedSeq the client's reads were based on (from the
// X-Vectra-Snapshot header); validation rejects the batch when any relevant
// key was modified by a raft entry with a higher index.
//
// gob evolution discipline (see encoding/gob doc): add exported fields only;
// never rename existing fields (gob matches by name, there are no field
// numbers — renaming silently drops data); never change a field's type
// (including signedness — decode errors out). Zero-valued new fields are
// omitted from the wire. A nil Txn means auto-commit; a non-nil TxnGuard
// ALWAYS validates — Snapshot 0 means "read at epoch 0" and conflicts with
// any prior write to the same keys.
type TxnGuard struct {
	Snapshot  uint64    `json:"snapshot"`
	SER       bool      `json:"ser"`
	ReadNodes []string  `json:"readNodes"`
	ReadEdges []EdgeRef `json:"readEdges"`
}
```

`Batch` 增加 `Txn *TxnGuard` 字段（`NodeDeletes []string` 之后），注释：`// Txn != nil enables optimistic validation (nil = auto-commit, pre-0.4 behavior)`。

- [ ] **Step 4:** 同 Step 2 命令 → PASS；`cd server && go test ./...` 全绿
- [ ] **Step 5:** `git add server/batch.go server/txn_test.go && git commit -m "feat(server): Batch v3 carries TxnGuard (snapshot token + read set), gob-compatible"`

### Task 2: fsm 版本表 + 有效写集 + Apply 验证

**Files:**
- Modify: `server/fsm.go`（ApplyResult/Metrics/fsm 结构/Apply）
- Test: `server/txn_test.go` 追加

**Interfaces:**
- Consumes: Task 1 的 `TxnGuard`/`EdgeRef`；根模块 `live.EdgesOf(key, dir, labels...)`（`DirOut`/`DirIn`）。
- Produces: `ApplyResult{…, Committed bool, Index uint64, Conflicts []string}`；`Metrics{…, TxnCommits, TxnConflicts atomic.Int64}`；`fsm.nodeVer map[string]uint64`、`fsm.edgeVer map[edgeVerKey]uint64`、`fsm.appliedIndex atomic.Uint64`；`edgeVerKey struct{ Src, Dst, Label string }`；`maxConflicts = 100`。

- [ ] **Step 1: 失败测试**（txn_test.go 追加；注意 `raft.Log{Index:…}` 现在参与语义）

```go
func mustTxn(t *testing.T, b Batch, idx uint64) []byte {
	t.Helper()
	data, err := encodeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	_ = idx
	return data
}

// applyAt applies a batch under a chosen raft log index (validation compares
// against these indexes).
func applyAt(f *fsm, b Batch, idx uint64) interface{} {
	data, err := encodeBatch(b)
	if err != nil {
		panic(err) // test helper: gob encode of a valid Batch cannot fail
	}
	return f.Apply(&raft.Log{Data: data, Index: idx})
}

func TestFSMTxnSIWriteConflict(t *testing.T) {
	f := newFSM(time.Hour)
	// T1 @ index 5 writes node "a" (auto-commit, establishes lastMod[a]=5).
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}}}, 5)
	// T2 read at token 3 (< 5) then writes "a" with SI guard: rejected.
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("SI write-write conflict must be rejected")
	}
	if len(r.Conflicts) == 0 || r.Conflicts[0] != "a" {
		t.Fatalf("conflicts = %v, want [a]", r.Conflicts)
	}
	// Fresh token (>= 5) commits.
	r = applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{Snapshot: 5}}, 7).(ApplyResult)
	if !r.Committed || r.Index != 7 {
		t.Fatalf("fresh token must commit: %+v", r)
	}
}

func TestFSMTxnDifferentKeysCommit(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if !r.Committed {
		t.Fatal("disjoint write sets must both commit under SI")
	}
}

func TestFSMTxnSERReadWriteConflict(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}, {Key: "b", Label: "S"}}}, 5)
	// T reads "a" at token 5; concurrent edge write bumps nodeVer[a] @6.
	applyAt(f, Batch{Version: 3, Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}}, 6)
	// SER: writing untouched key "c" while having read "a" (modified after
	// snapshot) -> rejected on the read set alone.
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, SER: true, ReadNodes: []string{"a"}}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("SER must reject read-write anti-dependency")
	}
	if len(r.Conflicts) == 0 || r.Conflicts[0] != "a" {
		t.Fatalf("conflicts = %v, want [a]", r.Conflicts)
	}
	// Same scenario under SI (no read validation): commits — the two-level split.
	r = applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, ReadNodes: []string{"a"}}}, 8).(ApplyResult)
	if !r.Committed {
		t.Fatal("SI does not validate read sets")
	}
}

func TestFSMAutoCommitBlindWriteBumps(t *testing.T) {
	f := newFSM(time.Hour)
	// Auto-commit (nil Txn) blind write must still enter the version table,
	// otherwise concurrent guarded readers would never see it (false negative).
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 4, SER: true, ReadNodes: []string{"a"}}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("blind auto-commit write must be visible to SER validation")
	}
}

func TestFSMTxnReplayBumpIdempotent(t *testing.T) {
	f := newFSM(time.Hour)
	b := Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}
	applyAt(f, b, 5)
	applyAt(f, b, 5) // same entry replayed: same index, bump must not advance
	f.mu.RLock()
	got := f.nodeVer["a"]
	f.mu.RUnlock()
	if got != 5 {
		t.Fatalf("nodeVer[a] = %d, want 5 (replay bumps to the same index)", got)
	}
}

func TestFSMTxnConcurrentMixedLoad(t *testing.T) {
	f := newFSM(5 * time.Millisecond)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				b := Batch{Version: 3, Nodes: []NodeUpsert{{Key: fmt.Sprintf("w%d-%d", w, i), Label: "S"}}}
				if i%2 == 0 {
					b.Txn = &TxnGuard{Snapshot: 0}
				}
				applyAt(f, b, uint64(w*25+i+1))
			}
		}(w)
	}
	wg.Wait()
	f.sealNow()
	if st := f.Published().Stats(); st.Nodes != 100 {
		t.Fatalf("nodes = %d, want 100", st.Nodes)
	}
}

func TestFSMTxnCascadeKeys(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "x", Label: "S"}, {Key: "y", Label: "S"}},
		Edges: []EdgeUpsert{{SrcKey: "x", DstKey: "y", Label: "E"}}}, 5)
	// NodeDeletes["x"] cascades the x->y edge: peer y's adjacency changes.
	applyAt(f, Batch{Version: 3, NodeDeletes: []string{"x"}}, 6)
	// SER txn that read y's adjacency at token 5 must now be rejected.
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "z", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, SER: true, ReadNodes: []string{"y"}}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("cascade delete must bump peer adjacency version (audit P1-3)")
	}
}

func TestFSMTxnRejectedNoSideEffects(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	before := f.metrics.AppliedBatches.Load()
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("expected rejection")
	}
	f.mu.RLock()
	n := f.live.NodeCount()
	f.mu.RUnlock()
	if n != 1 {
		t.Fatalf("live nodes = %d, want 1 (rejected txn must not touch live)", n)
	}
	if got := f.metrics.AppliedBatches.Load(); got != before {
		t.Fatalf("AppliedBatches = %d, want unchanged %d", got, before)
	}
	if got := f.metrics.TxnConflicts.Load(); got != 1 {
		t.Fatalf("TxnConflicts = %d, want 1", got)
	}
	// Replay of a rejected entry stays rejected and never bumps.
	r2 := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r2.Committed {
		t.Fatal("rejected entry must stay rejected on replay")
	}
}

func TestFSMTxnDeterminismAcrossReplicas(t *testing.T) {
	run := func() []bool {
		f := newFSM(time.Hour)
		var out []bool
		for i, b := range []Batch{
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
		} {
			out = append(out, applyAt(f, b, uint64(i+1)).(ApplyResult).Committed)
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replica divergence at %d: %v vs %v", i, a, b)
		}
	}
	// Expected: [true, false, true] — entry 2 reads at token 0 < lastMod[a]=1.
	if a[0] != true || a[1] != false || a[2] != true {
		t.Fatalf("commit sequence = %v, want [true false true]", a)
	}
}

func TestFSMTxnConflictsTruncated(t *testing.T) {
	f := newFSM(time.Hour)
	nodes := make([]NodeUpsert, 150)
	reads := make([]string, 150)
	for i := range nodes {
		nodes[i] = NodeUpsert{Key: fmt.Sprintf("k%03d", i), Label: "S"}
		reads[i] = fmt.Sprintf("k%03d", i)
	}
	applyAt(f, Batch{Version: 3, Nodes: nodes}, 5)
	r := applyAt(f, Batch{Version: 3, Nodes: nodes,
		Txn: &TxnGuard{Snapshot: 3, SER: true, ReadNodes: reads}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("expected rejection")
	}
	if len(r.Conflicts) != maxConflicts || r.ConflictTotal != 150 {
		t.Fatalf("len(Conflicts)=%d ConflictTotal=%d, want %d/150", len(r.Conflicts), r.ConflictTotal, maxConflicts)
	}
}
```

（txn_test.go 累积 import 备注：Task 2 需要 `fmt`、`sync`、`time`、`github.com/hashicorp/raft`；Task 3 追加 `io`；Task 4 已含；Task 5/6 追加 `encoding/json`、`net/http`、`strconv`。）

- [ ] **Step 2:** `cd server && go test -run TestFSMTxn -v .` → FAIL（`Committed`/`Index`/`Conflicts`/`TxnConflicts`/`maxConflicts` 未定义）
- [ ] **Step 3:** 实现——fsm.go：

  1. `ApplyResult` 加字段：

```go
type ApplyResult struct {
	NodesApplied int
	EdgesApplied int
	EdgesSkipped int
	EdgesDeleted int
	NodesDeleted int
	NodesSkipped int
	Committed    bool     // false only for TxnGuard-rejected batches
	Index        uint64   // raft log index of this entry
	Conflicts    []string // conflicting keys, truncated to maxConflicts
	ConflictTotal int     // total conflict count before truncation
}
```

  2. `Metrics` 加 `TxnCommits, TxnConflicts atomic.Int64`。
  3. fsm 结构加字段（`published` 之后）：`nodeVer map[string]uint64`、`edgeVer map[edgeVerKey]uint64`（注释：guarded by mu）；`appliedIndex atomic.Uint64`；`newFSM` 初始化两 map。
  4. 包级：`const maxConflicts = 100`、`type edgeVerKey struct{ Src, Dst, Label string }`。
  5. 新函数（fsm.go 底部）：

```go
// effectiveWriteSet enumerates every logical key the batch will modify,
// including cascade peers of node deletes (their adjacency changes too).
// Must run under f.mu with replay not yet started. Unconditional: request
// keys are included even if replay will skip them (safe over-rejection only).
func (f *fsm) effectiveWriteSet(b Batch) (nodes []string, edges []edgeVerKey) {
	seenN := map[string]struct{}{}
	seenE := map[edgeVerKey]struct{}{}
	addN := func(k string) {
		if _, ok := seenN[k]; !ok {
			seenN[k] = struct{}{}
			nodes = append(nodes, k)
		}
	}
	addE := func(src, dst, label string) {
		k := edgeVerKey{Src: src, Dst: dst, Label: label}
		if _, ok := seenE[k]; !ok {
			seenE[k] = struct{}{}
			edges = append(edges, k)
		}
	}
	for _, n := range b.Nodes {
		addN(n.Key)
	}
	for _, e := range b.Edges {
		addN(e.SrcKey)
		addN(e.DstKey)
		addE(e.SrcKey, e.DstKey, e.Label)
	}
	for _, d := range b.EdgeDeletes {
		addN(d.SrcKey)
		addN(d.DstKey)
		addE(d.SrcKey, d.DstKey, d.Label)
	}
	for _, key := range b.NodeDeletes {
		addN(key)
		for _, dir := range []vectra.Direction{vectra.DirOut, vectra.DirIn} {
			es, err := f.live.EdgesOf(key, dir)
			if err != nil {
				continue // nonexistent key: no incident edges to cascade
			}
			for _, e := range es {
				// Edge.Src/Dst are NodeIDs; resolve back to string keys —
				// the version table is key-addressed (Seal renumbers IDs).
				src, ok1 := f.live.NodeByID(e.Src)
				dst, ok2 := f.live.NodeByID(e.Dst)
				if !ok1 || !ok2 {
					continue
				}
				addN(src.Key)
				addN(dst.Key)
				addE(src.Key, dst.Key, e.Label)
			}
		}
	}
	return nodes, edges
}

// validateTxn returns the first maxConflicts conflicting keys and the total
// conflict count for a guarded batch, or (nil, 0) when it may commit. Must
// run under f.mu before replay.
func (f *fsm) validateTxn(tg *TxnGuard, wNodes []string, wEdges []edgeVerKey) ([]string, int) {
	var conflicts []string
	seen := map[string]struct{}{}
	total := 0
	add := func(k string) {
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		total++
		if len(conflicts) < maxConflicts {
			conflicts = append(conflicts, k)
		}
	}
	for _, k := range wNodes {
		if f.nodeVer[k] > tg.Snapshot {
			add(k)
		}
	}
	for _, k := range wEdges {
		if f.edgeVer[k] > tg.Snapshot {
			add(edgeKeyString(k))
		}
	}
	if tg.SER {
		for _, k := range tg.ReadNodes {
			if f.nodeVer[k] > tg.Snapshot {
				add(k)
			}
		}
		for _, e := range tg.ReadEdges {
			if f.edgeVer[edgeVerKey{Src: e.Src, Dst: e.Dst, Label: e.Label}] > tg.Snapshot {
				add(edgeKeyString(edgeVerKey{Src: e.Src, Dst: e.Dst, Label: e.Label}))
			}
		}
	}
	if total == 0 {
		return nil, 0
	}
	return conflicts, total
}

func edgeKeyString(k edgeVerKey) string { return k.Src + "\x00" + k.Dst + "\x00" + k.Label }
```

  6. `Apply` 改造（`f.mu.Lock()` 之后、节点循环之前插入验证；replay 循环不动；`f.version++` 之后 bump + `appliedIndex.Store(l.Index)` + `res.Committed/Index` 赋值；metrics 段加 Txn 计数且被拒不计）：

```go
	f.mu.Lock()
	wNodes, wEdges := f.effectiveWriteSet(b) // always: blind writes version too
	if b.Txn != nil {
		if conflicts, total := f.validateTxn(b.Txn, wNodes, wEdges); conflicts != nil {
			f.mu.Unlock()
			f.metrics.TxnConflicts.Add(1)
			return ApplyResult{Committed: false, Index: l.Index, Conflicts: conflicts, ConflictTotal: total}
		}
	}
	// … existing four replay loops unchanged …
	f.dirty = true
	f.version++
	f.appliedIndex.Store(l.Index)
	for _, k := range wNodes {
		f.nodeVer[k] = l.Index
	}
	for _, k := range wEdges {
		f.edgeVer[k] = l.Index
	}
	small := f.live.NodeCount()+f.live.EdgeCount() < f.smallGraphLimit
	f.mu.Unlock()

	if b.Txn != nil {
		f.metrics.TxnCommits.Add(1)
	}
	// … existing metrics block (AppliedBatches etc.) — now after the Txn
	// early-return, so rejected batches never reach it …
	res.Committed = true
	res.Index = l.Index
```

- [ ] **Step 4:** `cd server && go test -run TestFSMTxn -v .` → PASS；`go test ./...` 全绿（既有测试不受影响——它们不设 `Index`，lastMod=0，token 0 不冲突）
- [ ] **Step 5:** `git commit -am "feat(server): OCC validation in fsm.Apply — per-key version table (raft Index), SI write-set / SER read-set tiers"`

### Task 3: VCTX 快照框架（表持久化 + 拒绝旧格式）

**Files:**
- Modify: `server/fsm.go`（graphSnapshot/Snapshot/Persist/Restore）
- Test: `server/txn_test.go` 追加

**Interfaces:**
- Produces: 快照字节格式 `u32 graphLen(LE) + graphBytes + "VCTX" + u16 fmtVer(1) + gob(verSnapshot{NodeVer, EdgeVer, AppliedIndex})`；`Restore` 对无 VCTX 输入返回 error。

- [ ] **Step 1: 失败测试**

```go
func persistSnap(t *testing.T, f *fsm) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestFSMSnapshotVCTXPersistRestore(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5}}, 6) // committed -> nodeVer[a]=6
	raw := persistSnap(t, f)

	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	if got := f2.nodeVer["a"]; got != 6 {
		t.Fatalf("nodeVer[a] = %d, want 6", got)
	}
	if got := f2.appliedIndex.Load(); got != 6 {
		t.Fatalf("appliedIndex = %d, want 6", got)
	}
	// Post-restore log replay continues from restored table state.
	r := applyAt(f2, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("token 5 < lastMod 6 must reject after restore")
	}
}

func TestFSMRestoreRejectsLegacy(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	legacy, err := f.live.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(legacy))); err == nil {
		t.Fatal("legacy snapshot (no VCTX) must be rejected")
	}
}

func TestFSMRestoreReplayEquivalence(t *testing.T) {
	// The core audit-P1-4 property: a replica restored mid-stream and replaying
	// the same suffix must reach identical accept/reject decisions (and table).
	build := func() []Batch {
		return []Batch{
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}, {Key: "b", Label: "S"}}},
			{Version: 3, Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 1}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}}, Txn: &TxnGuard{Snapshot: 2, SER: true, ReadNodes: []string{"a"}}},
		}
	}
	cont := func(f *fsm, from int) []bool {
		var out []bool
		for i, b := range build()[from:] {
			out = append(out, applyAt(f, b, uint64(from+i+1)).(ApplyResult).Committed)
		}
		return out
	}
	// Uninterrupted.
	full := newFSM(time.Hour)
	want := cont(full, 0)
	// Restored after entry 2.
	part := newFSM(time.Hour)
	cont(part, 0)
	// persist WITHOUT letting further applies leak in: snapshot now.
	raw := persistSnap(t, part)
	res := newFSM(time.Hour)
	if err := res.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	got := cont(res, 2)
	wantTail := want[2:]
	for i := range wantTail {
		if got[i] != wantTail[i] {
			t.Fatalf("divergence at suffix %d: restored=%v uninterrupted=%v", i, got, wantTail)
		}
	}
	if res.nodeVer["a"] != full.nodeVer["a"] {
		t.Fatalf("nodeVer[a]: restored=%d uninterrupted=%d", res.nodeVer["a"], full.nodeVer["a"])
	}
}

func TestFSMSnapshotFrozenDuringConcurrentApply(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Applies after Snapshot() must not appear in what Persist writes.
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}}}, 6)
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if _, ok := f2.nodeVer["b"]; ok {
		t.Fatal("post-Snapshot apply leaked into persisted table")
	}
}

func TestSplitSnapshotBadInputs(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"too short", []byte("ab")},
		{"length overflow", append([]byte{0xFF, 0xFF, 0xFF, 0xFF}, []byte("VCTX...")...)},
		{"graph only (legacy shape)", []byte("VCTR")},
	}
	for _, c := range cases {
		if _, _, err := splitSnapshot(c.data); err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
}
```

- [ ] **Step 2:** `cd server && go test -run 'TestFSMSnapshot|TestFSMRestore' -v .` → FAIL（持久化无 VCTX；legacy 不报错）
- [ ] **Step 3:** 实现——fsm.go：

  1. `graphSnapshot` 加字段并改 `Snapshot()`：

```go
type graphSnapshot struct {
	clone   *vectra.Graph
	nodeVer map[string]uint64
	edgeVer map[edgeVerKey]uint64
	applied uint64
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// Copy the version tables under the same RLock as the graph clone: the
	// snapshot's raft metadata index aligns with this call on the FSM thread
	// (raft v1.8), and Persist runs later on the snapshot thread — it must
	// only ever see this frozen copy.
	return &graphSnapshot{
		clone:   f.live.Clone(),
		nodeVer: maps.Clone(f.nodeVer),
		edgeVer: maps.Clone(f.edgeVer),
		applied: f.appliedIndex.Load(),
	}, nil
}
```

  2. `Persist`（写入外层框架；`encoding/binary` import）：

```go
type verSnapshot struct {
	NodeVer      map[string]uint64
	EdgeVer      map[edgeVerKey]uint64
	AppliedIndex uint64
}

const vctxMagic = "VCTX"

func (s *graphSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := s.clone.ExportSnapshot()
	if err != nil {
		return err
	}
	var verBuf bytes.Buffer
	if err := gob.NewEncoder(&verBuf).Encode(verSnapshot{
		NodeVer: s.nodeVer, EdgeVer: s.edgeVer, AppliedIndex: s.applied,
	}); err != nil {
		return err
	}
	var head [4]byte
	binary.LittleEndian.PutUint32(head[:], uint32(len(data)))
	var fv [2]byte
	binary.LittleEndian.PutUint16(fv[:], 1) // u16 LE format version, matches splitSnapshot
	for _, chunk := range [][]byte{head[:], data, []byte(vctxMagic), fv[:], verBuf.Bytes()} {
		if _, err := sink.Write(chunk); err != nil {
			return err
		}
	}
	return sink.Close()
}
```

（fsm.go 需补 `bytes`、`encoding/binary`、`encoding/gob`、`maps` import。）

  3. `Restore` 开头解析框架（替换现有 `vectra.ImportSnapshot(data)`）：

```go
	graph, vers, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	g, err := vectra.ImportSnapshot(graph)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.live = g
	f.nodeVer = vers.NodeVer
	f.edgeVer = vers.EdgeVer
	f.appliedIndex.Store(vers.AppliedIndex)
	f.dirty = true
	f.version++
	f.mu.Unlock()
```

```go
// splitSnapshot separates the graph payload from the VCTX version-table
// trailer. Legacy snapshots (pre-0.4.0, graph only) are rejected: replicas
// restored from them would validate transactions against an empty table and
// diverge permanently from log-derived replicas.
func splitSnapshot(data []byte) ([]byte, verSnapshot, error) {
	var vs verSnapshot
	if len(data) < 6 { // u32 len + "VCTX" minimum
		return nil, vs, errors.New("server: snapshot too short")
	}
	n := binary.LittleEndian.Uint32(data[:4])
	if int(n)+4+4+2 > len(data) {
		return nil, vs, errors.New("server: snapshot framing corrupt")
	}
	graph := data[4 : 4+n]
	rest := data[4+n:]
	if !bytes.HasPrefix(rest, []byte(vctxMagic)) {
		return nil, vs, errors.New("server: legacy snapshot without VCTX rejected; re-seed from a 0.4.0+ snapshot (see upgrade runbook)")
	}
	rest = rest[len(vctxMagic):]
	if len(rest) < 2 {
		return nil, vs, errors.New("server: VCTX version missing")
	}
	if binary.LittleEndian.Uint16(rest[:2]) != 1 {
		return nil, vs, fmt.Errorf("server: unsupported VCTX version %d", binary.LittleEndian.Uint16(rest[:2]))
	}
	if err := gob.NewDecoder(bytes.NewReader(rest[2:])).Decode(&vs); err != nil {
		return nil, vs, err
	}
	if vs.NodeVer == nil {
		vs.NodeVer = map[string]uint64{}
	}
	if vs.EdgeVer == nil {
		vs.EdgeVer = map[edgeVerKey]uint64{}
	}
	return graph, vs, nil
}
```

- [ ] **Step 4:** `cd server && go test -run 'TestFSMSnapshot|TestFSMRestore' -v .` → PASS；`go test ./...` 全绿（`TestFSMPersistRestore` 走新框架仍过）
- [ ] **Step 5:** `git commit -am "feat(server): VCTX snapshot framing — version table persists/restores with the graph; legacy snapshots rejected"`

### Task 4: 原子束发布 + token 暴露

**Files:**
- Modify: `server/fsm.go`（pubState/publishOnce/Published/PublishedSeq）、`server/api.go`（graph() 头、stats 字段）
- Test: `server/txn_test.go` 追加

**Interfaces:**
- Produces: `fsm.Published() *vectra.Graph`（签名不变，返回束内图）；`fsm.PublishedSeq() uint64`；查询响应头 `X-Vectra-Snapshot`；stats 新键 `appliedIndex`/`publishedSeq`。

- [ ] **Step 1: 失败测试**

```go
func TestPublishedBundleSeqInvariant(t *testing.T) {
	f := newFSM(5 * time.Millisecond)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var observed atomic.Int32
	wg.Add(1)
	go func() { // Reader: every observed (graph, seq) must contain all writes ≤ seq.
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if ps := f.published.Load(); ps != nil {
				observed.Add(1)
				// Write k lives at raft index k; the graph must contain every
				// node whose index is ≤ the published seq.
				for k := 1; uint64(k) <= ps.seq; k++ {
					if _, ok := ps.graph.NodeByKey(fmt.Sprintf("n%03d", k)); !ok {
						t.Errorf("seq=%d but node n%03d (index %d) missing", ps.seq, k, k)
						return
					}
				}
			}
		}
	}()
	for k := 1; k <= 50; k++ {
		applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{
			{Key: fmt.Sprintf("n%03d", k), Label: "S"}}}, uint64(k))
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if observed.Load() < 2 {
		t.Fatalf("reader only observed %d bundles; invariant never exercised", observed.Load())
	}
}
```

（直接读 `f.published` 需要它变成导出字段的束类型——它本就是包内可见；测试与实现同包，直接访问合法。）

- [ ] **Step 2:** `cd server && go test -run TestPublishedBundle -v .` → FAIL（`ps.seq` 未定义）
- [ ] **Step 3:** 实现——fsm.go：

```go
// pubState bundles the published graph with the applied raft index it
// reflects, so readers can never pair a newer seq with an older graph
// (audit P1-2: token must never overshoot graph content).
type pubState struct {
	graph *vectra.Graph
	seq   uint64
}
```

fsm 字段 `published atomic.Pointer[vectra.Graph]` → `published atomic.Pointer[pubState]`；`publishOnce` 采样段改为：

```go
	clone := f.live.Clone()
	version := f.version
	seq := f.appliedIndex.Load()
	f.mu.RUnlock()
	// … clone.Seal() unchanged …
	f.pubMu.Lock()
	if version > f.publishedVer {
		f.publishedVer = version
		f.published.Store(&pubState{graph: clone, seq: seq})
		// … metrics/lastSealNanos unchanged …
```

`Published()` 与新增：

```go
func (f *fsm) Published() *vectra.Graph {
	if ps := f.published.Load(); ps != nil {
		return ps.graph
	}
	return nil
}

// PublishedSeq reports the applied raft index reflected by the current
// published graph (0 before the first publish).
func (f *fsm) PublishedSeq() uint64 {
	if ps := f.published.Load(); ps != nil {
		return ps.seq
	}
	return 0
}
```

api.go：`graph()` 拿束并设头（`strconv` 已 import）：

```go
func (n *Node) graph(w http.ResponseWriter) *vectra.Graph {
	if !n.gateReady(w) {
		return nil
	}
	ps := n.fsm.published.Load()
	if ps == nil {
		writeErr(w, http.StatusServiceUnavailable, "NOT_READY", "snapshot not yet published")
		return nil
	}
	w.Header().Set("X-Vectra-Snapshot", strconv.FormatUint(ps.seq, 10))
	return ps.graph
}
```

`handleStats` 响应 map 增两键：

```go
		"appliedIndex": n.fsm.appliedIndex.Load(),
		"publishedSeq": n.fsm.PublishedSeq(),
```

- [ ] **Step 4:** `go test -run TestPublishedBundle -v .` → PASS；`go test -race ./...` 全绿（`f.Published()` 调用点全部兼容）
- [ ] **Step 5:** `git commit -am "feat(server): publish (graph, seq) as one atomic bundle; expose X-Vectra-Snapshot header and stats indexes"`

### Task 5: POST /v1/txn 端点

**Files:**
- Modify: `server/api.go`（路由 + handleTxn）
- Test: `server/txn_test.go` 追加（走 testCluster HTTP 面）

**Interfaces:**
- Consumes: Task 1-4 全部；`testCluster`/`waitAllPublished`（api_test.go）。
- Produces: `POST /v1/txn`，请求体 `{nodes, edges, edgeDeletes, nodeDeletes, txn:{snapshot, ser, readNodes, readEdges}}`；200 `{committed:true, …counts, index}`；409 `{committed:false, conflicts, conflictTotal}`；400 `TXN_READSET_TOO_LARGE`。

- [ ] **Step 1: 失败测试**

```go
func TestTxnEndpointCommitThenConflict(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	body := func(snapshot uint64, key, val string) []byte {
		b := map[string]any{
			"nodes": []map[string]any{{"key": key, "label": "Svc", "props": map[string]any{"v": val}}},
			"txn":   map[string]any{"snapshot": snapshot},
		}
		raw, _ := json.Marshal(b)
		return raw
	}
	post := func(raw []byte) (int, map[string]any) {
		resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	// Seed one write so the first publish completes (query gate opens), then
	// read the token from a stats response header (RYW pattern).
	seed, _ := json.Marshal(map[string]any{"nodes": []map[string]any{{"key": "seed", "label": "S"}}})
	sresp0, err := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	sresp0.Body.Close()
	hresp, err := http.Get(lead.URL() + "/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	hresp.Body.Close()
	if hresp.StatusCode != 200 {
		t.Fatalf("stats after seed: %d", hresp.StatusCode)
	}
	tok, _ := strconv.ParseUint(hresp.Header.Get("X-Vectra-Snapshot"), 10, 64)
	code, out := post(body(tok, "a", "1"))
	if code != 200 || out["committed"] != true {
		t.Fatalf("first txn: %d %v", code, out)
	}
	idx := uint64(out["index"].(float64))
	// Stale token (< idx) writing the same key must conflict.
	code, out = post(body(idx-1, "a", "2"))
	if code != 409 || out["committed"] != false {
		t.Fatalf("stale txn: %d %v", code, out)
	}
	if out["conflictTotal"].(float64) < 1 {
		t.Fatalf("conflictTotal = %v", out["conflictTotal"])
	}
	// Fresh token commits.
	code, out = post(body(idx, "a", "3"))
	if code != 200 {
		t.Fatalf("fresh txn: %d %v", code, out)
	}
	tc.waitAllPublished(1, 0, 5*time.Second)
}

func TestTxnEndpointNotLeaderAndStats(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != lead {
			follower = tn
			break
		}
	}
	raw, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
		"txn":   map[string]any{"snapshot": 0}})
	resp, err := http.Post(follower.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("follower /v1/txn: status = %d, want 503", resp.StatusCode)
	}
	// Stats exposes both indexes, and the invariant publishedSeq <= appliedIndex.
	sresp, _ := http.Get(lead.URL() + "/v1/stats")
	defer sresp.Body.Close()
	var st map[string]any
	_ = json.NewDecoder(sresp.Body).Decode(&st)
	applied := st["appliedIndex"].(float64)
	published := st["publishedSeq"].(float64)
	if published > applied {
		t.Fatalf("publishedSeq(%v) > appliedIndex(%v)", published, applied)
	}
}

func TestTxnEndpointReadsetCap(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	big := make([]string, 100_001)
	raw, _ := json.Marshal(map[string]any{
		"txn": map[string]any{"snapshot": 0, "readNodes": big},
	})
	resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestTxnSERPhantomOverHTTP(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	seed := map[string]any{"nodes": []map[string]any{
		{"key": "u", "label": "S"}, {"key": "v", "label": "S"}}}
	raw, _ := json.Marshal(seed)
	resp, _ := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(raw))
	resp.Body.Close()
	tc.waitAllPublished(2, 0, 5*time.Second)

	// Token from a query (add node w + edge u->w concurrently first? No:
	// establish base, then read u's neighbors at token T, then bump u).
	nresp, _ := http.Get(lead.URL() + "/v1/neighbors?key=u")
	nresp.Body.Close()
	tok64, _ := strconv.ParseUint(nresp.Header.Get("X-Vectra-Snapshot"), 10, 64)

	// Concurrent auto-commit write bumps nodeVer[u].
	bump, _ := json.Marshal(map[string]any{"edges": []map[string]any{
		{"srcKey": "u", "dstKey": "v", "label": "E"}}})
	bresp, _ := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(bump))
	bresp.Body.Close()

	ser, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "w", "label": "S"}},
		"txn":   map[string]any{"snapshot": tok64, "ser": true, "readNodes": []string{"u"}}})
	resp2, _ := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(ser))
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Fatalf("SER phantom: status = %d, want 409", resp2.StatusCode)
	}
	// SI with the same read set commits (write-skew split).
	si, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "w", "label": "S"}},
		"txn":   map[string]any{"snapshot": tok64, "readNodes": []string{"u"}}})
	resp3, _ := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(si))
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("SI same read set: status = %d, want 200", resp3.StatusCode)
	}
}
```

（`tc.leader`/`tc.closeAll` 若不存在：api_test.go 已有 `tc.nodes`；补 helper 到 txn_test.go，注意 failover 轮询需要无 fatal 的变体：）

```go
// currentLeader returns nil when no leader is elected (failover window).
func (tc *testCluster) currentLeader() *testNode {
	for _, tn := range tc.nodes {
		if tn.node.IsLeader() {
			return tn
		}
	}
	return nil
}

func (tc *testCluster) leader(t *testing.T) *testNode {
	t.Helper()
	if n := tc.currentLeader(); n != nil {
		return n
	}
	t.Fatal("no leader")
	return nil
}

func (tc *testCluster) closeAll() {
	for _, tn := range tc.nodes {
		tn.srv.Close()
		tn.node.Close()
	}
}
```

- [ ] **Step 2:** `cd server && go test -run TestTxnEndpoint -v .` → FAIL（404，无 /v1/txn 路由）
- [ ] **Step 3:** 实现——api.go：`Handler()` 注册 `mux.HandleFunc("POST /v1/txn", n.handleTxn)`；

```go
const maxReadSet = 100_000

func (n *Node) handleTxn(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Nodes       []NodeUpsert `json:"nodes"`
		Edges       []EdgeUpsert `json:"edges"`
		EdgeDeletes []EdgeDelete `json:"edgeDeletes"`
		NodeDeletes []string     `json:"nodeDeletes"`
		Txn         *TxnGuard    `json:"txn"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	if req.Txn == nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "txn field required (use /v1/batch for auto-commit)")
		return
	}
	if len(req.Txn.ReadNodes) > maxReadSet {
		writeErr(w, http.StatusBadRequest, "TXN_READSET_TOO_LARGE",
			fmt.Sprintf("readNodes exceeds %d keys", maxReadSet))
		return
	}
	res, err := n.Apply(r.Context(), Batch{
		Version:     batchVersion,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
		EdgeDeletes: req.EdgeDeletes,
		NodeDeletes: req.NodeDeletes,
		Txn:         req.Txn,
	})
	if err != nil {
		if err == ErrNotLeader {
			if hint := n.LeaderHint(); hint != "" {
				w.Header().Set("X-Vectra-Leader", hint)
			}
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusServiceUnavailable, "NOT_LEADER", "send writes to the leader")
			return
		}
		if errors.Is(err, ErrTimeout) {
			writeErr(w, http.StatusGatewayTimeout, "TIMEOUT", err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	if !res.Committed {
		writeJSON(w, http.StatusConflict, map[string]any{
			"committed":     false,
			"conflicts":     res.Conflicts,
			"conflictTotal": res.ConflictTotal,
			"hint":          "refresh snapshot (X-Vectra-Snapshot) and retry with backoff",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"committed":    true,
		"index":        res.Index,
		"nodesApplied": res.NodesApplied,
		"edgesApplied": res.EdgesApplied,
		"edgesSkipped": res.EdgesSkipped,
		"edgesDeleted": res.EdgesDeleted,
		"nodesDeleted": res.NodesDeleted,
		"nodesSkipped": res.NodesSkipped,
	})
}
```

（conflictTotal 直接来自 `res.ConflictTotal`（Task 2 的 ApplyResult 字段），409 体不再自行计算。）

- [ ] **Step 4:** `go test -run TestTxn -v .` → PASS；`go test -race ./...` 全绿
- [ ] **Step 5:** `git commit -am "feat(server): POST /v1/txn — OCC commit endpoint with deterministic 409 conflicts"`

### Task 6: 集群确定性与 failover

**Files:**
- Test: `server/txn_test.go` 追加

**Interfaces:**
- Consumes: Task 5 的端点与 helper。

- [ ] **Step 1: 失败测试**

```go
func TestTxnClusterDeterminism(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	// Interleave commits and conflicts over HTTP; then compare all replicas'
	// version tables (same package access).
	send := func(snapshot uint64, key string) bool {
		raw, _ := json.Marshal(map[string]any{
			"nodes": []map[string]any{{"key": key, "label": "S"}},
			"txn":   map[string]any{"snapshot": snapshot}})
		resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode == 200 && out["committed"] == true
	}
	send(0, "a") // commits at index 1
	if send(0, "a") {
		t.Fatal("second write to a with stale token must conflict")
	}
	tc.waitAllPublished(1, 0, 5*time.Second)
	// The absolute index depends on preceding config entries; the invariant is
	// that every replica has the SAME nodeVer[a] (>= 1: the write landed).
	var first uint64
	for i, tn := range tc.nodes {
		tn.node.fsm.mu.RLock()
		got := tn.node.fsm.nodeVer["a"]
		tn.node.fsm.mu.RUnlock()
		if got < 1 {
			t.Fatalf("node %s: nodeVer[a]=%d, want >=1", tn.node.cfg.NodeID, got)
		}
		if i == 0 {
			first = got
		} else if got != first {
			t.Fatalf("replica divergence: nodeVer[a] = %d vs %d", first, got)
		}
	}
}

func TestTxnFailoverNoDrift(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	raw, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
		"txn":   map[string]any{"snapshot": 0}})
	resp, _ := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("seed txn status = %d", resp.StatusCode)
	}
	// Failover: stop the leader, wait for a new one (poll the fatal-free
	// variant — the no-leader window is expected), retry with a stale token.
	lead.srv.Close()
	lead.node.Close()
	deadline := time.Now().Add(10 * time.Second)
	var next *testNode
	for time.Now().Before(deadline) {
		if n := tc.currentLeader(); n != nil && n != lead {
			next = n
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if next == nil {
		t.Fatal("no new leader elected")
	}
	stale, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
		"txn":   map[string]any{"snapshot": 0}})
	resp2, err := http.Post(next.URL()+"/v1/txn", "application/json", bytes.NewReader(stale))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Fatalf("post-failover stale token: status = %d, want 409", resp2.StatusCode)
	}
}
```

（`currentLeader`/`leader`/`closeAll` helper 已在 Task 5 定义；failover 轮询用无 fatal 的 `currentLeader`。）

- [ ] **Step 1b: 并发冲突恰一提交测试**（同文件追加）

```go
func TestTxnConcurrentExactlyOneCommits(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	seed, _ := json.Marshal(map[string]any{"nodes": []map[string]any{{"key": "a", "label": "S"}}})
	sr, _ := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(seed))
	sr.Body.Close()

	var wg sync.WaitGroup
	results := make([]int, 4) // HTTP status per contender
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]any{
				"nodes": []map[string]any{{"key": "a", "label": "S", "props": map[string]any{"w": i}}},
				"txn":   map[string]any{"snapshot": 1}})
			resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
			results[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, c := range results {
		if c == 200 {
			ok++
		} else if c != 409 {
			t.Fatalf("unexpected status %d", c)
		}
	}
	if ok != 1 {
		t.Fatalf("committed = %d, want exactly 1 (statuses: %v)", ok, results)
	}
}
```

- [ ] **Step 2:** `go test -run 'TestTxnCluster|TestTxnFailover' -v .` → 预期 FAIL（若 Task 5 已全绿则可能直接 PASS——此时把它当作回归测试保留，commit）
- [ ] **Step 3:** 如有失败：排查多副本 nodeVer 不一致（通常是 bump 在非 leader 副本路径缺失——Apply 在所有副本都执行，理论上一致）；修复后重跑
- [ ] **Step 4:** `go test -race ./...` 全绿
- [ ] **Step 5:** `git commit -am "test(server): cluster-wide txn determinism and failover drift checks"`

### Task 7: 基准 + 文档 + 版本发布

**Files:**
- Create: `server/txn_bench_test.go`
- Modify: `README.md`、`README.zh-CN.md`；版本串 6 处（见 Global Constraints）

- [ ] **Step 1: fsm 级基准**（txn_bench_test.go）

```go
package server

import "testing"

func benchApply(b *testing.B, tg *TxnGuard, reads int) {
	f := newFSM(time.Hour)
	keys := make([]string, reads)
	nodes := make([]NodeUpsert, reads+1)
	for i := range keys {
		keys[i] = "r" + strconv.Itoa(i)
		if i < reads {
			nodes[i] = NodeUpsert{Key: keys[i], Label: "S"}
		}
	}
	nodes[reads] = NodeUpsert{Key: "w", Label: "S"}
	if tg != nil {
		tg.ReadNodes = keys[:reads]
		tg.Snapshot = ^uint64(0) // never conflicts: measure pure validation cost
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := encodeBatch(Batch{Version: 3, Nodes: nodes, Txn: tg})
		if err != nil {
			b.Fatal(err)
		}
		f.Apply(&raft.Log{Data: data, Index: uint64(i + 1)})
	}
}

func BenchmarkFSMAutoCommit(b *testing.B) { benchApply(b, nil, 16) }
func BenchmarkFSMTxnSI(b *testing.B)      { benchApply(b, &TxnGuard{}, 0) }
func BenchmarkFSMTxnSER16(b *testing.B)   { benchApply(b, &TxnGuard{SER: true}, 16) }
```

（`time`/`raft`/`strconv` import。）

Run: `cd server && go test -bench 'BenchmarkFSM(Txn|AutoCommit)' -benchtime 2s .`
验收：SI/SER 与 AutoCommit 的 ns/op 差 < 25%（读集 16 键）。

- [ ] **Step 2: README 双语**——新增 Transactions 小节（英文/中文对译）：
  - 用法：读任意查询端点取 `X-Vectra-Snapshot` 头 → 组装 `/v1/txn` 请求（`txn.snapshot`/`txn.ser`/`txn.readNodes`）→ 409 后 full-jitter 退避重试（`sleep = rand(0, min(500ms, 5ms·2^attempt))`，参考 AWS Exponential Backoff and Jitter）；
  - 读集覆盖表（哪些端点记什么键；`/v1/path`、`/v1/stats`、`/v1/cycles` 为 SI-only）；
  - 隔离级别说明：SI=first-committer-wins（盲写也拒，强于 badger/FDB 默认）；SER=加验读集；
  - read-your-writes：提交响应 `index` → 轮询 `/v1/stats` 的 `publishedSeq`；
  - 基准口径说明：v0.4.0 以 fsm 级基准（`BenchmarkFSMAutoCommit/TxnSI/TxnSER16`，ns/op 差 <25%）作为验证开销验收线，vectra-bench 的 txnsi/txnser HTTP 负载为后续工作；
  - 升级 runbook：0.4.0 快照含 VCTX 框架，**旧二进制读不了新快照、新二进制拒绝旧快照**——小部署停机升级，升级后 leader 拍新快照；
  - Coverage 行按 Step 4 实测更新（双语同步）。
- [ ] **Step 3: 版本串** 0.2.1 → 0.4.0（6 处，位置见 Global Constraints；`package-lock.json` 两处用 `sed` 或手工，改完 `cd mcp/ts && npm pkg get version` 验证）
- [ ] **Step 4: 覆盖率**：三模块 `go test -cover ./...` 记录各包数字；server < 85% 则补 `cover_extra_test.go` 风格的防御分支测试（坏 VCTX 框架、u32 溢出、txn 缺失字段 400 等）直至达标；README Coverage 行如实更新
- [ ] **Step 5:** 三模块 `go test -race ./...` + `go vet ./...` 全绿 → `git commit -am "chore(release): v0.4.0 — OLTP transactions, docs, coverage refresh"`
- [ ] **Step 6: 确认门**——merge `feat/oltp-txn` → main、push、`gh release create v0.4.0` **必须先请用户确认**（release notes 要点：/v1/txn、X-Vectra-Snapshot、409 语义、快照格式不兼容与升级 runbook、SI 档位强度说明）

## Self-Review

- Spec 覆盖：§2.1 版本表+有效写集（Task 2）、§2.2 token/原子束（Task 4）、§2.3 Batch v3+上限（Task 1/5）、§2.4 验证（Task 2）、§2.5 VCTX（Task 3）、§2.6 /v1/txn（Task 5）、§2.7 覆盖表与 SI-only（Task 7 README + Task 5 SER 测试）、§2.8 轮询文档化（Task 7）、§3 基准（Task 7，fsm 级；HTTP 负载推迟已记入设计决策并在 README 说明替代口径）、§4 测试矩阵（逐行落位，含 503/stats/并发混载/重放幂等/坏框架输入/并发恰一提交）、§5 覆盖率（Task 7 Step 4）、§6 版本与兼容（Task 7 Step 3 + README runbook）✓
- 占位符扫描：所有代码块完整可编译；`ConflictTotal` 已落入 Task 2 的 ApplyResult/validateTxn/截断测试，Task 5 的 409 体直接使用 `res.ConflictTotal`，无残留 marker 方案 ✓
- 类型一致性：`edgeVerKey`（级联枚举经 `NodeByID` 反查 string key）/`TxnGuard`/`pubState`/`verSnapshot`/`maxConflicts`/`maxReadSet`/`ApplyResult{Committed, Index, Conflicts, ConflictTotal}` 各任务引用一致；`currentLeader`（无 fatal）/`leader`（fatal 版）/`closeAll` 在 Task 5 定义、Task 6 复用 ✓
- `Txn{Snapshot:0}` 语义：严格解释（非 nil 一律验证，0=空图基线，与既有写冲突）——spec §2.3 与 Task 1 注释已同步 ✓
- 既有行为保持：`/v1/batch`、`Published()` 签名、`TestFSMPersistRestore`/`TestFSMApplyUpsertIdempotent` 等不受影响（`Index` 零值 → lastMod 0 → token 0 不冲突；Persist 新框架对同包测试透明）；mcp 模块零改动已 grep 验证 ✓
- Plan 审计（agent）P0-1~P0-4、P1-5~P1-8 已全部合入本文件；P2 项（import 备注、503/stats/并发扩展/幂等重放/坏框架/观察数断言/死代码清理）一并处理 ✓
