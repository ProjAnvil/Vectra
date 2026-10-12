# 方案 A：OLTP 语义层（边 upsert / 删除 + 节点删除）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 Vectra 补全批量写语义：边的替换式更新（`UpsertEdge`，三元组唯一）、边删除与节点删除（级联，墓碑记录），贯穿 server（raft Batch v2）与 MCP（inject_batch 删除数组），快照格式不变。

**Architecture:** 写态维护 `(src,dst,label)` 边键索引与墓碑数组；删除只打标，`Seal()`/`ExportSnapshot()` 时压实并重排 ID（byKey 重建）；server 侧删除以逻辑键寻址保证 raft 确定性回放；墓碑在快照编码前压实，v2 格式与 v1 读兼容不动。

**Tech Stack:** Go stdlib（根模块零三方依赖红线）；server/mcp 各自模块（hashicorp/raft、MCP SDK 不变）。

**Spec:** 本文件「设计决策」一节（源自 2026-10-12 OLTP 可行性调研·方案 A，用户已批准）。

## Global Constraints

- 根模块保持零第三方依赖（不得出现 go.sum）。
- 快照格式 v2 不变：墓碑在编码前压实；store 包零改动。
- gob Batch 演进只加字段；FSM 应用顺序固定：节点 upsert → 边 upsert → 边 delete → 点 delete（raft 确定性回放）。
- 语义锁定：`UpsertEdge` 保证 `(src,dst,label)` 唯一（多余平行边删除、保留最近一条、ID 不变）；`DeleteNode` 级联删边（DETACH DELETE 语义）、同 key 重新 `AddNode` 可复活（ID 复用）；`AddEdge` 保持纯追加（多图语义，不破坏）。
- 压实后节点/边 ID 可能重排——所有对外文档必须写明。
- 注释为英文 doc comment（2026-10-12 全仓注释英文化为基线；计划文档中的中文注释代码块以英文落地）；测试用 stdlib 断言风格（参照 vectra_test.go）。
- 每个 task 走红→绿→commit；提交消息对齐 git log 风格（feat(core)/feat(server)/feat(mcp)/docs）。在 `feat/oltp-upsert-delete` 分支上按 task 提交（executing-plans 禁止直接在 main 实现）；完成后走 finishing-a-development-branch 呈现合并选项。

## 设计决策（Spec）

| 决策 | 选择 | 理由 |
|---|---|---|
| 边的更新入口 | 新增 `UpsertEdge`，`AddEdge` 不动 | 保持多图追加语义零破坏；权重更新是简单图语义 |
| UpsertEdge 对平行边的处理 | 保留最近一条、删除其余 | 确定性语义：调用后三元组必然唯一 |
| 删除寻址 | 逻辑键 `(srcKey,dstKey,label)`，不用 EdgeID | ID 压实后会重排，raft 日志/HTTP 不能依赖 |
| DeleteNode 策略 | 级联删边 | 对齐 Neo4j DETACH DELETE；依赖图删服务必须连带删边 |
| 墓碑的生命周期 | 只存在于写态；Seal/ExportSnapshot 压实 | 快照格式零改动；live 图中墓碑由发布器的 Clone+Seal 消化 |
| 节点复活 | 同 key AddNode 清墓碑、复用 ID | 与 AddNode 既有幂等语义连续 |
| 缺失键的删除 | 节点不存在 → `ErrNodeNotFound`；标签不存在 → `(0, nil)` | 删边幂等；引用不存在实体报错与 AddEdge 一致 |

---

### Task 1: 写态边键索引 + UpsertEdge

**Files:**
- Modify: `vectra.go`（Graph 结构、New、AddNode、AddEdge、新增 resolveEdge/appendEdge/UpsertEdge）
- Test: `upsert_delete_test.go`（新建）

**Interfaces:**
- Produces: `func (g *Graph) UpsertEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error)`；内部类型 `edgeKey struct{ src, dst NodeID; label uint32 }`；内部字段 `edgeIdx map[edgeKey][]EdgeID`、`tombE/tombN []bool`、`deadE/deadN int`。后续 task 依赖这些字段名。

- [ ] **Step 1: 写失败测试**（新建 `upsert_delete_test.go`）

```go
package vectra_test

import (
	"errors"
	"testing"

	"github.com/projanvil/vectra"
)

func TestUpsertEdgeReplacesPropsAndKeepsID(t *testing.T) {
	g := mustBuild(t)
	id0, err := g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2})
	if err != nil {
		t.Fatal(err)
	}
	if id0 != 0 { // mustBuild 的第一条边即 gateway->auth CALLS
		t.Errorf("upsert existing edge returned id %d, want 0", id0)
	}
	if g.EdgeCount() != 4 {
		t.Errorf("EdgeCount = %d, want 4 (replace must not add)", g.EdgeCount())
	}
	e, ok := g.EdgeByID(0)
	if !ok || e.Props["weight"] != 2 {
		t.Errorf("edge after upsert = %+v", e)
	}
}

func TestUpsertEdgeCreatesWhenAbsent(t *testing.T) {
	g := mustBuild(t)
	if _, err := g.UpsertEdge("auth", "gateway", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if g.EdgeCount() != 5 {
		t.Errorf("EdgeCount = %d, want 5", g.EdgeCount())
	}
}

func TestUpsertEdgeCollapsesParallelEdges(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := g.AddEdge("a", "b", "E", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := g.UpsertEdge("a", "b", "E", map[string]any{"i": 9})
	if err != nil {
		t.Fatal(err)
	}
	if id != 2 { // 最近一条平行边胜出
		t.Errorf("kept id = %d, want 2", id)
	}
	if g.EdgeCount() != 1 {
		t.Errorf("EdgeCount = %d, want 1", g.EdgeCount())
	}
	e, _ := g.EdgeByID(2)
	if e.Props["i"] != 9 {
		t.Errorf("props = %v, want i=9", e.Props)
	}
}

func TestUpsertEdgeUnknownNode(t *testing.T) {
	g := vectra.New()
	if _, err := g.UpsertEdge("x", "y", "E", nil); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("err = %v, want ErrNodeNotFound", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run TestUpsertEdge -v ./...`
Expected: FAIL，`g.UpsertEdge undefined`

- [ ] **Step 3: 最小实现**（vectra.go）

Graph 结构体追加字段（vectra.go:64-73 处）：

```go
	edgeIdx map[edgeKey][]EdgeID // (src,dst,label) -> 存活边 ID（追加序）
	tombE   []bool               // 与 edges 平行：true = 已删（Seal 时压实）
	tombN   []bool               // 与 nodes 平行：true = 已删（可复活）
	deadE   int                  // tombE 中 true 计数（可见边 = len(edges)-deadE）
	deadN   int                  // tombN 中 true 计数
```

新增类型与 `New()` 初始化：

```go
type edgeKey struct {
	src, dst NodeID
	label    uint32
}

func New() *Graph {
	return &Graph{byKey: map[string]NodeID{}, labIDs: map[string]uint32{}, edgeIdx: map[edgeKey][]EdgeID{}}
}
```

`AddNode`（vectra.go:105-118）改为复活感知；新节点路径追加墓碑标记：

```go
func (g *Graph) AddNode(key, label string, props map[string]any) (NodeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	if id, ok := g.byKey[key]; ok {
		if g.tombN[id] { // 复活：同 key 重新写入，清墓碑、复用 ID
			g.tombN[id] = false
			g.deadN--
		}
		g.nodes[id].Label = label
		g.nodes[id].Props = copyProps(props)
		return id, nil
	}
	id := NodeID(len(g.nodes))
	g.nodes = append(g.nodes, Node{ID: id, Key: key, Label: label, Props: copyProps(props)})
	g.tombN = append(g.tombN, false)
	g.byKey[key] = id
	return id, nil
}
```

（doc comment 保留原文，另加一句：「已删除（墓碑）节点同 key 重写即复活，ID 复用。」）

`AddEdge` 拆出共用尾部，并加墓碑节点守卫：

```go
func (g *Graph) AddEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, dst, lid, err := g.resolveEdge(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	return g.appendEdge(src, dst, lid, props), nil
}

// resolveEdge 解析边端点与标签；已删除（墓碑）节点视同不存在。
func (g *Graph) resolveEdge(srcKey, dstKey, label string) (src, dst NodeID, lid uint32, err error) {
	src, ok := g.byKey[srcKey]
	if !ok || g.tombN[src] {
		return 0, 0, 0, ErrNodeNotFound
	}
	dst, ok = g.byKey[dstKey]
	if !ok || g.tombN[dst] {
		return 0, 0, 0, ErrNodeNotFound
	}
	return src, dst, g.internLabel(label), nil
}

// appendEdge 是 AddEdge/UpsertEdge 的共用尾部：追加边记录并登记键索引。
func (g *Graph) appendEdge(src, dst NodeID, lid uint32, props map[string]any) EdgeID {
	g.edges = append(g.edges, edgeRec{src: src, dst: dst, label: lid, props: copyProps(props)})
	g.tombE = append(g.tombE, false)
	id := EdgeID(len(g.edges) - 1)
	k := edgeKey{src: src, dst: dst, label: lid}
	g.edgeIdx[k] = append(g.edgeIdx[k], id)
	return id
}
```

新增 `UpsertEdge`：

```go
// UpsertEdge 按 (src, dst, label) 三元组替换式写入：已有同键边则替换其
// Props（保留边 ID；多余平行边全部删除），没有则追加新边。与 AddEdge
// （纯追加、多图语义）相对，本方法保证三元组唯一——权重类属性
// （关系强度/事件计数）的更新入口。
func (g *Graph) UpsertEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, dst, lid, err := g.resolveEdge(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	k := edgeKey{src: src, dst: dst, label: lid}
	ids := g.edgeIdx[k]
	if len(ids) == 0 {
		return g.appendEdge(src, dst, lid, props), nil
	}
	for _, id := range ids[:len(ids)-1] { // 多余平行边删除，保留最近一条
		g.tombE[id] = true
		g.deadE++
	}
	keep := ids[len(ids)-1]
	g.edges[keep].props = copyProps(props) // 整体替换（与 AddNode 的契约一致）
	g.edgeIdx[k] = []EdgeID{keep}
	return keep, nil
}
```

注意：本 task 尚未实现 DeleteEdge/DeleteNode，`tombN` 守卫与 `deadE` 维护先就位（tombN 恒 false，不影响现有测试）。

**复审修正（必须随本 task 落地）**：`Clone()` 未密封分支（vectra.go:357-365）直接 append nodes/edges、绕过 AddNode/AddEdge——若墓碑数组不随拷贝，本 task 加的 `tombN[src]` 守卫在克隆图上会立刻越界 panic（既有 `TestClonePreSealIndependentEvolution` 的 `c.AddEdge("db","auth",...)` 即命中）。原计划放到 Task 3 的深拷贝提前到本 task，在 `c.indexKeys = slices.Clone(g.indexKeys)` 之后、`return c` 之前追加：

```go
	c.tombE = slices.Clone(g.tombE)
	c.tombN = slices.Clone(g.tombN)
	c.deadE, c.deadN = g.deadE, g.deadN
	c.edgeIdx = make(map[edgeKey][]EdgeID, len(g.edgeIdx))
	for k, ids := range g.edgeIdx {
		c.edgeIdx[k] = slices.Clone(ids)
	}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./...`
Expected: 全部 PASS（含既有测试——AddEdge 行为未变）

- [ ] **Step 5: Commit**

```bash
git add vectra.go upsert_delete_test.go
git commit -m "feat(core): UpsertEdge — replace-by-key edge writes with (src,dst,label) uniqueness"
```

---

### Task 2: DeleteEdge / DeleteNode + 可见计数 + 查询守卫

**Files:**
- Modify: `vectra.go`（新增 DeleteEdge/DeleteNode；NodeCount/EdgeCount/Stats/NodeByID/EdgeByID 墓碑感知）
- Test: `upsert_delete_test.go`（追加）

**Interfaces:**
- Consumes: Task 1 的 `edgeIdx`/`tombE`/`tombN`/`deadE`/`deadN`。
- Produces: `func (g *Graph) DeleteEdge(srcKey, dstKey, label string) (int, error)`、`func (g *Graph) DeleteNode(key string) (int, error)`（返回级联删除的边数）。Task 4 的 FSM 依赖这两个签名。

- [ ] **Step 1: 写失败测试**（追加到 upsert_delete_test.go）

```go
func TestDeleteEdgeIdempotent(t *testing.T) {
	g := mustBuild(t)
	n, err := g.DeleteEdge("gateway", "auth", "CALLS")
	if err != nil || n != 1 {
		t.Fatalf("DeleteEdge = %d, %v; want 1, nil", n, err)
	}
	if g.EdgeCount() != 3 {
		t.Errorf("EdgeCount = %d, want 3", g.EdgeCount())
	}
	if n, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil || n != 0 {
		t.Errorf("re-delete = %d, %v; want 0, nil", n, err)
	}
	if _, err := g.DeleteEdge("x", "auth", "CALLS"); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("missing node err = %v, want ErrNodeNotFound", err)
	}
}

func TestDeleteNodeCascadesAndResurrects(t *testing.T) {
	g := mustBuild(t)
	n, err := g.DeleteNode("gateway")
	if err != nil || n != 2 {
		t.Fatalf("DeleteNode = %d, %v; want 2 cascaded edges", n, err)
	}
	if g.NodeCount() != 3 || g.EdgeCount() != 2 {
		t.Fatalf("counts = %d/%d, want 3/2", g.NodeCount(), g.EdgeCount())
	}
	if _, ok := g.NodeByKey("gateway"); ok {
		t.Error("deleted node still visible")
	}
	if _, err := g.AddEdge("gateway", "auth", "CALLS", nil); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("AddEdge on deleted node: %v, want ErrNodeNotFound", err)
	}
	id, err := g.AddNode("gateway", "Service", nil) // 复活
	if err != nil {
		t.Fatal(err)
	}
	if id != 0 {
		t.Errorf("resurrected id = %d, want 0", id)
	}
	if g.NodeCount() != 4 {
		t.Errorf("NodeCount = %d, want 4", g.NodeCount())
	}
}

func TestMutationAfterSealRejected(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.UpsertEdge("gateway", "auth", "CALLS", nil); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("UpsertEdge after seal: %v", err)
	}
	if _, err := g.DeleteEdge("gateway", "auth", "CALLS"); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("DeleteEdge after seal: %v", err)
	}
	if _, err := g.DeleteNode("gateway"); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("DeleteNode after seal: %v", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestDelete|TestMutation' -v .`
Expected: FAIL，`g.DeleteEdge undefined`

- [ ] **Step 3: 实现**（vectra.go）

```go
// DeleteEdge 删除 (src, dst, label) 的全部存活平行边（幂等：不存在的
// 标签删 0 条不算错误）。返回实际删除数。
func (g *Graph) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, ok := g.byKey[srcKey]
	if !ok || g.tombN[src] {
		return 0, ErrNodeNotFound
	}
	dst, ok := g.byKey[dstKey]
	if !ok || g.tombN[dst] {
		return 0, ErrNodeNotFound
	}
	lid, ok := g.labIDs[label]
	if !ok {
		return 0, nil // 标签从未注册：必然无匹配，幂等成功
	}
	k := edgeKey{src: src, dst: dst, label: lid}
	for _, id := range g.edgeIdx[k] {
		g.tombE[id] = true
	}
	n := len(g.edgeIdx[k])
	g.deadE += n
	delete(g.edgeIdx, k)
	return n, nil
}

// DeleteNode 删除节点并级联删除其全部关联边（DETACH DELETE 语义）。
// 按边键索引扫描，代价 O(存活边键数)。同 key 重新 AddNode 可复活
// （ID 复用）。返回级联删除的边数。
func (g *Graph) DeleteNode(key string) (int, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	id, ok := g.byKey[key]
	if !ok || g.tombN[id] {
		return 0, ErrNodeNotFound
	}
	removed := 0
	for k, ids := range g.edgeIdx { // range 中 delete 安全（Go 语义）
		if k.src != id && k.dst != id {
			continue
		}
		for _, eid := range ids {
			g.tombE[eid] = true
		}
		g.deadE += len(ids)
		removed += len(ids)
		delete(g.edgeIdx, k)
	}
	g.tombN[id] = true
	g.deadN++
	return removed, nil
}
```

可见计数与查询守卫（改写既有方法）：

```go
func (g *Graph) NodeCount() int {
	if g.sealed {
		return g.snap.NodeCount()
	}
	return len(g.nodes) - g.deadN
}

func (g *Graph) EdgeCount() int {
	if g.sealed {
		return g.snap.EdgeCount()
	}
	return len(g.edges) - g.deadE
}
```

`Stats()` 未密封分支：`st := Stats{Nodes: g.NodeCount(), Edges: g.EdgeCount()}`。
`NodeByID` 未密封分支在返回前加 `if g.tombN[id] { return Node{}, false }`；`EdgeByID` 未密封分支加 `if g.tombE[id] { return Edge{}, false }`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./...`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add vectra.go upsert_delete_test.go
git commit -m "feat(core): DeleteEdge/DeleteNode with tombstones, cascade delete, visible counts"
```

---

### Task 3: Seal/ExportSnapshot 压实 + ID 重排 + Clone 保全

**Files:**
- Modify: `vectra.go`（新增 compact；改写 Seal、ExportSnapshot、Clone 未密封分支；包注释补写语义）
- Test: `upsert_delete_test.go`（追加）

**Interfaces:**
- Consumes: Task 1/2 的墓碑字段。
- Produces: `func (g *Graph) compact() (nodes []engine.NodeRec, edges []engine.EdgeRec, remap []NodeID)`（内部方法；remap 中被删节点为 `^NodeID(0)`）。快照字节不含墓碑（Task 4 的 FSM 快照路径依赖此性质）。

- [ ] **Step 1: 写失败测试**（追加到 upsert_delete_test.go）

```go
func TestSealCompactionRenumbersAndRebuildsKeyIndex(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range [][3]string{{"a", "b", "E"}, {"b", "c", "E"}} {
		if _, err := g.AddEdge(e[0], e[1], e[2], nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.DeleteNode("b"); err != nil { // 级联删两条边
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	st := g.Stats()
	if st.Nodes != 2 || st.Edges != 0 {
		t.Fatalf("Stats = %+v, want 2/0", st)
	}
	n, ok := g.NodeByKey("c") // 原 ID 2 → 压实后 1
	if !ok || n.ID != 1 {
		t.Errorf("NodeByKey(c) = %+v, want id 1", n)
	}
	if _, ok := g.NodeByKey("b"); ok {
		t.Error("deleted key still present after seal")
	}
}

func TestExportSnapshotCompactsTombstones(t *testing.T) {
	g := mustBuild(t)
	if _, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil {
		t.Fatal(err)
	}
	data, err := g.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.ImportSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if g2.NodeCount() != 4 || g2.EdgeCount() != 3 {
		t.Fatalf("imported = %d/%d, want 4/3", g2.NodeCount(), g2.EdgeCount())
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	ns, err := g2.NeighborsOf("gateway", vectra.DirOut, "CALLS")
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 1 || ns[0].Key != "ledger" {
		t.Errorf("neighbors = %+v, want only ledger", ns)
	}
}

func TestClonePreservesTombstoneState(t *testing.T) {
	g := mustBuild(t)
	if _, err := g.DeleteNode("gateway"); err != nil {
		t.Fatal(err)
	}
	c := g.Clone()
	if c.NodeCount() != 3 || c.EdgeCount() != 2 {
		t.Fatalf("clone counts = %d/%d, want 3/2", c.NodeCount(), c.EdgeCount())
	}
	if _, err := c.DeleteNode("auth"); err != nil {
		t.Fatal(err) // 克隆体上继续删除可用（级联 auth->db）
	}
	if err := c.Seal(); err != nil {
		t.Fatal(err)
	}
	st := c.Stats()
	if st.Nodes != 2 || st.Edges != 1 { // ledger->db 仍然存活
		t.Errorf("clone stats = %+v, want 2/1", st)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test -run 'TestSealCompaction|TestExportSnapshotCompacts|TestClonePreserves' -v .`
Expected: FAIL（压实前的 Seal 会让 Stats 显示 3/2、byKey 指向旧 ID；ExportSnapshot 会带出墓碑边）

- [ ] **Step 3: 实现**（vectra.go）

新增 `compact`：

```go
// compact 返回压实后的原料记录：跳过墓碑节点/边，端点被删的边一并
// 丢弃（防御导入路径），节点与边 ID 重新编号。remap 为旧节点 ID ->
// 新节点 ID（被删节点为 ^NodeID(0)）。Seal 与 ExportSnapshot 共用。
func (g *Graph) compact() (nodes []engine.NodeRec, edges []engine.EdgeRec, remap []NodeID) {
	remap = make([]NodeID, len(g.nodes))
	nodes = make([]engine.NodeRec, 0, len(g.nodes)-g.deadN)
	for i, n := range g.nodes {
		if g.tombN[i] {
			remap[i] = ^NodeID(0)
			continue
		}
		remap[i] = NodeID(len(nodes))
		nodes = append(nodes, engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props})
	}
	edges = make([]engine.EdgeRec, 0, len(g.edges)-g.deadE)
	for i, e := range g.edges {
		if g.tombE[i] || remap[e.src] == ^NodeID(0) || remap[e.dst] == ^NodeID(0) {
			continue
		}
		edges = append(edges, engine.EdgeRec{
			Src: uint32(remap[e.src]), Dst: uint32(remap[e.dst]),
			LabelID: e.label, Props: e.props,
		})
	}
	return nodes, edges, remap
}
```

改写 `Seal()`（vectra.go:213-233）：

```go
// Seal 压实墓碑（节点与边 ID 可能重排）后构建查询所需的全部不可变
// 结构（正/反向 CSR、Tarjan SCC、正/反向凝聚 DAG、成员表、自环标记、
// 属性索引），此后图只读、并发读免锁。
func (g *Graph) Seal() error {
	if g.sealed {
		return nil
	}
	nodes, edges, remap := g.compact()
	snap, err := engine.Seal(nodes, edges, g.labels, g.indexKeys)
	if err != nil {
		return err
	}
	for i, n := range g.nodes { // byKey 重指向压实后的新 ID
		if remap[i] != ^NodeID(0) {
			g.byKey[n.Key] = remap[i]
		} else {
			delete(g.byKey, n.Key)
		}
	}
	g.snap = snap
	g.sealed = true
	g.nodes, g.edges = nil, nil // 写态已迁移进快照，释放
	g.edgeIdx, g.tombE, g.tombN = nil, nil, nil
	return nil
}
```

改写 `ExportSnapshot()` 未密封分支（sealed 分支不动）：

```go
	nodesC, edgesC, _ := g.compact()
	nodes := make([]store.Node, len(nodesC))
	for i, n := range nodesC {
		nodes[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]store.Edge, 0, len(edgesC))
	for _, e := range edgesC {
		edges = append(edges, store.Edge{
			SrcKey: nodesC[e.Src].Key,
			DstKey: nodesC[e.Dst].Key,
			Label:  g.labels[e.LabelID],
			Props:  e.Props,
		})
	}
```

`Clone()` 不改（墓碑深拷贝已在 Task 1 随复审修正提前落地）。

包注释（vectra.go:1-7）第三行后加一句：「写入语义：AddNode 幂等、UpsertEdge 三元组唯一、AddEdge 纯追加；删除以墓碑记录，Seal/快照导出时压实（节点与边 ID 可能重排）。」

- [ ] **Step 4: 跑测试确认通过（含全量回归）**

Run: `go test ./... && go test -race ./...`
Expected: 全部 PASS（clone_test / integration_snap_test / reference_test 均不回归）

- [ ] **Step 5: Commit**

```bash
git add vectra.go upsert_delete_test.go
git commit -m "feat(core): compaction at Seal/ExportSnapshot — tombstones dropped, IDs renumbered, key index rebuilt"
```

---

### Task 4: server Batch v2（删除项 + FSM 应用 + 指标）

**Files:**
- Modify: `server/batch.go`（batchVersion 2、EdgeDelete 类型、Batch 字段、注释）
- Modify: `server/fsm.go`（Apply 顺序、ApplyResult/Metrics 字段）
- Modify: `server/api.go`（expvar 注册两项新指标）
- Test: `server/fsm_test.go`（追加）

**Interfaces:**
- Consumes: Task 2 的 `DeleteEdge`/`DeleteNode` 签名。
- Produces: `type EdgeDelete struct{ SrcKey, DstKey, Label string }`；`Batch.EdgeDeletes []EdgeDelete`、`Batch.NodeDeletes []string`；`ApplyResult.EdgesDeleted/NodesDeleted/NodesSkipped int`。Task 5 的 HTTP 与 Task 6 无直接依赖（MCP 走嵌入式 API）。

- [ ] **Step 1: 写失败测试**（追加到 server/fsm_test.go）

```go
func TestFSMApplyDeletes(t *testing.T) {
	f := newFSM(time.Hour)
	f.Apply(&raft.Log{Data: mustGob(t, sampleBatch())}) // 3 节点 / 2 有效边

	del := Batch{Version: 2,
		EdgeDeletes: []EdgeDelete{
			{SrcKey: "gateway", DstKey: "auth", Label: "CALLS"},
			{SrcKey: "ghost", DstKey: "db", Label: "QUERY"}, // 引用不存在节点 → 跳过
		},
		NodeDeletes: []string{"db"}, // 级联删 auth->db
	}
	r := f.Apply(&raft.Log{Data: mustGob(t, del)}).(ApplyResult)
	if r.EdgesDeleted != 2 || r.NodesDeleted != 1 || r.EdgesSkipped != 1 {
		t.Fatalf("Apply = %+v, want edgesDeleted=2 nodesDeleted=1 skipped=1", r)
	}
	waitPublished(t, f, 2, 0, 2*time.Second) // 剩 gateway/auth，边全没了
	if f.metrics.DeletedEdges.Load() != 2 || f.metrics.DeletedNodes.Load() != 1 {
		t.Errorf("metrics = %d/%d, want 2/1",
			f.metrics.DeletedEdges.Load(), f.metrics.DeletedNodes.Load())
	}
}

func TestFSMApplyDeleteDeterministicReplay(t *testing.T) {
	seq := []Batch{sampleBatch(), {Version: 2, NodeDeletes: []string{"auth"}}}
	stats := func() (int, int) {
		f := newFSM(time.Hour)
		for _, b := range seq {
			f.Apply(&raft.Log{Data: mustGob(t, b)})
		}
		f.sealNow()
		st := f.Published().Stats()
		return st.Nodes, st.Edges
	}
	n1, e1 := stats()
	n2, e2 := stats()
	if n1 != n2 || e1 != e2 {
		t.Fatalf("replay diverged: %d/%d vs %d/%d", n1, e1, n2, e2)
	}
	if n1 != 2 || e1 != 0 {
		t.Fatalf("stats = %d/%d, want 2/0 (auth 级联删两条边)", n1, e1)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd server && go test -run TestFSMApplyDelete -v .`
Expected: FAIL，`undefined: EdgeDelete`

- [ ] **Step 3: 实现**

`server/batch.go`：

```go
// batchVersion 是变更集格式版本。演进规则：只加字段（gob 兼容）。
const batchVersion = 2

// NodeUpsert / EdgeUpsert 是变更集的原子项。语义：节点 key 已存在则
// 更新属性；边按 AddEdge 追加（多边允许）。
type NodeUpsert struct {
	Key   string
	Label string
	Props map[string]any
}

type EdgeUpsert struct {
	SrcKey string
	DstKey string
	Label  string
	Props  map[string]any
}

// EdgeDelete 以逻辑键 (srcKey, dstKey, label) 寻址——边 ID 压实后会
// 重排，raft 日志与 HTTP 请求不能依赖它。删点级联删边。
type EdgeDelete struct {
	SrcKey string
	DstKey string
	Label  string
}

// Batch 是一条 raft 日志承载的变更集。应用顺序固定（确定性回放）：
// 节点 upsert → 边 upsert → 边 delete → 点 delete。
type Batch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
}
```

`server/fsm.go`：ApplyResult 追加三个字段；Metrics 追加两个计数器：

```go
type ApplyResult struct {
	NodesApplied int
	EdgesApplied int
	EdgesSkipped int // 引用不存在节点的边被确定性跳过（计数进指标）
	EdgesDeleted int // 显式删除 + 级联删除
	NodesDeleted int
	NodesSkipped int // 删除不存在的 key
}

type Metrics struct {
	AppliedBatches atomic.Int64
	AppliedNodes   atomic.Int64
	AppliedEdges   atomic.Int64
	SkippedEdges   atomic.Int64
	DeletedEdges   atomic.Int64
	DeletedNodes   atomic.Int64
	Restores       atomic.Int64
	LeaderChanges  atomic.Int64
	Seals          atomic.Int64
}
```

`fsm.Apply` 在既有两个循环之后、`f.dirty = true` 之前追加：

```go
	for _, d := range b.EdgeDeletes {
		n, err := f.live.DeleteEdge(d.SrcKey, d.DstKey, d.Label)
		if err != nil {
			res.EdgesSkipped++ // 引用不存在节点：确定性跳过，不分叉
			continue
		}
		res.EdgesDeleted += n
	}
	for _, key := range b.NodeDeletes {
		cascaded, err := f.live.DeleteNode(key)
		if err != nil {
			res.NodesSkipped++
			continue
		}
		res.NodesDeleted++
		res.EdgesDeleted += cascaded
	}
```

指标累加段追加：

```go
	f.metrics.DeletedEdges.Add(int64(res.EdgesDeleted))
	f.metrics.DeletedNodes.Add(int64(res.NodesDeleted))
```

`server/api.go` 的 registerExpvar 追加：

```go
		expvar.Publish("vectra_deleted_edges", expvar.Func(func() any { return m.DeletedEdges.Load() }))
		expvar.Publish("vectra_deleted_nodes", expvar.Func(func() any { return m.DeletedNodes.Load() }))
```

- [ ] **Step 4: 跑测试确认通过（含 gob 兼容证据）**

Run: `cd server && go test ./...`
Expected: 全部 PASS。既有 `TestBatchGobRoundtrip` 用 Version:1 旧负载解码出新结构（删除字段为零值）——即 gob 向后兼容的回归证据。

- [ ] **Step 5: Commit**

```bash
git add server/batch.go server/fsm.go server/api.go server/fsm_test.go
git commit -m "feat(server): Batch v2 — logical-key edge deletes + cascade node deletes in raft FSM"
```

---

### Task 5: HTTP /v1/batch 删除字段

**Files:**
- Modify: `server/api.go`（handleBatch 请求/响应）
- Test: `server/api_test.go`（追加）

**Interfaces:**
- Consumes: Task 4 的 `EdgeDelete` 类型与 ApplyResult 字段。
- Produces: POST /v1/batch 请求体新增 `edgeDeletes`/`nodeDeletes`；响应新增 `edgesDeleted`/`nodesDeleted`/`nodesSkipped`。

- [ ] **Step 1: 写失败测试**（追加到 server/api_test.go；helper 用现有 `newTestCluster`/`leaderWithin`/`postJSON`）

```go
func TestAPIBatchDeletes(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a", "label": "Svc"}, {"key": "b", "label": "Svc"},
		},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"edgeDeletes": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
		"nodeDeletes": []string{"b"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if out["edgesDeleted"] != float64(1) || out["nodesDeleted"] != float64(1) {
		t.Errorf("out = %v", out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := leader.node.Published(); p != nil && p.NodeCount() == 1 && p.EdgeCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("published graph never reached 1/0")
}
```

注：`postJSON` 的返回值类型以 api_test.go 现有定义为准（`resp, out := postJSON(...)` 用法见 api_test.go:190），断言按实际类型微调。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd server && go test -run TestAPIBatchDeletes -v .`
Expected: FAIL（响应缺 edgesDeleted/nodesDeleted 字段，发布图停在 2/1）

- [ ] **Step 3: 实现**（server/api.go handleBatch）

```go
	var req struct {
		Nodes       []NodeUpsert `json:"nodes"`
		Edges       []EdgeUpsert `json:"edges"`
		EdgeDeletes []EdgeDelete `json:"edgeDeletes"`
		NodeDeletes []string     `json:"nodeDeletes"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	res, err := n.Apply(r.Context(), Batch{
		Version:     batchVersion,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
		EdgeDeletes: req.EdgeDeletes,
		NodeDeletes: req.NodeDeletes,
	})
```

响应体追加三个字段：

```go
	writeJSON(w, http.StatusOK, map[string]any{
		"nodesApplied": res.NodesApplied,
		"edgesApplied": res.EdgesApplied,
		"edgesSkipped": res.EdgesSkipped,
		"edgesDeleted": res.EdgesDeleted,
		"nodesDeleted": res.NodesDeleted,
		"nodesSkipped": res.NodesSkipped,
	})
```

（错误分支不动。）

- [ ] **Step 4: 跑测试确认通过**

Run: `cd server && go test ./...`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add server/api.go server/api_test.go
git commit -m "feat(server): /v1/batch accepts edgeDeletes/nodeDeletes, reports delete counts"
```

---

### Task 6: MCP inject_batch 删除支持

**Files:**
- Modify: `mcp/server.go`（injectIn/countsOut/edgeDeleteIn 类型、injectBatch 处理器、工具描述、Instructions）
- Test: `mcp/server_test.go`（追加）

**Interfaces:**
- Consumes: Task 2 的 `DeleteEdge`/`DeleteNode`。
- Produces: inject_batch 入参 `deleteEdges: [{srcKey,dstKey,label}]`、`deleteNodes: [key]`；结构化输出新增 `edgesDeleted`/`nodesDeleted`。

- [ ] **Step 1: 写失败测试**（追加到 mcp/server_test.go；helper 用现有 `newTestSession`/`call[Out]`）

```go
func TestInjectDeleteSeal(t *testing.T) {
	cs := newTestSession(t)
	call[struct{}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}, {"key": "b"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	out := call[struct {
		EdgesDeleted int `json:"edgesDeleted"`
		NodesDeleted int `json:"nodesDeleted"`
	}](t, cs, "inject_batch", map[string]any{
		"deleteEdges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
		"deleteNodes": []string{"b"},
	})
	if out.EdgesDeleted != 1 || out.NodesDeleted != 1 {
		t.Fatalf("out = %+v", out)
	}
	call[struct{}](t, cs, "seal", map[string]any{})
	st := call[struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
	}](t, cs, "stats", map[string]any{})
	if st.Nodes != 1 || st.Edges != 0 {
		t.Fatalf("stats = %+v, want 1/0", st)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd mcp && go test -run TestInjectDeleteSeal -v ./...`
Expected: FAIL（deleteEdges 入参被忽略，counts 为 0；seal 后 stats 停在 2/1）

- [ ] **Step 3: 实现**（mcp/server.go）

类型追加/修改：

```go
type edgeDeleteIn struct {
	SrcKey string `json:"srcKey" jsonschema:"起点 key"`
	DstKey string `json:"dstKey" jsonschema:"终点 key"`
	Label  string `json:"label,omitempty"`
}

type injectIn struct {
	Nodes       []nodeIn       `json:"nodes,omitempty"`
	Edges       []edgeIn       `json:"edges,omitempty"`
	DeleteEdges []edgeDeleteIn `json:"deleteEdges,omitempty" jsonschema:"按 (srcKey,dstKey,label) 删除边"`
	DeleteNodes []string       `json:"deleteNodes,omitempty" jsonschema:"删除节点（级联删边）"`
}

type countsOut struct {
	NodesApplied int `json:"nodesApplied"`
	EdgesApplied int `json:"edgesApplied"`
	EdgesSkipped int `json:"edgesSkipped"`
	EdgesDeleted int `json:"edgesDeleted"`
	NodesDeleted int `json:"nodesDeleted"`
}
```

`injectBatch` 处理器在边循环之后追加：

```go
	for _, d := range in.DeleteEdges {
		if n, err := svc.live.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err == nil {
			out.EdgesDeleted += n
		}
	}
	for _, k := range in.DeleteNodes {
		if n, err := svc.live.DeleteNode(k); err == nil {
			out.NodesDeleted++
			out.EdgesDeleted += n
		}
	}
```

工具描述与 Instructions 更新（inject_batch）：

```go
		Description: "批量写入与删除节点/边（写入是 upsert：同 key 更新属性；" +
			"deleteEdges 按 (srcKey,dstKey,label) 删边，deleteNodes 删点并级联删边）。" +
			"写入后需调用 seal 才能查询。",
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd mcp && go test ./...`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add mcp/server.go mcp/server_test.go
git commit -m "feat(mcp): inject_batch accepts deleteEdges/deleteNodes with delete counts"
```

---

### Task 7: 文档、示例、全量验证

**Files:**
- Modify: `README.md`、`README.zh-CN.md`、`vectra.go` 包注释（Task 3 已加）、`example_test.go`
- Test: `example_test.go`（追加 Example）

**Interfaces:**
- Consumes: 全部前序 task 的公开 API。
- Produces: 无代码接口；文档与示例为准出物。

- [ ] **Step 1: 写失败示例**（example_test.go 追加）

```go
func ExampleGraph_UpsertEdge() {
	g := vectra.New()
	g.AddNode("a", "Agent", nil)
	g.AddNode("b", "Agent", nil)
	g.AddEdge("a", "b", "MENTIONS", map[string]any{"weight": 1})
	g.UpsertEdge("a", "b", "MENTIONS", map[string]any{"weight": 2}) // 同键替换，权重更新
	g.DeleteNode("b")                                                // 级联删边
	g.Seal()
	fmt.Println(g.EdgeCount())
	// Output: 0
}
```

- [ ] **Step 2: 跑示例确认失败**

Run: `go test -run ExampleGraph_UpsertEdge -v .`
Expected: FAIL，`g.UpsertEdge undefined`（若前序 task 已完成则直接 PASS——此时跳过本步进入 Step 3）

- [ ] **Step 3: README 双语更新**

`README.md` Quickstart（Seal 之前）追加两行：

```go
g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2}) // replace props on the same (src,dst,label)
g.DeleteNode("ledger")                                                // cascade-deletes incident edges
```

`README.md` Server mode 的 Semantics 行改为：

```
Semantics: writes are linearizable (raft commit) — upsert plus logical-key deletes (node delete cascades incident edges); reads are bounded-stale (≤ SealInterval, immediate for small graphs).
```

`README.zh-CN.md` 做完全对应的两处镜像修改（措辞与现有中文行文一致）。

- [ ] **Step 4: 全量验证 + 覆盖率如实更新**

Run（三个模块各自）:
```bash
go test -race ./... && go test -cover ./...
cd server && go test -race ./... && go test -cover ./...
cd ../mcp && go test -race ./... && go test -cover ./...
```
Expected: 全部 PASS；用实测数字更新 README.md / README.zh-CN.md 的 Coverage 行（对齐 b630516「coverage honestly」惯例）。

- [ ] **Step 5: Commit**

```bash
git add README.md README.zh-CN.md example_test.go
git commit -m "docs: upsert/delete semantics in quickstart, server semantics, coverage refresh"
```

---

## Self-Review 记录

- **Spec 覆盖**：嵌入式 upsert（Task 1）、删除+级联+复活（Task 2）、压实+快照零格式改动（Task 3）、server v2+确定性回放（Task 4）、HTTP（Task 5）、MCP（Task 6）、文档（Task 7）——方案 A 四个交付面全覆盖。
- **占位符扫描**：无 TBD/TODO；唯一的不确定点是 postJSON 返回类型，已标注「以 api_test.go 现有定义为准」。
- **类型一致性**：`edgeKey{src,dst,label}`、`appendEdge(src,dst NodeID, lid uint32, props)`、`resolveEdge`、`EdgeDelete{SrcKey,DstKey,Label}`、`ApplyResult.EdgesDeleted/NodesDeleted/NodesSkipped`、`countsOut.EdgesDeleted/NodesDeleted` 在各 task 间签名一致；测试中 mustBuild 的边序（gateway→auth 为 edges[0]）已核对 vectra_test.go:23-27。
