# v0.3.0: batch-path upsert + EdgesOf 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 server/MCP 批量写路径采用真 upsert 语义（`EdgeUpsert` 名实相符），并新增 `EdgesOf` 公共 API 按节点枚举带属性的边；发版 v0.3.0。

**Architecture:** 两处一行级替换（`AddEdge`→`UpsertEdge`）+ 一个查询门面方法（sealed 走 CSR Row，unsealed 走线性过滤）；零格式变更、零依赖变更。

**Tech Stack:** Go stdlib（根模块零三方依赖红线不变）。

**Spec:** 2026-10-13 会话中对 Corpit 接入评估的核对结论（用户已批准）。

## Global Constraints

- 注释英文（v0.2.0 基线）；测试 stdlib 断言风格。
- `EdgeUpsert` 行为变更（追加→按键替换）必须在 release notes 显式标注。
- 分支 `feat/upsert-batch-edgesof`，红→绿→commit；消息对齐 git log 风格。
- 完成后合并 main、push、发 v0.3.0（发布前走确认门）。

## 设计决策

| 决策 | 选择 | 理由 |
|---|---|---|
| batch 边写入语义 | `EdgeUpsert` = 真 upsert（同键替换、平行边收敛） | 类型名本就叫 Upsert；Corpit 权重更新需要；嵌入式 `AddEdge` 仍保留多图追加 |
| 批内顺序 | 保持 nodes→edges→deletes→node deletes | upsert 语义下「先删后加」不再需要；delete-wins 语义保持 |
| `EdgesOf` 状态 | Seal 前后都可用 | 读改写权重模式（嵌入式单写者）需要在未密封 live 图上读旧值；`NeighborsOf` 的 sealed-only 不适用于此场景 |
| MCP 工具面 | 不新增 edges 工具 | 评估只要求 Go API；MCP 面保持 10 工具稳定 |
| 数值聚合 | 不做 | 设计立场：核心只有结构语义 |

---

### Task 1: 批量路径真 upsert

**Files:** Modify `server/fsm.go`（Apply 边循环）、`mcp/server.go`（injectBatch 边循环）、`server/batch.go`（注释）；Test 追加 `server/fsm_test.go`、`mcp/server_test.go`。

**Interfaces:** Consumes 根包 `UpsertEdge(srcKey, dstKey, label, props) (EdgeID, error)`。Produces 无新接口。

- [ ] **Step 1: 失败测试**（fsm_test.go 追加）

```go
func TestFSMApplyEdgeUpsertCollapses(t *testing.T) {
	f := newFSM(time.Hour)
	b := Batch{Version: 2,
		Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}, {Key: "b", Label: "Svc"}},
		Edges: []EdgeUpsert{
			{SrcKey: "a", DstKey: "b", Label: "E", Props: map[string]any{"w": 1}},
			{SrcKey: "a", DstKey: "b", Label: "E", Props: map[string]any{"w": 2}}, // same key in one batch: upsert collapses
		},
	}
	r1 := f.Apply(&raft.Log{Data: mustGob(t, b)}).(ApplyResult)
	if r1.EdgesApplied != 2 {
		t.Fatalf("EdgesApplied = %d, want 2", r1.EdgesApplied)
	}
	waitPublished(t, f, 2, 1, 2*time.Second)
	f.Apply(&raft.Log{Data: mustGob(t, b)}) // replay must not duplicate
	f.sealNow()
	if st := f.Published().Stats(); st.Edges != 1 {
		t.Fatalf("edges after replay = %d, want 1", st.Edges)
	}
}
```

（mcp/server_test.go 追加 `TestInjectUpsertEdgeCollapses`：注入两条同键边 + 一条异键边 → seal → stats 3 节点 / 2 边。）

- [ ] **Step 2:** `cd server && go test -run TestFSMApplyEdgeUpsertCollapses -v .` → FAIL（出现两条平行边，published 2/2）
- [ ] **Step 3:** 实现——fsm.go Apply 边循环 `AddEdge`→`UpsertEdge`；mcp/server.go injectBatch 边循环同样；batch.go 注释改为 upsert 语义（含「多图追加仍可用嵌入式 AddEdge」说明）
- [ ] **Step 4:** 三模块 `go test ./...` 全绿 → `git commit -m "fix(server,mcp): batch edge writes are true upserts — EdgeUpsert collapses by (src,dst,label)"`

### Task 2: EdgesOf 公共 API

**Files:** Modify `query.go`（NeighborsOf 之后新增）；Test 新建 `edgesof_test.go`；`example_test.go` 追加 Example。

**Interfaces:** Produces `func (g *Graph) EdgesOf(key string, dir Direction, labels ...string) ([]Edge, error)`——返回全部存活平行边（各带 Props），按 dir 与可选 labels 过滤；Seal 前后皆可；缺 key 返回 `ErrNodeNotFound`。

- [ ] **Step 1: 失败测试**（edgesof_test.go：sealed 过滤/方向/缺 key；unsealed + 删除后；平行边全返回——代码见执行记录）
- [ ] **Step 2:** `go test -run TestEdgesOf -v .` → FAIL 编译错误
- [ ] **Step 3:** 实现（sealed: CSR Row + EdgeByID；unsealed: 线性过滤 tombE/src/dst/labelMask）+ Example
- [ ] **Step 4:** `go test -race ./...` 全绿 → `git commit -m "feat(core): EdgesOf — enumerate a node's incident edges with props (pre/post-Seal)"`

### Task 3: 版本 0.3.0 + 覆盖率 + 发布

- [ ] 版本串 6 处 0.2.0→0.3.0（mcp/server.go、ts×3、lock×2、pyproject）
- [ ] 三模块 `-race` + 覆盖率复测，README 双语 Coverage 行如实更新
- [ ] commit → 合并 main → push → `gh release create v0.3.0`（确认门→notes 含 ⚠ 行为变更段）

## Self-Review

- Spec 覆盖：评估三条 = ①不做聚合（决策表）②EdgesOf（Task 2）③batch upsert（Task 1）✓
- 类型一致：`UpsertEdge` 签名与 v0.2.0 一致；`EdgesOf` 返回 `[]Edge`（vectra.go:43-49 公共类型）✓
- 已知影响：`ApplyResult.EdgesApplied` 计数语义不变（成功即计）；`TestFSMApplyUpsertIdempotent` 重复 Apply 断言不受影响（edges 未在重复段断言）
