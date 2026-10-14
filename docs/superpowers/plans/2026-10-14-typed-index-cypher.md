# 类型化属性索引 + Cypher 子集 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 属性索引支持 string/number/bool（含数值范围查询），并新增 openCypher/GQL 子集查询语言，接入 Go API / REST / MCP。

**Architecture:** engine 层把单一 string 索引改为每 key 三类型桶（按 (val,id) 排序，二分查找）；新增 `internal/cypher` 包（lexer → 递归下降 parser → AST → 直接跑在 `*engine.Snapshot` 上的行式执行器），变长段用最短距离 BFS（节点对语义）。根包 facade、REST `POST /v1/cypher`、MCP `cypher` 工具三层接入。

**Tech Stack:** Go 标准库（无新依赖）；TS/Python MCP 客户端小改。

**Spec:** [docs/superpowers/specs/2026-10-14-typed-index-cypher.md](../specs/2026-10-14-typed-index-cypher.md)（含两处自主决策 D1 节点对语义、D2 全交付面）

## Global Constraints

- 不改 store v3 格式；索引声明仍持久化 key 列表，索引本体 Load/Seal 重建。
- 数值统一归一化为 float64 入索引（int/uint/float 各宽度 + json.Number）。
- 查询仅限已 Seal 图（`ErrNotSealed`）。
- 变长段语义 = 最短距离 BFS（n ≤ d_min ≤ m 即匹配），中间节点不施加目标节点谓词；变长段边变量不可被 RETURN/WHERE 引用（静态报错）。
- WHERE 三值逻辑简化：null 参与的比较/属性缺失按 false 处理。
- 执行器中间行数硬上限 1,000,000（超出报错，提示收窄 pattern 或加 LIMIT）。
- MCP 工具默认 limit 1000、`total` 报真实总数；REST 不截断。
- 每任务 TDD：先写失败测试再实现；`go test ./... && go vet ./...` 全绿后 commit。

---

### Task 1: engine 类型化 propIndex

**Files:**
- Modify: `internal/engine/engine.go`（indexEntry → propIndex；Seal 抽取；Lookup 族）
- Test: `internal/engine/engine_test.go`（新建）

**Interfaces（Produces）:**
- `func (s *Snapshot) LookupIndex(key, value string) ([]uint32, bool)`（保留）
- `func (s *Snapshot) LookupNumber(key string, value float64) ([]uint32, bool)`
- `func (s *Snapshot) LookupNumberRange(key string, lo, hi *float64) ([]uint32, bool)`
- `func (s *Snapshot) LookupBool(key string, value bool) ([]uint32, bool)`
- `func (s *Snapshot) LabelID(name string) (uint32, bool)`（执行器锚定/掩码用）
- `func toFloat(v any) (float64, bool)`（包内）

- [ ] **Step 1: 失败测试**（新建 `internal/engine/engine_test.go`）

```go
package engine

import (
	"encoding/json"
	"testing"
)

func sealIdx(t *testing.T, keys []string, props []map[string]any) *Snapshot {
	t.Helper()
	nodes := make([]NodeRec, len(props))
	for i, p := range props {
		nodes[i] = NodeRec{Key: string(rune('a' + i)), Label: "L", Props: p}
	}
	s, err := Seal(nodes, nil, nil, keys)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestTypedIndexBucketsAndLookups(t *testing.T) {
	s := sealIdx(t, []string{"type", "weight", "on"},
		[]map[string]any{
			{"type": "controller", "weight": 3, "on": true},
			{"type": "service", "weight": 7.5, "on": false},
			{"type": "controller", "weight": json.Number("5")}, // 无该类型字段 → 桶跳过
		})
	if ids, ok := s.LookupIndex("type", "controller"); !ok || len(ids) != 2 || ids[0] != 0 || ids[1] != 2 {
		t.Fatalf("string eq: %v %v", ids, ok)
	}
	if ids, ok := s.LookupNumber("weight", 7.5); !ok || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("number eq: %v %v", ids, ok)
	}
	if ids, _ := s.LookupNumber("weight", 3); len(ids) != 1 || ids[0] != 0 { // int 3 → float64 3 命中
		t.Fatalf("int normalization: %v", ids)
	}
	if ids, _ := s.LookupNumberRange("weight", fp(3), fp(5)); len(ids) != 2 { // [3,5] → a,c(json.Number("5"))
		t.Fatalf("range: %v", ids)
	}
	if ids, _ := s.LookupNumberRange("weight", nil, fp(4)); len(ids) != 1 { // (-∞,4] → a
		t.Fatalf("range hi only: %v", ids)
	}
	if ids, ok := s.LookupBool("on", true); !ok || len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("bool: %v %v", ids, ok)
	}
	if _, ok := s.LookupIndex("absent", "x"); ok {
		t.Fatal("unindexed key must return ok=false")
	}
	if ids, _ := s.LookupIndex("type", "nope"); len(ids) != 0 { // 注册但无命中 → 空集 + ok=true
		t.Fatalf("empty hit: %v", ids)
	}
	if _, ok := s.LabelID("L"); !ok {
		t.Fatal("LabelID")
	}
	if _, ok := s.LabelID("X"); ok {
		t.Fatal("LabelID miss")
	}
}

func fp(v float64) *float64 { return &v }
```

- [ ] **Step 2: 跑测试确认失败**（`go test ./internal/engine/` → 编译错误）
- [ ] **Step 3: 实现**——`indexEntry` 替换为：

```go
type strEntry struct { val string; id uint32 }
type numEntry struct { val float64; id uint32 }
type boolEntry struct { val bool; id uint32 }

// propIndex 是一个 property key 的三类型桶索引，各桶内按 (val, id) 升序。
type propIndex struct {
	strs  []strEntry
	nums  []numEntry
	bools []boolEntry
}
```

`Snapshot.indexes map[string][]indexEntry` → `map[string]*propIndex`。Seal 抽取 switch：`string→strs`、`bool→bools`、default 走 `toFloat`（int/int8..64/uint../float32/64/json.Number）→ nums；`nil/slice/map` 跳过。三桶各 `slices.SortFunc` 按 (val,id)。Lookup 族实现如 spec（数值范围用 `sort.Search` 下界/上界；bool 线性扫描）。`LabelID` 线性查 `s.labels`。
- [ ] **Step 4: 测试通过 + commit** `feat(engine): typed property index (string/number/bool + numeric range)`

### Task 2: 根包 Lookup facade + 归一化往返

**Files:**
- Modify: `vectra.go`（LookupByProperty 下方追加三个 facade + 私有 helper）
- Test: `typed_index_test.go`（根包新建）

**Interfaces（Produces）:** `(*Graph) LookupByNumber(key string, v float64) ([]Node, bool)`、`LookupByNumberRange(key string, min, max *float64) ([]Node, bool)`、`LookupByBool(key string, v bool) ([]Node, bool)`。

- [ ] **Step 1: 失败测试**——混合类型 props Seal 后三个 Lookup 命中；`Save→Load` 往返后 int prop 变 float64 但 `LookupByNumber(key, 42)` 仍命中（归一化断言）；未 Seal 时 ok=false。
- [ ] **Step 2/3/4:** helper `nodesFromIDs(ids []uint32, ok bool) ([]Node, bool)`（LookupByProperty 重构共用）→ 实现 → 绿 → commit `feat(api): typed LookupBy{Number,NumberRange,Bool} facades`

### Task 3: cypher lexer

**Files:**
- Create: `internal/cypher/lexer.go`、`internal/cypher/lexer_test.go`

**Interfaces（Produces）:** `type TokenKind int`（EOF/IDENT/STRING/NUMBER/符号与关键词枚举）、`type Token struct{ Kind TokenKind; Text string; Line, Col int }`、`func Lex(src string) ([]Token, error)`。关键词大小写不敏感（MATCH/WHERE/RETURN/ORDER/BY/LIMIT/ASC/DESC/AND/OR/NOT/IN/STARTS/ENDS/WITH/CONTAINS/TRUE/FALSE/NULL）；字符串 `'...'`/`"..."`（转义 `\\ \' \" \n \t \r`）；数字 `[0-9]+(\.[0-9]+)?`；符号 `-> < -- <= >= <> = < > ( ) [ ] { } : , . *`；`//` 行注释；错误带 line:col。

- [ ] **Step 1: 失败测试**——表驱动：`MATCH (a:Bean)-[:CALLS*1..2]->(b) WHERE a.type='controller' AND b.w>=3 RETURN a.key, b ORDER BY b.w DESC LIMIT 5` 全 token 断言；`--`、`<>`、字符串转义、注释、`1.5`、非法字符 `@` 报错位置。
- [ ] **Step 2/3/4:** 实现 → 绿 → commit `feat(cypher): lexer`

### Task 4: AST + parser + 静态校验

**Files:**
- Create: `internal/cypher/ast.go`、`internal/cypher/parser.go`、`internal/cypher/parser_test.go`

**Interfaces（Produces）:**
```go
type Query struct{ Match *MatchClause; Return *ReturnClause }
type MatchClause struct{ Patterns []*PathPattern; Where Expr }
type PathPattern struct{ Nodes []*NodePattern; Edges []*EdgePattern } // len(Edges)=len(Nodes)-1
type NodePattern struct{ Var, Label string; Props map[string]any }
type EdgePattern struct{ Var string; Labels []string; MinHops, MaxHops int; Dir int } // Dir: 0=out 1=in 2=both; 定长单跳 = {1,1}; MaxHops<0 = 无界
type ReturnClause struct{ Star bool; Items []*ReturnItem; OrderBy []OrderKey; Limit int } // Limit<0 = 无
type ReturnItem struct{ Var, Prop, Name string } // Name: "a" / "a.key"
type OrderKey struct{ Name string; Desc bool }
type Expr interface{ pos() (int, int) }
// Binary{Op,L,R}（= <> < <= > >= AND OR STARTS WITH ENDS WITH CONTAINS IN）、Not{X}、Prop{Var,Name}、Lit{Val}、List{Vals}
type Error struct{ Line, Col int; Msg string } // Error() => "cypher: %d:%d: %s"
func Parse(src string) (*Query, error)
```
静态校验（Parse 内）：WHERE/RETURN/ORDER 引用的 var 必须在 pattern 声明；变长边（Min!=1||Max!=1）的 Var 被 RETURN/WHERE 引用 → Error；ORDER BY 的 Name 必须等于某 RETURN Item.Name（Star 时报错）；`RETURN *` 时至少一个具名 var；propMap 值仅字面量。

- [ ] **Step 1: 失败测试**——spec EBNF 的正例全集（含 `--`、`-[v:A|B]-`、`<-[...]-`、`*`/`*2`/`*1..3`/`*..3`/`*2..`、propMap、`(a)-[:X]->(b)-[:Y]->(c)`、逗号双路径、WHERE 全操作符与优先级、RETURN `*`/var/var.prop、ORDER BY 多键 DESC、LIMIT）+ 负例（语法错位置、未声明 var、变长边变量引用、ORDER BY 不在 RETURN）。
- [ ] **Step 2/3/4:** 递归下降实现 → 绿 → commit `feat(cypher): parser + AST + static checks`

### Task 5: 执行器——锚定 + 定长链式展开 + 去重

**Files:**
- Create: `internal/cypher/exec.go`、`internal/cypher/exec_test.go`

**Interfaces（Consumes）** Task 4 AST、Task 1 Snapshot API、`csr.Adjacency.Row(u) (targets, edgeIDs, labels []uint32)`。
**Interfaces（Produces）:**
```go
type NodeVal struct{ ID uint32; Key, Label string; Props map[string]any } // 带 json tag id/key/label/props
type EdgeVal struct{ ID, Src, Dst uint32; Label string; Props map[string]any }
type Result struct{ Columns []string; Rows []map[string]any }
func Run(snap *engine.Snapshot, q string) (*Result, error)
```
内部：`type row struct{ pos uint32; nodes map[string]uint32; edges map[string]uint32 }`；行数上限 `maxRows = 1_000_000`。

- [ ] **Step 1: 失败测试**（每测试内联建图 + Seal + Run）：
  1. 锚定走索引：`MATCH (a {type:'controller'}) RETURN a`（IndexProperty("type")）与全扫回退（未索引 key）结果一致；
  2. label 匹配 + propMap 复合：`(b:Service {env:'prod'})`；
  3. 链式 `(:A)-[:X]->(:B)-[:Y]->(:C)` 传递；
  4. `<-[...]-` 反向、`-[:A|B]-` 掩码、`--` 双向（F∪R 去重）；
  5. 并行边单跳边变量：`-[e:X]->` 行数 = 并行边数（e.id 各异）；
  6. 重复 var 环模式 `(a)-[:X]->(b)-[:X]->(a)` 只匹配真环；
  7. 匿名节点不进 Columns；
  8. 行按绑定元组去重（菱形 A→B,A→C,B→D,C→D 的 `(a)-[:X]->()-[:X]->(d)` 对 (a,d) 一行）；
  9. 逗号路径共享 var join、无共享 var 笛卡尔积 + WHERE 过滤；
  10. 未 Seal 前（Run 直接吃 Snapshot，等价已 Seal）无需测。
- [ ] **Step 2/3/4:** 实现（锚点解析：propMap 中已索引 key 的等值交集，否则全扫过滤；展开：`Row(pos)` 按方向取 F/R/F+R，mask 过滤 label，目标节点过 NodePattern，重复 var 一致性检查；去重 key = 有序 `var:id` 串接）→ 绿 → commit `feat(cypher): executor — anchoring + fixed-hop chain expansion`

### Task 6: 执行器——变长段 + WHERE + 投影/排序/LIMIT

**Files:**
- Modify: `internal/cypher/exec.go`、`internal/cypher/exec_test.go`

- [ ] **Step 1: 失败测试**：
  1. `*1..2` 最短距离窗口（d=1 与 d=2 命中、d=3 不命中）；`*2..` 只含 d≥2；`*0..1` 含起点（d=0）；`*` 无界（=可达）；
  2. 变长段中间节点**不**施加目标谓词：`(a)-[:X*1..2]->(b:Service)` 中 1 跳处的 controller 中转不拦截 2 跳 service（构造反例图断言）；
  3. 变长 label 掩码 `[:A|B*2]`；
  4. WHERE：`=`/`<>`/`<`/`<=`/`>`/`>=`（数值与字符串）、`STARTS WITH`/`ENDS WITH`/`CONTAINS`、`IN`（字面量列表）、AND/OR/NOT 优先级、伪属性 `a.key`/`a.label`/`a.id`/`e.label`、边 props `e.weight > 3`（单跳边变量）、null 语义（缺失 prop 的行被过滤、`NOT` 不救活）；
  5. RETURN 投影：var → NodeVal/EdgeVal、var.prop → 标量（含伪属性）、`RETURN *` 列序 = 声明序；
  6. ORDER BY：数值/字符串升降、多键、含 null 行排最前；
  7. LIMIT；
  8. 行数超限报错（小上限常量注入测试：把 maxRows 提为包变量以便测试覆盖）。
- [ ] **Step 2/3/4:** 实现（变长段 = 逐行 BFS，labelMask 过滤、visited 深度数组、收集 d∈[n,m]、终点过 NodePattern；WHERE 求值 `evalExpr(e Expr, r row) (any, bool)` + `toFloat` 复用 engine 侧逻辑的本地副本；投影/排序比较器全序 null<bool<number<string<Node/Edge(按 key,id)）→ 绿 → commit `feat(cypher): variable-length segments, WHERE evaluation, projection/ORDER BY/LIMIT`

### Task 7: 根包 facade `Cypher()`

**Files:**
- Create: `cypher.go`（根包）；Test: `cypher_api_test.go`

**Interfaces（Produces）:** `type CypherResult struct{ Columns []string; Rows []map[string]any }`（行内 Node/Edge 为 `vectra.Node`/`vectra.Edge` 公有类型）、`func (g *Graph) Cypher(query string) (*CypherResult, error)`、`type CypherError = cypher.Error`（别名导出）。

- [ ] **Step 1: 失败测试**——未 Seal → ErrNotSealed；端到端一例（建图→Seal→Cypher→断言 Columns/Rows，行内 Node 断言 Key/Label/Props；`rows[i]["a.key"]` 标量）；解析错 `errors.As(&CypherError)` 命中。
- [ ] **Step 2/3/4:** NodeVal/EdgeVal→Node/Edge 转换 → 绿 → commit `feat(api): Graph.Cypher facade`

### Task 8: REST `POST /v1/cypher`

**Files:**
- Modify: `server/api.go`（路由 + handler）
- Test: `server/api_test.go`（追加；沿用现有 handler 测试风格——查现有文件名，若为 `server/*_test.go` 则就地追加）

- [ ] **Step 1: 失败测试**——合法查询 200（columns/rows/count、`X-Vectra-Snapshot` 头）；body 缺 query → 400 VALIDATION；解析错 → 400 PARSE；readiness 未就绪 → 503。
- [ ] **Step 2/3/4:** handler（`errors.As *vectra.CypherError` → 400 PARSE；其余执行错 500 INTERNAL；queryCount 计数）→ 绿 → commit `feat(server): POST /v1/cypher`

### Task 9: MCP `cypher` 工具 + `lookup_property` 类型化

**Files:**
- Modify: `mcp/server.go`
- Test: `mcp/server_test.go`（追加；查现有测试文件名就地追加）

- [ ] **Step 1: 失败测试**——`cypher` 工具：返回 {columns, rows, count, total}，limit 截断 + total 真实值；`lookup_property` propValue 传 number/bool 命中（float64 分派 LookupByNumber / bool 分派 LookupByBool）；其他类型报错文案。
- [ ] **Step 2/3/4:** 实现 + Instructions 更新（加 cypher）→ 绿 → commit `feat(mcp): cypher tool + typed lookup_property`

### Task 10: TS/Python 客户端镜像

**Files:**
- Modify: `mcp/ts/src/index.ts`、`mcp/python/vectra_mcp/server.py`（+ `client.py` 若需加方法）

- [ ] **Step 1:** TS：`server.tool("cypher", ...)` zod `{query, limit 默认 1000}` → POST `/v1/cypher`，rows.slice(0, limit)、total=rows.length；Python：`@mcp.tool def cypher(query, limit=1000)` 同义。两端 instructions 提及 cypher。
- [ ] **Step 2:** `cd mcp/ts && npm run build`（或 tsc 检查）+ `python -c "import vectra_mcp"` 语法验证（按仓库现有验证方式）→ commit `feat(mcp): cypher tool in TS/Python clients`

### Task 11: 文档 + 全量回归

**Files:**
- Modify: `README.md`、`README.zh-CN.md`

- [ ] **Step 1:** 两份 README：属性索引段落改类型化描述（LookupBy* 族 + lookup_property 值类型）；新增「Cypher 子集」节（语法表、示例、语义说明：节点对/最短距离语义、变长段无边绑定、null→false、REST/MCP 入口）。
- [ ] **Step 2:** `go test ./... && go vet ./...` 全绿；`gofmt -l .` 干净 → commit `docs: typed index + cypher subset`

## Self-Review

- Spec 覆盖：Q2 三桶/Lookup 族/facade/MCP 类型化/归一化（Task 1,2,9）；语法全集/静态校验（Task 4）；执行语义 D1（Task 5,6）；三层接入 D2（Task 7,8,9,10）；文档（Task 11）；行数上限护栏（Task 6）。✔
- 占位符：无 TBD；测试均给出断言语义。✔
- 类型一致：`LookupNumberRange(lo, hi *float64)`、`EdgePattern{MinHops,MaxHops,Dir}`、`ReturnItem{Name}` 等跨任务签名一致。✔
