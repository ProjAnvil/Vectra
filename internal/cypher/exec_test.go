package cypher

import (
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/engine"
)

// fixture 图（LabelID: CALLS=0, USES=1）：
//
//	gw(controller,prod,w5) ──CALLS──> order(service,prod,w3) ──USES──> store(store,w2)
//	│                                   │      ↑                        │
//	└──CALLS──> pay(service,dev,w7) <───┘ CALLS×2 (weight 3,1 并行)     │
//	             │   pay ──CALLS──> order（成环）                       │
//	             └──USES──────────────────────────────────────> store <┘? (见下方边表)
//	store ──CALLS──> gw（成环，供变长深度测试）
func execSnap(t *testing.T) *engine.Snapshot {
	t.Helper()
	nodes := []engine.NodeRec{
		{Key: "gw", Label: "Service", Props: map[string]any{"type": "controller", "env": "prod", "w": 5}},
		{Key: "order", Label: "Service", Props: map[string]any{"type": "service", "env": "prod", "w": 3}},
		{Key: "pay", Label: "Service", Props: map[string]any{"type": "service", "env": "dev", "w": 7}},
		{Key: "store", Label: "Store", Props: map[string]any{"type": "store", "w": 2}},
	}
	CALLS, USES := uint32(0), uint32(1)
	edges := []engine.EdgeRec{
		{Src: 0, Dst: 1, LabelID: CALLS, Props: map[string]any{"weight": 2}},
		{Src: 0, Dst: 2, LabelID: CALLS},
		{Src: 1, Dst: 3, LabelID: USES},
		{Src: 2, Dst: 3, LabelID: USES},
		{Src: 3, Dst: 0, LabelID: CALLS},
		{Src: 1, Dst: 2, LabelID: CALLS, Props: map[string]any{"weight": 3}}, // 并行边 1
		{Src: 1, Dst: 2, LabelID: CALLS, Props: map[string]any{"weight": 1}}, // 并行边 2
		{Src: 2, Dst: 1, LabelID: CALLS},
	}
	s, err := engine.Seal(nodes, edges, []string{"CALLS", "USES"}, []string{"type", "env", "w"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func run(t *testing.T, q string) *Result {
	t.Helper()
	res, err := Run(execSnap(t), q)
	if err != nil {
		t.Fatalf("Run(%q): %v", q, err)
	}
	return res
}

// keysOf 提取每行 name 列的字符串值（用于断言）。
func keysOf(res *Result, name string) []string {
	var out []string
	for _, r := range res.Rows {
		if s, ok := r[name].(string); ok {
			out = append(out, s)
		} else {
			out = append(out, "")
		}
	}
	return out
}

func sameSet(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, g := range got {
		seen[g]++
	}
	for _, w := range want {
		seen[w]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

func TestExecAnchorIndexedAndFallback(t *testing.T) {
	res := run(t, `MATCH (a {type:'controller'}) RETURN a.key`) // 命中索引
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "gw" {
		t.Fatalf("indexed anchor: %v", k)
	}
	res = run(t, `MATCH (a {w:5}) RETURN a.key`) // w 已索引；等值数值锚定
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "gw" {
		t.Fatalf("numeric anchor: %v", k)
	}
	res = run(t, `MATCH (a {nosuch: 1}) RETURN a`) // 未注册 key → 全扫回退，无命中
	if len(res.Rows) != 0 {
		t.Fatalf("unindexed fallback: %d rows", len(res.Rows))
	}
}

func TestExecLabelAndPropFilter(t *testing.T) {
	res := run(t, `MATCH (b:Service {env:'prod'}) RETURN b.key`)
	if k := keysOf(res, "b.key"); !sameSet(k, "gw", "order") {
		t.Fatalf("label+prop: %v", k)
	}
}

func TestExecChainAndDedup(t *testing.T) {
	// b 具名：经 order 与经 pay 两条路径绑定不同 → 两行（Cypher 行语义）
	res := run(t, `MATCH (a {type:'controller'})-[:CALLS]->(b)-[:USES]->(c) RETURN a.key, c.key`)
	if len(res.Rows) != 2 {
		t.Fatalf("named b: %d rows %+v", len(res.Rows), res.Rows)
	}
	// b 匿名：行身份 = 具名绑定 + 终点 → 两条路径终点都是 store → 去重为一行
	res = run(t, `MATCH (a {type:'controller'})-[:CALLS]->()-[:USES]->(c) RETURN a.key, c.key`)
	if len(res.Rows) != 1 {
		t.Fatalf("anonymous dedup: %d rows %+v", len(res.Rows), res.Rows)
	}
}

func TestExecDirections(t *testing.T) {
	res := run(t, `MATCH (b {type:'service'})<-[:CALLS]-(a) RETURN a.key`) // b 区分绑定 → 4 行
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "pay", "gw", "order") {
		t.Fatalf("reverse: %v", k)
	}
	res = run(t, `MATCH (a {key:'store'})--(b) RETURN b.key`) // 双向：gw + order + pay
	if k := keysOf(res, "b.key"); !sameSet(k, "gw", "order", "pay") {
		t.Fatalf("undirected: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS|USES]->(b) RETURN b.key`) // label 掩码并集
	if k := keysOf(res, "b.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("label alternation: %v", k)
	}
}

func TestExecEdgeVarParallelEdges(t *testing.T) {
	res := run(t, `MATCH (a {key:'order'})-[e:CALLS]->(b) RETURN e, b.key`)
	if len(res.Rows) != 2 { // 并行边各一行
		t.Fatalf("parallel rows: %d", len(res.Rows))
	}
	ids := map[any]bool{}
	for _, r := range res.Rows {
		if r["b.key"] != "pay" {
			t.Fatalf("parallel target: %v", r["b.key"])
		}
		ev := r["e"].(EdgeVal)
		ids[ev.ID] = true
	}
	if len(ids) != 2 {
		t.Fatalf("distinct edge ids: %v", ids)
	}
}

func TestExecRepeatedVarCycle(t *testing.T) {
	res := run(t, `MATCH (a)-[:CALLS]->(b)-[:CALLS]->(a) RETURN a.key`)
	if k := keysOf(res, "a.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("cycle pattern: %v", k)
	}
}

func TestExecAnonymousNotProjected(t *testing.T) {
	res := run(t, `MATCH (a {key:'gw'})-[:CALLS]->() RETURN a`)
	if len(res.Columns) != 1 || res.Columns[0] != "a" {
		t.Fatalf("columns: %v", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
	if nv, ok := res.Rows[0]["a"].(NodeVal); !ok || nv.Key != "gw" {
		t.Fatalf("node value: %+v", res.Rows[0]["a"])
	}
}

func TestExecCommaCartesianAndJoin(t *testing.T) {
	res := run(t, `MATCH (a {key:'gw'}), (b {type:'service'}) RETURN a.key, b.key`)
	if len(res.Rows) != 2 { // gw × {order,pay}
		t.Fatalf("cartesian: %d rows", len(res.Rows))
	}
	res = run(t, `MATCH (a)-[:CALLS]->(b), (b)-[:USES]->(c) RETURN a.key, b.key, c.key`)
	// 共享 b join：gw→{order,pay}→store，加上 order↔pay 的 CALLS 环 → 4 行
	if len(res.Rows) != 4 {
		t.Fatalf("join: %d rows %+v", len(res.Rows), res.Rows)
	}
}

func TestExecVarLengthWindows(t *testing.T) {
	res := run(t, `MATCH (a {key:'gw'})-[:CALLS*1..2]->(b) RETURN b.key`)
	if k := keysOf(res, "b.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("*1..2 CALLS: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[*2..]->(b) RETURN b.key`) // 最短距离 d>=2：只有 store(2)；gw 自身 d=0 不匹配（D1 语义）
	if k := keysOf(res, "b.key"); !sameSet(k, "store") {
		t.Fatalf("*2..: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[*0..1]->(b) RETURN b.key`) // 含起点 d=0
	if k := keysOf(res, "b.key"); !sameSet(k, "gw", "order", "pay") {
		t.Fatalf("*0..1: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS*]->(b) RETURN b.key`) // 无界=经 CALLS 可达
	if k := keysOf(res, "b.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("*: %v", k)
	}
	// *2 = 最短距离恰好为 2：经 CALLS 的目标最短距离都是 1（环路带来的"长度 2 的路径"不算，D1 最短距离语义）→ 0 行
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS*2]->(b) RETURN b.key`)
	if k := keysOf(res, "b.key"); len(k) != 0 {
		t.Fatalf("*2: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[*2]->(b) RETURN b.key`) // 全 label：store 最短距离恰为 2
	if k := keysOf(res, "b.key"); !sameSet(k, "store") {
		t.Fatalf("*2 all-labels: %v", k)
	}
}

func TestExecVarLengthIntermediatesNotFiltered(t *testing.T) {
	// 中间节点 order/pay 不是 Store，但 2 跳端点 store 命中 —— 变长段中间节点不施加目标谓词
	res := run(t, `MATCH (a {key:'gw'})-[:CALLS|USES*2]->(b:Store) RETURN b.key`)
	if k := keysOf(res, "b.key"); len(k) != 1 || k[0] != "store" {
		t.Fatalf("intermediate unfiltered: %v", k)
	}
	// 对照：定长 1 跳直接邻居没有 Store
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS|USES]->(b:Store) RETURN b.key`)
	if len(res.Rows) != 0 {
		t.Fatalf("fixed hop filtered: %d", len(res.Rows))
	}
}

func TestExecWhereOperators(t *testing.T) {
	res := run(t, `MATCH (a:Service) WHERE a.w > 3 RETURN a.key`)
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "pay") {
		t.Fatalf("gt: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.key STARTS WITH 'or' RETURN a.key`)
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "order" {
		t.Fatalf("starts: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.env CONTAINS 'r' RETURN a.key`)
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "order") {
		t.Fatalf("contains: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.env ENDS WITH 'v' RETURN a.key`) // 只有 "dev" 以 v 结尾（"prod" 也以 d 结尾）
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "pay" {
		t.Fatalf("ends: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.env IN ['dev','stg'] RETURN a.key`)
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "pay" {
		t.Fatalf("in: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.label = 'Store' RETURN a.key`) // 伪属性 label
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "store" {
		t.Fatalf("pseudo label: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.id = 2 RETURN a.key`) // 伪属性 id（数值）
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "pay" {
		t.Fatalf("pseudo id: %v", k)
	}
	// 边 props 谓词：并行边只有 weight=3 的那条满足 >=2
	res = run(t, `MATCH (a {key:'order'})-[e:CALLS]->(b) WHERE e.weight >= 2 RETURN b.key`)
	if len(res.Rows) != 1 {
		t.Fatalf("edge props: %d rows", len(res.Rows))
	}
	// null 简化：缺失属性的比较为假，NOT 不救活
	if res := run(t, `MATCH (a) WHERE a.missing = 1 RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("missing prop: %d", len(res.Rows))
	}
	if res := run(t, `MATCH (a) WHERE NOT a.missing = 1 RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("NOT must not rescue null: %d", len(res.Rows))
	}
	// 优先级：AND 高于 OR
	res = run(t, `MATCH (a) WHERE a.env = 'prod' AND a.w > 4 OR a.type = 'store' RETURN a.key`)
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "store") {
		t.Fatalf("precedence: %v", k)
	}
}

func TestExecProjectionOrderLimit(t *testing.T) {
	res := run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC LIMIT 2`)
	if k := keysOf(res, "a.key"); len(k) != 2 || k[0] != "pay" || k[1] != "gw" {
		t.Fatalf("order desc limit: %v", k)
	}
	res = run(t, `MATCH (a:Service) RETURN a.key, a.w ORDER BY a.w ASC`)
	if k := keysOf(res, "a.key"); len(k) != 3 || k[0] != "order" || k[2] != "pay" {
		t.Fatalf("order asc: %v", k)
	}
	// ORDER BY 可引用不在 RETURN 中的 var.prop（贴近 Cypher）
	res = run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC`)
	if k := keysOf(res, "a.key"); k[0] != "pay" {
		t.Fatalf("order by non-returned: %v", k)
	}
	// RETURN * 列序 = 声明序（含边变量）
	res = run(t, `MATCH (a {key:'gw'})-[e:CALLS]->(b) RETURN *`)
	if len(res.Columns) != 3 || res.Columns[0] != "a" || res.Columns[2] != "b" {
		t.Fatalf("star columns: %v", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("star rows: %d", len(res.Rows))
	}
	if _, has := res.Rows[0]["e"]; !has {
		t.Fatalf("star must include edge var")
	}
}

// TestExecProjectionSkip pins the SKIP trailer on both projection paths:
// sort → skip → limit; skip ≥ len → empty; each bound independently
// optional.
func TestExecProjectionSkip(t *testing.T) {
	// non-aggregating: w DESC over pay(7), gw(5), order(3)
	res := run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC SKIP 1 LIMIT 2`)
	if k := keysOf(res, "a.key"); len(k) != 2 || k[0] != "gw" || k[1] != "order" {
		t.Fatalf("skip+limit: %v", k)
	}
	res = run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC SKIP 2`)
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "order" {
		t.Fatalf("skip only: %v", k)
	}
	res = run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC SKIP 3`)
	if len(res.Rows) != 0 {
		t.Fatalf("skip >= len: %d rows, want 0", len(res.Rows))
	}
	res = run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC SKIP 1 LIMIT 10`)
	if k := keysOf(res, "a.key"); len(k) != 2 || k[0] != "gw" {
		t.Fatalf("limit beyond end after skip: %v", k)
	}
	// aggregating: env groups prod(2), dev(1) by count DESC
	res = run(t, `MATCH (a:Service) RETURN a.env, count(*) AS c ORDER BY c DESC SKIP 1 LIMIT 1`)
	if len(res.Rows) != 1 || res.Rows[0]["a.env"] != "dev" {
		t.Fatalf("agg skip+limit: %+v", res.Rows)
	}
	res = run(t, `MATCH (a:Service) RETURN a.env, count(*) AS c ORDER BY c DESC SKIP 5`)
	if len(res.Rows) != 0 {
		t.Fatalf("agg skip >= len: %d rows, want 0", len(res.Rows))
	}
	// SKIP without ORDER BY keeps the natural row order
	res = run(t, `MATCH (a:Service) RETURN a.key SKIP 2`)
	if k := keysOf(res, "a.key"); len(k) != 1 {
		t.Fatalf("no-order skip: %v", k)
	}
}

func TestExecRowCap(t *testing.T) {
	old := maxRows
	maxRows = 3
	defer func() { maxRows = old }()
	_, err := Run(execSnap(t), `MATCH (a), (b), (c) RETURN a.key`) // 4×4×4=64 > 3
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("row cap: %v", err)
	}
}
