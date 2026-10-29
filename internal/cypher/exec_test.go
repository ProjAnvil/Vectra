package cypher

import (
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/engine"
)

// fixture graph (LabelID: CALLS=0, USES=1):
//
//	gw(controller,prod,w5) ──CALLS──> order(service,prod,w3) ──USES──> store(store,w2)
//	│                                   │      ↑                        │
//	└──CALLS──> pay(service,dev,w7) <───┘ CALLS×2 (weight 3,1 parallel)  │
//	             │   pay ──CALLS──> order (cycle)                        │
//	             └──USES──────────────────────────────────────> store <┘? (see the edge table below)
//	store ──CALLS──> gw (cycle, for var-length depth tests)
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
		{Src: 1, Dst: 2, LabelID: CALLS, Props: map[string]any{"weight": 3}}, // parallel edge 1
		{Src: 1, Dst: 2, LabelID: CALLS, Props: map[string]any{"weight": 1}}, // parallel edge 2
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

// keysOf extracts each row's string value of the name column (for assertions).
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
	res := run(t, `MATCH (a {type:'controller'}) RETURN a.key`) // hits the index
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "gw" {
		t.Fatalf("indexed anchor: %v", k)
	}
	res = run(t, `MATCH (a {w:5}) RETURN a.key`) // w is indexed; numeric equality anchor
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "gw" {
		t.Fatalf("numeric anchor: %v", k)
	}
	res = run(t, `MATCH (a {nosuch: 1}) RETURN a`) // unregistered key → full-scan fallback, no hit
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
	// b named: the via-order and via-pay paths bind it differently → two
	// rows (Cypher row semantics)
	res := run(t, `MATCH (a {type:'controller'})-[:CALLS]->(b)-[:USES]->(c) RETURN a.key, c.key`)
	if len(res.Rows) != 2 {
		t.Fatalf("named b: %d rows %+v", len(res.Rows), res.Rows)
	}
	// b anonymous: row identity = named bindings + endpoint → both paths
	// end at store → dedup to one row
	res = run(t, `MATCH (a {type:'controller'})-[:CALLS]->()-[:USES]->(c) RETURN a.key, c.key`)
	if len(res.Rows) != 1 {
		t.Fatalf("anonymous dedup: %d rows %+v", len(res.Rows), res.Rows)
	}
}

func TestExecDirections(t *testing.T) {
	res := run(t, `MATCH (b {type:'service'})<-[:CALLS]-(a) RETURN a.key`) // b distinguishes bindings → 4 rows
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "pay", "gw", "order") {
		t.Fatalf("reverse: %v", k)
	}
	res = run(t, `MATCH (a {key:'store'})--(b) RETURN b.key`) // undirected: gw + order + pay
	if k := keysOf(res, "b.key"); !sameSet(k, "gw", "order", "pay") {
		t.Fatalf("undirected: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS|USES]->(b) RETURN b.key`) // label-mask union
	if k := keysOf(res, "b.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("label alternation: %v", k)
	}
}

func TestExecEdgeVarParallelEdges(t *testing.T) {
	res := run(t, `MATCH (a {key:'order'})-[e:CALLS]->(b) RETURN e, b.key`)
	if len(res.Rows) != 2 { // one row per parallel edge
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
	// shared-b join: gw→{order,pay}→store, plus the order↔pay CALLS cycle → 4 rows
	if len(res.Rows) != 4 {
		t.Fatalf("join: %d rows %+v", len(res.Rows), res.Rows)
	}
}

func TestExecVarLengthWindows(t *testing.T) {
	res := run(t, `MATCH (a {key:'gw'})-[:CALLS*1..2]->(b) RETURN b.key`)
	if k := keysOf(res, "b.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("*1..2 CALLS: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[*2..]->(b) RETURN b.key`) // shortest distance d>=2: only store(2); gw itself at d=0 does not match (D1 semantics)
	if k := keysOf(res, "b.key"); !sameSet(k, "store") {
		t.Fatalf("*2..: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[*0..1]->(b) RETURN b.key`) // includes the start at d=0
	if k := keysOf(res, "b.key"); !sameSet(k, "gw", "order", "pay") {
		t.Fatalf("*0..1: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS*]->(b) RETURN b.key`) // unbounded = reachable via CALLS
	if k := keysOf(res, "b.key"); !sameSet(k, "order", "pay") {
		t.Fatalf("*: %v", k)
	}
	// *2 = shortest distance exactly 2: every CALLS target sits at shortest
	// distance 1 (the "length-2 path" through the cycle does not count, D1
	// shortest-distance semantics) → 0 rows
	res = run(t, `MATCH (a {key:'gw'})-[:CALLS*2]->(b) RETURN b.key`)
	if k := keysOf(res, "b.key"); len(k) != 0 {
		t.Fatalf("*2: %v", k)
	}
	res = run(t, `MATCH (a {key:'gw'})-[*2]->(b) RETURN b.key`) // all labels: store sits at shortest distance exactly 2
	if k := keysOf(res, "b.key"); !sameSet(k, "store") {
		t.Fatalf("*2 all-labels: %v", k)
	}
}

func TestExecVarLengthIntermediatesNotFiltered(t *testing.T) {
	// the intermediate nodes order/pay are not Stores, but the 2-hop
	// endpoint store hits — intermediates of a var-length segment take no
	// target predicate
	res := run(t, `MATCH (a {key:'gw'})-[:CALLS|USES*2]->(b:Store) RETURN b.key`)
	if k := keysOf(res, "b.key"); len(k) != 1 || k[0] != "store" {
		t.Fatalf("intermediate unfiltered: %v", k)
	}
	// control: the fixed 1-hop direct neighbors contain no Store
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
	res = run(t, `MATCH (a) WHERE a.env ENDS WITH 'v' RETURN a.key`) // only "dev" ends with v
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "pay" {
		t.Fatalf("ends: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.env IN ['dev','stg'] RETURN a.key`)
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "pay" {
		t.Fatalf("in: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.label = 'Store' RETURN a.key`) // pseudo-property label
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "store" {
		t.Fatalf("pseudo label: %v", k)
	}
	res = run(t, `MATCH (a) WHERE a.id = 2 RETURN a.key`) // pseudo-property id (numeric)
	if k := keysOf(res, "a.key"); len(k) != 1 || k[0] != "pay" {
		t.Fatalf("pseudo id: %v", k)
	}
	// edge-prop predicate: of the parallel edges only the weight=3 one satisfies >=2
	res = run(t, `MATCH (a {key:'order'})-[e:CALLS]->(b) WHERE e.weight >= 2 RETURN b.key`)
	if len(res.Rows) != 1 {
		t.Fatalf("edge props: %d rows", len(res.Rows))
	}
	// null simplification: a comparison over a missing property is false, NOT does not rescue it
	if res := run(t, `MATCH (a) WHERE a.missing = 1 RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("missing prop: %d", len(res.Rows))
	}
	if res := run(t, `MATCH (a) WHERE NOT a.missing = 1 RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("NOT must not rescue null: %d", len(res.Rows))
	}
	// precedence: AND binds tighter than OR
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
	// ORDER BY may reference a var.prop absent from RETURN (close to Cypher)
	res = run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC`)
	if k := keysOf(res, "a.key"); k[0] != "pay" {
		t.Fatalf("order by non-returned: %v", k)
	}
	// RETURN * column order = declaration order (edge variables included)
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

// renderRows renders result rows in order as "col=val" strings (order
// preserved, for ordered comparison); rowSet copies and sorts them for
// multiset (order-insensitive) comparison — var-length expansion iterates
// a map, so its row order is nondeterministic by construction.
func renderRows(res *Result) []string {
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		var sb strings.Builder
		for _, c := range res.Columns {
			v := r[c]
			switch x := v.(type) {
			case string:
				sb.WriteString(c + "=" + x + " ")
			case float64:
				sb.WriteString(c + "=" + strconv.Itoa(int(x)) + " ")
			case int:
				sb.WriteString(c + "=" + strconv.Itoa(x) + " ")
			case NodeVal:
				sb.WriteString(c + "=n" + strconv.Itoa(int(x.ID)) + " ")
			case EdgeVal:
				sb.WriteString(c + "=e" + strconv.Itoa(int(x.ID)) + " ")
			case nil:
				sb.WriteString(c + "=null ")
			default:
				sb.WriteString(c + "=other ")
			}
		}
		out = append(out, sb.String())
	}
	return out
}

func rowSet(rows []string) []string {
	s := slices.Clone(rows)
	sort.Strings(s)
	return s
}

// TestExecStageLoopDegenerateGoldens: the stage loop over [Match, Return]
// must produce byte-identical results to the pre-stage executor's recorded
// goldens (expectations copied verbatim from the tests above — they ARE the
// golden table for the degenerate clause shape). ordered=true compares row
// order exactly (ORDER BY cases); otherwise the multiset.
func TestExecStageLoopDegenerateGoldens(t *testing.T) {
	tests := []struct {
		name    string
		q       string
		want    []string
		ordered bool
	}{
		{
			name: "chain named b",
			q:    `MATCH (a {type:'controller'})-[:CALLS]->(b)-[:USES]->(c) RETURN a.key, c.key`,
			want: []string{"a.key=gw c.key=store ", "a.key=gw c.key=store "},
		},
		{
			name: "chain anonymous b dedup",
			q:    `MATCH (a {type:'controller'})-[:CALLS]->()-[:USES]->(c) RETURN a.key, c.key`,
			want: []string{"a.key=gw c.key=store "},
		},
		{
			name: "undirected",
			q:    `MATCH (a {key:'store'})--(b) RETURN b.key`,
			want: []string{"b.key=gw ", "b.key=order ", "b.key=pay "},
		},
		{
			name: "var length 0..1",
			q:    `MATCH (a {key:'gw'})-[*0..1]->(b) RETURN b.key`,
			want: []string{"b.key=gw ", "b.key=order ", "b.key=pay "},
		},
		{
			name:    "order desc limit",
			q:       `MATCH (a:Service) RETURN a.key ORDER BY a.w DESC LIMIT 2`,
			want:    []string{"a.key=pay ", "a.key=gw "},
			ordered: true,
		},
		{
			name:    "order asc full",
			q:       `MATCH (a:Service) RETURN a.key, a.w ORDER BY a.w ASC`,
			want:    []string{"a.key=order a.w=3 ", "a.key=gw a.w=5 ", "a.key=pay a.w=7 "},
			ordered: true,
		},
		{
			name: "where operators",
			q:    `MATCH (a:Service) WHERE a.w > 3 RETURN a.key`,
			want: []string{"a.key=gw ", "a.key=pay "},
		},
		{
			name:    "agg skip limit",
			q:       `MATCH (a:Service) RETURN a.env, count(*) AS c ORDER BY c DESC SKIP 1 LIMIT 1`,
			want:    []string{"a.env=dev c=1 "},
			ordered: true,
		},
		{
			name: "star",
			q:    `MATCH (a {key:'gw'})-[e:CALLS]->(b) RETURN *`,
			want: []string{"a=n0 e=e0 b=n1 ", "a=n0 e=e1 b=n2 "},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := renderRows(run(t, tt.q))
			want := tt.want
			if !tt.ordered {
				got, want = rowSet(got), rowSet(want)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("q=%s\n got  %v\n want %v", tt.q, got, want)
			}
		})
	}
}

// TestExecRepeatedMatchCartesian: the second MATCH shares no variable with
// the first → a cross-stage cartesian product; the result multiset must
// match the comma-separated single-MATCH form exactly (2 rows: gw × {order,pay}).
func TestExecRepeatedMatchCartesian(t *testing.T) {
	q := `MATCH (a {key:'gw'}) MATCH (b {type:'service'}) RETURN a.key, b.key`
	res := run(t, q)
	if len(res.Rows) != 2 {
		t.Fatalf("repeated MATCH cartesian: %d rows, want 2 (%+v)", len(res.Rows), res.Rows)
	}
	got := rowSet(renderRows(res))
	comma := rowSet(renderRows(run(t, `MATCH (a {key:'gw'}), (b {type:'service'}) RETURN a.key, b.key`)))
	if !slices.Equal(got, comma) {
		t.Fatalf("repeated MATCH ≠ comma form:\n stage %v\n comma %v", got, comma)
	}
	if want := []string{"a.key=gw b.key=order ", "a.key=gw b.key=pay "}; !slices.Equal(got, want) {
		t.Fatalf("cartesian golden:\n got  %v\n want %v", got, want)
	}
}

// TestExecRepeatedMatchJoin: a shared variable joins across stages — the
// semantics match the single-MATCH comma form (4 rows), including row
// identity after parallel-edge dedup.
func TestExecRepeatedMatchJoin(t *testing.T) {
	q := `MATCH (a)-[:CALLS]->(b) MATCH (b)-[:USES]->(c) RETURN a.key, b.key, c.key`
	res := run(t, q)
	if len(res.Rows) != 4 {
		t.Fatalf("repeated MATCH join: %d rows, want 4 (%+v)", len(res.Rows), res.Rows)
	}
	got := rowSet(renderRows(res))
	comma := rowSet(renderRows(run(t, `MATCH (a)-[:CALLS]->(b), (b)-[:USES]->(c) RETURN a.key, b.key, c.key`)))
	if !slices.Equal(got, comma) {
		t.Fatalf("repeated MATCH ≠ comma form:\n stage %v\n comma %v", got, comma)
	}
	want := []string{
		"a.key=gw b.key=order c.key=store ",
		"a.key=gw b.key=pay c.key=store ",
		"a.key=order b.key=pay c.key=store ",
		"a.key=pay b.key=order c.key=store ",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("join golden:\n got  %v\n want %v", got, want)
	}
}

// TestExecStageWhereAcrossStages: WHERE may reference variables bound by
// an earlier stage (the scope chain accumulates clause by clause); the
// filter runs over the full stage row set, and the first MATCH's WHERE
// prunes the rows before the second MATCH executes.
func TestExecStageWhereAcrossStages(t *testing.T) {
	// Service w: gw=5, order=3, pay=7 → pairs with b.w>a.w: (gw,pay),(order,gw),(order,pay)
	res := run(t, `MATCH (a:Service) MATCH (b:Service) WHERE b.w > a.w RETURN a.key, b.key`)
	got := rowSet(renderRows(res))
	want := []string{
		"a.key=gw b.key=pay ",
		"a.key=order b.key=gw ",
		"a.key=order b.key=pay ",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("WHERE across stages:\n got  %v\n want %v", got, want)
	}

	// Cross-stage filtering: the first MATCH's WHERE prunes to the prod
	// services {gw,order}; the second MATCH cartesiates the USES out-edges
	// {order,pay}→store → 2×2=4 rows.
	res = run(t, `MATCH (a:Service) WHERE a.env = 'prod' MATCH (b)-[:USES]->(c) RETURN a.key, b.key, c.key`)
	got = rowSet(renderRows(res))
	want = []string{
		"a.key=gw b.key=order c.key=store ",
		"a.key=gw b.key=pay c.key=store ",
		"a.key=order b.key=order c.key=store ",
		"a.key=order b.key=pay c.key=store ",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("inter-stage WHERE:\n got  %v\n want %v", got, want)
	}
}

// TestExecStrictEmptyStage pins the controller ruling for the stage
// pipeline: a MATCH stage whose input is zero rows yields zero rows (the
// cartesian product with the empty set is empty) — stages NEVER re-anchor.
// The comma form inside ONE MATCH clause keeps its legacy re-anchoring of
// an empty first pattern (runPath channels within-clause emptiness as
// nil, which anchors); only the clause stages are strict.
func TestExecStrictEmptyStage(t *testing.T) {
	if res := run(t, `MATCH (a {nosuch:1}) MATCH (b) RETURN b.key`); len(res.Rows) != 0 {
		t.Fatalf("empty first stage must not re-anchor: %d rows %+v", len(res.Rows), res.Rows)
	}
	// Emptiness propagates through later stages (non-nil empty row set
	// flows stage to stage).
	if res := run(t, `MATCH (a:Service) MATCH (x {nosuch:1}) MATCH (b) RETURN b.key`); len(res.Rows) != 0 {
		t.Fatalf("empty middle stage must propagate: %d rows", len(res.Rows))
	}
	// Join shape with an empty stage: still zero rows.
	if res := run(t, `MATCH (a {nosuch:1}) MATCH (b)-[:USES]->(c) RETURN c.key`); len(res.Rows) != 0 {
		t.Fatalf("empty stage into join must stay empty: %d rows", len(res.Rows))
	}
	// Aggregation over a strict-empty pipeline still yields its single
	// zero row (count=0), like any zero-row MATCH.
	res := run(t, `MATCH (a {nosuch:1}) MATCH (b) RETURN count(*) AS c`)
	if len(res.Rows) != 1 || res.Rows[0]["c"] != float64(0) {
		t.Fatalf("count over strict-empty pipeline: %+v", res.Rows)
	}
	// Comma-form legacy: an empty first PATTERN re-anchors within one
	// MATCH clause (documented legacy; stages are the strict form).
	if res := run(t, `MATCH (a {nosuch:1}), (b) RETURN b.key`); len(res.Rows) != 4 {
		t.Fatalf("comma-form legacy re-anchoring: %d rows, want 4", len(res.Rows))
	}
}
