package cypher

import (
	"slices"
	"strconv"
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

// cowSource is a small view-path graph for the COW isolation pins: node 1
// fans out to 2 and 3, node 2 continues to 9, and 9 closes the cycle back
// to 1. LabelIDs are 0 throughout: the stub's joint label table has one
// label, so any other LabelID would filter out (or index past the mask).
// Every query shape below produces sibling rows that share a parent's
// binding maps — the exact hazard copy-on-write must contain.
type cowSource struct{ stubSource }

func newCOWSource() *cowSource {
	return &cowSource{stubSource{n: 10, rows: map[uint32][]csr.AdjRow{
		1: {
			{Target: 2, EdgeID: 10, LabelID: 0},
			{Target: 3, EdgeID: 11, LabelID: 0},
		},
		2: {{Target: 9, EdgeID: 12, LabelID: 0}},
		9: {{Target: 1, EdgeID: 13, LabelID: 0}},
	}}}
}

// TestRowCOWSiblingIsolation pins the copy-on-write contract at the row
// level: a clone shares its parent's maps, and the FIRST write through
// ownNodes/ownEdges detaches that one map. Neither sibling nor parent may
// observe the other's writes, in either direction.
func TestRowCOWSiblingIsolation(t *testing.T) {
	parent := row{pos: 1, nodes: map[string]uint32{"a": 1}, edges: map[string]uint32{"e": 9}}

	c1 := parent.clone()
	c2 := parent.clone()
	if c1.nodes["a"] != 1 || c1.edges["e"] != 9 || !c1.nodesShared || !c1.edgesShared {
		t.Fatalf("clone must share and flag both maps: %+v", c1)
	}
	c1.ownNodes()
	c1.ownEdges()
	c1.nodes["b"], c1.edges["f"] = 2, 10

	if parent.nodes["b"] != 0 || parent.edges["f"] != 0 || len(parent.nodes) != 1 || len(parent.edges) != 1 {
		t.Fatalf("parent saw child write: nodes=%v edges=%v", parent.nodes, parent.edges)
	}
	if c2.nodes["b"] != 0 || c2.edges["f"] != 0 || !c2.nodesShared {
		t.Fatalf("sibling saw sibling write: nodes=%v edges=%v", c2.nodes, c2.edges)
	}
	if c1.nodes["b"] != 2 || c1.edges["f"] != 10 || c1.nodesShared {
		t.Fatalf("child lost its own write: nodes=%v edges=%v", c1.nodes, c1.edges)
	}

	// Parent-side detachment: writing through the parent after cloning must
	// not leak into the child either.
	p2 := row{pos: 5, nodes: map[string]uint32{"k": 5}}
	ch := p2.clone()
	p2.ownNodes()
	p2.nodes["z"] = 7
	if ch.nodes["z"] != 0 || len(ch.nodes) != 1 {
		t.Fatalf("child saw parent write: nodes=%v", ch.nodes)
	}

	// Nil-map creation: a fresh row (or a clone of a row with no edge
	// bindings) must be able to start a map through own*.
	fresh := row{pos: 4}
	fresh.ownNodes()
	fresh.nodes["n"] = 4
	cloneNil := fresh.clone()
	cloneNil.ownEdges()
	cloneNil.edges["x"] = 99
	if fresh.edges != nil || cloneNil.edges["x"] != 99 {
		t.Fatalf("nil-map own* broken: fresh.edges=%v clone.edges=%v", fresh.edges, cloneNil.edges)
	}
}

// runCOWQuery executes q and returns sorted "col=val" row renderings so
// goldens compare as sets.
func runCOWQuery(t *testing.T, q string) []string {
	t.Helper()
	res, err := RunView(newCOWSource(), q)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		s := ""
		for _, c := range res.Columns {
			s += c + "=" + fmtVal(r[c]) + " "
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

func fmtVal(v any) string {
	switch x := v.(type) {
	case float64:
		return strconv.Itoa(int(x))
	case NodeVal:
		return "n" + strconv.Itoa(int(x.ID))
	case EdgeVal:
		return "e" + strconv.Itoa(int(x.ID))
	case string:
		return x
	}
	return "?"
}

// TestRowCOWBranchingGolden pins executor-level isolation across every
// mutation site (extendNode's node binding, expand's edge binding,
// runPath's cartesian anchor, the join-branch clone). Goldens are the
// pre-COW deep-clone semantics: sibling rows sharing a parent map must
// bind exactly their own extension. A COW regression collapses the
// sibling bindings (every row would show the last writer's value).
func TestRowCOWBranchingGolden(t *testing.T) {
	tests := []struct {
		name string
		q    string
		want []string
	}{
		{
			// Pattern-2 join on a clones {a,b} per row, then each sibling
			// binds its own c into a copy (extendNode site). Goldens captured
			// from the pre-COW deep-clone executor.
			name: "branch second hop",
			q:    `MATCH (a)-[:CALLS]->(b), (a)-[:CALLS]->(c) RETURN b.id, c.id`,
			want: []string{
				"b.id=1 c.id=1 ", "b.id=2 c.id=2 ", "b.id=2 c.id=3 ",
				"b.id=3 c.id=2 ", "b.id=3 c.id=3 ", "b.id=9 c.id=9 ",
			},
		},
		{
			// Named edge on parallel-target siblings (expand's ownEdges site).
			name: "named edge siblings",
			q:    `MATCH (a)-[e:CALLS]->(b) RETURN b.id, e.id`,
			want: []string{"b.id=1 e.id=13 ", "b.id=2 e.id=10 ", "b.id=3 e.id=11 ", "b.id=9 e.id=12 "},
		},
		{
			// Cartesian: x is unbound in pattern 2, so every pattern-1 row is
			// cloned per anchor and binds its own x (runPath's site).
			name: "cartesian anchor binding",
			q:    `MATCH (a)-[:CALLS]->(b), (x)-[:CALLS]->(y) RETURN b.id, y.id`,
			want: []string{
				"b.id=1 y.id=1 ", "b.id=1 y.id=2 ", "b.id=1 y.id=3 ", "b.id=1 y.id=9 ",
				"b.id=2 y.id=1 ", "b.id=2 y.id=2 ", "b.id=2 y.id=3 ", "b.id=2 y.id=9 ",
				"b.id=3 y.id=1 ", "b.id=3 y.id=2 ", "b.id=3 y.id=3 ", "b.id=3 y.id=9 ",
				"b.id=9 y.id=1 ", "b.id=9 y.id=2 ", "b.id=9 y.id=3 ", "b.id=9 y.id=9 ",
			},
		},
		{
			// Mixed: named edge + named node + second branch with an edge var.
			name: "edge var across branches",
			q:    `MATCH (a)-[e:CALLS]->(b), (a)-[f:CALLS]->(c) RETURN e.id, f.id`,
			want: []string{
				"e.id=10 f.id=10 ", "e.id=10 f.id=11 ",
				"e.id=11 f.id=10 ", "e.id=11 f.id=11 ",
				"e.id=12 f.id=12 ", "e.id=13 f.id=13 ",
			},
		},
		{
			// Anonymous end nodes still isolate through shared maps; the
			// frontier advances without any binding write.
			name: "anonymous extension",
			q:    `MATCH (a)-[:CALLS]->(), (a)-[:CALLS]->(c) RETURN c.id`,
			want: []string{"c.id=1 ", "c.id=2 ", "c.id=3 ", "c.id=9 "},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runCOWQuery(t, tt.q)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("q=%s\n got  %v\n want %v", tt.q, got, tt.want)
			}
		})
	}
}

// TestRowCOWRepeatedVarCycle re-pins the repeated-variable agreement under
// COW: a later hop re-reads a var bound by an earlier row generation (the
// SHARED map) and must agree exactly as with deep clones. The 3-hop cycle
// 1->2->9->1 (and rotations) is the golden from the pre-COW executor.
func TestRowCOWRepeatedVarCycle(t *testing.T) {
	got := runCOWQuery(t, `MATCH (a)-[:CALLS]->(b)-[:CALLS]->(c)-[:CALLS]->(a) RETURN a.id, b.id, c.id`)
	want := []string{
		"a.id=1 b.id=2 c.id=9 ",
		"a.id=2 b.id=9 c.id=1 ",
		"a.id=9 b.id=1 c.id=2 ",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("cycle under COW:\n got  %v\n want %v", got, want)
	}
}

// TestRowValsCOWSiblingIsolation pins the third copy-on-write arm: vals is
// shared by clone and detached by the first ownVals, with the same
// neither-sibling-nor-parent-observes-writes contract as nodes/edges.
func TestRowValsCOWSiblingIsolation(t *testing.T) {
	parent := row{pos: 1, nodes: map[string]uint32{"a": 1}, vals: map[string]any{"n": float64(1)}}

	c1 := parent.clone()
	c2 := parent.clone()
	if c1.vals["n"] != float64(1) || !c1.valsShared || !c2.valsShared {
		t.Fatalf("clone must share and flag vals: c1=%+v c2=%+v", c1, c2)
	}
	c1.ownVals()
	c1.vals["m"] = "x"

	if parent.vals["m"] != nil || len(parent.vals) != 1 {
		t.Fatalf("parent saw child vals write: %v", parent.vals)
	}
	if c2.vals["m"] != nil || !c2.valsShared {
		t.Fatalf("sibling saw sibling vals write: %v", c2.vals)
	}
	if c1.vals["m"] != "x" || c1.valsShared {
		t.Fatalf("child lost its own vals write: %v", c1.vals)
	}

	// Nil-vals rows start a map through ownVals exactly like nil nodes/edges.
	fresh := row{pos: 4}
	fresh.ownVals()
	fresh.vals["k"] = true
	if fresh.vals["k"] != true || fresh.valsShared {
		t.Fatalf("ownVals must create an owned map: %+v", fresh)
	}

	// A clone of a nil-vals row carries no vals map and no shared flag:
	// rows without scalars must be indistinguishable from the two-arm shape.
	noScalar := row{pos: 5, nodes: map[string]uint32{"a": 5}}
	ch := noScalar.clone()
	if ch.vals != nil || ch.valsShared {
		t.Fatalf("nil-vals clone must stay nil/unflagged: %+v", ch)
	}
	ch.ownVals()
	ch.vals["q"] = nil
	if noScalar.vals != nil || len(ch.vals) != 1 {
		t.Fatalf("nil-vals ownVals broken: noScalar=%v ch=%v", noScalar.vals, ch.vals)
	}
}

// TestRowValsCloneZeroAlloc pins the third arm's cost contract: cloning
// allocates nothing — with or without a vals map. A nil-vals clone must
// equal the two-map shape's zero allocations (rows without scalars pay
// nothing for the arm's existence).
func TestRowValsCloneZeroAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	twoMaps := row{pos: 1, nodes: map[string]uint32{"a": 1}, edges: map[string]uint32{"e": 2}}
	if n := testing.AllocsPerRun(100, func() { twoMaps.clone() }); n != 0 {
		t.Fatalf("nil-vals clone allocates %.0f, want 0 (two-map shape)", n)
	}
	withVals := twoMaps
	withVals.vals = map[string]any{"n": float64(1)}
	if n := testing.AllocsPerRun(100, func() { withVals.clone() }); n != 0 {
		t.Fatalf("shared-vals clone allocates %.0f, want 0", n)
	}
}

// TestRowValsDedupIdentity pins row identity with scalar bindings: vals
// enter dedupKey/sameRow via encodeVal value identity ONLY when non-nil —
// rows differing solely in vals stay distinct through dedupRows on both the
// pairwise and the map path, structurally identical rows still collapse,
// and nil-vals rows keep the legacy identity bit-for-bit.
func TestRowValsDedupIdentity(t *testing.T) {
	base := row{pos: 1, nodes: map[string]uint32{"a": 1}}
	r1 := base.clone()
	r1.ownVals()
	r1.vals["n"] = float64(1)
	r2 := base.clone()
	r2.ownVals()
	r2.vals["n"] = float64(2)
	r3 := base.clone()
	r3.ownVals()
	r3.vals["n"] = float64(1)

	if r1.dedupKey() == r2.dedupKey() {
		t.Fatal("dedupKey must distinguish rows differing only in vals")
	}
	if r1.dedupKey() != r3.dedupKey() {
		t.Fatal("dedupKey must agree on equal vals")
	}
	if !sameRow(r1, r3) || sameRow(r1, r2) {
		t.Fatalf("sameRow vals identity: r1=%v r2=%v r3=%v", r1.vals, r2.vals, r3.vals)
	}
	// encodeVal value identity: numeric normalization across dynamic types.
	ri := base.clone()
	ri.ownVals()
	ri.vals["n"] = 1 // int
	if !sameRow(ri, r1) {
		t.Fatal("int 1 and float64 1 are the same value under encodeVal identity")
	}

	// Pairwise path (<= dedupPairwiseMax): r3 collapses into r1, r2 survives.
	kept := dedupRows([]row{r1, r2, r3})
	if len(kept) != 2 || kept[0].vals["n"] != float64(1) || kept[1].vals["n"] != float64(2) {
		t.Fatalf("pairwise dedup kept %d rows: %+v", len(kept), kept)
	}

	// Map path (> dedupPairwiseMax): 69 distinct vals + 1 exact duplicate.
	big := make([]row, 0, dedupPairwiseMax+6)
	for i := 0; i <= dedupPairwiseMax+5; i++ {
		r := base.clone()
		r.ownVals()
		r.vals["n"] = float64(i)
		big = append(big, r)
	}
	big = append(big, big[0].clone())
	if kept := dedupRows(big); len(kept) != dedupPairwiseMax+6 {
		t.Fatalf("map-path dedup kept %d rows, want %d", len(kept), dedupPairwiseMax+6)
	}

	// Legacy path: rows without vals keep the two-arm identity — equal
	// bindings collapse, and the key format is unchanged by the third arm.
	n1 := row{pos: 2, nodes: map[string]uint32{"a": 2}}
	n2 := row{pos: 2, nodes: map[string]uint32{"a": 2}}
	if n1.dedupKey() != n2.dedupKey() || !sameRow(n1, n2) {
		t.Fatal("nil-vals identity must be unchanged")
	}
	if kept := dedupRows([]row{n1, n2}); len(kept) != 1 {
		t.Fatalf("nil-vals dedup kept %d, want 1", len(kept))
	}
}

// TestRowCOWAllocShape locks the allocation shape per expansion: anonymous
// end nodes extend with zero binding-map allocations (clone shares, no
// write), named end nodes allocate exactly one map (the copied one).
func TestRowCOWAllocShape(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	rows0 := make([]csr.AdjRow, 0, 33)
	rows0 = append(rows0, csr.AdjRow{Target: 1, EdgeID: 100, LabelID: 0})
	for i := 0; i < 32; i++ {
		rows0 = append(rows0, csr.AdjRow{Target: uint32(2 + i), EdgeID: uint32(101 + i), LabelID: 0})
	}
	src := &sinkSource{stubSource{n: 64, rows: map[uint32][]csr.AdjRow{0: rows0}}}
	one := []row{{pos: 0, nodes: map[string]uint32{"a": 0}}}
	shape := func(q string) float64 {
		ast, err := Parse(q)
		if err != nil {
			t.Fatal(err)
		}
		var pat *PathPattern
		for _, c := range ast.Clauses {
			if m, ok := c.(*MatchClause); ok {
				pat = m.Patterns[0]
				break
			}
		}
		if pat == nil {
			t.Fatalf("q=%s: no MATCH clause", q)
		}
		ex := &executor{src: src}
		ex.expand(one, pat.Edges[0], pat.Nodes[1]) // warm executor scratch
		return testing.AllocsPerRun(100, func() {
			ex.expand(one, pat.Edges[0], pat.Nodes[1])
		})
	}
	// 33 extended rows: anonymous = out-slice growth only; named = growth
	// + 2 per row for the one copied map (hmap + bucket).
	if n := shape(`MATCH (a)-[:CALLS]->() RETURN a.key`); n > 12 {
		t.Fatalf("anonymous expansion allocs = %.0f, want <= 12 (slice growth only; no binding maps)", n)
	}
	if n := shape(`MATCH (a)-[:CALLS]->(b) RETURN b.key`); n > 80 {
		t.Fatalf("named expansion allocs = %.0f, want <= 80 (one copied map per row at 2 allocs + growth)", n)
	}
}
