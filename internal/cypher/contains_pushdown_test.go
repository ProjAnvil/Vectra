package cypher

import (
	"slices"
	"sort"
	"testing"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/engine"
)

// containsSnap seals six Doc nodes whose "name" values cover the prefilter's
// edge cases: a case variant (the bigram index folds, Cypher CONTAINS does
// not), a CJK value, a gram collision ("xxabyy bc zz" carries ab+bc but not
// "abc"), a non-string, and an absent prop. fts selects whether "name" is
// FTS-declared — the two snapshots must answer every CONTAINS query
// identically, because the prefilter only narrows the anchor scan.
func containsSnap(t *testing.T, fts bool) *engine.Snapshot {
	t.Helper()
	nodes := []engine.NodeRec{
		{Key: "a", Label: "Doc", Props: map[string]any{"name": "payment gateway"}},
		{Key: "b", Label: "Doc", Props: map[string]any{"name": "Payment Gateway"}},
		{Key: "c", Label: "Doc", Props: map[string]any{"name": "图数据库"}},
		{Key: "d", Label: "Doc", Props: map[string]any{"name": "xxabyy bc zz"}},
		{Key: "e", Label: "Doc", Props: map[string]any{"name": 42}},
		{Key: "f", Label: "Doc", Props: map[string]any{}},
	}
	var ftsKeys []string
	if fts {
		ftsKeys = []string{"name"}
	}
	s, err := engine.Seal(nodes, nil, nil, nil, ftsKeys)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func keysOfResult(t *testing.T, res *Result) []string {
	t.Helper()
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		k, _ := r["n.key"].(string)
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestContainsPushdownMatchesScan(t *testing.T) {
	withFTS, noFTS := containsSnap(t, true), containsSnap(t, false)
	cases := []struct {
		q    string
		want []string
	}{
		// case-sensitive: the folded prefilter admits "Payment Gateway", the WHERE re-check rejects it
		{`MATCH (n) WHERE n.name CONTAINS 'payment' RETURN n.key`, []string{"a"}},
		{`MATCH (n) WHERE n.name CONTAINS 'Payment' RETURN n.key`, []string{"b"}},
		{`MATCH (n:Doc) WHERE n.name CONTAINS '数据' RETURN n.key`, []string{"c"}},
		// gram collision: ab+bc postings admit d, Contains("abc") rejects it
		{`MATCH (n) WHERE n.name CONTAINS 'abc' RETURN n.key`, []string{}},
		// 1-rune needle: no grams, scan path on both snapshots
		{`MATCH (n) WHERE n.name CONTAINS 'z' RETURN n.key`, []string{"d"}},
		// AND chain: both conjuncts narrow; "Payment Gateway" fails the case-sensitive 'gate'
		{`MATCH (n) WHERE n.name CONTAINS 'ay' AND n.name CONTAINS 'gate' RETURN n.key`, []string{"a"}},
		// OR is not lifted: full row-filter semantics
		{`MATCH (n) WHERE n.name CONTAINS 'ay' OR n.name CONTAINS '图' RETURN n.key`, []string{"a", "b", "c"}},
		// undeclared key: no prefilter, identical scan
		{`MATCH (n) WHERE n.other CONTAINS 'x' RETURN n.key`, []string{}},
		// mixed with an equality that is NOT typed-indexed on this snapshot
		{`MATCH (n) WHERE n.name CONTAINS 'gateway' AND n.key = 'a' RETURN n.key`, []string{"a"}},
	}
	for _, tc := range cases {
		r1, err := Run(withFTS, tc.q)
		if err != nil {
			t.Fatalf("%s: fts: %v", tc.q, err)
		}
		r2, err := Run(noFTS, tc.q)
		if err != nil {
			t.Fatalf("%s: scan: %v", tc.q, err)
		}
		got, ref := keysOfResult(t, r1), keysOfResult(t, r2)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: fts path = %v, want %v", tc.q, got, tc.want)
		}
		if !slices.Equal(got, ref) {
			t.Errorf("%s: fts path %v != scan path %v", tc.q, got, ref)
		}
	}
}

func TestExtractContainsHints(t *testing.T) {
	q, err := Parse(`MATCH (n), (m) WHERE n.a CONTAINS 'x' AND (n.b CONTAINS 'y' OR n.c = 1) AND m.d CONTAINS 'zz' AND NOT n.e CONTAINS 'q' AND n.f CONTAINS '' RETURN n`)
	if err != nil {
		t.Fatal(err)
	}
	m := q.Clauses[0].(*MatchClause)
	hints := extractContainsHints(m.Where)
	want := map[string][]containsHint{
		"n": {{Key: "a", Needle: "x"}},
		"m": {{Key: "d", Needle: "zz"}},
	}
	if len(hints) != len(want) {
		t.Fatalf("hints = %v, want %v", hints, want)
	}
	for v, hs := range want {
		if !slices.Equal(hints[v], hs) {
			t.Errorf("hints[%q] = %v, want %v", v, hints[v], hs)
		}
	}
	if extractContainsHints(nil) != nil {
		t.Error("nil WHERE must yield nil hints")
	}
	plain, _ := Parse(`MATCH (n) WHERE n.a = 'x' RETURN n`)
	if extractContainsHints(plain.Clauses[0].(*MatchClause).Where) != nil {
		t.Error("a WHERE without CONTAINS must allocate no hints")
	}
}

// prefilterSource is a three-source stub whose LookupContains returns a
// controlled candidate subset and records its calls. If the executor ever
// stopped consulting the prefilter, the row filter alone would admit both
// matching nodes and the subset assertion would fail — pinning the wiring,
// not just the (superset-guaranteed) correctness.
type prefilterSource struct {
	stubSource
	recs  []engine.NodeRec
	cands []uint32
	calls []containsHint
}

func (p *prefilterSource) NodeInfo(id uint32) (engine.NodeRec, bool) {
	if int(id) >= len(p.recs) {
		return engine.NodeRec{}, false
	}
	return p.recs[id], true
}

func (p *prefilterSource) LookupContains(key, substr string) ([]uint32, bool) {
	p.calls = append(p.calls, containsHint{Key: key, Needle: substr})
	return slices.Clone(p.cands), true
}

func TestContainsPushdownConsultsPrefilter(t *testing.T) {
	src := &prefilterSource{
		stubSource: stubSource{n: 3, rows: map[uint32][]csr.AdjRow{}},
		recs: []engine.NodeRec{
			{Key: "k0", Props: map[string]any{"name": "alpha beta"}},
			{Key: "k1", Props: map[string]any{"name": "beta gamma"}},
			{Key: "k2", Props: map[string]any{"name": "delta"}},
		},
		cands: []uint32{1}, // the prefilter (deliberately) names only k1
	}
	res, err := RunView(src, `MATCH (n) WHERE n.name CONTAINS 'beta' RETURN n.key`)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOfResult(t, res); !slices.Equal(got, []string{"k1"}) {
		t.Fatalf("anchor scan ignored the prefilter: got %v, want [k1]", got)
	}
	if !slices.Equal(src.calls, []containsHint{{Key: "name", Needle: "beta"}}) {
		t.Fatalf("prefilter calls = %v", src.calls)
	}
	// A clause without CONTAINS must never touch the prefilter.
	src.calls = nil
	if _, err := RunView(src, `MATCH (n) WHERE n.name = 'delta' RETURN n.key`); err != nil {
		t.Fatal(err)
	}
	if len(src.calls) != 0 {
		t.Fatalf("equality WHERE consulted the prefilter: %v", src.calls)
	}
}
