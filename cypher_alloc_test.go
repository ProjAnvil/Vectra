package vectra

// Cypher view-path allocation evidence (hotfix/0.11.x first target).
//
// The row-path zero-alloc fix (f50d0d0, docs/benchmarks/2026-10-19-
// row-path-zero-alloc.md) left one gap on record: the cypher expansion
// zip — internal/cypher exec.go's viewRow closure calls Source.Rows
// (fresh slice per expanded node) and then makes three parallel-slice
// copies, the same 4+ allocs/node churn that produced mheap span-lock
// tails at 16 workers on the BFS path. This test pins the allocation
// count of a 1-hop MATCH against a LAYERED (unsealed) graph, which is
// the path g.Cypher routes through RunView; the concurrent-tail probe
// below measures the latency side (env-gated: VECTRA_CYHP=1).

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// cypherLayeredGraph builds a chain plus a fan-out layer without sealing:
// the base (sealed) holds N nodes in a chain; one unmerged layer adds
// FAN extra edges from node 0, so Cypher runs on the three-source View.
func cypherLayeredGraph(t *testing.T, n, fan int) *Graph {
	t.Helper()
	g := New()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = itoa(i)
	}
	for _, k := range keys {
		if _, err := g.AddNode(k, "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i+1 < n; i++ {
		if _, err := g.AddEdge(keys[i], keys[i+1], "CALLS", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	// Unmerged layer: node 0 fans out to `fan` extra targets.
	if err := g.Batch(func(g *Graph) {
		for i := 0; i < fan; i++ {
			k := fmt.Sprintf("x%d", i)
			if _, err := g.AddNode(k, "SVC", nil); err != nil {
				t.Error(err)
			}
			if _, err := g.AddEdge(keys[0], k, "CALLS", nil); err != nil {
				t.Error(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestCypherViewPathAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	g := cypherLayeredGraph(t, 50, 32)
	q := `MATCH (a {key:'0'})-[:CALLS]->(b) RETURN b.key`
	// Warm: view cache, label tables, result plumbing.
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	}
	n := testing.AllocsPerRun(100, func() {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	})
	// Evidence: 545 allocs/query pre-f50d0d0; 244 after the row-path reuse
	// and boxing fixes; 244 after COW bindings too — the NAMED end node of
	// this query keeps exactly one copied binding map per row (hmap+bucket
	// = 2 allocs), now allocated at first write instead of at clone. The
	// COW win shows in the anonymous variant below: 243 -> 177.
	// Decomposition (measured): base 31 (parse + anchor + plumbing) +
	// per result row: 2 for the copied binding map (named hops only),
	// 2 for the result map + 1 boxed cell (the public Result contract:
	// []map[string]any), plus slice-growth mechanics. A bound below ~200
	// for the named shape would require dropping the per-row binding map
	// or the result contract itself.
	t.Logf("cypher 1-hop view-path allocs/query = %.1f (fan 32)", n)
	if n > 260 {
		t.Fatalf("allocs = %v, want <= 260 (named hops: one copied binding map + result contract per row)", n)
	}

	// COW pin: with an ANONYMOUS end node the clone shares the parent's
	// binding maps and no write happens, so expansion allocates zero
	// binding maps — per-row cost drops to the result contract only
	// (243 pre-COW: the eager clone copied the nodes map per row).
	qAnon := `MATCH (a {key:'0'})-[:CALLS]->() RETURN a.key`
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(qAnon); err != nil {
			t.Fatal(err)
		}
	}
	nAnon := testing.AllocsPerRun(100, func() {
		if _, err := g.Cypher(qAnon); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("cypher 1-hop anonymous-end allocs/query = %.1f (fan 32)", nAnon)
	if nAnon > 200 {
		t.Fatalf("allocs = %v, want <= 200 (anonymous hops must allocate no binding maps under copy-on-write)", nAnon)
	}
}

// TestCypherWithAllocs pins the WITH-stage allocation shapes on the same
// layered fixture: the passthrough shape (one entity re-seed map + one vals
// map + the result contract per row) and the aggregating shape (group
// accumulation over the 33 expansion rows, then ONE output row). These are
// NEW baselines for v0.13.0 T4 — they do not interact with the four
// pre-refactor characterization shapes above.
func TestCypherWithAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	g := cypherLayeredGraph(t, 50, 32)

	// WITH passthrough: MATCH 33 rows -> WITH a, b.key AS k -> RETURN k.
	// Measured 422 = 244 (the no-WITH twin's whole-query budget) + ~5 per
	// row over 33 rows (the re-seeded node binding map pair for a, the vals
	// map pair for k, and one NodeVal interface boxing on itemValue) + ~13
	// parse/stage overhead (WITH items + trailer fields). Fenced with ~25%
	// headroom per the house style for new baselines.
	qWith := `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a, b.key AS k RETURN k`
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(qWith); err != nil {
			t.Fatal(err)
		}
	}
	n := testing.AllocsPerRun(100, func() {
		if _, err := g.Cypher(qWith); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("cypher WITH passthrough allocs/query = %.1f (fan 32)", n)
	if n > 530 {
		t.Fatalf("WITH passthrough allocs = %v, want <= 530", n)
	}

	// Aggregating WITH: 33 input rows collapse into ONE group (a is the
	// single anchor), so the output side is one row; the input side pays
	// the named-hop binding maps plus the per-row group machinery (key
	// slice, encodeVal buffer + its string, NodeVal boxing) — measured 302.
	// Fenced with ~25% headroom.
	qAgg := `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a, count(*) AS c RETURN c`
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(qAgg); err != nil {
			t.Fatal(err)
		}
	}
	nAgg := testing.AllocsPerRun(100, func() {
		if _, err := g.Cypher(qAgg); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("cypher WITH aggregate allocs/query = %.1f (fan 32)", nAgg)
	if nAgg > 380 {
		t.Fatalf("WITH aggregate allocs = %v, want <= 380", nAgg)
	}
}

// TestCypherUnwindAllocs pins the UNWIND-stage allocation shape on the
// same layered fixture: the killer idiom — the 33 named-hop expansion rows
// collapse via collect() into ONE aggregate row, then UNWIND fans the
// 33-element list back out to 33 stage rows before the result contract.
// Measured 350 = 208 (the no-UNWIND twin: expansion input + keyless
// collect + single-row result) + ~4 per UNWIND output row (the copied
// vals map pair under copy-on-write, the result map, one boxed cell) plus
// slice-growth mechanics. NEW baseline for v0.13.0 T5 — it does not
// interact with the four pre-refactor characterization shapes above.
// Fenced with ~25% headroom per the house style for new baselines.
func TestCypherUnwindAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	g := cypherLayeredGraph(t, 50, 32)
	q := `MATCH (a {key:'0'})-[:CALLS]->(b) WITH collect(b.key) AS ks UNWIND ks AS k RETURN k`
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	}
	n := testing.AllocsPerRun(100, func() {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("cypher UNWIND fan-32 allocs/query = %.1f", n)
	if n > 440 {
		t.Fatalf("UNWIND fan-32 allocs = %v, want <= 440", n)
	}
}

// TestCypherOptionalAllocs pins the OPTIONAL-MATCH null-extension shape on
// the same layered fixture: every one of the 33 named-hop expansion rows
// hits an optional pattern that matches nothing (the graph has no USES
// edges) and is kept unchanged — pattern variables read as null, and the
// kept rows SHARE the incoming rows' copy-on-write binding maps (no
// re-seed, no copies). Measured 398 = 244 (the no-OPTIONAL twin's
// whole-query budget) + ~4.7 per row over 33 rows (the per-base-row
// machinery: the join-branch clone's rows slice and the view-path
// adjacency probe on the USES label — the classic optional-edge idiom's
// real cost; the disjoint-label variant `OPTIONAL MATCH (c:Void)`, which
// skips the adjacency probe, measures 292). NEW baseline for v0.13.0 T6 —
// it does not interact with the four pre-refactor characterization shapes
// above. Fenced with ~25% headroom per the house style for new baselines.
func TestCypherOptionalAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	g := cypherLayeredGraph(t, 50, 32)
	q := `MATCH (a {key:'0'})-[:CALLS]->(b) OPTIONAL MATCH (b)-[:USES]->(c) RETURN b.key, c.key`
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	}
	n := testing.AllocsPerRun(100, func() {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("cypher OPTIONAL null-extension allocs/query = %.1f (fan 32)", n)
	if n > 500 {
		t.Fatalf("OPTIONAL null-extension allocs = %v, want <= 500", n)
	}
}

// TestCypherViewPathEquivalence guards the scratch reuse: the SAME query
// must return identical results across runs, and match the graph's ground
// truth. The degree-mixing shape matters — node 0 expands 33 out-rows and
// the chain nodes 1 each, so the reused zip buffers grow to 33 then get
// truncated per call; a stale tail here is exactly the bug class f50d0d0's
// equivalence test caught. The var-length and undirected forms cover
// reachableWithin's and the two-pass DirBoth consumption of the same
// executor scratch.
func TestCypherViewPathEquivalence(t *testing.T) {
	g := cypherLayeredGraph(t, 12, 32)
	cases := []struct {
		name string
		q    string
		want map[string]bool // "a-key->b-key" pairs
	}{
		{
			name: "1-hop fan+chain",
			q:    `MATCH (a:SVC)-[:CALLS]->(b) RETURN a.key, b.key`,
			want: func() map[string]bool {
				m := map[string]bool{"0->1": true} // node 0: chain edge
				for i := 0; i < 32; i++ {
					m[fmt.Sprintf("0->x%d", i)] = true // node 0: layer fan
				}
				for i := 1; i <= 10; i++ { // chain nodes 1..10 (11 has no out-edge)
					m[fmt.Sprintf("%d->%d", i, i+1)] = true
				}
				return m
			}(),
		},
		{
			name: "var-length 1..3",
			q:    `MATCH (a {key:'2'})-[:CALLS*1..3]->(b) RETURN a.key, b.key`,
			want: map[string]bool{"2->3": true, "2->4": true, "2->5": true},
		},
		{
			name: "undirected both",
			q:    `MATCH (a {key:'1'})-[c:CALLS]-(b) RETURN a.key, b.key`,
			want: map[string]bool{"1->0": true, "1->2": true},
		},
	}
	for _, tc := range cases {
		r1, err := g.Cypher(tc.q)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		r2, err := g.Cypher(tc.q)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		// Compare order-insensitively: variable-length expansion ranges a
		// map (reachableWithin), so row ORDER is nondeterministic by
		// construction; the multiset of rows is the contract.
		pairs := func(r *CypherResult) []string {
			out := make([]string, 0, len(r.Rows))
			for _, row := range r.Rows {
				out = append(out, fmt.Sprintf("%v->%v", row["a.key"], row["b.key"]))
			}
			sort.Strings(out)
			return out
		}
		if !reflect.DeepEqual(pairs(r1), pairs(r2)) {
			t.Fatalf("%s: same query diverged across runs", tc.name)
		}
		got := map[string]bool{}
		for _, p := range pairs(r1) {
			got[p] = true
		}
		if len(got) != len(tc.want) || len(r1.Rows) != len(tc.want) {
			t.Fatalf("%s: got %d rows %v, want %d distinct", tc.name, len(r1.Rows), got, len(tc.want))
		}
		for k := range tc.want {
			if !got[k] {
				t.Fatalf("%s: missing expected pair %q (scratch reuse corrupted results)", tc.name, k)
			}
		}
	}
}

// TestCypherConcurrentViewTail is the latency-side probe for the same
// gap: N workers hammering 1-hop MATCHes against a layered graph. The
// pre-fix expectation (mirroring the BFS row-path finding) is a p99
// inflated by allocator/scheduler contention that collapses once the
// expansion path stops allocating. Env-gated acceptance style:
// VECTRA_CYHP=1; run alone on a quiet machine.
func TestCypherConcurrentViewTail(t *testing.T) {
	if os.Getenv("VECTRA_CYHP") == "" {
		t.Skip("set VECTRA_CYHP=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}
	g := cypherLayeredGraph(t, 2000, 512)
	queries := []string{
		`MATCH (a {key:'0'})-[:CALLS]->(b) RETURN b.key`,
		// Var-length: thousands of expansions per query — the regime
		// where per-node allocation churn turned into allocator
		// contention on the BFS row path (f50d0d0 evidence).
		`MATCH (a {key:'0'})-[:CALLS*1..3]->(b) RETURN b.key`,
	}
	for _, q := range queries {
		if _, err := g.Cypher(q); err != nil {
			t.Fatal(err)
		}
	}
	const workers = 16
	per := 300
	for qi, q := range queries {
		var wg sync.WaitGroup
		lat := make([][]time.Duration, workers)
		start := time.Now()
		for w := 0; w < workers; w++ {
			lat[w] = make([]time.Duration, 0, per)
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < per; i++ {
					s := time.Now()
					if _, err := g.Cypher(q); err != nil {
						t.Error(err)
						return
					}
					lat[w] = append(lat[w], time.Since(s))
				}
			}(w)
		}
		wg.Wait()
		all := make([]time.Duration, 0, workers*per)
		for _, l := range lat {
			all = append(all, l...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		t.Logf("cypher view-path q=%d w=%d n=%d wall=%s p50=%s p99=%s max=%s",
			qi+1, workers, len(all), time.Since(start).Round(time.Millisecond),
			all[len(all)/2], all[len(all)*99/100], all[len(all)-1])
	}
}
