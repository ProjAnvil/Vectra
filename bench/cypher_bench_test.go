// Characterization benchmarks for the Cypher executor path — the
// pre-refactor A/B anchor for the v0.13.0 clause-list pipeline work
// (WITH/UNWIND/OPTIONAL MATCH). The T3 and T7 perf gates compare their
// post-refactor numbers against the baseline recorded below; run the
// same command on a quiet machine to reproduce.
//
// Baseline (pre-refactor, branch feat/v0.13.0-with-pipeline, task 1):
//
//	go test ./bench/ -bench 'BenchmarkCypher1Hop|BenchmarkCypherVarLength|BenchmarkCypherUndirected' -benchtime=1s -count=10 -run '^$'
//
//	Machine: Apple M5 Pro (darwin/arm64), Go 1.27.2, quiet solo run,
//	2026-10-26. Medians of -count=10 (allocs/op and B/op were identical
//	across all 10 runs of every shape):
//
//		BenchmarkCypher1Hop/named       13998 ns/op   30648 B/op   244 allocs/op
//		BenchmarkCypher1Hop/anonymous   11139 ns/op   22192 B/op   177 allocs/op
//		BenchmarkCypherVarLength        15327 ns/op   34288 B/op   272 allocs/op
//		BenchmarkCypherUndirected       16160 ns/op   39160 B/op   312 allocs/op
//
//	Cross-check: the 1-hop allocs/op (244 named / 177 anonymous) equal
//	the TestCypherViewPathAllocs pins in the root package, which measure
//	the same-shaped 50+32 layered fixture via AllocsPerRun.
package bench

import (
	"strconv"
	"testing"

	"github.com/projanvil/vectra"
)

// Fixture shape: mirror of the root cypherLayeredGraph fixture
// (cypher_alloc_test.go), which lives in package vectra and cannot be
// imported here. A chain of SVC nodes joined by CALLS edges is sealed,
// then an unmerged Batch layer fans node "0" out to `cypherFanout` extra
// targets — leaving the graph unsealed so every Cypher query routes
// through the RunView (three-source) executor path rather than the
// sealed fast path. Node "0" is the degree-mixing hotspot: 33 out-rows
// while every other node expands at most 1.
const (
	cypherChainNodes = 50
	cypherFanout     = 32
)

// The four characterization queries (verbatim — do not edit: the shape
// under test is the current executor's handling of these exact forms).
const (
	cypherQ1HopNamed  = `MATCH (a {key:'0'})-[:CALLS]->(b) RETURN b.key`
	cypherQ1HopAnon   = `MATCH (a {key:'0'})-[:CALLS]->() RETURN a.key`
	cypherQVarLength  = `MATCH (a {key:'0'})-[:CALLS*1..3]->(b) RETURN b.key`
	cypherQUndirected = `MATCH (a {key:'0'})-[c:CALLS]-(b) RETURN b.key`
)

// cypherLayeredGraph builds the layered (unsealed) fixture. Callers own
// the graph and must Close it.
func cypherLayeredGraph(tb testing.TB) *vectra.Graph {
	tb.Helper()
	g := vectra.New()
	keys := make([]string, cypherChainNodes)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	for _, k := range keys {
		if _, err := g.AddNode(k, "SVC", nil); err != nil {
			tb.Fatal(err)
		}
	}
	for i := 0; i+1 < cypherChainNodes; i++ {
		if _, err := g.AddEdge(keys[i], keys[i+1], "CALLS", nil); err != nil {
			tb.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		tb.Fatal(err)
	}
	// Unmerged layer: node 0 fans out to `cypherFanout` extra targets.
	if err := g.Batch(func(g *vectra.Graph) {
		for i := 0; i < cypherFanout; i++ {
			k := "x" + strconv.Itoa(i)
			if _, err := g.AddNode(k, "SVC", nil); err != nil {
				tb.Error(err)
			}
			if _, err := g.AddEdge(keys[0], k, "CALLS", nil); err != nil {
				tb.Error(err)
			}
		}
	}); err != nil {
		tb.Fatal(err)
	}
	return g
}

// benchCypherQuery times a single query against one graph built in
// setup: warm the view cache and result plumbing (3 runs, mirroring the
// root alloc pins), then one g.Cypher per loop iteration.
func benchCypherQuery(b *testing.B, q string) {
	b.Helper()
	g := cypherLayeredGraph(b)
	defer g.Close()
	for i := 0; i < 3; i++ {
		if _, err := g.Cypher(q); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := g.Cypher(q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCypher1Hop: 1-hop CALLS from the fan-out node — named vs
// anonymous end node (the anonymous form must skip per-row binding-map
// copies under copy-on-write, so its allocs/op anchors that property).
func BenchmarkCypher1Hop(b *testing.B) {
	for _, c := range []struct{ name, q string }{
		{"named", cypherQ1HopNamed},
		{"anonymous", cypherQ1HopAnon},
	} {
		b.Run(c.name, func(b *testing.B) {
			benchCypherQuery(b, c.q)
		})
	}
}

// BenchmarkCypherVarLength: bounded var-length expansion (*1..3) —
// exercises reachableWithin and its ranged-map scratch on the view path.
func BenchmarkCypherVarLength(b *testing.B) {
	benchCypherQuery(b, cypherQVarLength)
}

// BenchmarkCypherUndirected: undirected single hop — exercises the
// two-pass DirBoth consumption of the executor scratch.
func BenchmarkCypherUndirected(b *testing.B) {
	benchCypherQuery(b, cypherQUndirected)
}

// TestCypherBenchQueries is the smoke test: all four characterization
// queries must succeed on the current engine, on the exact fixture the
// benchmarks use. Row counts are pinned to the fixture's ground truth
// (node 0: 1 chain edge + 32 fan edges = 33 neighbors; var-length 1..3
// reaches nodes 1..3 plus the 32 fan targets = 35 endpoints) so a
// fixture drift that would silently invalidate the anchor is caught.
func TestCypherBenchQueries(t *testing.T) {
	g := cypherLayeredGraph(t)
	defer g.Close()
	cases := []struct {
		name string
		q    string
		want int
	}{
		{"1hop-named", cypherQ1HopNamed, cypherFanout + 1},
		{"1hop-anon", cypherQ1HopAnon, cypherFanout + 1},
		{"var-length", cypherQVarLength, cypherFanout + 3},
		{"undirected", cypherQUndirected, cypherFanout + 1},
	}
	for _, tc := range cases {
		r, err := g.Cypher(tc.q)
		if err != nil {
			t.Fatalf("%s: query failed on current engine: %v", tc.name, err)
		}
		if len(r.Rows) != tc.want {
			t.Fatalf("%s: got %d rows, want %d (fixture drift?)", tc.name, len(r.Rows), tc.want)
		}
	}
}
