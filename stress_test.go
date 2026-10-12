package vectra_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/projanvil/vectra"
)

// ============================================================
// Robustness dimension (the robustness axis of LDBC Graphalytics)
// ============================================================

// TestConcurrentReadersMixedQueries verifies the core claim: "immutable
// after Seal, lock-free concurrent reads". 8 goroutines mix all read-only
// APIs (including parallel BFS and snapshot export); passing under -race
// means no data races. Any query result must remain self-consistent when
// interleaved with other goroutines' queries (compared against
// single-threaded baseline values).
func TestConcurrentReadersMixedQueries(t *testing.T) {
	g := randGraph(t, 5000, 50000)
	// single-threaded baseline values
	baseReach, err := g.ReachableIDs(0)
	if err != nil {
		t.Fatal(err)
	}
	basePath, _, err := g.ShortestPathIDs(0, 4999)
	if err != nil {
		t.Fatal(err)
	}
	baseBFS, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 30; i++ {
				var err error
				switch rng.Intn(5) {
				case 0: // parallel BFS
					res, e := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0))
					if e != nil {
						err = e
					} else if res.Count() != baseBFS.Count() {
						err = fmt.Errorf("concurrent BFS count=%d baseline=%d", res.Count(), baseBFS.Count())
					}
				case 1: // reachability
					r, e := g.ReachableIDs(0)
					if e != nil {
						err = e
					} else if len(r) != len(baseReach) {
						err = fmt.Errorf("concurrent Reachable=%d baseline=%d", len(r), len(baseReach))
					}
				case 2: // shortest path
					p, found, e := g.ShortestPathIDs(0, 4999)
					if e != nil {
						err = e
					} else if !found || p.Len() != basePath.Len() {
						err = fmt.Errorf("concurrent shortest path len=%d baseline=%d", p.Len(), basePath.Len())
					}
				case 3: // cycle list
					if cs := g.Cycles(); cs == nil && g.Stats().CyclicNodes > 0 {
						err = fmt.Errorf("Cycles() empty but CyclicNodes>0")
					}
				case 4: // snapshot export to per-goroutine buffer
					var buf bytes.Buffer
					if e := g.Save(&buf); e != nil {
						err = e
					} else if buf.Len() == 0 {
						err = fmt.Errorf("empty snapshot")
					}
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestDegenerateGraphs: table-driven degenerate shapes — after Seal every
// query must give structurally correct answers for each shape (empty
// graph, isolated node, dense clique, chain, star, multi-edges, a sea of
// self-loops).
func TestDegenerateGraphs(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		g := vectra.New()
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if st := g.Stats(); st.Nodes != 0 || st.Edges != 0 || st.SCCs != 0 {
			t.Errorf("empty graph stats = %+v", st)
		}
		if cs := g.Cycles(); len(cs) != 0 {
			t.Errorf("empty graph Cycles = %d", len(cs))
		}
		res, err := g.BFS(nil)
		if err != nil || res.Count() != 0 {
			t.Errorf("empty graph BFS = %d err=%v", res.Count(), err)
		}
		var buf bytes.Buffer
		if err := g.Save(&buf); err != nil {
			t.Fatal(err)
		}
		if g2, err := vectra.Load(&buf); err != nil || g2.Stats() != g.Stats() {
			t.Errorf("empty graph snapshot round-trip failed: %v", err)
		}
	})
	t.Run("SingleIsolatedNode", func(t *testing.T) {
		g := vectra.New()
		if _, err := g.AddNode("only", "V", nil); err != nil {
			t.Fatal(err)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if st := g.Stats(); st.Nodes != 1 || st.SCCs != 1 || st.CyclicNodes != 0 {
			t.Errorf("stats = %+v", st)
		}
		r, err := g.ReachableIDs(0)
		if err != nil || len(r) != 1 {
			t.Errorf("Reachable = %v err=%v", r, err)
		}
	})
	t.Run("SingleSelfLoop", func(t *testing.T) {
		g := vectra.New()
		if _, err := g.AddNode("a", "V", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := g.AddEdge("a", "a", "E", nil); err != nil {
			t.Fatal(err)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		cs := g.Cycles()
		if len(cs) != 1 || cs[0].Len() != 1 {
			t.Errorf("self-loop Cycles = %v", cs)
		}
	})
	t.Run("ParallelMultiEdges", func(t *testing.T) {
		g := vectra.New()
		for _, k := range []string{"a", "b"} {
			if _, err := g.AddNode(k, "V", nil); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 5; i++ {
			if _, err := g.AddEdge("a", "b", "E", nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if g.EdgeCount() != 5 {
			t.Errorf("multi-edge preservation failed: %d", g.EdgeCount())
		}
		res, err := g.BFS([]vectra.NodeID{0})
		if err != nil || res.Count() != 2 {
			t.Errorf("multi-edge BFS count = %d err=%v", res.Count(), err)
		}
		p, found, err := g.ShortestPathIDs(0, 1)
		if err != nil || !found || p.Len() != 1 {
			t.Errorf("multi-edge shortest path = %v found=%v", p, found)
		}
		ns, _ := g.NeighborsOf("a", vectra.DirOut)
		if len(ns) != 1 {
			t.Errorf("one-hop neighbors should be deduplicated: %d", len(ns))
		}
	})
	t.Run("Clique50", func(t *testing.T) {
		n := 50
		g := vectra.New()
		for i := 0; i < n; i++ {
			_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
		}
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if i != j {
					_, _ = g.AddEdge(strconv.Itoa(i), strconv.Itoa(j), "E", nil)
				}
			}
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		st := g.Stats()
		if st.SCCs != 1 || st.CyclicNodes != n {
			t.Errorf("complete graph should be a single SCC: %+v", st)
		}
		p, found, _ := g.ShortestPathIDs(0, vectra.NodeID(n-1))
		if !found || p.Len() != 1 {
			t.Errorf("any two nodes in a complete graph should be 1 hop: %v", p)
		}
	})
	t.Run("Star1000", func(t *testing.T) {
		g := vectra.New()
		_, _ = g.AddNode("c", "V", nil)
		for i := 0; i < 1000; i++ {
			_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
			_, _ = g.AddEdge("c", strconv.Itoa(i), "E", nil)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		res, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0)) // center ID=0, forced parallel
		if err != nil || res.Count() != 1001 {
			t.Errorf("star BFS = %d err=%v", res.Count(), err)
		}
		if st := g.Stats(); st.SCCs != 1001 || st.CyclicNodes != 0 {
			t.Errorf("star should be acyclic: %+v", st)
		}
	})
	t.Run("AllSelfLoops", func(t *testing.T) {
		g := vectra.New()
		for i := 0; i < 100; i++ {
			k := strconv.Itoa(i)
			_, _ = g.AddNode(k, "V", nil)
			_, _ = g.AddEdge(k, k, "E", nil)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if cs := g.Cycles(); len(cs) != 100 {
			t.Errorf("all-self-loops Cycles = %d, want 100", len(cs))
		}
		if st := g.Stats(); st.SCCs != 100 || st.CyclicNodes != 0 {
			t.Errorf("self-loops must not count toward CyclicNodes: %+v", st)
		}
	})
}

// TestDeepChain1M: million-node chain — call-stack pressure on iterative
// Tarjan (a recursive implementation would balloon the stack on such deep
// graphs) + whole-chain BFS depth correctness + bidirectional search.
// Takes a few seconds; skipped in -short mode.
func TestDeepChain1M(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-node stress test in short mode")
	}
	const n = 1_000_000
	g := vectra.New()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = strconv.Itoa(i)
		_, _ = g.AddNode(keys[i], "V", nil)
	}
	for i := 0; i+1 < n; i++ {
		_, _ = g.AddEdge(keys[i], keys[i+1], "E", nil)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if st := g.Stats(); st.Nodes != n || st.SCCs != n {
		t.Errorf("chain stats = %+v", st)
	}
	ser, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(1<<30))
	if err != nil {
		t.Fatal(err)
	}
	if ser.Count() != n {
		t.Errorf("chain BFS count = %d, want %d", ser.Count(), n)
	}
	if d := ser.Depth(vectra.NodeID(n - 1)); d != n-1 {
		t.Errorf("chain tail depth = %d, want %d", d, n-1)
	}
	par, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	if par.Count() != ser.Count() || par.Depth(vectra.NodeID(n-1)) != ser.Depth(vectra.NodeID(n-1)) {
		t.Error("parallel and serial results differ on the deep chain")
	}
	// bidirectional search on the longest chain: a 1M-hop path whose length exactly equals the midpoint distance
	p, found, err := g.ShortestPathIDs(0, vectra.NodeID(n-1))
	if err != nil || !found {
		t.Fatalf("chain shortest path: found=%v err=%v", found, err)
	}
	if p.Len() != n-1 {
		t.Errorf("chain shortest path = %d, want %d", p.Len(), n-1)
	}
	r, err := g.ReachableIDs(0)
	if err != nil || len(r) != n {
		t.Errorf("chain Reachable = %d err=%v", len(r), err)
	}
}

// TestDeterminismSameSeedSameResults: build twice with the same seed, all
// query results bit-for-bit identical (LDBC benchmark validate mode
// requires repeatability).
func TestDeterminismSameSeedSameResults(t *testing.T) {
	build := func() *vectra.Graph {
		g := vectra.New()
		rng := rand.New(rand.NewSource(2024))
		for i := 0; i < 2000; i++ {
			_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
		}
		for i := 0; i < 20000; i++ {
			_, _ = g.AddEdge(strconv.Itoa(rng.Intn(2000)), strconv.Itoa(rng.Intn(2000)), "E", nil)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		return g
	}
	g1, g2 := build(), build()
	if g1.Stats() != g2.Stats() {
		t.Fatalf("stats mismatch: %+v vs %+v", g1.Stats(), g2.Stats())
	}
	c1, c2 := g1.Cycles(), g2.Cycles()
	if len(c1) != len(c2) {
		t.Fatalf("Cycles count mismatch")
	}
	for i := range c1 {
		for j := range c1[i].Nodes {
			if c1[i].Nodes[j] != c2[i].Nodes[j] {
				t.Fatalf("Cycles[%d] members mismatch", i)
			}
		}
	}
	r1, _ := g1.ReachableIDs(7)
	r2, _ := g2.ReachableIDs(7)
	for i := range r1 {
		if r1[i] != r2[i] {
			t.Fatalf("Reachable results differ at position %d", i)
		}
	}
	p1, ok1, _ := g1.ShortestPathIDs(3, 1997)
	p2, ok2, _ := g2.ShortestPathIDs(3, 1997)
	if ok1 != ok2 {
		t.Fatal("reachability determination mismatch")
	}
	for i := range p1.Nodes {
		if p1.Nodes[i] != p2.Nodes[i] {
			t.Fatalf("shortest paths differ at hop %d", i)
		}
	}
}
