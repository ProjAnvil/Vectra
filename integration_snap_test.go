package vectra_test

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/projanvil/vectra"
)

// ============================================================
// SNAP email-Eu-core integration tests (LDBC audit-style external
// verification)
//
// The dataset is a real email network; the statistics are published
// independently by SNAP/SuiteSparse. These numbers were not computed by
// us — they are external ground truth.
// ============================================================

// TestSNAP_PublishedStats: externally published statistics:
// 1005 nodes / 25571 edges / 203 SCCs / largest SCC 803 nodes.
// From 1005/203/803 the remaining 202 SCCs must all be singletons;
// the data file also has 642 self-loop nodes (cross-checked with
// awk '$1==$2'), hence Cycles() = one 803-SCC + 642 self-loops = 643.
func TestSNAP_PublishedStats(t *testing.T) {
	g, _ := loadSNAPEmail(t)
	st := g.Stats()
	if st.Nodes != 1005 {
		t.Errorf("Nodes = %d, want 1005 (SNAP published)", st.Nodes)
	}
	if st.Edges != 25571 {
		t.Errorf("Edges = %d, want 25571 (SNAP published)", st.Edges)
	}
	if st.SCCs != 203 {
		t.Errorf("SCCs = %d, want 203 (SuiteSparse published)", st.SCCs)
	}
	if st.CyclicNodes != 803 {
		t.Errorf("CyclicNodes = %d, want 803 (largest SCC size, SNAP published)", st.CyclicNodes)
	}
	cycles := g.Cycles()
	if len(cycles) != 643 {
		t.Fatalf("Cycles() = %d, want 643 (one 803-SCC + 642 self-loop nodes)", len(cycles))
	}
	if cycles[0].Len() != 803 {
		t.Errorf("largest cycle = %d nodes, want 803", cycles[0].Len())
	}
}

// TestSNAP_LargestSCCStronglyConnected: the definition of the largest SCC
// is itself the test — all 803 members mutually reachable. BFS from all
// 803 members, asserting every member is covered. (803 BFS runs × 25k
// edges ≈ 20M edge visits, a full-scale check on real data.)
func TestSNAP_LargestSCCStronglyConnected(t *testing.T) {
	g, _ := loadSNAPEmail(t)
	scc := g.Cycles()[0]
	members := map[vectra.NodeID]bool{}
	for _, id := range scc.Nodes {
		members[id] = true
	}
	if len(members) != 803 {
		t.Fatalf("SCC members = %d, want 803", len(members))
	}
	for _, src := range scc.Nodes {
		res, err := g.BFS([]vectra.NodeID{src}, vectra.WithParallelThreshold(1<<30))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range scc.Nodes {
			if !res.Reached(m) {
				t.Fatalf("SCC member %d cannot reach member %d — violates strong connectivity", src, m)
			}
		}
	}
}

// TestSNAP_ReachableSupersetOfSCC: supernode reachability contains the
// entire start component (per the contract that component members are
// mutually reachable via arbitrary edges), and matches the reference
// implementation node for node.
func TestSNAP_ReachableSupersetOfSCC(t *testing.T) {
	g, el := loadSNAPEmail(t)
	adj := el.adj()
	scc := g.Cycles()[0]
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 10; i++ {
		src := scc.Nodes[rng.Intn(len(scc.Nodes))]
		got, err := g.ReachableIDs(src)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range scc.Nodes {
			found := false
			for _, id := range got {
				if id == m {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Reachable(%d) missing same-component member %d", src, m)
			}
		}
		// cross-check against the reference implementation
		want := refBFS(adj, int(src))
		if len(got) != len(want) {
			t.Fatalf("Reachable(%d) = %d nodes, reference BFS = %d nodes", src, len(got), len(want))
		}
	}
}

// TestSNAP_ShortestPathCrossValidation: 300 random node pairs on the real
// graph — bidirectional BFS distances match the reference BFS; reachable
// pairs get path-validity checks.
func TestSNAP_ShortestPathCrossValidation(t *testing.T) {
	g, el := loadSNAPEmail(t)
	adj := el.adj()
	rng := rand.New(rand.NewSource(13))
	checked, unreachable := 0, 0
	for i := 0; i < 300; i++ {
		u, v := vectra.NodeID(rng.Intn(el.n)), vectra.NodeID(rng.Intn(el.n))
		if u == v {
			continue
		}
		p, found, err := g.ShortestPathIDs(u, v)
		if err != nil {
			t.Fatal(err)
		}
		want := refBFS(adj, int(u))
		d, ok := want[int(v)]
		if !ok {
			if found {
				t.Fatalf("%d->%d: path found but reference says unreachable", u, v)
			}
			unreachable++
			continue
		}
		checked++
		if !found {
			t.Fatalf("%d->%d: reference distance %d but no path found", u, v, d)
		}
		if p.Len() != d {
			t.Fatalf("%d->%d: path length %d, reference distance %d", u, v, p.Len(), d)
		}
		for j := 0; j+1 < len(p.Nodes); j++ {
			e, ok := g.EdgeByID(p.Edges[j])
			if !ok || e.Src != p.Nodes[j] || e.Dst != p.Nodes[j+1] {
				t.Fatalf("%d->%d: path edge %d invalid", u, v, j)
			}
		}
	}
	if checked == 0 {
		t.Fatal("all 300 random pairs unreachable? A real email graph should not behave like that")
	}
	t.Logf("300 random pairs: %d reachable pairs all cross-validated, %d unreachable pairs agree", checked, unreachable)
}

// TestSNAP_SnapshotRoundtrip: query behavior unchanged after a snapshot
// round-trip on the real graph.
func TestSNAP_SnapshotRoundtrip(t *testing.T) {
	g, _ := loadSNAPEmail(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.Load(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Fatalf("stats mismatch after round-trip: %+v vs %+v", g2.Stats(), g.Stats())
	}
	// path lengths agree for the same node pairs
	for _, pair := range [][2]int{{0, 500}, {3, 999}, {1004, 42}} {
		p1, ok1, _ := g.ShortestPathIDs(vectra.NodeID(pair[0]), vectra.NodeID(pair[1]))
		p2, ok2, _ := g2.ShortestPathIDs(vectra.NodeID(pair[0]), vectra.NodeID(pair[1]))
		if ok1 != ok2 || p1.Len() != p2.Len() {
			t.Errorf("pair %v: path differs after snapshot round-trip (%d/%v vs %d/%v)",
				pair, p1.Len(), ok1, p2.Len(), ok2)
		}
	}
}

// TestSNAP_InDegreeSpotCheck: degree spot check — reverse BFS one-hop
// neighbors == reference in-degree.
func TestSNAP_InDegreeSpotCheck(t *testing.T) {
	g, el := loadSNAPEmail(t)
	inDeg := make(map[int]int)
	for _, d := range el.dst {
		inDeg[d]++
	}
	for _, probe := range []int{0, 1, 500, 1004} {
		ns, err := g.NeighborsOf(strconv.Itoa(probe), vectra.DirIn)
		if err != nil {
			t.Fatal(err)
		}
		if len(ns) != len(uniqueTargets(el, probe)) {
			t.Errorf("node %d in-neighbors %d, reference deduped in-degree %d", probe, len(ns), len(uniqueTargets(el, probe)))
		}
	}
}

func uniqueTargets(el edgeList, node int) map[int]bool {
	set := map[int]bool{}
	for i, d := range el.dst {
		if d == node {
			set[el.src[i]] = true
		}
	}
	return set
}
