package vectra_test

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/projanvil/vectra"
)

func TestClonePreSealIndependentEvolution(t *testing.T) {
	g := mustBuild(t) // 4 nodes 4 edges, unsealed
	c := g.Clone()
	if _, err := c.AddNode("extra", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddEdge("db", "auth", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if g.NodeCount() != 4 || g.EdgeCount() != 4 {
		t.Errorf("original graph polluted by clone's writes: %d/%d, want 4/4", g.NodeCount(), g.EdgeCount())
	}
	if c.NodeCount() != 5 || c.EdgeCount() != 5 {
		t.Errorf("writes on cloned graph failed: %d/%d, want 5/5", c.NodeCount(), c.EdgeCount())
	}
	if c.Sealed() {
		t.Error("clone's writes must leave unmerged layers (Sealed()==false)")
	}
}

func TestCloneSealedGraphIsMutableAgain(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	r1, _ := g.Reachable("gateway")
	c := g.Clone()
	if !c.Sealed() {
		t.Fatal("clone starts fully merged (union built into a fresh base)")
	}
	// writable regardless: the sealed gate is dissolved
	if _, err := c.AddNode("kafka", "Bus", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddEdge("db", "kafka", "EVENT", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Seal(); err != nil {
		t.Fatal(err)
	}
	// original graph unaffected
	r2, _ := g.Reachable("gateway")
	if !slices.Equal(r1, r2) {
		t.Error("clone's writes affected the original graph")
	}
	// clone query semantics intact
	r3, _ := c.Reachable("gateway")
	if !slices.Contains(r3, "kafka") {
		t.Errorf("clone's reachable set missing the added node: %v", r3)
	}
	st := c.Stats()
	if st.Nodes != 5 || st.Edges != 5 || st.SCCs != 5 {
		t.Errorf("clone stats = %+v", st)
	}
}

func TestWithNodeFilter(t *testing.T) {
	g := buildDAG(t) // gateway->auth/ledger->db->kafka
	// traverse only nodes with key!="db": db (kafka's only in-edge) is filtered → kafka unreachable
	res, err := g.BFS([]vectra.NodeID{ids(t, g, "gateway")[0]},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return n.Key != "db" }))
	if err != nil {
		t.Fatal(err)
	}
	if res.Reached(ids(t, g, "db")[0]) {
		t.Error("db(Storage) should be filtered out by the predicate")
	}
	if !res.Reached(ids(t, g, "auth")[0]) {
		t.Error("auth(Svc) should be reachable")
	}
	// start node is always included (even if it fails the predicate)
	res2, err := g.BFS([]vectra.NodeID{ids(t, g, "db")[0]},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return false }))
	if err != nil {
		t.Fatal(err)
	}
	if res2.Count() != 1 {
		t.Errorf("start node should still be included under a reject-all predicate: %d", res2.Count())
	}
	// combined with the parallel path
	res3, err := g.BFS([]vectra.NodeID{ids(t, g, "gateway")[0]},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return n.Key != "db" }),
		vectra.WithParallelThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	if res3.Count() != res.Count() {
		t.Errorf("predicate results inconsistent on the parallel path: %d vs %d", res3.Count(), res.Count())
	}
}

func TestQueryWhere(t *testing.T) {
	g := buildDAG(t)
	res, err := g.Query().From("gateway").Out().Where(func(n vectra.Node) bool {
		return n.Key != "db"
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if res.Reached(ids(t, g, "db")[0]) {
		t.Error("Where should filter out the Storage node")
	}
	if res.Count() != 3 { // gateway, auth, ledger
		t.Errorf("Count = %d, want 3", res.Count())
	}
}

func TestIndexPropertyLookup(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("b", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("c", "Svc", map[string]any{"env": "dev"})
	_, _ = g.AddNode("d", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("e", "Svc", map[string]any{"tier": float64(3)}) // non-strings are not indexed
	g.IndexProperty("env")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	// unregistered key
	if _, ok := g.LookupByProperty("tier", "3"); ok {
		t.Error("unregistered key should return ok=false")
	}
	// keys registered before Seal hit (ascending)
	nodes, ok := g.LookupByProperty("env", "prod")
	if !ok || len(nodes) != 3 {
		t.Fatalf("env=prod hit %d, ok=%v, want 3/true", len(nodes), ok)
	}
	want := []string{"a", "b", "d"}
	for i, n := range nodes {
		if n.Key != want[i] {
			t.Errorf("hit[%d] = %s, want %s", i, n.Key, want[i])
		}
		if n.Props["env"] != "prod" {
			t.Errorf("hit[%d] props wrong", i)
		}
	}
	// empty hit set (registered but no match)
	if nodes, ok := g.LookupByProperty("env", "staging"); !ok || len(nodes) != 0 {
		t.Errorf("no-match value should return empty set + ok=true, got %d/%v", len(nodes), ok)
	}
	// registering after Seal is pending until the next merge (R14): it
	// answers ok=true via the delta line scan, and Seal harvests it
	g.IndexProperty("team")
	if _, ok := g.LookupByProperty("team", "x"); !ok {
		t.Error("pending registration must answer ok=true (line-scan semantics)")
	}
	if _, err := g.AddNode("t", "S", map[string]any{"team": "core"}); err != nil {
		t.Fatal(err)
	}
	if ns, _ := g.LookupByProperty("team", "core"); len(ns) != 1 || ns[0].Key != "t" {
		t.Errorf("pending lookup hits = %+v, want [t]", ns)
	}
	// query before Seal
	g2 := vectra.New()
	_, _ = g2.AddNode("x", "S", map[string]any{"env": "prod"})
	if _, ok := g2.LookupByProperty("env", "prod"); ok {
		t.Error("LookupByProperty before Seal should return ok=false")
	}
}

func TestExportImportSnapshotUnsealed(t *testing.T) {
	g := mustBuild(t) // unsealed
	data, err := g.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.ImportSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if !g2.Sealed() {
		t.Error("ImportSnapshot merges into the initial base (no unmerged layers)")
	}
	if _, err := g2.AddNode("extra", "Svc", nil); err != nil { // writable regardless (gates dissolved)
		t.Fatal(err)
	}
	if g2.NodeCount() != 5 {
		t.Errorf("writes after import failed: %d, want 5", g2.NodeCount())
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	st1, st2 := g.Stats(), g2.Stats()
	if st1.Nodes != 4 || st2.Nodes != 5 || st2.Edges != 4 {
		t.Errorf("stats: %+v / %+v", st1, st2)
	}
}

func TestExportSealedMatchesSave(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	exp, err := g.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var saved bytes.Buffer
	if err := g.Save(&saved); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exp, saved.Bytes()) {
		t.Error("sealed-state ExportSnapshot should be byte-identical to Save")
	}
	g2, err := vectra.ImportSnapshot(exp)
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Errorf("imported sealed stats mismatch: %+v vs %+v", g2.Stats(), g.Stats())
	}
}

// ---- snapshot format v3: CRC integrity ----

func TestSnapshotCRCv3Roundtrip(t *testing.T) {
	g := buildCyclic(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	// version = 3
	if data[4] != 3 || data[5] != 0 {
		t.Errorf("version bytes = %d/%d, want 3/0", data[4], data[5])
	}
	g2, err := vectra.Load(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Error("CRC snapshot round-trip stats mismatch")
	}
}

func TestSnapshotCRCRejectsCorruption(t *testing.T) {
	g := buildCyclic(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	// flip one byte in the middle of the body
	corrupt := slices.Clone(data)
	corrupt[len(corrupt)/2] ^= 0xFF
	if _, err := vectra.Load(bytes.NewReader(corrupt)); err == nil {
		t.Error("corrupted body should be rejected by CRC")
	}
	// flip the CRC itself
	corrupt2 := slices.Clone(data)
	corrupt2[len(corrupt2)-1] ^= 0x01
	if _, err := vectra.Load(bytes.NewReader(corrupt2)); err == nil {
		t.Error("corrupted CRC should be rejected")
	}
	// truncated (before the CRC)
	if _, err := vectra.Load(bytes.NewReader(data[:len(data)-2])); err == nil {
		t.Error("truncated file should be rejected")
	}
}

func TestSnapshotV1StillReadable(t *testing.T) {
	// hand-build a v1 snapshot (no CRC trailer): assemble a minimal graph per the store package format spec
	v1 := bytes.NewBuffer(nil)
	v1.WriteString("VCTR")
	v1.Write([]byte{1, 0})       // version 1
	v1.Write([]byte{1, 0, 0, 0}) // 1 node
	v1.Write([]byte{0, 0, 0, 0}) // 0 edges
	v1.Write([]byte{1, 0, 0, 0}) // key len 1
	v1.WriteByte('a')            // key
	v1.Write([]byte{1, 0, 0, 0}) // label len 1
	v1.WriteByte('S')            // label
	v1.Write([]byte{0, 0, 0, 0}) // props len 0
	g, err := vectra.Load(bytes.NewReader(v1.Bytes()))
	if err != nil {
		t.Fatalf("v1 snapshot should be readable: %v", err)
	}
	n, ok := g.NodeByKey("a")
	if !ok || n.Label != "S" {
		t.Errorf("v1 content wrong: %+v", n)
	}
	if err := g.Seal(); err != nil { // Load already Sealed automatically; idempotent
		t.Error(err)
	}
}

func TestSaveFileAtomicNoTmpLeftover(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/atomic.snap"
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := vectra.LoadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := vectra.LoadFile(t.TempDir() + "/missing.snap"); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Error("missing file should error")
		}
	}
}
