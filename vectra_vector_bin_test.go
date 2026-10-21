package vectra_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra"
)

// Write-path frame selection (spec 2026-10-21 §1.2): a graph with declared
// vector indexes logs vector changesets as v2 frames (self-describing
// binary section, "VVB2" magic at the payload start); everything else keeps
// byte-identical v1 frames. The byte assertion reads the segment files
// directly; the reopen round-trips prove the decode side.

// walBytes concatenates every segment under the default WAL dir (db+".wal").
func walBytes(t *testing.T, db string) []byte {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(db+".wal", "wal-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	for _, s := range segs {
		b, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b...)
	}
	if len(out) == 0 {
		t.Fatalf("no WAL bytes under %s.wal", db)
	}
	return out
}

// evec returns the i-th basis vector of dim d: orthogonal inputs, so a
// K=1 cosine query on evec(i) can only answer the node carrying it.
func evec(d, i int) []float32 {
	v := make([]float32, d)
	v[i] = 1
	return v
}

// f64any widens v to the []any{float64...} shape JSON pipelines deliver.
func f64any(v []float32) []any {
	out := make([]any, len(v))
	for i, x := range v {
		out[i] = float64(x)
	}
	return out
}

func TestVectorWriteProducesV2Frame(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g := mustOpen(t, db, vectra.ModeAlways)
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	va, vb := evec(8, 0), evec(8, 1)
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddNode("a", "Doc", map[string]any{"emb": va, "t": "x"})
		g.AddNode("b", "Doc", map[string]any{"emb": vb, "t": "y"})
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(walBytes(t, db), []byte("VVB2")) {
		t.Fatal("declared vector write must land a v2 frame (VVB2 magic)")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Clean reopen: Close checkpointed, so this leg exercises the snapshot
	// channel (vector props reloaded as JSON floats).
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if hits, err := g2.QueryVector("mem", va, vectra.QueryVectorOptions{K: 1}); err != nil {
		t.Fatal(err)
	} else if len(hits) != 1 || hits[0].Key != "a" {
		t.Fatalf("snapshot round-trip: hits = %v, want [a]", hits)
	}

	// Crash leg: a v2 frame above the checkpoint never snapshots; Open must
	// decode it straight from the WAL (the v2 replay round-trip).
	vc := evec(8, 2)
	if err := g2.Batch(func(g *vectra.Graph) {
		g.AddNode("c", "Doc", map[string]any{"emb": vc})
	}); err != nil {
		t.Fatal(err)
	}
	g3 := mustOpen(t, db, vectra.ModeAlways) // g2 abandoned without Close
	if hits, err := g3.QueryVector("mem", vc, vectra.QueryVectorOptions{K: 1}); err != nil {
		t.Fatal(err)
	} else if len(hits) != 1 || hits[0].Key != "c" {
		t.Fatalf("v2 WAL replay: hits = %v, want [c]", hits)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFloat64SourcedVectorRidesV2(t *testing.T) {
	// Review Focus #1: values arriving as []any{float64...} (JSON pipeline
	// shape) ride v2 exactly like []float32 does — detection is
	// declaration-based, never Go-type-based.
	db := filepath.Join(t.TempDir(), "graph.db")
	g := mustOpen(t, db, vectra.ModeAlways)
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	va, vb := evec(8, 0), evec(8, 1)
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddNode("a", "Doc", map[string]any{"emb": f64any(va)})
		g.AddNode("b", "Doc", map[string]any{"emb": f64any(vb)})
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(walBytes(t, db), []byte("VVB2")) {
		t.Fatal("float64-sourced vector write must land a v2 frame")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2 := mustOpen(t, db, vectra.ModeAlways)
	defer g2.Close()
	if hits, err := g2.QueryVector("mem", va, vectra.QueryVectorOptions{K: 1}); err != nil {
		t.Fatal(err)
	} else if len(hits) != 1 || hits[0].Key != "a" {
		t.Fatalf("float64-sourced round-trip: hits = %v, want [a]", hits)
	}
}

func TestPlainGraphStaysV1(t *testing.T) {
	// No declared index: plain nodes AND undeclared numeric arrays stay in
	// byte-identical v1 JSON frames (the fallback is the guarantee, not a
	// behavior change).
	db := filepath.Join(t.TempDir(), "graph.db")
	g := mustOpen(t, db, vectra.ModeAlways)
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddNode("a", "Svc", map[string]any{"w": 3})
		g.AddNode("b", "Svc", map[string]any{"arr": []float32{1, 2, 3}})
	}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(walBytes(t, db), []byte("VVB2")) {
		t.Fatal("graph without vector declarations must not write v2 frames")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2 := mustOpen(t, db, vectra.ModeAlways)
	defer g2.Close()
	n, ok := g2.NodeByKey("b")
	if !ok {
		t.Fatal("node b lost across round-trip")
	}
	// JSON channel reloads arrays as []any{float64...} — data, not vectors.
	arr, ok := n.Props["arr"].([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("undeclared array prop survived as %T(%v), want []any len 3", n.Props["arr"], n.Props["arr"])
	}
	for i, want := range []float64{1, 2, 3} {
		if f, ok := arr[i].(float64); !ok || f != want {
			t.Fatalf("arr[%d] = %#v, want %v", i, arr[i], want)
		}
	}
}
