package wal

// v0.11.2 scope evidence: what dominates the 512-vector batch boundary
// (gate-2b line 1, p50 ~7.0ms) now that vectors ride the binary section —
// the JSON skeleton encode, or the fsync? Runs with -bench:
//
//	go test ./internal/wal/ -bench 'AppendHoisted|SkeletonMarshal' -benchtime 50x -count 3
//
// Read: Always - Interval ~= fsync share; Interval ~= encode share;
// SkeletonMarshal isolates json.Marshal of the hoisted skeleton alone.

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/projanvil/vectra/internal/vecbin"
)

func benchChangeset(n int) Changeset {
	cs := Changeset{Nodes: make([]NodeUpsert, n)}
	for i := range cs.Nodes {
		vec := make([]float32, 768)
		for j := range vec {
			vec[j] = float32(i+j%97) / 97.0
		}
		cs.Nodes[i] = NodeUpsert{
			Key:   fmt.Sprintf("node-%08d", i),
			Label: "m",
			Props: map[string]any{"emb": vec, "seq": i, "kind": "doc"},
		}
	}
	return cs
}

func benchDecls() vecbin.PropDecls {
	return vecbin.PropDecls{"emb": 768}
}

func BenchmarkAppendHoistedAlways(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	cs := benchChangeset(512)
	decls := benchDecls()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.AppendHoisted(cs, decls); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendHoistedInterval(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir, ModeInterval)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	cs := benchChangeset(512)
	decls := benchDecls()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.AppendHoisted(cs, decls); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSkeletonMarshal isolates the JSON half of the v2 encode: the
// hoisted skeleton (vector props replaced by $vec refs) through
// json.Marshal — the candidate work "frame skeleton encoding" targets.
func BenchmarkSkeletonMarshal(b *testing.B) {
	cs := benchChangeset(512)
	decls := benchDecls()
	var sec []float32
	skel := Changeset{Nodes: make([]NodeUpsert, len(cs.Nodes))}
	for i, n := range cs.Nodes {
		out, _ := vecbin.Hoist(n.Props, decls, &sec)
		skel.Nodes[i] = NodeUpsert{Key: n.Key, Label: n.Label, Props: out}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(skel); err != nil {
			b.Fatal(err)
		}
	}
}

var _ = os.Getenv // keep os import if assertions evolve
