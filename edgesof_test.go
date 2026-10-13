package vectra_test

import (
	"errors"
	"testing"

	"github.com/projanvil/vectra"
)

func TestEdgesOfSealed(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	out, err := g.EdgesOf("gateway", vectra.DirOut)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("out edges = %d, want 2", len(out))
	}
	for _, e := range out {
		if e.Src != 0 || (e.Dst != 1 && e.Dst != 2) || e.Label != "CALLS" {
			t.Errorf("edge = %+v", e)
		}
	}
	// direction + label filter
	in, err := g.EdgesOf("db", vectra.DirIn, "QUERY")
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 2 {
		t.Fatalf("in edges = %d, want 2", len(in))
	}
	callOnly, err := g.EdgesOf("db", vectra.DirIn, "CALLS")
	if err != nil {
		t.Fatal(err)
	}
	if len(callOnly) != 0 {
		t.Fatalf("label-filtered in edges = %d, want 0", len(callOnly))
	}
	if _, err := g.EdgesOf("x", vectra.DirOut); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("missing key err = %v, want ErrNodeNotFound", err)
	}
}

func TestEdgesOfUnsealedAndAfterDelete(t *testing.T) {
	g := mustBuild(t) // unsealed: the read-modify-write weight pattern reads here
	out, err := g.EdgesOf("gateway", vectra.DirOut)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("unsealed out = %d, want 2", len(out))
	}
	if _, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil {
		t.Fatal(err)
	}
	out, err = g.EdgesOf("gateway", vectra.DirOut)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Dst != 2 {
		t.Fatalf("after delete = %+v, want only the ledger edge", out)
	}
}

func TestEdgesOfReturnsAllParallelEdges(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := g.AddEdge("a", "b", "E", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	es, err := g.EdgesOf("a", vectra.DirOut)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 3 { // multigraph: every parallel edge carries its own props
		t.Fatalf("parallel edges = %d, want 3", len(es))
	}
}
