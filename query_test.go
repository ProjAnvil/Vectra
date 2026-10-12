package vectra_test

import (
	"slices"
	"testing"

	"github.com/projanvil/vectra"
)

func TestQueryChainOut(t *testing.T) {
	g := buildDAG(t)
	res, err := g.Query().From("gateway").Out("CALLS").MaxDepth(1).Run()
	if err != nil {
		t.Fatal(err)
	}
	got := keysOf(t, g, res.Nodes())
	slices.Sort(got)
	if !slices.Equal(got, []string{"auth", "gateway", "ledger"}) {
		t.Errorf("nodes = %v", got)
	}
}

func TestQueryChainIn(t *testing.T) {
	g := buildDAG(t)
	res, err := g.Query().From("db").In("QUERY").MaxDepth(1).Run()
	if err != nil {
		t.Fatal(err)
	}
	got := keysOf(t, g, res.Nodes())
	slices.Sort(got)
	if !slices.Equal(got, []string{"auth", "db", "ledger"}) {
		t.Errorf("nodes = %v", got)
	}
}

func TestQueryMultiFromNoLabel(t *testing.T) {
	g := buildDAG(t)
	res, err := g.Query().From("auth", "ledger").Out().MaxDepth(2).Run()
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 4 {
		t.Errorf("Count = %d, want 4", res.Count())
	}
}

func TestQueryUnknownFrom(t *testing.T) {
	g := buildDAG(t)
	if _, err := g.Query().From("nope").Out().Run(); err == nil {
		t.Error("expected error for unknown From key")
	}
}

func TestQueryEmptyFrom(t *testing.T) {
	g := buildDAG(t)
	if _, err := g.Query().Out().Run(); err == nil {
		t.Error("expected error for empty From")
	}
}

func TestNeighborsOf(t *testing.T) {
	g := buildDAG(t)
	ns, err := g.NeighborsOf("gateway", vectra.DirOut, "CALLS")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(ns))
	for i, n := range ns {
		got[i] = n.Key
	}
	if !slices.Equal(got, []string{"auth", "ledger"}) {
		t.Errorf("neighbors = %v", got)
	}
	// 反向 + 无过滤
	ns, err = g.NeighborsOf("db", vectra.DirIn)
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 2 {
		t.Errorf("in-neighbors = %d, want 2", len(ns))
	}
	if _, err := g.NeighborsOf("nope", vectra.DirOut); err == nil {
		t.Error("expected error for unknown key")
	}
}
