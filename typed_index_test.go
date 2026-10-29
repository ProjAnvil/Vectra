package vectra

import (
	"bytes"
	"testing"
)

func buildTypedGraph(t *testing.T) *Graph {
	t.Helper()
	g := New()
	g.IndexProperty("type").IndexProperty("weight").IndexProperty("on")
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := g.AddNode("gw", "Service", map[string]any{"type": "controller", "weight": 3, "on": true})
	must(err)
	_, err = g.AddNode("orders", "Service", map[string]any{"type": "service", "weight": 7.5, "on": false})
	must(err)
	_, err = g.AddNode("db", "Store", map[string]any{"type": "store", "weight": 42}) // int, no "on" field
	must(err)
	_, err = g.AddEdge("gw", "orders", "CALLS", nil)
	must(err)
	_, err = g.AddEdge("orders", "db", "USES", nil)
	must(err)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestLookupByNumberAndRangeAndBool(t *testing.T) {
	g := buildTypedGraph(t)
	if ns, ok := g.LookupByNumber("weight", 42); !ok || len(ns) != 1 || ns[0].Key != "db" {
		t.Fatalf("LookupByNumber: %v %v", ns, ok)
	}
	lo, hi := 3.0, 8.0
	if ns, ok := g.LookupByNumberRange("weight", &lo, &hi); !ok || len(ns) != 2 {
		t.Fatalf("range [3,8]: %v %v", ns, ok)
	}
	if ns, _ := g.LookupByNumberRange("weight", nil, &lo); len(ns) != 1 || ns[0].Key != "gw" {
		t.Fatalf("range (-inf,3]: %v", ns)
	}
	if ns, ok := g.LookupByBool("on", true); !ok || len(ns) != 1 || ns[0].Key != "gw" {
		t.Fatalf("LookupByBool: %v %v", ns, ok)
	}
	if _, ok := g.LookupByNumber("absent", 1); ok {
		t.Fatal("unindexed key must be ok=false")
	}
}

func TestLookupTypedSurvivesJSONRoundTrip(t *testing.T) {
	g := buildTypedGraph(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	g2, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	// written as int 42, JSON round-trip reads float64 — the normalized numeric lookup still hits
	if ns, ok := g2.LookupByNumber("weight", 42); !ok || len(ns) != 1 || ns[0].Key != "db" {
		t.Fatalf("round-trip number eq: %v %v", ns, ok)
	}
	if ns, ok := g2.LookupByBool("on", true); !ok || len(ns) != 1 || ns[0].Key != "gw" {
		t.Fatalf("round-trip bool: %v %v", ns, ok)
	}
}

func TestLookupTypedPendingBeforeMerge(t *testing.T) {
	g := New()
	g.IndexProperty("w")
	_, _ = g.AddNode("a", "L", map[string]any{"w": 1})
	// pending declarations answer via line scan (R14) — ok=true with hits
	ns, ok := g.LookupByNumber("w", 1)
	if !ok || len(ns) != 1 || ns[0].Key != "a" {
		t.Fatalf("pending number lookup = %+v ok=%v, want [a]/true", ns, ok)
	}
	if _, ok := g.LookupByBool("w", true); !ok {
		t.Fatal("registered-but-empty pending key must answer ok=true")
	}
	if _, ok := g.LookupByNumber("absent", 1); ok {
		t.Fatal("never-declared key must return ok=false")
	}
}
