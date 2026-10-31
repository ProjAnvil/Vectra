package vectra

import (
	"encoding/json"
	"testing"
)

// json.Number properties (handed straight in by a caller that decodes JSON
// with encoding/json's UseNumber) must match on the PENDING path — the
// unmerged-layer scan, before any Seal/merge folds the records into a base —
// exactly as they match in the sealed base's typed index (the engine admits
// json.Number; propval.ToFloat made the pending predicates agree; audit
// F-18). Removing the arm from the shared helper must fail this test, not
// silently degrade lookups to index-only matching.
func TestJsonNumberMatchesOnPendingPath(t *testing.T) {
	g := New()
	g.IndexProperty("n")
	if _, err := g.AddNode("a", "N", map[string]any{"n": json.Number("42")}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("b", "N", map[string]any{"n": 43}); err != nil { // plain-int sibling: range sanity
		t.Fatal(err)
	}
	// No Seal/merge anywhere above: every record still lives in the pending
	// layers, so each lookup below answers from the pending predicate
	// (propval.ToFloat), not from a sealed base index.

	if ns, ok := g.LookupByNumber("n", 42); !ok || len(ns) != 1 || ns[0].Key != "a" {
		t.Fatalf("pending LookupByNumber(json.Number 42) = %v, %v; want [a]", ns, ok)
	}
	lo, hi := 41.5, 42.5
	if ns, ok := g.LookupByNumberRange("n", &lo, &hi); !ok || len(ns) != 1 || ns[0].Key != "a" {
		t.Fatalf("pending LookupByNumberRange [41.5,42.5] = %v, %v; want [a]", ns, ok)
	}
	if ns, ok := g.LookupByNumber("n", 43); !ok || len(ns) != 1 || ns[0].Key != "b" {
		t.Fatalf("pending LookupByNumber(int 43) = %v, %v; want [b] (sibling unchanged)", ns, ok)
	}
	res, err := g.Cypher(`MATCH (n:N) WHERE n.n = 42 RETURN n.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["n.key"] != "a" {
		t.Fatalf("cypher WHERE n.n = 42 over pending layer: rows = %v; want [a]", res.Rows)
	}

	// Guard the test's own premise: the reads above materialized the pend
	// window into unmerged layers, and nothing sealed them into the base.
	if v := g.view.Load(); len(v.layers) == 0 {
		t.Fatal("test premise broken: no unmerged layer — the pending path was never exercised")
	}
}
