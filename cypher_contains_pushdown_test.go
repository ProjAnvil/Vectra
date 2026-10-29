package vectra

import (
	"slices"
	"testing"
)

// The Cypher CONTAINS prefilter must answer exactly like the scan it
// narrows: nodes added since the last Seal live in unmerged layers and a
// deleted node is tombstoned — the three-source compensation runs under
// the prefilter too, on the RunView path g.Cypher takes.
func TestCypherContainsPushdownViewPath(t *testing.T) {
	g := New()
	g.IndexFullText("title")
	ftsAdd(t, g, "s1", map[string]any{"title": "alpha beta"})
	ftsAdd(t, g, "s2", map[string]any{"title": "Beta Gamma"})
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	// Post-Seal additions ride unmerged layers; s2's override drops it out
	// of "Beta" matches while keeping the slot alive.
	ftsAdd(t, g, "u1", map[string]any{"title": "gamma delta"})
	ftsAdd(t, g, "u2", map[string]any{"title": "Beta delta"})
	ftsAdd(t, g, "s2", map[string]any{"title": "theta"})
	if _, err := g.DeleteNode("u2"); err != nil {
		t.Fatal(err)
	}

	// Twin graph without the FTS declaration answers through the plain scan.
	twin := New()
	ftsAdd(t, twin, "s1", map[string]any{"title": "alpha beta"})
	ftsAdd(t, twin, "s2", map[string]any{"title": "Beta Gamma"})
	_ = twin.Seal()
	ftsAdd(t, twin, "u1", map[string]any{"title": "gamma delta"})
	ftsAdd(t, twin, "u2", map[string]any{"title": "Beta delta"})
	ftsAdd(t, twin, "s2", map[string]any{"title": "theta"})
	_, _ = twin.DeleteNode("u2")

	cases := []struct {
		needle string
		want   []string
	}{
		{"beta", []string{"s1"}},  // case-sensitive: u2 deleted, s2 overridden, s2-old and u2 had capital Beta
		{"Beta", []string{}},      // s2's original "Beta Gamma" is overridden; u2 deleted
		{"gamma", []string{"u1"}}, // unmerged-layer match through the prefilter
		{"delta", []string{"u1"}}, // u2's tombstone must hold under the prefilter
		{"theta", []string{"s2"}}, // override value visible
		{"zz", []string{}},        // no hits, ok=true semantics
	}
	for _, tc := range cases {
		q := "MATCH (n) WHERE n.title CONTAINS '" + tc.needle + "' RETURN n.key"
		res, err := g.Cypher(q)
		if err != nil {
			t.Fatalf("needle %q: %v", tc.needle, err)
		}
		got := make([]string, 0, len(res.Rows))
		for _, r := range res.Rows {
			got = append(got, r["n.key"].(string))
		}
		slices.Sort(got)
		ref, err := twin.Cypher(q)
		if err != nil {
			t.Fatalf("twin %q: %v", tc.needle, err)
		}
		refKeys := make([]string, 0, len(ref.Rows))
		for _, r := range ref.Rows {
			refKeys = append(refKeys, r["n.key"].(string))
		}
		slices.Sort(refKeys)
		if !slices.Equal(got, tc.want) {
			t.Errorf("needle %q: prefilter path = %v, want %v", tc.needle, got, tc.want)
		}
		if !slices.Equal(got, refKeys) {
			t.Errorf("needle %q: prefilter path %v != scan twin %v", tc.needle, got, refKeys)
		}
	}
}
