package cypher

import (
	"testing"

	"github.com/projanvil/vectra/internal/engine"
)

// TestStarWithOrderBySupported pins the parser.go RETURN * path: ORDER BY
// together with * is supported (keys evaluate on the star-expanded
// bindings), not rejected.
func TestStarWithOrderBySupported(t *testing.T) {
	nodes := []engine.NodeRec{
		{Key: "a", Label: "N", Props: map[string]any{"k": "a"}},
		{Key: "b", Label: "N", Props: map[string]any{"k": "b"}},
	}
	snap, err := engine.Seal(nodes, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Run(snap, `MATCH (n:N) RETURN * ORDER BY n.k`)
	if err != nil {
		t.Fatalf("RETURN * with ORDER BY must parse and run: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(res.Rows))
	}
}
