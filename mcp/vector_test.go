package mcpserver

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
)

// newVectorSession starts a session over a graph with one index "mem"
// (dim 32) and n vectors, returning the graph and the vector slice.
func newVectorSession(t *testing.T, n int) (*mcp.ClientSession, [][]float32) {
	t.Helper()
	g := vectra.New()
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 32, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(5))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, 32)
		var norm float64
		for j := range v {
			v[j] = rng.Float32() - 0.5
			norm += float64(v[j]) * float64(v[j])
		}
		norm = math.Sqrt(norm)
		for j := range v {
			v[j] = float32(float64(v[j]) / norm)
		}
		vecs[i] = v
		g.AddNode(vectorKey(i), "m", map[string]any{"emb": v})
	}
	g.FlushPend()
	s := New(g, Options{})
	ct, st := mcp.NewInMemoryTransports()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx, st) }()
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close(); cancel() })
	return cs, vecs
}

func vectorKey(i int) string {
	return "n" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

func TestVectorSearchTool(t *testing.T) {
	cs, vecs := newVectorSession(t, 120)
	out := call[struct {
		Count int `json:"count"`
		Hits  []struct {
			Key   string  `json:"key"`
			Score float64 `json:"score"`
		} `json:"hits"`
		Note string `json:"note"`
	}](t, cs, "vector_search", map[string]any{
		"index":        "mem",
		"query_vector": floats64(vecs[3]),
		"k":            5,
	})
	if out.Count != 5 || len(out.Hits) != 5 {
		t.Fatalf("count=%d hits=%d", out.Count, len(out.Hits))
	}
	if out.Hits[0].Key != vectorKey(3) || out.Hits[0].Score != 0 {
		t.Fatalf("self-hit must lead: %+v", out.Hits[0])
	}
	if out.Note == "" {
		t.Fatal("approximation note missing")
	}
	// similar_to: query with node 7's own vector through the key path.
	out2 := call[struct {
		Count int `json:"count"`
		Hits  []struct {
			Key   string  `json:"key"`
			Score float64 `json:"score"`
		} `json:"hits"`
	}](t, cs, "vector_search", map[string]any{
		"index":      "mem",
		"similar_to": vectorKey(7),
		"k":          1,
	})
	if out2.Count != 1 || out2.Hits[0].Key != vectorKey(7) {
		t.Fatalf("similar_to self-hit: %+v", out2.Hits)
	}
}

func TestVectorSearchValidation(t *testing.T) {
	cs, _ := newVectorSession(t, 10)
	ctx := context.Background()

	mustErr := func(args map[string]any) {
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "vector_search", Arguments: args})
		if err == nil && !res.IsError {
			t.Fatalf("expected error for %+v", args)
		}
	}
	mustErr(map[string]any{"index": "mem", "k": 5})                                                            // neither param
	mustErr(map[string]any{"index": "mem", "query_vector": floats64(make([]float32, 32)), "similar_to": "n0"}) // both
	mustErr(map[string]any{"index": "nope", "query_vector": floats64(make([]float32, 32))})                    // unknown index
	mustErr(map[string]any{"index": "mem", "query_vector": floats64(make([]float32, 8))})                      // dim mismatch
	// k clamps to 100 rather than erroring.
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "vector_search", Arguments: map[string]any{
		"index": "mem", "query_vector": floats64(make([]float32, 32)), "k": 5000,
	}})
	if err != nil || res.IsError {
		t.Fatalf("k clamp should pass: err=%v isError=%v", err, res.IsError)
	}
}

func floats64(v []float32) []float64 {
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = float64(x)
	}
	return out
}
