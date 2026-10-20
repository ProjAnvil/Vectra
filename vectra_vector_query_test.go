package vectra_test

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/projanvil/vectra"
)

// buildVectorGraph creates a graph with one index over prop "emb" and n
// nodes with clustered 64-dim embeddings (in-distribution query protocol).
func buildVectorGraph(t *testing.T, n, dim int) (*vectra.Graph, [][]float32) {
	t.Helper()
	g := vectra.New()
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: dim, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(101))
	centers := make([][]float32, 16)
	for c := range centers {
		v := make([]float32, dim)
		for i := range v {
			v[i] = rng.Float32() - 0.5
		}
		norm := 0.0
		for _, x := range v {
			norm += float64(x) * float64(x)
		}
		norm = math.Sqrt(norm)
		for i := range v {
			v[i] = float32(float64(v[i]) / norm)
		}
		centers[c] = v
	}
	sigma := 0.35 / math.Sqrt(float64(dim))
	vecs := make([][]float32, n)
	for i := range vecs {
		c := centers[rng.Intn(len(centers))]
		v := make([]float32, dim)
		for j := range v {
			v[j] = c[j] + float32(rng.NormFloat64()*sigma)
		}
		norm := 0.0
		for _, x := range v {
			norm += float64(x) * float64(x)
		}
		norm = math.Sqrt(norm)
		for j := range v {
			v[j] = float32(float64(v[j]) / norm)
		}
		vecs[i] = v
		g.AddNode(keyN(i), "m", map[string]any{"emb": v})
	}
	g.FlushPend() // publish the absorption window: concurrent-reader tests read a settled generation
	return g, vecs
}

func keyN(i int) string {
	if i < 0 || i > 999999 {
		panic("bad key")
	}
	buf := []byte("n000000")
	for pos, v := 6, i; pos >= 1; pos-- {
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(buf)
}

func TestQueryVectorBasics(t *testing.T) {
	g, vecs := buildVectorGraph(t, 500, 64)
	defer g.Close()
	// Read-your-write: exact hits for an indexed vector (overlay state).
	hits, err := g.QueryVector("mem", vecs[7], vectra.QueryVectorOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Key != keyN(7) || hits[0].Score != 0 {
		t.Fatalf("exact self-query: %+v", hits)
	}
	// Unknown index / dim mismatch error; K defaults; ef override.
	if _, err := g.QueryVector("nope", vecs[0], vectra.QueryVectorOptions{}); err == nil {
		t.Fatal("unknown index must error")
	}
	if _, err := g.QueryVector("mem", vecs[0][:8], vectra.QueryVectorOptions{}); err == nil {
		t.Fatal("dim mismatch must error")
	}
	hits, err = g.QueryVector("mem", vecs[0], vectra.QueryVectorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 10 {
		t.Fatalf("K default = 10, got %d", len(hits))
	}
	for i := 1; i < len(hits); i++ {
		if hits[i-1].Score > hits[i].Score {
			t.Fatal("hits must ascend by score")
		}
	}
}

// TestQueryVectorRecall: in-distribution perturbed queries must hold the
// recall line against brute force (overlay state = exact, so this pins
// wiring; the packed-path bound rides the T8 lane test).
func TestQueryVectorRecall(t *testing.T) {
	g, vecs := buildVectorGraph(t, 3000, 64)
	defer g.Close()
	rng := rand.New(rand.NewSource(103))
	recall := 0.0
	for qi := 0; qi < 20; qi++ {
		src := vecs[rng.Intn(len(vecs))]
		q := make([]float32, len(src))
		copy(q, src)
		sigma := 0.35 / math.Sqrt(64)
		for j := range q {
			q[j] += float32(rng.NormFloat64() * sigma)
		}
		hits, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10})
		if err != nil {
			t.Fatal(err)
		}
		// Brute truth.
		type kd struct {
			k string
			d float64
		}
		all := make([]kd, len(vecs))
		for i, v := range vecs {
			var dot float64
			for j := range v {
				dot += float64(v[j]) * float64(q[j])
			}
			all[i] = kd{keyN(i), 1 - dot}
		}
		// small-n selection sort for top 10.
		for i := 0; i < 10; i++ {
			for j := i + 1; j < len(all); j++ {
				if all[j].d < all[i].d {
					all[i], all[j] = all[j], all[i]
				}
			}
		}
		want := map[string]bool{}
		for i := 0; i < 10; i++ {
			want[all[i].k] = true
		}
		hit := 0
		for _, h := range hits {
			if want[h.Key] {
				hit++
			}
		}
		recall += float64(hit) / 10
	}
	recall /= 20
	if recall < 0.95 {
		t.Fatalf("recall@10 = %.4f, want >= 0.95", recall)
	}
}

// TestQueryVectorLabelFilter: the 3x-oversample post-filter returns only
// matching labels and still fills K when enough matches exist.
func TestQueryVectorLabelFilter(t *testing.T) {
	g := vectra.New()
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	rng := rand.New(rand.NewSource(105))
	for i := 0; i < 100; i++ {
		label := "odd"
		if i%2 == 0 {
			label = "even"
		}
		v := make([]float32, 8)
		for j := range v {
			v[j] = rng.Float32()
		}
		g.AddNode(keyN(i), label, map[string]any{"emb": v})
	}
	// Query an odd node's own vector: with Label "odd", the self-hit must
	// come back (post-filter keeps it) and every hit must be odd.
	v := make([]float32, 8)
	// n3 is odd; fetch its vector via NodeByKey.
	node, ok := g.NodeByKey(keyN(3))
	if !ok {
		t.Fatal("node missing")
	}
	for j := 0; j < 8; j++ {
		v[j] = node.Props["emb"].([]float32)[j]
	}
	hits, err := g.QueryVector("mem", v, vectra.QueryVectorOptions{K: 5, Label: "odd"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("label filter returned nothing")
	}
	for _, h := range hits {
		n, _ := g.NodeByKey(h.Key)
		if n.Label != "odd" {
			t.Fatalf("label filter leaked %q (label %s)", h.Key, n.Label)
		}
	}
	if hits[0].Key != keyN(3) || hits[0].Score != 0 {
		t.Fatalf("self-hit must lead: %+v", hits[0])
	}
	if len(hits) < 5 {
		t.Fatalf("K=5 with 50 matching nodes must fill: %d", len(hits))
	}
}

// TestQueryVectorConcurrentReaders: 8 goroutines query concurrently
// against the published state (read-read contract; the single-writer
// discipline matches every other query API). -race is the gate.
func TestQueryVectorConcurrentReaders(t *testing.T) {
	g, vecs := buildVectorGraph(t, 2000, 64)
	defer g.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for i := 0; i < 30; i++ {
				q := vecs[rng.Intn(len(vecs))]
				hits, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 5})
				if err != nil {
					t.Errorf("query error: %v", err)
					return
				}
				if len(hits) != 5 {
					t.Errorf("k=5 got %d", len(hits))
					return
				}
				if hits[0].Score > hits[len(hits)-1].Score {
					t.Error("order broken")
					return
				}
			}
		}(w)
	}
	wg.Wait()
}
