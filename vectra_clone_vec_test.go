package vectra_test

import (
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/projanvil/vectra"
)

// unitVec returns one seeded unit vector of dim d (queries and stored
// embeddings alike; cosine indexes store the normalized form anyway).
func unitVec(rng *rand.Rand, d int) []float32 {
	v := make([]float32, d)
	norm := 0.0
	for i := range v {
		v[i] = rng.Float32() - 0.5
		norm += float64(v[i]) * float64(v[i])
	}
	norm = math.Sqrt(norm)
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
	return v
}

// sameHits pins exact result identity: same keys in the same order with
// bitwise-equal scores (VectorHit is comparable).
func sameHits(a, b []vectra.VectorHit) bool { return slices.Equal(a, b) }

// TestCloneCarriesVectorState pins the Clone read-only vector mount
// (ruling #5): the clone answers QueryVector from the source's published
// snapshot heads — identical hits and identical declarations — and stays
// frozen when the source keeps writing (snapshot isolation). The corpus
// crosses the pack size twice, so the mounted head carries packed
// segments, not just overlay entries. Writing to the clone and Sealing it
// (the server publisher's clone → Seal shape) must neither panic nor move
// the frozen results.
func TestCloneCarriesVectorState(t *testing.T) {
	g := vectra.New()
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{
		Name: "emb", Prop: "v", Dim: 8, Metric: "cosine",
	}); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	rng := rand.New(rand.NewSource(7))
	const packed = 2 * 2048 // two full chunks: the folder lane packs them
	const tail = 100        // stays in the overlay (read-your-write)
	vecs := make([][]float32, packed+tail)
	for i := range vecs {
		vecs[i] = unitVec(rng, 8)
		if _, err := g.AddNode(keyN(i), "m", map[string]any{"v": vecs[i]}); err != nil {
			t.Fatal(err)
		}
	}
	g.FlushPend()
	// Quiesce until no full chunk is pending: at least one chunk packed
	// into a segment and the head swapped (spec 7.1).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if g.VectorOverlayLen("emb") < 2048 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := g.VectorOverlayLen("emb"); n >= 2048 {
		t.Fatalf("overlay did not drain below one chunk: %d", n)
	}

	// Perturbed in-distribution query (the recall-test protocol).
	q := append([]float32(nil), vecs[rng.Intn(len(vecs))]...)
	for j := range q {
		q[j] += float32(0.01 * rng.NormFloat64())
	}
	opt := vectra.QueryVectorOptions{K: 10, Ef: 200} // fixed ef: determinism
	want, err := g.QueryVector("emb", q, opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) != 10 {
		t.Fatalf("source hits = %d, want 10", len(want))
	}

	c := g.Clone()
	defer c.Close()
	got, err := c.QueryVector("emb", q, opt)
	if err != nil {
		t.Fatal(err)
	}
	if !sameHits(want, got) {
		t.Fatalf("clone hits diverge:\nsource: %v\nclone:  %v", want, got)
	}
	if !slices.Equal(g.VectorIndexes(), c.VectorIndexes()) {
		t.Fatalf("clone declarations %v, want %v", c.VectorIndexes(), g.VectorIndexes())
	}
	if n := c.VectorOverlayLen("emb"); n != 0 {
		t.Fatalf("clone overlay len = %d, want 0 (read-only mount)", n)
	}

	// Isolation: vectors almost equal to q land on the SOURCE; its top hits
	// must move to the new keys while the clone's stay bitwise identical.
	for i := 0; i < 50; i++ {
		v := append([]float32(nil), q...)
		norm := 0.0
		for j := range v {
			v[j] += float32(0.001 * rng.NormFloat64())
			norm += float64(v[j]) * float64(v[j])
		}
		norm = math.Sqrt(norm)
		for j := range v {
			v[j] = float32(float64(v[j]) / norm)
		}
		if _, err := g.AddNode(keyN(packed+tail+i), "m", map[string]any{"v": v}); err != nil {
			t.Fatal(err)
		}
	}
	g.FlushPend()
	moved, err := g.QueryVector("emb", q, opt)
	if err != nil {
		t.Fatal(err)
	}
	if sameHits(moved, want) {
		t.Fatal("source results did not move after new nearest vectors")
	}
	if top := moved[0].Key; top < keyN(packed+tail) {
		t.Fatalf("source top hit = %s, want one of the new keys", top)
	}
	again, err := c.QueryVector("emb", q, opt)
	if err != nil {
		t.Fatal(err)
	}
	if !sameHits(want, again) {
		t.Fatalf("clone results moved after source writes:\nfrozen: %v\nmoved:  %v", want, again)
	}

	// Clone-side writes skip the mounted index (no writer State, no panic)
	// and a Seal — the server publisher's fold — keeps the mount frozen.
	if _, err := c.AddNode("clone-write", "m", map[string]any{"v": append([]float32(nil), q...)}); err != nil {
		t.Fatal(err)
	}
	c.FlushPend()
	if err := c.Seal(); err != nil {
		t.Fatal(err)
	}
	after, err := c.QueryVector("emb", q, opt)
	if err != nil {
		t.Fatal(err)
	}
	if !sameHits(want, after) {
		t.Fatalf("clone results moved after clone write+Seal:\nfrozen: %v\nafter:  %v", want, after)
	}
}

// TestCloneWithoutVectors keeps the vector-less clone untouched: no
// declarations materialize out of nowhere.
func TestCloneWithoutVectors(t *testing.T) {
	g := vectra.New()
	defer g.Close()
	if _, err := g.AddNode("a", "m", nil); err != nil {
		t.Fatal(err)
	}
	c := g.Clone()
	defer c.Close()
	if idx := c.VectorIndexes(); len(idx) != 0 {
		t.Fatalf("clone of a vector-less graph declares %v", idx)
	}
	if _, err := c.QueryVector("none", make([]float32, 4), vectra.QueryVectorOptions{K: 1}); err != vectra.ErrVectorIndexNotFound {
		t.Fatalf("clone QueryVector error = %v, want ErrVectorIndexNotFound", err)
	}
}
