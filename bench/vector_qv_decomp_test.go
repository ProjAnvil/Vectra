package bench

// Gate-2b line-3 p99 decomposition (deviation follow-up, 2026-10-21):
// the integrated run's uniform ~2x p50 offset vs the Phase-0 reference
// has two candidate causes - per-query integration overhead in
// QueryVector, or measurement protocol: the gate-2b test interleaves a
// 100kx768 brute-force truth scan (~300MB streaming the cache) between
// TIMED queries, while the Phase-0 harness ran recall first (untimed)
// and then timed back-to-back queries on a warm cache.
//
// Three experiments on one machine, one session, same vector set:
//   E1  g.QueryVector, Phase-0 protocol (untimed recall pass, then
//       back-to-back timed queries)
//   E2  g.QueryVector, gate-2b protocol (timed query, then brute-force
//       truth scan, next query cold)
//   E3  vector.State.Search on an equivalent forest, Phase-0 protocol -
//       the fresh same-day reference the comparison should have used
//
// 500 queries give a real p99 (the 50-query "p99" is a maximum, one GC
// or scheduler outlier away from meaningless). Env: VECTRA_QV_DECOMP=1.

import (
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/vector"
)

func TestVectorQueryDecomposition(t *testing.T) {
	if os.Getenv("VECTRA_QV_DECOMP") == "" {
		t.Skip("set VECTRA_QV_DECOMP=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}

	const n = 100_000
	const dim = 768
	rng := rand.New(rand.NewSource(1000 + 100000)) // gate-2b line-3 seed
	t.Log("generating vectors...")
	vecs := gate2bVecs(rng, n, dim)

	// vecSeed replicated from vectra_vector.go so the direct State forest
	// matches the Graph's (same name -> same HNSW seed -> same shape).
	vecSeed := gate2bVecSeed

	// 500 queries, gate-2b protocol (one source per query, perturbed,
	// normalized) with the Phase-0 query seed.
	qrng := rand.New(rand.NewSource(777))
	sigma := 0.35 / math.Sqrt(dim)
	const nq = 500
	queries := make([][]float32, nq)
	for qi := range queries {
		src := vecs[qrng.Intn(n)]
		q := make([]float32, dim)
		for j := range q {
			q[j] = src[j] + float32(qrng.NormFloat64()*sigma)
		}
		norm := 0.0
		for _, x := range q {
			norm += float64(x) * float64(x)
		}
		norm = math.Sqrt(norm)
		for j := range q {
			q[j] = float32(float64(q[j]) / norm)
		}
		queries[qi] = q
	}

	// bruteTruth streams the whole vector set through the CPU cache -
	// exactly what the gate-2b loop does between timed queries.
	bruteTruth := func(q []float32, k int) map[string]bool {
		type kd struct {
			k string
			d float64
		}
		all := make([]kd, n)
		for i, v := range vecs {
			var dot float64
			for j := range v {
				dot += float64(v[j]) * float64(q[j])
			}
			all[i] = kd{gate2bKey(i), 1 - dot}
		}
		for i := 0; i < k; i++ {
			for j := i + 1; j < len(all); j++ {
				if all[j].d < all[i].d {
					all[i], all[j] = all[j], all[i]
				}
			}
		}
		want := map[string]bool{}
		for i := 0; i < k; i++ {
			want[all[i].k] = true
		}
		return want
	}

	// Build both forests concurrently (each single-threaded in its HNSW
	// work; the Graph drains on its folder lane). Graph via batches (the
	// forest depends on the vector sequence, not the batch grouping).
	var g *vectra.Graph
	var st *vector.State
	var wg sync.WaitGroup
	var gErr error
	wg.Add(2)
	t0 := time.Now()
	go func() {
		defer wg.Done()
		g = vectra.New()
		if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: dim, Metric: "cosine"}); err != nil {
			gErr = err
			return
		}
		const batch = 512
		for b := 0; b < n/batch; b++ {
			err := g.Batch(func(g *vectra.Graph) {
				for j := 0; j < batch; j++ {
					i := b*batch + j
					if _, err := g.AddNode(gate2bKey(i), "m", map[string]any{"emb": vecs[i]}); err != nil {
						gErr = err
						return
					}
				}
			})
			if err != nil || gErr != nil {
				gErr = err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		st = vector.NewState(vector.Cosine, dim, 16, 200, vecSeed("mem"))
		for i := 0; i < n; i++ {
			st.Add(gate2bKey(i), vecs[i])
			if (i+1)%2048 == 0 {
				st.Pack(false)
			}
		}
		st.Pack(true)
	}()
	wg.Wait()
	if gErr != nil {
		t.Fatal(gErr)
	}
	t.Logf("ingest+state build: %s", time.Since(t0).Round(time.Millisecond))

	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		if g.VectorOverlayLen("mem") < 2048 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if left := g.VectorOverlayLen("mem"); left >= 2048 {
		t.Fatalf("lane did not converge: overlay=%d", left)
	}
	t.Logf("converged (graph drain total %s); tail overlay=%d", time.Since(t0).Round(time.Millisecond), g.VectorOverlayLen("mem"))

	pct := func(ts []time.Duration) (p50, p99 time.Duration) {
		s := append([]time.Duration(nil), ts...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s[len(s)/2], s[len(s)*99/100]
	}
	recallOf := func(hits []vectra.VectorHit, want map[string]bool) float64 {
		hit := 0
		for _, h := range hits {
			if want[h.Key] {
				hit++
			}
		}
		return float64(hit) / 10
	}

	// Untimed recall pass (Phase-0 protocol step 1; also warms everything
	// the timed pass is about to touch).
	for _, ef := range []int{32, 64, 128} {
		recall := 0.0
		for _, q := range queries {
			want := bruteTruth(q, 10)
			hits, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10, Ef: ef})
			if err != nil {
				t.Fatal(err)
			}
			recall += recallOf(hits, want) / nq
		}
		t.Logf("[graph ef=%d] recall@10 (untimed pass) = %.4f", ef, recall)
	}

	for _, ef := range []int{32, 64, 128} {
		// E1: graph, warm back-to-back
		ts := make([]time.Duration, nq)
		for i, q := range queries {
			s := time.Now()
			if _, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10, Ef: ef}); err != nil {
				t.Fatal(err)
			}
			ts[i] = time.Since(s)
		}
		p50, p99 := pct(ts)
		t.Logf("E1 [graph warm ef=%d] p50=%s p99=%s", ef, p50, p99)

		// E2: graph, gate-2b interleaved protocol
		ts = make([]time.Duration, nq)
		for i, q := range queries {
			s := time.Now()
			g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10, Ef: ef})
			ts[i] = time.Since(s)
			bruteTruth(q, 10)
		}
		p50i, p99i := pct(ts)
		t.Logf("E2 [graph interleaved ef=%d] p50=%s p99=%s", ef, p50i, p99i)

		// E3: direct State, warm back-to-back (fresh same-day reference)
		ts = make([]time.Duration, nq)
		for i, q := range queries {
			s := time.Now()
			st.Search(q, 10, ef)
			ts[i] = time.Since(s)
		}
		p50s, p99s := pct(ts)
		t.Logf("E3 [state warm ef=%d] p50=%s p99=%s", ef, p50s, p99s)

		t.Logf("ef=%d ratios: E1/E3 p50=%.2fx  E2/E1 p50=%.2fx  E1/E3 p99=%.2fx",
			ef, float64(p50)/float64(p50s), float64(p50i)/float64(p50), float64(p99)/float64(p99s))
	}
}
