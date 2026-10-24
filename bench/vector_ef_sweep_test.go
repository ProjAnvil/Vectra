package bench

// ef-budget experiment (2026-10-24, fix-session design input): the 1M
// direct measurement put ef64 p99 at 12.16ms against spec 6.3's 10ms
// line. Qdrant/Weaviate ship no auto-ef; the latency-recall point is a
// caller knob. Before deciding between "engine optimization arc" and
// "ef budget recalibration + spec amendment", sweep ef against the REAL
// 1M library left in VECTRA_1M_DIR by the measurement session (post-C
// state: converged, fresh forest, n = 1,001,471 base + 43,520 chain
// vectors). Truth is computed over the 1,001,471 base vectors only -
// the 43,520 chain-phase vectors can steal top-10 slots, so recall here
// reads slightly LOW; the trend across ef values is the decision input.
//
// Env-gated: VECTRA_EF_SWEEP=1, library via VECTRA_1M_DIR (default
// /tmp/vectra-1m), base n via VECTRA_1M_N (default 1,001,471), the ef
// list via VECTRA_EF_LIST (comma-separated, default 40,48,56,64,80,96).

import (
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/projanvil/vectra"
)

func TestVectorEfSweep(t *testing.T) {
	if os.Getenv("VECTRA_EF_SWEEP") == "" {
		t.Skip("set VECTRA_EF_SWEEP=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}
	dir := os.Getenv("VECTRA_1M_DIR")
	if dir == "" {
		dir = "/tmp/vectra-1m"
	}
	n := oneMEnvInt("VECTRA_1M_N", 1_001_471)
	efs := []int{40, 48, 56, 64, 80, 96}
	if s := os.Getenv("VECTRA_EF_LIST"); s != "" {
		efs = efs[:0]
		for _, p := range strings.Split(s, ",") {
			v, err := strconv.Atoi(strings.TrimSpace(p))
			if err != nil || v <= 0 {
				t.Fatalf("VECTRA_EF_LIST=%q: bad ef %q", s, p)
			}
			efs = append(efs, v)
		}
	}

	g, openDur, overlayAtOpen := oneMOpenTimed(t, dir)
	defer g.Close()
	t.Logf("open %s overlay-at-open=%d (chain replay; small for the post-C library)", openDur.Round(time.Millisecond), overlayAtOpen)
	deadline := time.Now().Add(30 * time.Minute)
	for g.VectorOverlayLen("mem") >= 2048 && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("converged: overlay=%d", g.VectorOverlayLen("mem"))

	// Same query protocol as the 1M run: 50 perturbed queries off base
	// vectors, single source per query (chimeric noise collapsed recall).
	vecs := oneMBaseVecs(n)
	qrng := rand.New(rand.NewSource(777))
	sigma := 0.35 / math.Sqrt(768)
	queries := make([][]float32, 50)
	for qi := range queries {
		src := vecs[qrng.Intn(n)]
		q := make([]float32, 768)
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

	// Truth over the base set, untimed. The library's chain-phase extras
	// are NOT in this truth set: recall reads low where they outrank base
	// vectors (noted above).
	type kd struct {
		k string
		d float64
	}
	truths := make([]map[string]bool, len(queries))
	for qi, q := range queries {
		all := make([]kd, n)
		for i, v := range vecs {
			var dot float64
			for j := range v {
				dot += float64(v[j]) * float64(q[j])
			}
			all[i] = kd{gate2bKey(i), 1 - dot}
		}
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
		truths[qi] = want
	}
	vecs = nil

	for _, ef := range efs {
		var recall float64
		lat := make([]time.Duration, len(queries))
		for qi, q := range queries {
			s := time.Now()
			hits, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10, Ef: ef})
			lat[qi] = time.Since(s)
			if err != nil {
				t.Fatal(err)
			}
			hit := 0
			for _, h := range hits {
				if truths[qi][h.Key] {
					hit++
				}
			}
			recall += float64(hit) / 10
		}
		recall /= float64(len(queries))
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		t.Logf("ef=%3d: recall@10=%.4f p50=%s p99(max of %d)=%s",
			ef, recall, lat[len(lat)/2], len(queries), lat[len(lat)*99/100])
	}
}
