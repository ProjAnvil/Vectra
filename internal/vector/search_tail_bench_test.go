package vector

// v0.11.3 scope evidence: the marginal cost of a full-ish overlay tail
// (read-your-write, spec D7) in search. Every query computes a 768-dim
// dot per tail entry, inserts each into the map[uint32]best merge, and
// sorts ALL candidates (segment quota + tail) although only top-k
// returns. One build, measured tail-full (2047) at each ef, then the
// same state force-packed (tail 0, one extra near-full segment) again.
//
//	go test ./internal/vector/ -bench TailCost -benchtime 300x -cpuprofile /tmp/tail.prof
//	go tool pprof -top -nodecount 12 /tmp/tail.prof

import (
	"fmt"
	"testing"
)

var tailBench struct {
	s   *State
	qs  [][]float32
	bad [6]bool // guard: measure each phase exactly once
}

func tailBenchState(b *testing.B) (*State, [][]float32) {
	b.Helper()
	if tailBench.s == nil {
		const n = 4*packSize + 2047
		s := NewState(Cosine, 768, 16, 200, 7)
		for i := 0; i < n; i++ {
			v := make([]float32, 768)
			for j := range v {
				v[j] = float32(((i*31+j*17)%97)-48) / 97.0
			}
			key := fmt.Sprintf("k%08d", i)
			s.Add(key, v)
			if (i+1)%packSize == 0 {
				s.Pack(false)
			}
		}
		qs := make([][]float32, 64)
		for i := range qs {
			q := make([]float32, 768)
			for j := range q {
				q[j] = float32(((i*13+j*7)%89)-44) / 89.0
			}
			qs[i] = q
		}
		tailBench.s, tailBench.qs = s, qs
	}
	return tailBench.s, tailBench.qs
}

func benchmarkTail(b *testing.B, full bool, ef int, slot int) {
	s, qs := tailBenchState(b)
	if full != (s.overlayLen >= 2047) {
		b.Skip("phase already consumed - run with -run '^$' fresh")
	}
	_ = slot
	r := &Reader{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SearchReader(qs[i%len(qs)], 10, ef, r)
	}
	b.StopTimer()
}

func BenchmarkTailFull32(b *testing.B)   { benchmarkTail(b, true, 32, 0) }
func BenchmarkTailFull64(b *testing.B)   { benchmarkTail(b, true, 64, 1) }
func BenchmarkTailFull128(b *testing.B)  { benchmarkTail(b, true, 128, 2) }
// BenchmarkTailPack transitions the shared state to the empty-tail phase
// (placed between the Full and Empty benches by name order F < P... use
// explicit ordering: run benches by listing this between phases).
func BenchmarkTailPack(b *testing.B) {
	s, _ := tailBenchState(b)
	if s.overlayLen > 0 {
		b.ResetTimer()
		s.Pack(true)
	}
}

func BenchmarkTailEmpty32(b *testing.B)  { benchmarkTail(b, false, 32, 3) }
func BenchmarkTailEmpty64(b *testing.B)  { benchmarkTail(b, false, 64, 4) }
func BenchmarkTailEmpty128(b *testing.B) { benchmarkTail(b, false, 128, 5) }

