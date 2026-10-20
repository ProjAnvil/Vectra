package vector

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestStateConcurrentReaders is the Phase 1 concurrency prerequisite
// (spec deviation log: the epoch-visited array was mutable read-path
// state). Eight goroutines search the same packed State through
// reader-local state; run under -race. Concurrent read-read is this
// test's contract - read-during-write needs the immutable snapshot
// model. The packed graph is immutable and same-seed searches are
// deterministic, so each worker also deep-compares against results
// computed once up front.
func TestStateConcurrentReaders(t *testing.T) {
	const n, dim = 2000, 64
	vecs := genClustered(rand.New(rand.NewSource(11)), n, dim)
	s := NewState(Cosine, dim, 16, 100, 12)
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
	}
	s.Pack(true)
	queries := perturbedQueries(rand.New(rand.NewSource(13)), vecs, 20)
	want := make([][]VectorHit, len(queries))
	for qi, q := range queries {
		want[qi] = s.Search(q, 10, 64)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := s.NewReader()
			for round := 0; round < 10; round++ {
				for qi, q := range queries {
					hits := s.SearchReader(q, 10, 64, r)
					if len(hits) != len(want[qi]) {
						t.Errorf("worker %d round %d q%d: %d hits, want %d", w, round, qi, len(hits), len(want[qi]))
						return
					}
					for i := range want[qi] {
						if hits[i] != want[qi][i] {
							t.Errorf("worker %d round %d q%d [%d]: %+v, want %+v", w, round, qi, i, hits[i], want[qi][i])
							return
						}
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestSearchReaderEquivalence: the reader-based path returns exactly
// what the convenience path returns. n=6000 spans the 4-way cascade, so
// one Reader serves segments of different sizes in a single query -
// the cross-segment growth and stamp-continuity contract.
func TestSearchReaderEquivalence(t *testing.T) {
	const n, dim = 6000, 32
	vecs := genClustered(rand.New(rand.NewSource(17)), n, dim)
	s := NewState(Cosine, dim, 16, 100, 18)
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
	}
	s.Pack(true)
	queries := perturbedQueries(rand.New(rand.NewSource(19)), vecs, 10)
	want := make([][]VectorHit, len(queries))
	for qi, q := range queries {
		want[qi] = s.Search(q, 10, 64)
	}
	r := s.NewReader()
	for round := 0; round < 3; round++ { // reuse the reader across rounds
		for qi, q := range queries {
			got := s.SearchReader(q, 10, 64, r)
			if len(got) != len(want[qi]) {
				t.Fatalf("round %d q%d: len %d != %d", round, qi, len(got), len(want[qi]))
			}
			for i := range want[qi] {
				if got[i] != want[qi][i] {
					t.Fatalf("round %d q%d [%d]: %+v != %+v", round, qi, i, got[i], want[qi][i])
				}
			}
		}
	}
}
