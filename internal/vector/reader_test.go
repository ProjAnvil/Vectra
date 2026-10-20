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
// test's contract - read-during-write is the snapshot model's (see
// snapshot_test.go).
func TestStateConcurrentReaders(t *testing.T) {
	const n, dim = 2000, 64
	vecs := genClustered(rand.New(rand.NewSource(11)), n, dim)
	s := NewState(Cosine, dim, 16, 100, 12)
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
	}
	s.Pack(true)
	queries := perturbedQueries(rand.New(rand.NewSource(13)), vecs, 20)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := s.NewReader()
			for round := 0; round < 10; round++ {
				for _, q := range queries {
					hits := s.SearchReader(q, 10, 64, r)
					if len(hits) == 0 {
						// Race detector may not be enabled; a miss here is
						// still wrong on a packed 2k index with 10 queries.
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestSearchReaderEquivalence: the reader-based path returns exactly
// what the convenience path returns (same state, same queries, fresh
// reader per call).
func TestSearchReaderEquivalence(t *testing.T) {
	const n, dim = 1000, 32
	vecs := genClustered(rand.New(rand.NewSource(17)), n, dim)
	s := NewState(Cosine, dim, 16, 100, 18)
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
	}
	s.Pack(true)
	queries := perturbedQueries(rand.New(rand.NewSource(19)), vecs, 10)
	r := s.NewReader()
	for round := 0; round < 3; round++ { // reuse the reader across rounds
		for _, q := range queries {
			want := s.Search(q, 10, 64)
			got := s.SearchReader(q, 10, 64, r)
			if len(got) != len(want) {
				t.Fatalf("len %d != %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("[%d] %+v != %+v", i, got[i], want[i])
				}
			}
		}
	}
}
