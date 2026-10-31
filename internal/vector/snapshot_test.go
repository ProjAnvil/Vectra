package vector

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"sync"
	"testing"
)

// buildStateFor builds a live State with n vectors, packing along the way.
func buildStateFor(t *testing.T, n, dim int, seed int64) (*State, [][]float32) {
	t.Helper()
	vecs := genClustered(rand.New(rand.NewSource(seed)), n, dim)
	s := NewState(Cosine, dim, 16, 100, seed+1)
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
		if (i+1)%2048 == 0 {
			s.Pack(false)
		}
	}
	s.Pack(true)
	return s, vecs
}

// TestSnapshotImmutability pins the read-during-write contract: a taken
// Snapshot keeps returning identical results while the writer Adds,
// Deletes, and Packs. This is what concurrent Graph readers hold.
func TestSnapshotImmutability(t *testing.T) {
	const n, dim = 3000, 32
	s, vecs := buildStateFor(t, n, dim, 31)
	queries := perturbedQueries(rand.New(rand.NewSource(33)), vecs, 10)
	snap := s.Snapshot()
	r := s.NewReader()
	before := make([][]VectorHit, len(queries))
	for qi, q := range queries {
		before[qi] = snap.Search(q, 10, 64, r)
		if len(before[qi]) == 0 {
			t.Fatal("empty snapshot result on a packed 3k index")
		}
	}

	// Mutate hard: fresh adds, deletes of indexed keys, a full repack.
	extra := genClustered(rand.New(rand.NewSource(35)), 500, dim)
	for i, v := range extra {
		s.Add(fmt.Sprintf("x%d", i), v)
	}
	for i := 0; i < 200; i++ {
		s.Delete(fmt.Sprintf("k%d", i*3))
	}
	s.Pack(true)

	for round := 0; round < 2; round++ {
		for qi, q := range queries {
			after := snap.Search(q, 10, 64, r)
			if len(after) != len(before[qi]) {
				t.Fatalf("round %d q%d: snapshot changed under mutation: %d hits, was %d",
					round, qi, len(after), len(before[qi]))
			}
			for i := range before[qi] {
				if after[i] != before[qi][i] {
					t.Fatalf("round %d q%d [%d]: %+v, was %+v", round, qi, i, after[i], before[qi][i])
				}
			}
		}
	}
}

// TestSnapshotConcurrentWithWriter: readers hold an OLD snapshot while
// the writer mutates and republishes - the zero-lock read contract
// (spec D1). Snapshot() is a writer-thread call (materialization reads
// live structures); real readers receive snapshots through the Graph's
// atomic head. This test pins that no mutable memory is shared between
// a published snapshot and the writer. -race is the gate.
func TestSnapshotConcurrentWithWriter(t *testing.T) {
	const dim = 32
	s, vecs := buildStateFor(t, 2000, dim, 41)
	queries := perturbedQueries(rand.New(rand.NewSource(43)), vecs, 10)
	snap := s.Snapshot() // what a concurrent reader would hold

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := s.NewReader()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, q := range queries {
					snap.Search(q, 5, 32, r)
				}
			}
		}()
	}
	// Writer: adds + deletes + republish, concurrent with the readers.
	rng := rand.New(rand.NewSource(45))
	for i := 0; i < 20; i++ {
		batch := genClustered(rng, 100, dim)
		for j, v := range batch {
			s.Add(fmt.Sprintf("w%d_%d", i, j), v)
		}
		s.Delete(fmt.Sprintf("k%d", i*7))
		_ = s.Snapshot() // republish (publish-point rate, not per-Add)
		if (i+1)%5 == 0 {
			s.Pack(false)
		}
	}
	close(stop)
	wg.Wait()
}

// TestDeadBitsParity: the chunked bitmap must match a plain map on a
// randomized set/clear/unset sequence, including revive (clear).
func TestDeadBitsParity(t *testing.T) {
	rng := rand.New(rand.NewSource(47))
	ref := map[uint32]bool{}
	var d deadBits
	d.chunks = map[uint32][1024]uint64{}
	for op := 0; op < 3000; op++ {
		vid := uint32(rng.Intn(300000)) // spans several chunks
		switch rng.Intn(3) {
		case 0, 1:
			d.set(vid)
			ref[vid] = true
		case 2:
			d.unset(vid)
			delete(ref, vid)
		}
	}
	for vid, want := range ref {
		if d.has(vid) != want {
			t.Fatalf("vid %d: has=%v want %v", vid, d.has(vid), want)
		}
	}
	// And every set bit we know about:
	for vid := range ref {
		d.unset(vid)
	}
	for cid := range d.chunks {
		chunk := d.chunks[cid]
		for w, word := range chunk {
			if word != 0 {
				t.Fatalf("chunk %d word %d not zero after unsetting all", cid, w)
			}
		}
	}
}

// TestSnapshotWithLiveOverlay pins the overlay half of the snapshot
// contract: a Snapshot taken while the overlay is non-empty must return
// the overlay's vectors (chunked overlay shared by pointer, tail chunk
// duplicated), and must stay byte-identical while the writer keeps
// appending to the tail chunk and packs it away. Without this, a
// regression dropping the overlay from Snapshot() would pass every other
// snapshot test (they all pack first).
func TestSnapshotWithLiveOverlay(t *testing.T) {
	const dim = 32
	vecs := genClustered(rand.New(rand.NewSource(61)), 2048, dim) // one full chunk
	s := NewState(Cosine, dim, 16, 100, 62)
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
	}
	// No Pack: everything lives in the overlay (one frozen full chunk).
	snap := s.Snapshot()
	r := s.NewReader()
	for qi := 0; qi < 10; qi++ {
		hits := snap.Search(vecs[qi*100], 1, 32, r)
		if len(hits) != 1 || hits[0].Key != fmt.Sprintf("k%d", qi*100) {
			t.Fatalf("q%d: overlay key missing from snapshot: %+v", qi, hits)
		}
	}
	before := snap.Search(vecs[0], 10, 64, r)

	// Writer: append a tail chunk, re-add an existing key (newest wins),
	// then pack everything away.
	tail := genClustered(rand.New(rand.NewSource(63)), 500, dim)
	for i, v := range tail {
		s.Add(fmt.Sprintf("t%d", i), v)
	}
	s.Add("k0", vecs[1]) // re-add: newer value must win in LIVE search
	s.Pack(true)
	if s.OverlayLen() != 0 {
		t.Fatal("pack must drain the overlay")
	}

	after := snap.Search(vecs[0], 10, 64, r)
	if len(after) != len(before) {
		t.Fatalf("snapshot changed under overlay append+pack: %d hits, was %d", len(after), len(before))
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("[%d]: %+v, was %+v", i, after[i], before[i])
		}
	}
	// Live side sees the re-added value win: k0 now holds vecs[1], the
	// same vector k1 holds - both sit at distance 0, and tie order is not
	// guaranteed (spec D8), so assert set membership in the top-2. The
	// score is a float32 distance that leaves a ~6e-8 rounding residue
	// on some platforms (amd64 CI runners), so match within epsilon.
	live := s.Search(vecs[1], 2, 64)
	foundK0 := false
	for _, h := range live {
		if h.Key == "k0" && h.Score <= 1e-6 {
			foundK0 = true
		}
	}
	if !foundK0 {
		t.Fatalf("re-added k0 (value vecs[1]) missing from top-2: %+v", live)
	}
}

// TestSnapshotKeysRendered: results carry the original key strings after
// packing and merging (key arrays on segments - the old path read a
// shared vid->key map; overlay keys are covered by
// TestSnapshotWithLiveOverlay).
func TestSnapshotKeysRendered(t *testing.T) {
	const n, dim = 5000, 16
	s, vecs := buildStateFor(t, n, dim, 51)
	snap := s.Snapshot()
	r := s.NewReader()
	for qi := 0; qi < 20; qi++ {
		q := vecs[(qi*211)%n]
		hits := snap.Search(q, 1, 32, r)
		if len(hits) != 1 {
			t.Fatalf("q%d: %d hits", qi, len(hits))
		}
		var idx int
		if _, err := fmt.Sscanf(hits[0].Key, "k%d", &idx); err != nil || idx < 0 || idx >= n {
			t.Fatalf("bad key %q (err %v)", hits[0].Key, err)
		}
	}
}

// TestTwoPhasePackParity: the two-phase drain (BeginPack/Build/CommitPack)
// must produce the same query results as the classic whole-overlay Pack at
// a scale that exercises the 4-way cascade (n=10000: tier0 fills and
// promotes). Written after integrated gate-2b recall collapsed: every
// small lane test (4096 = two chunks, no cascade) passed.
func TestTwoPhasePackParity(t *testing.T) {
	const n, dim = 10000, 64
	vecs := genClustered(rand.New(rand.NewSource(71)), n, dim)
	build := func(twoPhase bool) *State {
		s := NewState(Cosine, dim, 16, 100, 72)
		for i, v := range vecs {
			s.Add(fmt.Sprintf("k%d", i), v)
		}
		if !twoPhase {
			s.Pack(true)
			return s
		}
		for {
			p := s.BeginPack()
			if p == nil {
				break
			}
			p.Build(Cosine, 16, 100, 72)
			if !s.CommitPack(p) {
				t.Fatal("commit rejected on a quiescent state")
			}
		}
		// Tail below one chunk stays by design; Pack(true) drains it. The
		// parity target is the SEGMENT forest, so force-drain both to the
		// same final shape: pack the tail via the classic path.
		s.Pack(true)
		return s
	}
	a := build(false)
	b := build(true)
	if a.Len() != b.Len() || a.OverlayLen() != 0 || b.OverlayLen() != 0 {
		t.Fatalf("shape: old len=%d overlay=%d; new len=%d overlay=%d", a.Len(), a.OverlayLen(), b.Len(), b.OverlayLen())
	}
	queries := perturbedQueries(rand.New(rand.NewSource(73)), vecs, 30)
	ra, rb := a.NewReader(), b.NewReader()
	for qi, q := range queries {
		ha := normHits(a.SearchReader(q, 10, 64, ra))
		hb := normHits(b.SearchReader(q, 10, 64, rb))
		if len(ha) == 0 {
			t.Fatalf("q%d: old path empty", qi)
		}
		if len(ha) != len(hb) {
			t.Fatalf("q%d: %d vs %d hits", qi, len(ha), len(hb))
		}
		for i := range ha {
			if ha[i] != hb[i] {
				t.Fatalf("q%d [%d]: old=%+v new=%+v", qi, i, ha[i], hb[i])
			}
		}
	}
	// And against brute force: the new path must hold the recall line too.
	hit := 0
	for _, q := range queries {
		want := map[string]bool{}
		for _, i := range bruteTopIDs(vecs, q, 10) {
			want[fmt.Sprintf("k%d", i)] = true
		}
		for _, h := range b.SearchReader(q, 10, 128, rb) {
			if want[h.Key] {
				hit++
			}
		}
	}
	if r := float64(hit) / float64(len(queries)*10); r < 0.95 {
		t.Fatalf("two-phase recall@10 (ef128) = %.4f, want >= 0.95", r)
	}
}

func normHits(hs []VectorHit) []VectorHit {
	sort.Slice(hs, func(i, j int) bool {
		if hs[i].Score != hs[j].Score {
			return hs[i].Score < hs[j].Score
		}
		return hs[i].Key < hs[j].Key
	})
	return hs
}

// assertSameForest compares two States' segment forests structurally:
// tier shape, per-segment seq/keys/ids/vectors/levels/links/entry, plus
// the seq allocator and overlay length. Same seed + same insertion order
// build identical HNSW graphs, so links are compared too — the strongest
// form available.
func assertSameForest(t *testing.T, step int, a, b *State) {
	t.Helper()
	if a.overlayLen != b.overlayLen {
		t.Fatalf("step %d: overlayLen %d vs %d", step, a.overlayLen, b.overlayLen)
	}
	if a.seq != b.seq {
		t.Fatalf("step %d: seq allocator ended at %d vs %d", step, a.seq, b.seq)
	}
	if len(a.tiers) != len(b.tiers) {
		t.Fatalf("step %d: tier count %d vs %d", step, len(a.tiers), len(b.tiers))
	}
	for ti := range a.tiers {
		if len(a.tiers[ti]) != len(b.tiers[ti]) {
			t.Fatalf("step %d: tier %d holds %d vs %d segments", step, ti, len(a.tiers[ti]), len(b.tiers[ti]))
		}
		for si := range a.tiers[ti] {
			sa, sb := a.tiers[ti][si], b.tiers[ti][si]
			if sa.seq != sb.seq {
				t.Fatalf("step %d: tier %d seg %d: seq %d vs %d", step, ti, si, sa.seq, sb.seq)
			}
			if !slices.Equal(sa.keys, sb.keys) {
				t.Fatalf("step %d: tier %d seg %d: key arrays differ", step, ti, si)
			}
			ha, hb := sa.h, sb.h
			if !slices.Equal(ha.ids, hb.ids) || !slices.Equal(ha.level, hb.level) || ha.entry != hb.entry {
				t.Fatalf("step %d: tier %d seg %d: graph shape differs", step, ti, si)
			}
			for i := range ha.vecs {
				if !slices.Equal(ha.vecs[i], hb.vecs[i]) {
					t.Fatalf("step %d: tier %d seg %d: vector %d differs", step, ti, si, i)
				}
			}
			if !reflect.DeepEqual(ha.links, hb.links) {
				t.Fatalf("step %d: tier %d seg %d: link lists differ", step, ti, si)
			}
		}
	}
}

// TestPackDriversDifferential (audit F-25): drive BOTH pack drivers over
// the SAME chunk-granular input stream — the synchronous Pack draining
// exactly one full chunk per call vs the folder lane's
// BeginPack/Build/CommitPack — and compare the resulting segment FORESTS
// structurally after every step. Identical inputs through one shared
// merge core must pack to identical outputs. Unlike
// TestTwoPhasePackParity (which feeds the paths different granularities —
// whole-overlay drain vs per-chunk — and compares query results), this
// pins the structural contract: cascade shape, merge newest-wins dedup,
// dead filtering, compaction rebuilds, and seq stamping. Re-Adds of older
// keys are folded INTO each chunk (a slot swap, keeping every round
// exactly packSize entries) because a post-chunk Add makes the contracts
// genuinely differ: sync Pack drains the whole overlay, the folder lane
// takes only the full head chunk.
func TestPackDriversDifferential(t *testing.T) {
	const dim = 16
	const chunks = 5 // chunks 0-3 fill tier 0 and cascade at the 4th; chunk 5 lands on the emptied tier 0
	rng := rand.New(rand.NewSource(81))
	vecs := make([][]float32, chunks*packSize)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
	}
	add := func(s *State, key string, v []float32) { s.Add(key, v) }
	sync := NewState(Cosine, dim, 16, 100, 82)
	folder := NewState(Cosine, dim, 16, 100, 82)
	for c := 0; c < chunks; c++ {
		for i := 0; i < packSize; i++ {
			key, vec := fmt.Sprintf("k%d", c*packSize+i), vecs[c*packSize+i]
			if c > 0 && i == 999 {
				// Slot swap: this slot re-adds a previous chunk's key (a
				// delete victim from round c-1) with a fresh vector —
				// revive + shadow update, resolved by seq recency at the
				// next merge — while keeping the chunk exactly packSize
				// entries so both drivers pack identical input.
				key = fmt.Sprintf("k%d", (c-1)*packSize+5)
				vec = vecs[(c*packSize+i+1234)%len(vecs)]
			}
			add(sync, key, vec)
			add(folder, key, vec)
		}
		if c > 0 {
			// Delete a not-yet-packed key (dead at pack time) ...
			doomed := fmt.Sprintf("k%d", c*packSize+13)
			if !sync.Delete(doomed) || !folder.Delete(doomed) {
				t.Fatalf("round %d: delete of a pending key failed", c)
			}
			// ... and 700 keys of the previous chunk's segment (>34% dead
			// — over the compaction line). Odd offsets keep the re-added
			// key (offset 5) and prior doomed keys (13) out of the sweep.
			for i := 0; i < 700; i++ {
				key := fmt.Sprintf("k%d", (c-1)*packSize+2*i)
				sync.Delete(key)
				folder.Delete(key)
			}
		}
		sync.Pack(false) // exactly one full chunk pending
		p := folder.BeginPack()
		if p == nil {
			t.Fatalf("round %d: no full chunk pending on the folder state (overlay=%d)", c, folder.OverlayLen())
		}
		p.Build(Cosine, 16, 100, 82)
		if !folder.CommitPack(p) {
			t.Fatalf("round %d: commit rejected on a quiescent state", c)
		}
		assertSameForest(t, c, sync, folder)
	}
}
