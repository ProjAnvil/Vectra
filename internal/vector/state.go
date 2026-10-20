package vector

import (
	"math"
	"sort"
)

// packSize is the overlay target: one pack builds an L0 segment (spec §4.2).
const packSize = 2048

// tierFactor is the per-tier capacity growth and the merge fan-in: tier i
// holds segments of ~2048·4^i vectors (spec §4.2), at most tierFactor-1 of
// them; the tierFactor-th arrival merges all of them into one and promotes
// it upward. 4-way merging keeps write amplification at ~2.97x for 100k
// (vs 5.59x pairwise, 2026-10-19-vector-build-optimization.md).
const tierFactor = 4

// entry is one overlay (not yet indexed) vector.
type entry struct {
	vid uint32
	seq uint64
	vec []float32
}

type segment struct {
	seq uint64
	h   *HNSW
}

// State is one vector index: overlay + tiered segment forest + dead set
// (spec §4). Single-writer structure; Phase 1 publishes immutable
// snapshots through the generation view.
type State struct {
	metric Metric
	dim    int
	m      int
	efc    int
	seed   int64

	nextVID uint32
	vidOf   map[string]uint32
	keyOf   map[uint32]string
	dead    map[uint32]struct{} // vid tombstones; revive clears (spec §4.4)

	overlay []entry
	ovIdx   map[uint32]int // vid -> overlay position: same-cycle update replaces in place
	tiers   [][]*segment
	seq     uint64
}

// VectorHit is the public result type (spec §3.2).
type VectorHit struct {
	Key   string
	Score float64
}

func NewState(metric Metric, dim, m, efc int, seed int64) *State {
	return &State{
		metric: metric,
		dim:    dim,
		m:      m,
		efc:    efc,
		seed:   seed,
		vidOf:  map[string]uint32{},
		keyOf:  map[uint32]string{},
		dead:   map[uint32]struct{}{},
		ovIdx:  map[uint32]int{},
	}
}

// Add upserts key's vector. Wrong-dim vectors are skipped silently; the
// NormalizeVec intake layer above this decides skip+warning (Phase 1).
// A same-cycle re-Add of an existing key replaces its overlay entry in
// place: the newest vector must survive the pack (spec §4.2 seq rule).
func (s *State) Add(key string, v []float32) {
	if len(v) != s.dim {
		return
	}
	vid, ok := s.vidOf[key]
	if !ok {
		vid = s.nextVID
		s.nextVID++
		s.vidOf[key] = vid
		s.keyOf[vid] = key
	}
	delete(s.dead, vid)
	if s.metric == Cosine {
		v = Normalize(v)
	} else {
		v = append([]float32(nil), v...)
	}
	e := entry{vid: vid, seq: s.seq, vec: v}
	if pos, ok := s.ovIdx[vid]; ok {
		s.overlay[pos] = e
	} else {
		s.overlay = append(s.overlay, e)
		s.ovIdx[vid] = len(s.overlay) - 1
	}
	s.seq++
}

// Delete tombstones key's vid. A later Add revives the same vid (spec §4.4).
func (s *State) Delete(key string) bool {
	vid, ok := s.vidOf[key]
	if !ok {
		return false
	}
	s.dead[vid] = struct{}{}
	return true
}

// OverlayLen returns the pending (not yet packed) vector count.
func (s *State) OverlayLen() int { return len(s.overlay) }

func (s *State) alive(vid uint32) bool {
	_, d := s.dead[vid]
	return !d
}

// Search merges all live segments and the overlay; the same vid resolves
// to its most recent occurrence (overlay beats segments, higher segment
// seq beats lower). Returns up to k hits ascending by score. A wrong-dim
// query returns nil.
func (s *State) Search(q []float32, k, ef int) []VectorHit {
	if k <= 0 || len(q) != s.dim {
		return nil
	}
	if ef < k {
		ef = k
	}
	if s.metric == Cosine {
		q = Normalize(q)
	} else {
		q = append([]float32(nil), q...)
	}
	quota := ef / 4 // per-segment candidate quota feeding the merge (spec §6.2)
	if quota < k {
		quota = k
	}
	type best struct {
		score float64
		seq   uint64
	}
	m := map[uint32]best{}
	for _, tier := range s.tiers {
		for _, seg := range tier {
			for _, hit := range seg.h.Search(q, quota, ef, s.alive) {
				if b, ok := m[hit.ID]; !ok || seg.seq > b.seq {
					m[hit.ID] = best{score: hit.Score, seq: seg.seq}
				}
			}
		}
	}
	for _, e := range s.overlay {
		if !s.alive(e.vid) {
			continue
		}
		m[e.vid] = best{score: Distance(s.metric, q, e.vec), seq: math.MaxUint64}
	}
	all := make([]VectorHit, 0, len(m))
	for vid, b := range m {
		all = append(all, VectorHit{Key: s.keyOf[vid], Score: b.score})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score < all[j].Score })
	if len(all) > k {
		all = all[:k]
	}
	return all
}

// Pack builds an L0 segment from the overlay once it reached packSize
// (or force), cascades in-tier merges upward, then compacts dead-heavy
// segments (spec §4.2/§5.2).
func (s *State) Pack(force bool) {
	if len(s.overlay) == 0 {
		return
	}
	if !force && len(s.overlay) < packSize {
		return
	}
	seg := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+int64(s.seq))}
	s.seq++
	for _, e := range s.overlay {
		if s.alive(e.vid) {
			seg.h.insertPrepared(e.vid, e.vec) // overlay vecs are normalized copies
		}
	}
	// Drop instead of truncating: a [:0] slice keeps stale entry pointers
	// alive in the backing array (heap bitmap scans the whole object),
	// pinning dead vectors and inflating steady-state memory.
	s.overlay = nil
	s.ovIdx = map[uint32]int{}
	carry := seg
	for i := 0; carry != nil; i++ {
		for i >= len(s.tiers) {
			s.tiers = append(s.tiers, nil)
		}
		s.tiers[i] = append(s.tiers[i], carry)
		if len(s.tiers[i]) < tierFactor {
			break
		}
		parts := s.tiers[i]
		s.tiers[i] = nil // nil, not [:0]: stale segment pointers pin dead graphs
		carry = s.mergeSegments(parts...)
	}
	s.compact()
}

// mergeSegments rebuilds one segment from the live vectors of the given
// segments (4-way fan-in per tier). Same vid in several inputs resolves to
// the newest segment's vector: inputs are visited latest-built first, and
// seq recency is the update-order rule (spec §4.2).
func (s *State) mergeSegments(segs ...*segment) *segment {
	seen := map[uint32]bool{}
	merged := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+int64(s.seq))}
	s.seq++
	for j := len(segs) - 1; j >= 0; j-- { // tiers append oldest-first: reverse = newest-first
		ids, vecs := segs[j].h.All()
		for i, id := range ids {
			if seen[id] || !s.alive(id) {
				continue
			}
			seen[id] = true
			merged.h.insertPrepared(id, vecs[i]) // All() vecs are stored-form; source graph dies here
		}
	}
	return merged
}

// compact rebuilds any segment whose dead ratio exceeds 30% (spec §4.2).
func (s *State) compact() {
	const threshold = 0.3
	for _, tier := range s.tiers {
		for i, seg := range tier {
			ids, vecs := seg.h.All()
			if len(ids) == 0 {
				continue
			}
			dead := 0
			for _, id := range ids {
				if !s.alive(id) {
					dead++
				}
			}
			if dead == 0 || float64(dead)/float64(len(ids)) <= threshold {
				continue
			}
			fresh := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+int64(s.seq))}
			s.seq++
			for j, id := range ids {
				if s.alive(id) {
					fresh.h.insertPrepared(id, vecs[j]) // stored-form; old graph dropped below
				}
			}
			tier[i] = fresh
		}
	}
}
