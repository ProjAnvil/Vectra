package vector

import (
	"math"
	"sort"
)

// packSize is the overlay chunk size and the L0 pack target (spec §4.2).
const packSize = 2048

// tierFactor is the per-tier capacity growth and the merge fan-in: tier i
// holds segments of ~2048·4^i vectors (spec §4.2), at most tierFactor-1 of
// them; the tierFactor-th arrival merges all of them into one and promotes
// it upward. 4-way merging keeps write amplification at ~2.97x for 100k
// (vs 5.59x pairwise, 2026-10-19-vector-build-optimization.md).
const tierFactor = 4

// deadBits is the vid-space dead set as a chunked bitmap (spec D6): vid
// >> 16 selects a [1024]uint64 chunk (65536 vids per chunk). Writer-side
// chunks are map VALUES - Snapshot copies the whole map, so a published
// snapshot never shares mutable memory with the writer.
type deadBits struct {
	chunks map[uint32][1024]uint64
}

func (d deadBits) has(vid uint32) bool {
	ch, ok := d.chunks[vid>>16]
	if !ok {
		return false
	}
	return ch[(vid&0xffff)>>6]&(1<<(vid&63)) != 0
}

func (d *deadBits) set(vid uint32) {
	if d.chunks == nil {
		d.chunks = map[uint32][1024]uint64{}
	}
	c := d.chunks[vid>>16]
	c[(vid&0xffff)>>6] |= 1 << (vid & 63)
	d.chunks[vid>>16] = c
}

func (d *deadBits) unset(vid uint32) {
	c, ok := d.chunks[vid>>16]
	if !ok {
		return
	}
	c[(vid&0xffff)>>6] &^= 1 << (vid & 63)
	d.chunks[vid>>16] = c
}

// entry is one overlay (not yet packed) vector. Re-Adds of the same key
// append again; later chunks win the scan-order dedup, so no in-place
// replacement is needed and full chunks stay frozen (shareable by
// snapshots without copying).
type entry struct {
	key string
	vid uint32
	vec []float32
}

// segment is one packed HNSW plus its key array (parallel to h.ids:
// keys[i] is the key of h.ids[i]). Segments are immutable once built.
type segment struct {
	seq  uint64
	h    *HNSW
	keys []string
}

// State is one vector index's writer-side state (spec §4): vid space,
// dead set, chunked overlay, tiered segment forest. Single-writer (the
// Graph writer thread); every mutation is cheap - no graph operations on
// Add (D5). Snapshots for concurrent readers come from Snapshot().
type State struct {
	metric Metric
	dim    int
	m      int
	efc    int
	seed   int64

	nextVID uint32
	vidOf   map[string]uint32

	dead deadBits

	overlay    [][]entry // chunks of <=packSize; all but the tail are frozen
	overlayLen int

	tiers [][]*segment
	seq   uint64
}

// Snapshot is an immutable view of one index at a publish point: tier
// slices copied, overlay chunk pointers copied with the mutable tail
// chunk duplicated, dead bitmap copied. Safe for unsynchronized
// concurrent Search; holds readers back from nothing (spec D1).
type Snapshot struct {
	v vecView
}

type vecView struct {
	metric  Metric
	dim     int
	segs    []*segment // tier-flattened, oldest-first
	overlay [][]entry
	dead    map[uint32][1024]uint64
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
	}
}

// Add upserts key's vector. Wrong-dim vectors are skipped silently; the
// NormalizeVec intake layer above this decides skip+warning (Phase 1
// integration). A same-cycle re-Add of an existing key appends a newer
// overlay entry - scan-order dedup makes the newest win (spec §4.2 seq
// rule); a revive (previously deleted key) clears its dead bit.
func (s *State) Add(key string, v []float32) {
	if len(v) != s.dim {
		return
	}
	vid, ok := s.vidOf[key]
	if !ok {
		vid = s.nextVID
		s.nextVID++
		s.vidOf[key] = vid
	}
	s.dead.unset(vid)
	if s.metric == Cosine {
		v = Normalize(v)
	} else {
		v = append([]float32(nil), v...)
	}
	if n := len(s.overlay); n == 0 || len(s.overlay[n-1]) == packSize {
		s.overlay = append(s.overlay, make([]entry, 0, packSize))
	}
	tail := len(s.overlay) - 1
	s.overlay[tail] = append(s.overlay[tail], entry{key: key, vid: vid, vec: v})
	s.overlayLen++
}

// Delete tombstones key's vid. A later Add revives the same vid (spec §4.4).
func (s *State) Delete(key string) bool {
	vid, ok := s.vidOf[key]
	if !ok {
		return false
	}
	s.dead.set(vid)
	return true
}

// OverlayLen returns the pending (not yet packed) ENTRY count. Same-key
// re-Adds each count once (newest wins at scan time) - callers using this
// as a uniqueness threshold see up to re-Add-count inflation; Pack's
// non-forced gate shares the same entry-count semantics.
func (s *State) OverlayLen() int { return s.overlayLen }

// Snapshot materializes the current state for concurrent readers. Must
// run on the writer thread (it reads the live structures); the returned
// Snapshot is immutable forever after. Cost is O(live segments + overlay
// chunks + tail copy + dead bytes) - called at generation publish rate,
// not per Add.
func (s *State) Snapshot() *Snapshot {
	var segs []*segment
	for _, tier := range s.tiers {
		segs = append(segs, tier...)
	}
	overlay := make([][]entry, len(s.overlay))
	for i, ch := range s.overlay {
		if i == len(s.overlay)-1 && len(ch) < packSize {
			// Tail chunk is still mutable: duplicate it.
			tail := make([]entry, len(ch))
			copy(tail, ch)
			overlay[i] = tail
			continue
		}
		overlay[i] = ch // frozen: full chunks are never appended to again
	}
	dead := make(map[uint32][1024]uint64, len(s.dead.chunks))
	for c, bits := range s.dead.chunks {
		dead[c] = bits
	}
	return &Snapshot{v: vecView{metric: s.metric, dim: s.dim, segs: segs, overlay: overlay, dead: dead}}
}

// Search merges all live segments and the overlay (live-state form, for
// single-threaded callers and tests): the same vid resolves to its most
// recent occurrence - overlay beats segments, later overlay chunks win.
// Convenience form: fresh Reader per call; hot paths use SearchReader.
func (s *State) Search(q []float32, k, ef int) []VectorHit {
	return s.SearchReader(q, k, ef, &Reader{})
}

// NewReader returns a per-goroutine search Reader (see hnsw.go Reader).
func (s *State) NewReader() *Reader { return &Reader{} }

// SearchReader is Search with caller-owned reader state. Live-state form:
// safe for concurrent readers only against a State that is not being
// written; concurrent read-during-write goes through Snapshot.Search.
func (s *State) SearchReader(q []float32, k, ef int, r *Reader) []VectorHit {
	var segs []*segment
	for _, tier := range s.tiers {
		segs = append(segs, tier...)
	}
	return vecView{metric: s.metric, dim: s.dim, segs: segs, overlay: s.overlay, dead: s.dead.chunks}.search(q, k, ef, r)
}

// Search is the zero-lock concurrent entry: immutable snapshot +
// per-goroutine Reader. Nothing in this path writes.
func (s *Snapshot) Search(q []float32, k, ef int, r *Reader) []VectorHit {
	return s.v.search(q, k, ef, r)
}

// search is the single query core shared by the live and snapshot forms
// (spec §6.2): per-segment searches at a quota, overlay linear scan,
// seq-aware merge (newest occurrence wins), dead filter, top-k. Keys ride
// the merge directly from segment key arrays (indexed by the internal idx
// the search returns) and overlay entry fields - no shared vid->key map
// on the read path.
func (v vecView) search(q []float32, k, ef int, r *Reader) []VectorHit {
	if k <= 0 || len(q) != v.dim {
		return nil
	}
	if v.metric == Cosine {
		q = Normalize(q)
	} else {
		q = append([]float32(nil), q...)
	}
	if ef < k {
		ef = k
	}
	quota := ef / 4 // per-segment candidate quota feeding the merge (spec §6.2)
	if quota < k {
		quota = k
	}
	alive := func(vid uint32) bool {
		ch, ok := v.dead[vid>>16]
		return !ok || ch[(vid&0xffff)>>6]&(1<<(vid&63)) == 0
	}
	type best struct {
		key   string
		score float64
		seq   uint64
	}
	m := map[uint32]best{}
	for _, seg := range v.segs {
		for _, c := range seg.h.searchIdx(q, quota, ef, alive, r) {
			vid := seg.h.ids[c.idx]
			if b, ok := m[vid]; !ok || seg.seq > b.seq {
				m[vid] = best{key: seg.keys[c.idx], score: c.d, seq: seg.seq}
			}
		}
	}
	for _, ch := range v.overlay {
		for _, e := range ch {
			if !alive(e.vid) {
				continue
			}
			m[e.vid] = best{key: e.key, score: Distance(v.metric, q, e.vec), seq: math.MaxUint64}
		}
	}
	all := make([]VectorHit, 0, len(m))
	for _, b := range m {
		all = append(all, VectorHit{Key: b.key, Score: b.score})
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
	if s.overlayLen == 0 {
		return
	}
	if !force && s.overlayLen < packSize {
		return
	}
	// Dedup by vid, newest entry wins, first-occurrence order kept
	// (deterministic builds).
	pos := map[uint32]int{}
	var list []entry
	for _, ch := range s.overlay {
		for _, e := range ch {
			if i, ok := pos[e.vid]; ok {
				list[i] = e
				continue
			}
			pos[e.vid] = len(list)
			list = append(list, e)
		}
	}
	seg := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+int64(s.seq)), keys: make([]string, 0, len(list))}
	s.seq++
	for _, e := range list {
		if !s.dead.has(e.vid) {
			seg.h.insertPrepared(e.vid, e.vec) // overlay vecs are stored-form
			seg.keys = append(seg.keys, e.key)
		}
	}
	// Drop instead of truncating: a [:0] slice keeps stale entry pointers
	// alive in the backing array (heap bitmap scans the whole object),
	// pinning dead vectors and inflating steady-state memory.
	s.overlay = nil
	s.overlayLen = 0
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
		keys := segs[j].keys
		for i, id := range ids {
			if seen[id] || s.dead.has(id) {
				continue
			}
			seen[id] = true
			merged.h.insertPrepared(id, vecs[i]) // All() vecs are stored-form; source graph dies here
			merged.keys = append(merged.keys, keys[i])
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
				if s.dead.has(id) {
					dead++
				}
			}
			if dead == 0 || float64(dead)/float64(len(ids)) <= threshold {
				continue
			}
			fresh := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+int64(s.seq))}
			s.seq++
			for j, id := range ids {
				if !s.dead.has(id) {
					fresh.h.insertPrepared(id, vecs[j]) // stored-form; old graph dropped below
					fresh.keys = append(fresh.keys, seg.keys[j])
				}
			}
			tier[i] = fresh
		}
	}
}

// Len returns the total vector count across segments plus the overlay
// (entry count). Feeds the adaptive ef formula; approximate is fine.
func (s *State) Len() int {
	n := s.overlayLen
	for _, tier := range s.tiers {
		for _, seg := range tier {
			n += seg.h.Len()
		}
	}
	return n
}

// FullChunkPending reports whether the overlay holds at least one full
// pack chunk - the folder lane's trigger.
func (s *State) FullChunkPending() bool { return s.overlayLen >= packSize }

// PackPlan is a two-phase drain (spec D5: the writer must never wait on
// graph building). BeginPack captures immutable inputs under a short
// lock; Build does every HNSW construction lock-free on the folder
// goroutine; CommitPack identity-checks and swaps under another short
// lock. Inputs are immutable by construction - a full overlay chunk
// freezes forever, built segments never mutate - so the check is a
// pointer identity, not a deep compare. The writer's intake only
// appends tail chunks and flips dead bits, neither of which
// invalidates a plan: a delete racing the build leaves a since-dead vid
// inside the packed segment, and every snapshot's dead copy filters it
// at search time.
type PackPlan struct {
	chunk   []entry      // frozen chunk being packed
	chunkID *entry       // &chunk[0]: commit-time identity
	tiers   [][]*segment // deep-copied tier snapshot
	dead    map[uint32][1024]uint64
	seq     uint64 // segment seq allocator at begin

	newTiers [][]*segment // Build output
	seqEnd   uint64
}

// BeginPack captures one chunk's pack inputs; nil when no full chunk
// is pending. Under the State lock.
func (s *State) BeginPack() *PackPlan {
	if s.overlayLen < packSize {
		return nil
	}
	tiers := make([][]*segment, len(s.tiers))
	for i, t := range s.tiers {
		tiers[i] = append([]*segment(nil), t...)
	}
	dead := make(map[uint32][1024]uint64, len(s.dead.chunks))
	for c, bits := range s.dead.chunks {
		dead[c] = bits
	}
	chunk := s.overlay[0]
	return &PackPlan{chunk: chunk, chunkID: &chunk[0], tiers: tiers, dead: dead, seq: s.seq}
}

// Build constructs the L0 segment, the 4-way cascade, and the dead
// compaction over the captured inputs - all lock-free. HNSW building is
// the entire cost; nothing here touches the State.
func (p *PackPlan) Build(metric Metric, m, efc int, seed int64) {
	dead := deadBits{chunks: p.dead}
	newSeg := func() *segment {
		sg := &segment{seq: p.seq, h: NewHNSW(metric, m, efc, seed+int64(p.seq))}
		p.seq++
		return sg
	}
	packSeg := func(entries []entry) *segment {
		pos := map[uint32]int{}
		list := make([]entry, 0, len(entries))
		for _, e := range entries {
			if i, ok := pos[e.vid]; ok {
				list[i] = e
				continue
			}
			pos[e.vid] = len(list)
			list = append(list, e)
		}
		sg := newSeg()
		for _, e := range list {
			if !dead.has(e.vid) {
				sg.h.insertPrepared(e.vid, e.vec)
				sg.keys = append(sg.keys, e.key)
			}
		}
		return sg
	}
	mergeSeg := func(parts []*segment) *segment {
		sg := newSeg()
		seen := map[uint32]bool{}
		for j := len(parts) - 1; j >= 0; j-- { // tiers append oldest-first
			ids, vecs := parts[j].h.All()
			for i, id := range ids {
				if seen[id] || dead.has(id) {
					continue
				}
				seen[id] = true
				sg.h.insertPrepared(id, vecs[i])
				sg.keys = append(sg.keys, parts[j].keys[i])
			}
		}
		return sg
	}

	tiers := p.tiers
	if len(tiers) == 0 {
		tiers = append(tiers, nil)
	}
	tiers[0] = append(tiers[0], packSeg(p.chunk))
	i := 0
	for {
		for i >= len(tiers) {
			tiers = append(tiers, nil)
		}
		if len(tiers[i]) < tierFactor {
			break
		}
		carry := mergeSeg(tiers[i])
		tiers[i] = nil
		i++
		for i >= len(tiers) { // the promoted segment lands at i
			tiers = append(tiers, nil)
		}
		tiers[i] = append(tiers[i], carry)
	}
	// Dead compaction over the copy (30% line, spec 4.2).
	for ti, tier := range tiers {
		for si, seg := range tier {
			ids, _ := seg.h.All()
			if len(ids) == 0 {
				continue
			}
			deadc := 0
			for _, id := range ids {
				if dead.has(id) {
					deadc++
				}
			}
			if deadc == 0 || float64(deadc)/float64(len(ids)) <= 0.3 {
				continue
			}
			fresh := newSeg()
			ids2, vecs := seg.h.All()
			for j, id := range ids2 {
				if !dead.has(id) {
					fresh.h.insertPrepared(id, vecs[j])
					fresh.keys = append(fresh.keys, seg.keys[j])
				}
			}
			tiers[ti][si] = fresh
		}
	}
	p.newTiers = tiers
	p.seqEnd = p.seq
}

// CommitPack swaps the built plan in. Under the State lock; false when
// the captured chunk is no longer the overlay head (retry).
func (s *State) CommitPack(p *PackPlan) bool {
	if len(s.overlay) == 0 || len(s.overlay[0]) == 0 || &s.overlay[0][0] != p.chunkID {
		return false
	}
	s.overlay[0] = nil // drop the packed chunk's backing array
	s.overlay = s.overlay[1:]
	s.overlayLen -= len(p.chunk)
	s.tiers = p.newTiers
	s.seq = p.seqEnd
	return true
}

// Len returns the total vector count across segments plus the overlay
// (entry count). Feeds the adaptive ef formula; approximate is fine.
