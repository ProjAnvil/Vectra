package engine

// KillSet is a copy-on-write chunked bitmap of shadowed edge IDs — the
// aggregated DeadBaseEdges/KilledEdgeIDs of a View's sources. Chunks cover
// KillChunkIDs consecutive IDs (512 uint64 words); `with` copies only the
// chunks the new IDs touch, so successive Views of appending-source stacks
// share untouched chunk backings and the incremental builder's per-publish
// aggregation costs O(new kills + chunks) instead of O(all kills) — the
// re-aggregating NewView loop was the writer's dominant per-publish cost
// once the folded run accumulated tombstones (2026-10-17 round 3).
//
// The zero value is an empty set (all lookups miss); a nil KillSet is valid.
// Sets handed to a View must be treated as immutable afterwards.
type KillSet map[uint32][]uint64

// KillChunkIDs is the ID span of one chunk (must be a power of two).
const KillChunkIDs = 1 << 15

func KillChunkBase(chunk uint32) uint32 { return chunk * KillChunkIDs }

func (ks KillSet) Has(eid uint32) bool {
	ch, ok := ks[eid/KillChunkIDs]
	if !ok {
		return false
	}
	return ch[(eid>>6)&(KillChunkIDs/64-1)]&(1<<(eid&63)) != 0
}

// with returns a set additionally containing eids, sharing every chunk the
// new IDs do not touch. The receiver is not modified.
func (ks KillSet) With(eids []uint32) KillSet {
	if len(eids) == 0 {
		return ks
	}
	out := make(KillSet, len(ks)+1)
	for c, ch := range ks {
		out[c] = ch
	}
	for _, eid := range eids {
		c := eid / KillChunkIDs
		ch, ok := out[c]
		if !ok {
			ch = make([]uint64, KillChunkIDs/64)
			out[c] = ch
		} else if orig, existed := ks[c]; existed && &ch[0] == &orig[0] {
			// still the shared backing: copy before writing
			cp := make([]uint64, len(ch))
			copy(cp, ch)
			ch = cp
			out[c] = ch
		}
		ch[(eid>>6)&(KillChunkIDs/64-1)] |= 1 << (eid & 63)
	}
	return out
}
