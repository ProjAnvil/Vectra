package delta

// FoldLayers folds one stack of micro-layers (oldest first, same base) into a
// single layer with identical composition semantics:
//
//   - node winner per key: the newest live record wins and keeps its
//     writer-assigned ID; a key whose final state is dead contributes no
//     record, but its tombstone survives (it must keep shadowing the base);
//   - edge survivors: a later layer's edge tombstone drops every older-layer
//     record of that key (parallels wholesale, matching the publish-time
//     reconciliation); surviving records keep their IDs and relative order;
//     global emission order is the original stack order restricted to
//     survivors;
//   - tombstone sets are the full union — over-approximation is safe, a
//     tombstone on a key the base lacks kills nothing, and each layer's own
//     live records already won over its own tombstones at build time;
//   - labels and index declarations: first-seen union.
//
// The input layers are not modified. A stack of ID-less layers folds to an
// ID-less layer; a mixed stack degrades to ID-less (root never mixes).
// FoldLayers is pure: no locks, no shared state, safe on any goroutine.
func FoldLayers(layers []*Layer) *Layer {
	if len(layers) == 0 {
		return nil
	}
	if len(layers) == 1 {
		return layers[0]
	}

	type nodeWin struct {
		rec   NodeRec
		id    uint32
		hasID bool
		dead  bool
	}
	type edgeSlot struct {
		rec  EdgeRec
		id   uint32
		live bool
	}
	nodes := make(map[string]*nodeWin)
	var nodeOrder []string
	edges := make(map[EdgeKey][]*edgeSlot)
	var edgeFlat []*edgeSlot
	nodeTomb := make(map[string]bool)
	edgeTomb := make(map[EdgeKey]bool)
	labelSeen := make(map[string]bool)
	var labels []string
	indexSeen := make(map[string]bool)
	var indexKeys []string
	hasIDs := true

	for _, l := range layers {
		if l.nodeIDs == nil || l.edgeIDs == nil {
			hasIDs = false
		}
		for k := range l.nodeTomb {
			nodeTomb[k] = true
			if w := nodes[k]; w != nil {
				w.dead = true
			}
		}
		for _, rec := range l.nodes {
			w := nodes[rec.Key]
			if w == nil {
				w = &nodeWin{}
				nodes[rec.Key] = w
				nodeOrder = append(nodeOrder, rec.Key)
			}
			w.rec, w.dead = rec, false
			if id, ok := l.LookupNodeID(rec.Key); ok {
				w.id, w.hasID = id, true
			}
		}
		for k := range l.edgeTomb {
			edgeTomb[k] = true
			for _, s := range edges[k] {
				s.live = false
			}
		}
		for j, rec := range l.edges {
			s := &edgeSlot{rec: rec, live: true}
			if l.edgeIDs != nil {
				s.id = l.edgeIDs[j]
			}
			k := EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
			edges[k] = append(edges[k], s)
			edgeFlat = append(edgeFlat, s)
		}
		for _, name := range l.labels {
			if !labelSeen[name] {
				labelSeen[name] = true
				labels = append(labels, name)
			}
		}
		for _, ik := range l.indexKeys {
			if !indexSeen[ik] {
				indexSeen[ik] = true
				indexKeys = append(indexKeys, ik)
			}
		}
	}

	f := &Layer{
		nodeTomb:  nodeTomb,
		edgeTomb:  edgeTomb,
		labels:    labels,
		indexKeys: indexKeys,
	}
	f.nodes = make([]NodeRec, 0, len(nodeOrder))
	f.nodeIdx = make(map[string]int, len(nodeOrder))
	if hasIDs {
		f.nodeIDs = make(map[string]uint32, len(nodeOrder))
		f.nodeIDIdx = make(map[uint32]int, len(nodeOrder))
	}
	for _, k := range nodeOrder {
		w := nodes[k]
		if w.dead {
			continue
		}
		f.nodeIdx[k] = len(f.nodes)
		f.nodes = append(f.nodes, w.rec)
		if hasIDs && w.hasID {
			f.nodeIDs[k] = w.id
			f.nodeIDIdx[w.id] = len(f.nodes) - 1
		}
	}
	liveEdges := 0
	for _, s := range edgeFlat {
		if s.live {
			liveEdges++
		}
	}
	f.edges = make([]EdgeRec, 0, liveEdges)
	if hasIDs {
		f.edgeIDs = make([]uint32, 0, liveEdges)
	}
	for _, s := range edgeFlat {
		if !s.live {
			continue
		}
		f.edges = append(f.edges, s.rec)
		if hasIDs {
			f.edgeIDs = append(f.edgeIDs, s.id)
		}
	}
	return f
}
