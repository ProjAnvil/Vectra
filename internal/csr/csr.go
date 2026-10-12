// Package csr implements compressed sparse row adjacency: neighbors of node n
// live at Targets[Offsets[n]:Offsets[n+1]]. Flat uint32 arrays throughout, no
// pointers, so GC scan cost is independent of edge count.
package csr

// Adjacency is a CSR adjacency. EdgeIDs and Labels run parallel to Targets
// and may be nil (when built without payloads).
type Adjacency struct {
	Offsets []uint32 // len == numNodes+1
	Targets []uint32
	EdgeIDs []uint32
	Labels  []uint32
}

// Build builds a CSR from an edge list via counting sort; neighbors of the
// same node keep insertion order.
func Build(numNodes uint32, src, dst []uint32) Adjacency {
	return build(numNodes, src, dst, nil, nil)
}

// BuildPayloads is like Build, with each slot carrying its edge ID and label ID.
func BuildPayloads(numNodes uint32, src, dst, edgeIDs, labels []uint32) Adjacency {
	return build(numNodes, src, dst, edgeIDs, labels)
}

func build(numNodes uint32, src, dst, edgeIDs, labels []uint32) Adjacency {
	a := Adjacency{Offsets: make([]uint32, int(numNodes)+1)}
	for _, s := range src {
		a.Offsets[s+1]++
	}
	for i := uint32(1); i <= numNodes; i++ {
		a.Offsets[i] += a.Offsets[i-1]
	}
	n := len(src)
	a.Targets = make([]uint32, n)
	if edgeIDs != nil {
		a.EdgeIDs = make([]uint32, n)
	}
	if labels != nil {
		a.Labels = make([]uint32, n)
	}
	cursor := make([]uint32, numNodes)
	copy(cursor, a.Offsets[:numNodes])
	for i, s := range src {
		pos := cursor[s]
		cursor[s]++
		a.Targets[pos] = dst[i]
		if a.EdgeIDs != nil {
			a.EdgeIDs[pos] = edgeIDs[i]
		}
		if a.Labels != nil {
			a.Labels[pos] = labels[i]
		}
	}
	return a
}

// Degree returns the out-degree of node n.
func (a Adjacency) Degree(n uint32) uint32 {
	return a.Offsets[n+1] - a.Offsets[n]
}

// Neighbors returns node n's neighbor view (a slice of the underlying array;
// callers must not modify it).
func (a Adjacency) Neighbors(n uint32) []uint32 {
	return a.Targets[a.Offsets[n]:a.Offsets[n+1]]
}

// Row returns node n's full row: three parallel views of neighbors, edge IDs,
// and labels. Arrays without payloads return nil.
func (a Adjacency) Row(n uint32) (targets, edgeIDs, labels []uint32) {
	lo, hi := a.Offsets[n], a.Offsets[n+1]
	targets = a.Targets[lo:hi]
	if a.EdgeIDs != nil {
		edgeIDs = a.EdgeIDs[lo:hi]
	}
	if a.Labels != nil {
		labels = a.Labels[lo:hi]
	}
	return targets, edgeIDs, labels
}
