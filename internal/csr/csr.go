// Package csr 实现压缩稀疏行邻接结构：节点 n 的邻居存放在
// Targets[Offsets[n]:Offsets[n+1]]。全部为 uint32 平坦数组，无指针，
// GC 扫描成本与边数无关。
package csr

// Adjacency 是一份 CSR 邻接。EdgeIDs 与 Labels 与 Targets 平行，
// 可为 nil（Build 不带 payload 时）。
type Adjacency struct {
	Offsets []uint32 // len == numNodes+1
	Targets []uint32
	EdgeIDs []uint32
	Labels  []uint32
}

// Build 用计数排序从边表构建 CSR，同节点邻居保持插入序。
func Build(numNodes uint32, src, dst []uint32) Adjacency {
	return build(numNodes, src, dst, nil, nil)
}

// BuildPayloads 同 Build，且每个槽位携带对应的边 ID 与标签 ID。
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

// Degree 返回节点 n 的出度。
func (a Adjacency) Degree(n uint32) uint32 {
	return a.Offsets[n+1] - a.Offsets[n]
}

// Neighbors 返回节点 n 的邻居视图（底层数组切片，调用方不得修改）。
func (a Adjacency) Neighbors(n uint32) []uint32 {
	return a.Targets[a.Offsets[n]:a.Offsets[n+1]]
}

// Row 返回节点 n 的整行：邻居、边 ID、标签三段平行视图。
// 未携带 payload 的数组返回 nil。
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
