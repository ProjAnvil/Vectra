// Package dag 提供强连通分量（Tarjan 迭代版）、凝聚图构建与拓扑层级。
// 查询侧的"写入时压环"由本包支撑：Seal 时压缩 SCC，之后查询面对 DAG。
package dag

import "slices"

type frame struct {
	v  uint32
	ei uint32
}

// Tarjan 计算强连通分量。返回 sccOf（节点->分量 ID）与分量总数。
// 迭代实现：显式调用栈，深度不受 goroutine 栈限制。
func Tarjan(numNodes uint32, adj func(uint32) []uint32) (sccOf []uint32, count uint32) {
	const unvisited = ^uint32(0)
	index := make([]uint32, numNodes)
	for i := range index {
		index[i] = unvisited
	}
	low := make([]uint32, numNodes)
	onStack := make([]bool, numNodes)
	sccOf = make([]uint32, numNodes)
	var tarjanStack []uint32
	var callStack []frame
	var counter uint32

	for root := uint32(0); root < numNodes; root++ {
		if index[root] != unvisited {
			continue
		}
		index[root] = counter
		low[root] = counter
		counter++
		tarjanStack = append(tarjanStack, root)
		onStack[root] = true
		callStack = append(callStack, frame{v: root})
		for len(callStack) > 0 {
			f := &callStack[len(callStack)-1]
			nbrs := adj(f.v)
			if f.ei < uint32(len(nbrs)) {
				w := nbrs[f.ei]
				f.ei++
				if index[w] == unvisited {
					index[w] = counter
					low[w] = counter
					counter++
					tarjanStack = append(tarjanStack, w)
					onStack[w] = true
					callStack = append(callStack, frame{v: w})
				} else if onStack[w] && index[w] < low[f.v] {
					low[f.v] = index[w]
				}
				continue
			}
			// 节点 f.v 的所有邻居处理完毕
			if low[f.v] == index[f.v] {
				for {
					w := tarjanStack[len(tarjanStack)-1]
					tarjanStack = tarjanStack[:len(tarjanStack)-1]
					onStack[w] = false
					sccOf[w] = count
					if w == f.v {
						break
					}
				}
				count++
			}
			callStack = callStack[:len(callStack)-1]
			if len(callStack) > 0 {
				p := &callStack[len(callStack)-1]
				if low[f.v] < low[p.v] {
					low[p.v] = low[f.v]
				}
			}
		}
	}
	return sccOf, count
}

// Condensed 是凝聚图的 CSR 表示。Labels 平行于 Targets：该凝聚边继承的
// 原边标签（跨 SCC 且同标签的平行边去重后保留一条）。
type Condensed struct {
	Offsets []uint32 // len == sccCount+1
	Targets []uint32
	Labels  []uint32
}

func (c Condensed) Row(s uint32) (targets, labels []uint32) {
	lo, hi := c.Offsets[s], c.Offsets[s+1]
	if c.Labels == nil {
		return c.Targets[lo:hi], nil
	}
	return c.Targets[lo:hi], c.Labels[lo:hi]
}

type condEdge struct {
	src, dst, label uint32
}

// Condense 构建凝聚图。labelOf 返回与 adj(u) 平行的每条出边标签，
// 为 nil 时凝聚边不带标签。跨 SCC 边按 (src, dst, label) 三元组去重。
func Condense(numNodes uint32, sccOf []uint32, count uint32, adj func(uint32) []uint32, labelOf func(uint32) []uint32) Condensed {
	var edges []condEdge
	for u := uint32(0); u < numNodes; u++ {
		nbrs := adj(u)
		var labs []uint32
		if labelOf != nil {
			labs = labelOf(u)
		}
		for i, v := range nbrs {
			if sccOf[u] == sccOf[v] {
				continue
			}
			e := condEdge{src: sccOf[u], dst: sccOf[v]}
			if labs != nil {
				e.label = labs[i]
			}
			edges = append(edges, e)
		}
	}
	slices.SortFunc(edges, func(a, b condEdge) int {
		if a.src != b.src {
			return int(a.src) - int(b.src)
		}
		if a.dst != b.dst {
			return int(a.dst) - int(b.dst)
		}
		return int(a.label) - int(b.label)
	})
	edges = slices.CompactFunc(edges, func(a, b condEdge) bool { return a == b })

	c := Condensed{Offsets: make([]uint32, count+1)}
	for _, e := range edges {
		c.Offsets[e.src+1]++
	}
	for i := uint32(1); i <= count; i++ {
		c.Offsets[i] += c.Offsets[i-1]
	}
	c.Targets = make([]uint32, len(edges))
	if labelOf != nil {
		c.Labels = make([]uint32, len(edges))
	}
	for i, e := range edges {
		c.Targets[i] = e.dst
		if c.Labels != nil {
			c.Labels[i] = e.label
		}
	}
	return c
}

// Levels 用 Kahn 拓扑排序计算凝聚图最长路径层级：入度 0 为 0 层，
// 其余为 1 + max(前驱层级)。DAG 上所有边严格升层。
func Levels(c Condensed) []uint32 {
	n := uint32(len(c.Offsets) - 1)
	level := make([]uint32, n)
	indeg := make([]uint32, n)
	for _, d := range c.Targets {
		indeg[d]++
	}
	queue := make([]uint32, 0, n)
	for s := uint32(0); s < n; s++ {
		if indeg[s] == 0 {
			queue = append(queue, s)
		}
	}
	for i := 0; i < len(queue); i++ {
		s := queue[i]
		tg, _ := c.Row(s)
		for _, d := range tg {
			if level[s]+1 > level[d] {
				level[d] = level[s] + 1
			}
			indeg[d]--
			if indeg[d] == 0 {
				queue = append(queue, d)
			}
		}
	}
	return level
}
