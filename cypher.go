package vectra

import (
	"github.com/projanvil/vectra/internal/cypher"
)

// CypherError is the positioned error type of the Cypher-subset query
// language (parse, static-validation and execution-guard failures).
type CypherError = cypher.Error

// CypherResult is the outcome of one Cypher-subset statement. Cell values
// are Node, Edge, string, float64, bool or nil; Columns carries the RETURN
// item names in order.
type CypherResult struct {
	Columns []string
	Rows    []map[string]any
}

// Cypher runs one Cypher-subset statement against the graph:
//
//	g.Cypher(`MATCH (b {type:'controller'})-[:CALLS*1..2]->(s)
//	          WHERE s.env = 'prod' RETURN b.key, s.key ORDER BY s.key DESC LIMIT 10`)
//
// The grammar is an openCypher/GQL-compatible core; variable-length edges
// follow node-pair (shortest-distance) semantics.
//
// v0.7 traversal family (spec §3.2, Task 4): Cypher executes over the
// generation's three-source View — writes are visible immediately (edges,
// nodes, and pattern anchors alike). With nothing unmerged the merged-base
// hot path runs unchanged.
func (g *Graph) Cypher(query string) (*CypherResult, error) {
	g.flushPend()
	v := g.view.Load()
	var res *cypher.Result
	if runView, src := g.cypherRunViewOf(v); runView {
		r, err := cypher.RunView(src, query)
		if err != nil {
			return nil, err
		}
		res = r
	} else {
		r, err := cypher.Run(v.base, query)
		if err != nil {
			return nil, err
		}
		res = r
	}
	out := &CypherResult{Columns: res.Columns, Rows: res.Rows}
	// retype engine-facing values into the public Node/Edge types (resolved
	// through the same generation the query ran on — layer records included).
	// A resolution miss is impossible when query and retype share a
	// generation; nil the cell rather than leak the internal value if the
	// invariant ever breaks.
	for _, r := range out.Rows {
		for k, cell := range r {
			switch tv := cell.(type) {
			case cypher.NodeVal:
				if n, ok := v.nodeByID(NodeID(tv.ID)); ok {
					r[k] = Node{ID: n.ID, Key: n.Key, Label: n.Label, Props: n.Props}
				} else {
					r[k] = nil
				}
			case cypher.EdgeVal:
				if e, ok := v.edgeByID(EdgeID(tv.ID)); ok {
					r[k] = Edge{ID: e.ID, Src: e.Src, Dst: e.Dst, Label: e.Label, Props: e.Props}
				} else {
					r[k] = nil
				}
			}
		}
	}
	return out, nil
}
