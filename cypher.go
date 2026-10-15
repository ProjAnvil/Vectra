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
// v0.7 traversal family (spec §3.2): Cypher runs against the merged base of
// the current generation — no sealed precondition anymore, but layer writes
// become visible only after a merge (Seal); the three-source View is Task 4.
// TODO(Task4).
func (g *Graph) Cypher(query string) (*CypherResult, error) {
	snap := g.view.Load().base
	res, err := cypher.Run(snap, query)
	if err != nil {
		return nil, err
	}
	out := &CypherResult{Columns: res.Columns, Rows: res.Rows}
	// retype engine-facing values into the public Node/Edge types (read from
	// the same base the query ran on, keeping row cells consistent with it)
	for _, r := range out.Rows {
		for k, v := range r {
			switch tv := v.(type) {
			case cypher.NodeVal:
				if rec, ok := snap.NodeInfo(tv.ID); ok {
					r[k] = Node{ID: NodeID(tv.ID), Key: rec.Key, Label: rec.Label, Props: rec.Props}
				}
			case cypher.EdgeVal:
				if src, dst, label, props, ok := snap.EdgeInfo(tv.ID); ok {
					r[k] = Edge{ID: EdgeID(tv.ID), Src: NodeID(src), Dst: NodeID(dst), Label: label, Props: props}
				}
			}
		}
	}
	return out, nil
}
