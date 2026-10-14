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

// Cypher runs one Cypher-subset statement against the sealed graph:
//
//	g.Cypher(`MATCH (b {type:'controller'})-[:CALLS*1..2]->(s)
//	          WHERE s.env = 'prod' RETURN b.key, s.key ORDER BY s.key DESC LIMIT 10`)
//
// The grammar is an openCypher/GQL-compatible core; variable-length edges
// follow node-pair (shortest-distance) semantics. Requires Seal first.
func (g *Graph) Cypher(query string) (*CypherResult, error) {
	if !g.sealed {
		return nil, ErrNotSealed
	}
	res, err := cypher.Run(g.snap, query)
	if err != nil {
		return nil, err
	}
	out := &CypherResult{Columns: res.Columns, Rows: res.Rows}
	// retype engine-facing values into the public Node/Edge types
	for _, r := range out.Rows {
		for k, v := range r {
			switch tv := v.(type) {
			case cypher.NodeVal:
				n, _ := g.NodeByID(NodeID(tv.ID))
				r[k] = n
			case cypher.EdgeVal:
				e, _ := g.EdgeByID(EdgeID(tv.ID))
				r[k] = e
			}
		}
	}
	return out, nil
}
