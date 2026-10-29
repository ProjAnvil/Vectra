package vectra

import (
	"github.com/projanvil/vectra/internal/cypher"
)

// CypherError is the positioned error type of the Cypher-subset query
// language (parse, static-validation and execution-guard failures).
type CypherError = cypher.Error

// CypherResult is the outcome of one Cypher-subset statement. Cell values
// are Node, Edge, string, float64, bool or nil; Columns carries the RETURN
// item names in order. Summary carries the mutation counters of a WRITE
// statement; read statements leave it nil.
type CypherResult struct {
	Columns []string
	Rows    []map[string]any
	Summary *CypherWriteSummary
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
//
// v0.14 write clauses: statements that mutate (CypherIsWrite) take the
// RunWrite path; read statements keep the single-parse read path below,
// untouched.
func (g *Graph) Cypher(query string) (*CypherResult, error) {
	// Write routing sits behind a zero-allocation keyword prefilter so the
	// read path pays nothing for classification: any write statement MUST
	// spell one of the write-clause keywords, so a miss proves a read; a
	// false hit (the keyword inside a literal or identifier) only costs the
	// full IsWrite parse, which classifies it back onto the read path.
	if maybeWriteQuery(query) {
		isWrite, err := CypherIsWrite(query)
		if err != nil {
			return nil, err // the read path's own parse would fail identically
		}
		if isWrite {
			return g.cypherWrite(query)
		}
	}
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
	return g.retypeResult(res, v)
}

// cypherWrite executes one write statement: the read prefix pins the
// pre-write generation (flush-then-load, so prior absorbed writes are
// visible to it), the write stages drive the writeGraph, and the RETURN
// cells retype through a POST-write generation — a fresh load after the
// statement's own writes have flushed — so created entities resolve rather
// than nil.
func (g *Graph) cypherWrite(query string) (*CypherResult, error) {
	g.flushPend()
	v := g.view.Load()
	// RunWrite needs a Source even over a fully merged base: the pinned
	// genView's engine View serves the base CSR as its fast path.
	src := cypherSource{gv: v, ev: g.engineViewOf(v)}
	res, err := cypher.RunWrite(src, newWriteGraph(g), query)
	if err != nil {
		return nil, err
	}
	g.flushPend() // the statement's writes land before the retype reads them
	return g.retypeResult(res, g.view.Load())
}

// retypeResult publishes one internal Result as the public CypherResult:
// NodeVal/EdgeVal cells are resolved to the public Node/Edge types through
// the given generation (layer records included — pass the generation the
// rows' IDs belong to). A resolution miss is impossible when the rows and
// the view share a generation; nil the cell rather than leak the internal
// value if the invariant ever breaks.
func (g *Graph) retypeResult(res *cypher.Result, v *genView) (*CypherResult, error) {
	out := &CypherResult{Columns: res.Columns, Rows: res.Rows, Summary: res.Summary}
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

// writeClauseKeywords are the clause-head keywords a mutating statement
// must spell (CREATE covers CREATE INDEX, DELETE covers DETACH DELETE).
// maybeWriteQuery reports whether any of them appears in q as a substring,
// comparing ASCII case-insensitively with zero allocations — a deliberate
// SUPERSET of write statements: false negatives would misroute a write
// onto the read path, false positives cost one extra parse.
var writeClauseKeywords = []string{"create", "merge", "set", "delete"}

func maybeWriteQuery(q string) bool {
	for _, kw := range writeClauseKeywords {
		if containsFoldASCII(q, kw) {
			return true
		}
	}
	return false
}

// containsFoldASCII is a case-insensitive substring test for lowercase
// ASCII needles (strings.Contains semantics, no allocation, no Unicode
// case folding — Cypher keywords are ASCII by grammar).
func containsFoldASCII(s, sub string) bool {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		match := true
		for j := 0; j < n; j++ {
			c := s[i+j]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != sub[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
