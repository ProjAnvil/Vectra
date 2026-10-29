package cypher

// WriteSummary is the statement's mutation counters (the Neo4j counters
// analogue). Result carries it when the statement wrote.
type WriteSummary struct {
	CreatedNodes, CreatedEdges    int
	DeletedNodes, DeletedEdges    int
	PropertiesSet, IndexesCreated int
}

// Writer is the mutation surface the write clauses drive. Every method is
// key- or triple-addressed: the executor resolves bound IDs to keys once,
// against the pinned pre-write source, before the first write call. All
// calls arrive sequentially from one goroutine (the writer goroutine in
// direct mode, the FSM apply goroutine in server mode).
type Writer interface {
	// NewNodeKey returns a fresh unique node key for CREATE patterns
	// without an explicit key: "_c<id>" drawn from the node-ID ratchet.
	// Deterministic: replaying the same statement sequence on identical
	// state yields identical keys (required for raft FSM re-execution).
	NewNodeKey() string
	NodeIDByKey(key string) (uint32, bool)
	// CreateNode applies AddNode upsert semantics; created=false when the
	// key already existed (props replaced, per AddNode).
	CreateNode(key, label string, props map[string]any) (id uint32, created bool, err error)
	// SetNodeProps merges props; a nil value deletes the key; returns the
	// number of property keys written (sets + deletes).
	SetNodeProps(key string, props map[string]any) (int, error)
	NodeHasEdges(key string) bool
	DeleteNodeCascade(key string) error
	// DeleteEdgeTriple deletes every parallel edge on the triple (E6
	// divergence, pinned by test).
	DeleteEdgeTriple(srcKey, dstKey, label string) (int, error)
	UpsertEdgeTriple(srcKey, dstKey, label string, props map[string]any) (created bool, err error)
	// CreateEdge adds a NEW parallel edge (CREATE semantics — duplicates
	// allowed; contrast UpsertEdgeTriple's fold). Fails when an endpoint
	// is missing (statement error, first-error-stops).
	CreateEdge(srcKey, dstKey, label string, props map[string]any) (id uint32, err error)
	// SetEdgeProps writes props onto every live parallel edge of the
	// triple; returns the number of edges touched.
	SetEdgeProps(srcKey, dstKey, label string, props map[string]any) (int, error)
	// EdgeIDByTriple returns the first live parallel edge (used to rebind
	// RETURN'd edge variables after writes).
	EdgeIDByTriple(srcKey, dstKey, label string) (uint32, bool)
	IndexProperty(key string) error
	IndexFullText(key string) error
}

// IsWrite reports whether the statement mutates (write clauses or DDL).
// The parse error is returned unchanged: classification never invents
// errors, so an unparseable statement is the caller's parse failure, not
// a classification verdict.
func IsWrite(q string) (bool, error) {
	ast, err := Parse(q)
	if err != nil {
		return false, err
	}
	for _, c := range ast.Clauses {
		switch c.(type) {
		case *CreateClause, *SetClause, *DeleteClause, *MergeClause, *IndexClause:
			return true, nil
		}
	}
	return false, nil
}
