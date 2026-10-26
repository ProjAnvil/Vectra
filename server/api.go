package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

// httpPost posts body as JSON. The Authorization header is attached only
// when bearer is non-empty, so auth-off deployments keep today's wire
// behavior byte-for-byte.
func httpPost(url string, body []byte, timeout time.Duration, bearer string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// API error model: {"error":{"code":"...","message":"..."}}.
type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": apiError{Code: code, Message: msg}})
}

// queryRequest is the request body of POST /v1/query.
type queryRequest struct {
	From      []string `json:"from"`
	Labels    []string `json:"labels"`
	Direction string   `json:"direction"` // "out" (default)/"in"
	MaxDepth  uint32   `json:"maxDepth"`
}

// Handler returns this node's HTTP API (mountable on any mux/httptest).
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/batch", n.handleBatch)
	mux.HandleFunc("POST /v1/txn", n.handleTxn)
	mux.HandleFunc("POST /v1/join", n.handleJoin)
	mux.HandleFunc("GET /v1/health", n.handleHealth)
	mux.HandleFunc("GET /v1/stats", n.handleStats)
	mux.HandleFunc("GET /v1/await", n.handleAwait)
	mux.HandleFunc("GET /v1/nodes/{key}", n.handleNode)
	mux.HandleFunc("GET /v1/neighbors", n.handleNeighbors)
	mux.HandleFunc("POST /v1/query", n.handleQuery)
	mux.HandleFunc("POST /v1/cypher", n.handleCypher)
	mux.HandleFunc("GET /v1/reachable", n.handleReachable)
	mux.HandleFunc("GET /v1/path", n.handlePath)
	mux.HandleFunc("GET /v1/cycles", n.handleCycles)
	mux.HandleFunc("POST /v1/vector/search", n.handleVectorSearch)
	mux.HandleFunc("GET /v1/vector/indexes", n.handleVectorIndexList)
	mux.HandleFunc("POST /v1/vector/indexes", n.handleVectorIndexCreate)
	mux.HandleFunc("DELETE /v1/vector/indexes/{name}", n.handleVectorIndexDelete)
	mux.Handle("/debug/vars", expvar.Handler())
	n.registerExpvar()

	// Auth wraps the whole mux only when tokens are loaded: without
	// AuthTokenFile the bare mux keeps today's wire behavior byte-for-byte.
	if len(n.authDigests) > 0 {
		return n.authMiddleware(mux)
	}
	return mux
}

// expvarOnce is a process-level singleton: with multiple nodes in one process
// (e.g. a test cluster), only the first node's vitra_* metrics are visible.
// No impact on single-node production deployments.
var expvarOnce sync.Once

func (n *Node) registerExpvar() {
	expvarOnce.Do(func() {
		m := n.fsm.metrics
		expvar.Publish("vectra_applied_batches", expvar.Func(func() any { return m.AppliedBatches.Load() }))
		expvar.Publish("vectra_applied_nodes", expvar.Func(func() any { return m.AppliedNodes.Load() }))
		expvar.Publish("vectra_applied_edges", expvar.Func(func() any { return m.AppliedEdges.Load() }))
		expvar.Publish("vectra_skipped_edges", expvar.Func(func() any { return m.SkippedEdges.Load() }))
		expvar.Publish("vectra_deleted_edges", expvar.Func(func() any { return m.DeletedEdges.Load() }))
		expvar.Publish("vectra_deleted_nodes", expvar.Func(func() any { return m.DeletedNodes.Load() }))
		expvar.Publish("vectra_seals", expvar.Func(func() any { return m.Seals.Load() }))
		expvar.Publish("vectra_restores", expvar.Func(func() any { return m.Restores.Load() }))
		expvar.Publish("vectra_queries", expvar.Func(func() any { return n.queryCount.Load() }))
		expvar.Publish("vectra_leader_changes", expvar.Func(func() any { return m.LeaderChanges.Load() }))
	})
}

// gateReady is the readiness gate for query endpoints: 503 NOT_READY before the first publish.
func (n *Node) gateReady(w http.ResponseWriter) bool {
	select {
	case <-n.fsm.Ready():
		return true
	default:
		writeErr(w, http.StatusServiceUnavailable, "NOT_READY", "first publish not completed (recovering)")
		return false
	}
}

func (n *Node) graph(w http.ResponseWriter) *vectra.Graph {
	if !n.gateReady(w) {
		return nil
	}
	ps := n.fsm.published.Load()
	if ps == nil {
		writeErr(w, http.StatusServiceUnavailable, "NOT_READY", "snapshot not yet published")
		return nil
	}
	// Header must be set before any WriteHeader; writeJSON below is the first writer.
	w.Header().Set("X-Vectra-Snapshot", strconv.FormatUint(ps.seq, 10))
	return ps.graph
}

// writeApplyErr maps an n.Apply error onto the write-endpoint error faces
// shared by every raft-backed handler: non-leader → 503 with leader hint +
// Retry-After, shutdown → 503, timeout → 504, anything else → 500.
// Endpoint-specific classifiers (vector declaration ops) run before this
// and take precedence.
func (n *Node) writeApplyErr(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotLeader) {
		if hint := n.LeaderHint(); hint != "" {
			w.Header().Set("X-Vectra-Leader", hint)
		}
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "NOT_LEADER", "send writes to the leader")
		return
	}
	if errors.Is(err, ErrShutdown) {
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusServiceUnavailable, "SHUTTING_DOWN", "node is shutting down; retry another node")
		return
	}
	if errors.Is(err, ErrTimeout) {
		writeErr(w, http.StatusGatewayTimeout, "TIMEOUT", err.Error())
		return
	}
	writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
}

// wireableProps rewrites JSON-decoded numeric arrays in props in place:
// encoding/json decodes every JSON array into []any, and encoding/gob (the
// raft log codec) has no concrete type registered for []any, so a vector
// prop would fail the whole batch at encode with "type not registered".
// Uniform numeric arrays become []float64 — gob-native, and the engine
// ingests it identically (vector.NormalizeVec). Non-numeric arrays are left
// untouched and fail at encode as before.
func wireableProps(props map[string]any) {
	for k, v := range props {
		arr, ok := v.([]any)
		if !ok {
			continue
		}
		out := make([]float64, len(arr))
		for i, e := range arr {
			f, ok := e.(float64)
			if !ok {
				out = nil
				break
			}
			out[i] = f
		}
		if out != nil {
			props[k] = out
		}
	}
}

// wireableBatchProps applies the JSON-boundary normalization to a decoded
// write request's node and edge props.
func wireableBatchProps(nodes []NodeUpsert, edges []EdgeUpsert) {
	for i := range nodes {
		wireableProps(nodes[i].Props)
	}
	for i := range edges {
		wireableProps(edges[i].Props)
	}
}

func (n *Node) handleBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Nodes       []NodeUpsert `json:"nodes"`
		Edges       []EdgeUpsert `json:"edges"`
		EdgeDeletes []EdgeDelete `json:"edgeDeletes"`
		NodeDeletes []string     `json:"nodeDeletes"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	wireableBatchProps(req.Nodes, req.Edges)
	res, err := n.Apply(r.Context(), Batch{
		Version:     batchVersion,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
		EdgeDeletes: req.EdgeDeletes,
		NodeDeletes: req.NodeDeletes,
	})
	if err != nil {
		n.writeApplyErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nodesApplied": res.NodesApplied,
		"edgesApplied": res.EdgesApplied,
		"edgesSkipped": res.EdgesSkipped,
		"edgesDeleted": res.EdgesDeleted,
		"nodesDeleted": res.NodesDeleted,
		"nodesSkipped": res.NodesSkipped,
	})
}

// maxReadSet caps the per-request SER read set: bounded validation work per
// raft apply, and bounded request body size.
const maxReadSet = 100_000

func (n *Node) handleTxn(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Nodes       []NodeUpsert `json:"nodes"`
		Edges       []EdgeUpsert `json:"edges"`
		EdgeDeletes []EdgeDelete `json:"edgeDeletes"`
		NodeDeletes []string     `json:"nodeDeletes"`
		Txn         *TxnGuard    `json:"txn"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	if req.Txn == nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "txn field required (use /v1/batch for auto-commit)")
		return
	}
	if len(req.Txn.ReadNodes) > maxReadSet {
		writeErr(w, http.StatusBadRequest, "TXN_READSET_TOO_LARGE",
			fmt.Sprintf("readNodes exceeds %d keys", maxReadSet))
		return
	}
	if len(req.Txn.ReadEdges) > maxReadSet {
		writeErr(w, http.StatusBadRequest, "TXN_READSET_TOO_LARGE",
			fmt.Sprintf("readEdges exceeds %d keys", maxReadSet))
		return
	}
	wireableBatchProps(req.Nodes, req.Edges)
	res, err := n.Apply(r.Context(), Batch{
		Version:     batchVersion,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
		EdgeDeletes: req.EdgeDeletes,
		NodeDeletes: req.NodeDeletes,
		Txn:         req.Txn,
	})
	if err != nil {
		n.writeApplyErr(w, err)
		return
	}
	if !res.Committed {
		writeJSON(w, http.StatusConflict, map[string]any{
			"committed":     false,
			"conflicts":     res.Conflicts,
			"conflictTotal": res.ConflictTotal,
			"hint":          "refresh snapshot (X-Vectra-Snapshot) and retry with backoff",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"committed":    true,
		"index":        res.Index,
		"nodesApplied": res.NodesApplied,
		"edgesApplied": res.EdgesApplied,
		"edgesSkipped": res.EdgesSkipped,
		"edgesDeleted": res.EdgesDeleted,
		"nodesDeleted": res.NodesDeleted,
		"nodesSkipped": res.NodesSkipped,
	})
}

func (n *Node) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID   string `json:"nodeID"`
		RaftAddr string `json:"raftAddr"`
		HTTPAddr string `json:"httpAddr"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil ||
		req.NodeID == "" || req.RaftAddr == "" || req.HTTPAddr == "" {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "nodeID/raftAddr/httpAddr required")
		return
	}
	if !n.IsLeader() {
		if hint := n.LeaderHint(); hint != "" {
			w.Header().Set("X-Vectra-Leader", hint)
		}
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "NOT_LEADER", "send join to the leader")
		return
	}
	fut := n.r.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 15*time.Second)
	if err := fut.Error(); err != nil {
		// Duplicate join (node already in the configuration) is a normal ops action, idempotent success
		if n.alreadyMember(req.NodeID, req.RaftAddr) {
			n.mergeRegistry(map[string]string{req.RaftAddr: req.HTTPAddr})
			writeJSON(w, http.StatusOK, map[string]any{"registry": maps.Clone(safeRegistry(n))})
			return
		}
		writeErr(w, http.StatusConflict, "VALIDATION", "AddVoter: "+err.Error())
		return
	}
	n.mergeRegistry(map[string]string{req.RaftAddr: req.HTTPAddr})
	writeJSON(w, http.StatusOK, map[string]any{"registry": safeRegistry(n)})
}

func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	select {
	case <-n.fsm.Ready():
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "ok",
			"isLeader": n.IsLeader(),
			"term":     n.term(),
		})
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "starting"})
	}
}

func (n *Node) term() uint64 {
	if n.r == nil {
		return 0
	}
	v, _ := strconv.ParseUint(n.r.Stats()["term"], 10, 64)
	return v
}

func (n *Node) handleStats(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	st := g.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes":        st.Nodes,
		"edges":        st.Edges,
		"sccs":         st.SCCs,
		"cyclicNodes":  st.CyclicNodes,
		"isLeader":     n.IsLeader(),
		"term":         n.term(),
		"publishedAt":  n.fsm.lastSealNanos.Load() / int64(time.Millisecond),
		"appliedIndex": n.fsm.appliedIndex.Load(),
		"publishedSeq": n.fsm.PublishedSeq(),
	})
}

// awaitTimeoutBounds cap the server-side long poll and set the default when
// timeoutMs is omitted.
const (
	maxAwaitTimeout  = 30 * time.Second
	defaultAwaitWait = 5 * time.Second
)

// handleAwait is the read-your-writes long poll: block until publishedSeq
// >= seq (or the timeout / client disconnect), then return the current seq.
// Always 200 with reached reporting the outcome — an unsatisfied timeout is
// a normal "not yet published" answer, and the client decides to re-await.
// No readiness gate: waiting for the first publish is legitimate (Restore's
// first publish carries a seq and wakes waiters). Not counted as a query.
func (n *Node) handleAwait(w http.ResponseWriter, r *http.Request) {
	seq, err := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "seq query parameter required (uint)")
		return
	}
	wait := defaultAwaitWait
	if s := r.URL.Query().Get("timeoutMs"); s != "" {
		ms, err := strconv.Atoi(s)
		if err != nil || ms < 0 {
			writeErr(w, http.StatusBadRequest, "VALIDATION", "timeoutMs must be a non-negative integer")
			return
		}
		wait = min(time.Duration(ms)*time.Millisecond, maxAwaitTimeout)
	}
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	got := n.fsm.WaitPublished(ctx, seq)
	w.Header().Set("X-Vectra-Snapshot", strconv.FormatUint(got, 10))
	writeJSON(w, http.StatusOK, map[string]any{"publishedSeq": got, "reached": got >= seq})
}

func (n *Node) handleNode(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	node, ok := g.NodeByKey(r.PathValue("key"))
	if !ok {
		writeErr(w, http.StatusNotFound, "NODE_NOT_FOUND", "node not found")
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (n *Node) handleNeighbors(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	key := r.URL.Query().Get("key")
	dir, err := parseDirection(r.URL.Query().Get("dir"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", err.Error())
		return
	}
	label := r.URL.Query().Get("label")
	var labels []string
	if label != "" {
		labels = []string{label}
	}
	ns, err := g.NeighborsOf(key, dir, labels...)
	if err != nil {
		writeErr(w, http.StatusNotFound, "NODE_NOT_FOUND", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": ns})
}

func (n *Node) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	if len(req.From) == 0 {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "missing from")
		return
	}
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	// API semantics: maxDepth omitted/0 = unlimited hops
	dir, err := parseDirection(req.Direction)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", err.Error())
		return
	}
	opts := []vectra.BFSOption{vectra.WithDirection(dir)}
	if req.MaxDepth > 0 {
		opts = append(opts, vectra.WithMaxDepth(req.MaxDepth))
	}
	if len(req.Labels) > 0 {
		opts = append(opts, vectra.WithLabels(req.Labels...))
	}
	starts := make([]vectra.NodeID, 0, len(req.From))
	for _, k := range req.From {
		node, ok := g.NodeByKey(k)
		if !ok {
			writeErr(w, http.StatusBadRequest, "NODE_NOT_FOUND", "start node not found: "+k)
			return
		}
		starts = append(starts, node.ID)
	}
	res, err := g.BFS(starts, opts...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	type nodeDepth struct {
		Key   string `json:"key"`
		Depth uint32 `json:"depth"`
	}
	out := make([]nodeDepth, 0, res.Count())
	for _, id := range res.Nodes() {
		node, _ := g.NodeByID(id)
		out = append(out, nodeDepth{Key: node.Key, Depth: res.Depth(id)})
	}
	layers := make([][]string, len(res.Layers))
	for i, l := range res.Layers {
		layers[i] = make([]string, len(l))
		for j, id := range l {
			node, _ := g.NodeByID(id)
			layers[i][j] = node.Key
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": res.Count(), "nodes": out, "layers": layers})
}

// cypherRequest is the request body of POST /v1/cypher.
type cypherRequest struct {
	Query string `json:"query"`
}

// handleCypher runs one Cypher-subset statement against the published
// snapshot. Parse/semantic errors are client errors (400 PARSE); execution
// failures are 500.
func (n *Node) handleCypher(w http.ResponseWriter, r *http.Request) {
	var req cypherRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	if req.Query == "" {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "missing query")
		return
	}
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	res, err := g.Cypher(req.Query)
	if err != nil {
		var ce *vectra.CypherError
		if errors.As(err, &ce) {
			writeErr(w, http.StatusBadRequest, "PARSE", err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"columns": res.Columns, "rows": res.Rows, "count": len(res.Rows)})
}

func (n *Node) handleReachable(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	from := r.URL.Query().Get("from")
	reach, err := g.Reachable(from)
	if err != nil {
		writeErr(w, http.StatusNotFound, "NODE_NOT_FOUND", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": reach})
}

func (n *Node) handlePath(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	p, found, err := g.ShortestPath(r.URL.Query().Get("from"), r.URL.Query().Get("to"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "NODE_NOT_FOUND", err.Error())
		return
	}
	if !found {
		writeJSON(w, http.StatusOK, map[string]any{"found": false, "hops": 0})
		return
	}
	keys := make([]string, len(p.Nodes))
	for i, id := range p.Nodes {
		node, _ := g.NodeByID(id)
		keys[i] = node.Key
	}
	writeJSON(w, http.StatusOK, map[string]any{"found": true, "hops": p.Len(), "nodes": keys})
}

func (n *Node) handleCycles(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	cycles := g.Cycles()
	type cycleOut struct {
		Nodes []string `json:"nodes"`
	}
	out := make([]cycleOut, len(cycles))
	for i, c := range cycles {
		out[i] = cycleOut{Nodes: make([]string, len(c.Nodes))}
		for j, id := range c.Nodes {
			node, _ := g.NodeByID(id)
			out[i].Nodes[j] = node.Key
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"cycles": out})
}

// ---- vector endpoints (spec 2026-10-21 §3.3) ---------------------------------
//
// Reads (search, list) serve from the published clone like every other
// read: eventually consistent on followers, no special handling. Writes
// (create, drop) ride a raft Batch{VectorIndexOps} (leader-only).

// k bounds for POST /v1/vector/search, copied from the MCP vector_search
// tool's semantics: k <= 0 selects the default 10, k above the cap clamps
// silently — never a client error. The cap is 1000 (raised from 100 after
// an industry comparison: FalkorDB/Weaviate impose no hard cap and Qdrant's
// is configurable, so silently truncating k>100 requests was a footgun).
const (
	defaultVectorK = 10
	maxVectorK     = 1000
)

// vectorSearchRequest is the request body of POST /v1/vector/search: exactly
// one of query_vector / similar_to (the MCP tool's mutual-exclusion rule).
type vectorSearchRequest struct {
	Index     string    `json:"index"`
	Vector    []float32 `json:"query_vector,omitempty"`
	SimilarTo string    `json:"similar_to,omitempty"`
	K         int       `json:"k,omitempty"`
	Ef        int       `json:"ef,omitempty"` // 0 = adaptive default
	Label     string    `json:"label,omitempty"`
}

// vectorHitOut is one search result (smaller score is better, as declared).
type vectorHitOut struct {
	Key   string  `json:"key"`
	Score float64 `json:"score"`
}

// handleVectorSearch serves approximate top-k search over a declared vector
// index. Error faces: unknown index → 404 (the handleNode resource-not-found
// precedent), dim mismatch / request-shape violations → 400, similar_to on a
// missing node → 400 NODE_NOT_FOUND (the handleQuery body-reference
// precedent) and on a node without a valid vector → 400.
func (n *Node) handleVectorSearch(w http.ResponseWriter, r *http.Request) {
	var req vectorSearchRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	if req.Index == "" {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "missing index")
		return
	}
	if (len(req.Vector) == 0) == (req.SimilarTo == "") {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "provide exactly one of query_vector or similar_to")
		return
	}
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	k := req.K
	if k <= 0 {
		k = defaultVectorK
	}
	if k > maxVectorK {
		k = maxVectorK
	}
	// Resolve the declaration first so the unknown-index face (404) wins
	// over similar_to / dim errors in either query mode.
	dim, found := 0, false
	for _, d := range g.VectorIndexes() {
		if d.Name == req.Index {
			dim, found = d.Dim, true
			break
		}
	}
	if !found {
		writeErr(w, http.StatusNotFound, "VECTOR_INDEX_NOT_FOUND", "vector index not found")
		return
	}
	var q []float32
	if req.SimilarTo != "" {
		if _, ok := g.NodeByKey(req.SimilarTo); !ok {
			writeErr(w, http.StatusBadRequest, "NODE_NOT_FOUND", "similar_to node not found: "+req.SimilarTo)
			return
		}
		fv, ok := g.VectorForNode(req.Index, req.SimilarTo)
		if !ok {
			writeErr(w, http.StatusBadRequest, "VALIDATION",
				fmt.Sprintf("node %q has no valid vector for index %q", req.SimilarTo, req.Index))
			return
		}
		q = fv
	} else {
		if len(req.Vector) != dim {
			writeErr(w, http.StatusBadRequest, "VALIDATION",
				fmt.Sprintf("query_vector dim %d does not match index %q dim %d", len(req.Vector), req.Index, dim))
			return
		}
		q = req.Vector
	}
	hits, err := g.QueryVector(req.Index, q, vectra.QueryVectorOptions{K: k, Ef: req.Ef, Label: req.Label})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	out := struct {
		Count int            `json:"count"`
		Hits  []vectorHitOut `json:"hits"`
	}{Count: len(hits), Hits: make([]vectorHitOut, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, vectorHitOut{Key: h.Key, Score: h.Score})
	}
	writeJSON(w, http.StatusOK, out)
}

// vectorIndexRequest is the request body of POST /v1/vector/indexes (a
// declaration; zero M/EfConstruction take the engine defaults 16/200).
type vectorIndexRequest struct {
	Name           string `json:"name"`
	Prop           string `json:"prop"`
	Dim            int    `json:"dim"`
	Metric         string `json:"metric"`
	M              int    `json:"m,omitempty"`
	EfConstruction int    `json:"efConstruction,omitempty"`
}

// vectorIndexOut is the JSON shape of a declaration (GET list, POST create).
type vectorIndexOut struct {
	Name           string `json:"name"`
	Prop           string `json:"prop"`
	Dim            int    `json:"dim"`
	Metric         string `json:"metric"`
	M              int    `json:"m"`
	EfConstruction int    `json:"efConstruction"`
}

func vecIndexOut(d vectra.VectorIndexDecl) vectorIndexOut {
	return vectorIndexOut{
		Name: d.Name, Prop: d.Prop, Dim: d.Dim, Metric: d.Metric,
		M: d.M, EfConstruction: d.EfConstruction,
	}
}

// handleVectorIndexCreate declares a vector index through raft as
// Batch{VectorIndexOps:[{Create}]} (leader-only; the shared apply error
// faces apply). Validation mirrors the engine's declaration rules so bad
// requests fail fast as 400 instead of a committed no-op entry surfacing
// as 500; the engine's duplicate-name error replays deterministically on
// every replica and maps to 409.
func (n *Node) handleVectorIndexCreate(w http.ResponseWriter, r *http.Request) {
	var req vectorIndexRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "bad request body: "+err.Error())
		return
	}
	if req.Name == "" || req.Prop == "" {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "vector index name and prop are required")
		return
	}
	if req.Dim < 1 || req.Dim > 4096 {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "vector index dim must be 1..4096")
		return
	}
	switch req.Metric {
	case "", "cosine", "l2", "dot": // "" = cosine, the engine default
	default:
		writeErr(w, http.StatusBadRequest, "VALIDATION", "vector index metric must be cosine, l2, or dot")
		return
	}
	decl := vectra.VectorIndexDecl{
		Name: req.Name, Prop: req.Prop, Dim: req.Dim, Metric: req.Metric,
		M: req.M, EfConstruction: req.EfConstruction,
	}
	if _, err := n.Apply(r.Context(), Batch{
		Version:        batchVersion,
		VectorIndexOps: []VectorIndexOp{{Create: &decl}},
	}); err != nil {
		if errors.Is(err, vectra.ErrVectorIndexExists) {
			writeErr(w, http.StatusConflict, "VECTOR_INDEX_EXISTS", "vector index name already declared")
			return
		}
		n.writeApplyErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"index": vecIndexOut(decl)})
}

// handleVectorIndexList serves the declared indexes from the published clone.
func (n *Node) handleVectorIndexList(w http.ResponseWriter, r *http.Request) {
	g := n.graph(w)
	if g == nil {
		return
	}
	n.queryCount.Add(1)
	decls := g.VectorIndexes()
	out := make([]vectorIndexOut, len(decls))
	for i, d := range decls {
		out[i] = vecIndexOut(d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"indexes": out})
}

// handleVectorIndexDelete drops a declared index through raft as
// Batch{VectorIndexOps:[{Drop}]} (leader-only). The engine's not-found error
// replays deterministically on every replica and maps to 404.
func (n *Node) handleVectorIndexDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := n.Apply(r.Context(), Batch{
		Version:        batchVersion,
		VectorIndexOps: []VectorIndexOp{{Drop: name}},
	}); err != nil {
		if errors.Is(err, vectra.ErrVectorIndexNotFound) {
			writeErr(w, http.StatusNotFound, "VECTOR_INDEX_NOT_FOUND", "vector index not found")
			return
		}
		n.writeApplyErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dropped": name})
}

// alreadyMember checks whether nodeID is already in the cluster configuration (idempotent join check).
func (n *Node) alreadyMember(nodeID, raftAddr string) bool {
	cfgf := n.r.GetConfiguration()
	if cfgf.Error() != nil {
		return false
	}
	for _, s := range cfgf.Configuration().Servers {
		if string(s.ID) == nodeID || string(s.Address) == raftAddr {
			return true
		}
	}
	return false
}

func safeRegistry(n *Node) map[string]string {
	n.regMu.Lock()
	defer n.regMu.Unlock()
	return maps.Clone(n.registry)
}

// parseDirection maps the direction query parameter; anything but the
// exact "in" or "out" (or empty) is a client error — silently answering
// out-neighbors for a typo'd "IN"/"incoming" would be a wrong answer.
func parseDirection(s string) (vectra.Direction, error) {
	switch s {
	case "", "out":
		return vectra.DirOut, nil
	case "in":
		return vectra.DirIn, nil
	default:
		return 0, fmt.Errorf("invalid direction %q (want \"in\" or \"out\")", s)
	}
}
