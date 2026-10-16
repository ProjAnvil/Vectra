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

func httpPost(url string, body []byte, timeout time.Duration) (*http.Response, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
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
	mux.Handle("/debug/vars", expvar.Handler())
	n.registerExpvar()

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
	res, err := n.Apply(r.Context(), Batch{
		Version:     batchVersion,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
		EdgeDeletes: req.EdgeDeletes,
		NodeDeletes: req.NodeDeletes,
	})
	if err != nil {
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
	res, err := n.Apply(r.Context(), Batch{
		Version:     batchVersion,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
		EdgeDeletes: req.EdgeDeletes,
		NodeDeletes: req.NodeDeletes,
		Txn:         req.Txn,
	})
	if err != nil {
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
