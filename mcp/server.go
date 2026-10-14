// Package mcpserver uses the official Go SDK (modelcontextprotocol/go-sdk)
// to expose the Vectra engine as an MCP service: the engine is embedded
// directly (single binary, usable over stdio, no network), for MCP hosts
// like Claude Desktop / Claude Code.
//
// The tool surface follows database-MCP best practices: 10 small but
// complete tools, separated read/write annotations, and results capped
// by default (limit, to prevent context-window blowup).
package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
)

// Version is the implementation version advertised by the MCP service.
const Version = "0.5.0"

// defaultLimit is the default cap on nodes returned by bfs/reachable
// (results are capped; the total field still reflects the true size,
// a safe default practice for database MCPs).
const defaultLimit = 1000

// Options configures the MCP service.
type Options struct {
	// SnapshotPath, when non-empty: loaded at startup, saved after each seal (single-file snapshot).
	SnapshotPath string
}

// service holds the embedded engine state. Writes accumulate in the live
// graph; after seal, queries go through the sealed view. MCP sessions may
// call tools concurrently: write tools are mutually exclusive (mu), queries
// are lock-free — the view pointer is atomic so seal's publish never races
// concurrent readers. ONE service backs every session of a server (stdio:
// the single session; HTTP: all connections share it — see Handler).
type service struct {
	mu   sync.Mutex
	live *vectra.Graph
	view atomic.Pointer[vectra.Graph] // sealed view (nil = not sealed)

	propKeys map[string]bool // property keys seen during injection, auto-indexed at seal
	opts     Options
}

// newService builds the embedded state once: live graph, snapshot view,
// auto-index seeds. Snapshot mode additionally lets writes continue from the
// snapshot's content when the embedded graph is empty and still writable, so
// post-restart injections extend the persisted graph instead of replacing it.
func newService(g *vectra.Graph, opts Options) *service {
	svc := &service{live: g, propKeys: map[string]bool{}, opts: opts}
	if opts.SnapshotPath != "" {
		if loaded, err := vectra.LoadFile(opts.SnapshotPath); err == nil {
			svc.view.Store(loaded) // Load seals automatically
			for _, k := range loaded.IndexedPropertyKeys() {
				svc.propKeys[k] = true // snapshot's indexes survive the next re-seal
			}
			if g != nil && !g.Sealed() && g.NodeCount() == 0 {
				svc.live = loaded.Clone() // writes continue from the snapshot content
			}
		}
	}
	return svc
}

// newServer wires a protocol-level server for one session; state lives in
// the (shared) service, so this stays cheap enough for per-session creation.
func newServer(svc *service) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: "github.com/projanvil/vectra", Version: Version},
		&mcp.ServerOptions{
			Instructions: "Vectra in-memory graph engine (dependency graphs). Write with inject_batch, " +
				"then call seal before querying; query tools: neighbors (one hop), " +
				"bfs (multi-hop), shortest_path, reachable (impact set), cycles (dependency cycles), " +
				"cypher (Cypher-subset pattern queries), stats, get_node, lookup_property.",
		},
	)
	registerTools(s, svc)
	return s
}

// New creates a stdio-mode MCP server (embedding the given graph; loads it if
// SnapshotPath is set).
func New(g *vectra.Graph, opts Options) *mcp.Server {
	return newServer(newService(g, opts))
}

// Handler returns an http.Handler for the Streamable HTTP transport (mounted
// at /mcp). One shared service backs every session: all connections
// serialize writes through the same mutex and watch the same sealed view
// (per-session services would share the live graph without sharing the lock
// — a data race — and would never observe each other's seals). The SDK asks
// for a server per session (and once more per request for protocol-version
// negotiation), so the callback only wires a lightweight mcp.Server — no
// snapshot reload, no state.
func Handler(g *vectra.Graph, opts Options) *mcp.StreamableHTTPHandler {
	svc := newService(g, opts)
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return newServer(svc)
	}, nil)
}

// ---- input/output types (JSON Schema auto-generated) ----

type nodeIn struct {
	Key   string         `json:"key" jsonschema:"unique node key"`
	Label string         `json:"label,omitempty"`
	Props map[string]any `json:"props,omitempty"`
}

type edgeIn struct {
	SrcKey string         `json:"srcKey" jsonschema:"source node key"`
	DstKey string         `json:"dstKey" jsonschema:"destination node key"`
	Label  string         `json:"label,omitempty"`
	Props  map[string]any `json:"props,omitempty"`
	// AllowParallel appends a true parallel edge instead of collapsing onto
	// the existing (srcKey,dstKey,label) upsert key (default false).
	AllowParallel bool `json:"allowParallel,omitempty"`
}

type edgeDeleteIn struct {
	SrcKey string `json:"srcKey" jsonschema:"source node key"`
	DstKey string `json:"dstKey" jsonschema:"destination node key"`
	Label  string `json:"label,omitempty"`
}

type injectIn struct {
	Nodes       []nodeIn       `json:"nodes,omitempty"`
	Edges       []edgeIn       `json:"edges,omitempty"`
	DeleteEdges []edgeDeleteIn `json:"deleteEdges,omitempty" jsonschema:"delete edges by (srcKey,dstKey,label)"`
	DeleteNodes []string       `json:"deleteNodes,omitempty" jsonschema:"delete nodes (cascades incident edges)"`
}

type countsOut struct {
	NodesApplied int `json:"nodesApplied"`
	EdgesApplied int `json:"edgesApplied"`
	EdgesSkipped int `json:"edgesSkipped"`
	EdgesDeleted int `json:"edgesDeleted"`
	NodesDeleted int `json:"nodesDeleted"`
}

type statsOut struct {
	Nodes       int  `json:"nodes"`
	Edges       int  `json:"edges"`
	SCCs        int  `json:"sccs"`
	CyclicNodes int  `json:"cyclicNodes"`
	Sealed      bool `json:"sealed"`
}

type nodeOut struct {
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

type neighborsIn struct {
	Key       string `json:"key"`
	Direction string `json:"direction,omitempty" jsonschema:"out (default) or in"`
	Label     string `json:"label,omitempty"`
}

type bfsIn struct {
	From      []string `json:"from" jsonschema:"one or more source node keys"`
	MaxDepth  uint32   `json:"maxDepth,omitempty" jsonschema:"omitted means unlimited hops"`
	Labels    []string `json:"labels,omitempty"`
	Direction string   `json:"direction,omitempty"`
	Limit     int      `json:"limit,omitempty" jsonschema:"max nodes returned, default 1000"`
}

type listOut struct {
	Count int      `json:"count" jsonschema:"number returned in this call"`
	Total int      `json:"total" jsonschema:"true total (may be truncated by limit)"`
	Nodes []string `json:"nodes"`
}

type depthOut struct {
	listOut
	Depths map[string]uint32 `json:"depths,omitempty" jsonschema:"key->hop count"`
}

type pathIn struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type pathOut struct {
	Found bool     `json:"found"`
	Hops  int      `json:"hops"`
	Nodes []string `json:"nodes,omitempty"`
}

type reachIn struct {
	From  string `json:"from"`
	Limit int    `json:"limit,omitempty"`
}

type cycleEdgeOut struct {
	Src   string `json:"src" jsonschema:"source node key"`
	Dst   string `json:"dst" jsonschema:"destination node key"`
	Label string `json:"label"`
}

type cyclesOut struct {
	Cycles [][]string       `json:"cycles" jsonschema:"member key list of each dependency cycle, sorted by size descending"`
	Edges  [][]cycleEdgeOut `json:"edges,omitempty" jsonschema:"per-cycle intra-cycle edges (src/dst/label), aligned with cycles by index"`
}

type lookupIn struct {
	PropKey   string `json:"propKey"`
	PropValue any    `json:"propValue" jsonschema:"string, number or boolean property value"`
	Limit     int    `json:"limit,omitempty"`
}

type cypherIn struct {
	Query string `json:"query" jsonschema:"Cypher-subset statement: MATCH ... [WHERE ...] RETURN ... [ORDER BY ...] [LIMIT n]"`
	Limit int    `json:"limit,omitempty" jsonschema:"max rows returned, default 1000"`
}

type cypherOut struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Count   int              `json:"count" jsonschema:"rows returned in this call"`
	Total   int              `json:"total" jsonschema:"true total (may be truncated by limit)"`
}

// ---- tool registration ----

func registerTools(s *mcp.Server, svc *service) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "inject_batch",
		Description: "Batch-write and delete nodes/edges (writes are upserts: same key updates props; " +
			"edge identity is (srcKey,dstKey,label) — set allowParallel=true on an edge to append a true " +
			"parallel edge instead of collapsing onto the existing one; deleteEdges removes by " +
			"(srcKey,dstKey,label), deleteNodes removes nodes and cascades their edges). " +
			"Call seal after writing, before querying.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false},
	}, svc.injectBatch)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "seal",
		Description: "Seal the graph: builds query indexes and makes the graph read-only and queryable (re-inject to write again). Returns graph stats.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false},
	}, svc.seal)

	mcp.AddTool(s, &mcp.Tool{
		Name: "stats", Description: "Graph stats: nodes/edges/strongly connected components/nodes on cycles.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.stats)

	mcp.AddTool(s, &mcp.Tool{
		Name: "get_node", Description: "Get a node by key (label and props).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.getNode)

	mcp.AddTool(s, &mcp.Tool{
		Name: "neighbors", Description: "One-hop neighbors. Params: key, direction (\"out\" default | \"in\"), label (optional edge-label filter).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.neighbors)

	mcp.AddTool(s, &mcp.Tool{
		Name: "bfs",
		Description: "Multi-hop traversal. Params: from (one or more start keys), maxDepth (0=unlimited), " +
			"labels (optional edge-label filter), direction (\"out\" | \"in\"), limit (default 1000). " +
			"Returns per-node hop counts; total is the true total.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.bfs)

	mcp.AddTool(s, &mcp.Tool{
		Name: "shortest_path", Description: "Shortest path between two nodes (bidirectional BFS). Params: from, to.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.shortestPath)

	mcp.AddTool(s, &mcp.Tool{
		Name: "reachable", Description: "All nodes reachable from a source (dependency impact set / blast radius). Params: from, limit (default 1000).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.reachable)

	mcp.AddTool(s, &mcp.Tool{
		Name: "cycles", Description: "All dependency cycles (strongly connected component member lists, sorted by size descending). The edges field lists each cycle's intra-cycle edges (src/dst/label) for severity triage. No params.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.cycles)

	mcp.AddTool(s, &mcp.Tool{
		Name: "lookup_property", Description: "Look up nodes by property value, typed equality (string/number/bool — label-like props such as env/team/tier, numeric ids, flags). Params: propKey, propValue, limit (default 1000).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.lookupProperty)

	mcp.AddTool(s, &mcp.Tool{
		Name: "cypher",
		Description: "Query the graph with a Cypher-subset statement: MATCH (a:Label {prop:value})-[e:REL*1..3]->(b) " +
			"WHERE b.w > 3 RETURN a.key, b ORDER BY b.w DESC LIMIT 10. Node-pair semantics for *hops " +
			"(shortest distance within the hop window); pseudo-props a.key/a.label/a.id; edge vars bind on " +
			"fixed single hops only. Limit defaults to 1000 (total reports the true size).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.cypher)
}

// ---- tool implementations ----

func (svc *service) injectBatch(_ context.Context, _ *mcp.CallToolRequest, in injectIn) (*mcp.CallToolResult, countsOut, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	// Write-after-seal re-open: the last seal published live as an immutable
	// view; the first new write lazily clones that view back into a writable
	// graph (O(V+E), carries index declarations). Queries never notice — they
	// keep reading the published view until the next seal, the same
	// bounded-stale read model as the server module.
	if (len(in.Nodes) > 0 || len(in.Edges) > 0 || len(in.DeleteEdges) > 0 || len(in.DeleteNodes) > 0) && svc.live.Sealed() {
		live := vectra.New()
		if v := svc.view.Load(); v != nil {
			live = v.Clone()
		}
		svc.live = live
	}
	out := countsOut{}
	for _, n := range in.Nodes {
		if _, err := svc.live.AddNode(n.Key, n.Label, n.Props); err == nil {
			out.NodesApplied++
		}
		for k := range n.Props {
			svc.propKeys[k] = true
		}
	}
	for _, e := range in.Edges {
		var err error
		if e.AllowParallel {
			_, err = svc.live.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
		} else {
			_, err = svc.live.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
		}
		if err == nil {
			out.EdgesApplied++
		} else {
			out.EdgesSkipped++
		}
		for k := range e.Props {
			svc.propKeys[k] = true
		}
	}
	for _, d := range in.DeleteEdges {
		if n, err := svc.live.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err == nil {
			out.EdgesDeleted += n
		}
	}
	for _, k := range in.DeleteNodes {
		if n, err := svc.live.DeleteNode(k); err == nil {
			out.NodesDeleted++
			out.EdgesDeleted += n
		}
	}
	return nil, out, nil
}

func (svc *service) seal(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, statsOut, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	for k := range svc.propKeys {
		svc.live.IndexProperty(k) // auto-index property keys seen during injection
	}
	if err := svc.live.Seal(); err != nil {
		return nil, statsOut{}, err
	}
	svc.view.Store(svc.live)
	if svc.opts.SnapshotPath != "" {
		if err := svc.live.SaveFile(svc.opts.SnapshotPath); err != nil {
			return nil, statsOut{}, fmt.Errorf("snapshot save failed: %w", err)
		}
	}
	st := svc.live.Stats()
	return nil, statsOut{
		Nodes: st.Nodes, Edges: st.Edges, SCCs: st.SCCs,
		CyclicNodes: st.CyclicNodes, Sealed: true,
	}, nil
}

// view returns the sealed view; gives an actionable error when not sealed.
// Lock-free: the atomic pointer makes the seal-time publish visible to
// concurrent query tools without taking the write mutex.
func (svc *service) viewOrSealHint() (*vectra.Graph, error) {
	if v := svc.view.Load(); v != nil {
		return v, nil
	}
	return nil, fmt.Errorf("graph is not sealed: call inject_batch to write, then seal before querying")
}

func (svc *service) stats(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, statsOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, statsOut{}, err
	}
	st := v.Stats()
	return nil, statsOut{
		Nodes: st.Nodes, Edges: st.Edges, SCCs: st.SCCs,
		CyclicNodes: st.CyclicNodes, Sealed: true,
	}, nil
}

func (svc *service) getNode(_ context.Context, _ *mcp.CallToolRequest, in struct {
	Key string `json:"key"`
}) (*mcp.CallToolResult, nodeOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, nodeOut{}, err
	}
	n, ok := v.NodeByKey(in.Key)
	if !ok {
		return nil, nodeOut{}, fmt.Errorf("node %q does not exist", in.Key)
	}
	return nil, nodeOut{Key: n.Key, Label: n.Label, Props: n.Props}, nil
}

func parseDir(s string) vectra.Direction {
	if s == "in" {
		return vectra.DirIn
	}
	return vectra.DirOut
}

func (svc *service) neighbors(_ context.Context, _ *mcp.CallToolRequest, in neighborsIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, listOut{}, err
	}
	var labels []string
	if in.Label != "" {
		labels = []string{in.Label}
	}
	ns, err := v.NeighborsOf(in.Key, parseDir(in.Direction), labels...)
	if err != nil {
		return nil, listOut{}, err
	}
	out := listOut{Total: len(ns), Nodes: make([]string, 0, len(ns))}
	for _, n := range ns {
		out.Nodes = append(out.Nodes, n.Key)
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) bfs(_ context.Context, _ *mcp.CallToolRequest, in bfsIn) (*mcp.CallToolResult, depthOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, depthOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	starts := make([]vectra.NodeID, 0, len(in.From))
	for _, k := range in.From {
		n, ok := v.NodeByKey(k)
		if !ok {
			return nil, depthOut{}, fmt.Errorf("source node %q does not exist", k)
		}
		starts = append(starts, n.ID)
	}
	opts := []vectra.BFSOption{vectra.WithDirection(parseDir(in.Direction))}
	if in.MaxDepth > 0 {
		opts = append(opts, vectra.WithMaxDepth(in.MaxDepth))
	}
	if len(in.Labels) > 0 {
		opts = append(opts, vectra.WithLabels(in.Labels...))
	}
	res, err := v.BFS(starts, opts...)
	if err != nil {
		return nil, depthOut{}, err
	}
	all := res.Nodes()
	out := depthOut{}
	out.Total = len(all)
	out.Depths = map[string]uint32{}
	for _, id := range all {
		n, _ := v.NodeByID(id)
		if len(out.Nodes) < limit {
			out.Nodes = append(out.Nodes, n.Key)
			out.Depths[n.Key] = res.Depth(id)
		}
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) shortestPath(_ context.Context, _ *mcp.CallToolRequest, in pathIn) (*mcp.CallToolResult, pathOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, pathOut{}, err
	}
	p, found, err := v.ShortestPath(in.From, in.To)
	if err != nil {
		return nil, pathOut{}, err
	}
	if !found {
		return nil, pathOut{Found: false, Hops: 0}, nil
	}
	out := pathOut{Found: true, Hops: p.Len(), Nodes: make([]string, len(p.Nodes))}
	for i, id := range p.Nodes {
		n, _ := v.NodeByID(id)
		out.Nodes[i] = n.Key
	}
	return nil, out, nil
}

func (svc *service) reachable(_ context.Context, _ *mcp.CallToolRequest, in reachIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, listOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	keys, err := v.Reachable(in.From)
	if err != nil {
		return nil, listOut{}, err
	}
	out := listOut{Total: len(keys)}
	for i, k := range keys {
		if i >= limit {
			break
		}
		out.Nodes = append(out.Nodes, k)
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) cycles(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, cyclesOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, cyclesOut{}, err
	}
	cs := v.Cycles()
	out := cyclesOut{Cycles: make([][]string, 0, len(cs)), Edges: make([][]cycleEdgeOut, 0, len(cs))}
	for _, c := range cs {
		mem := make([]string, len(c.Nodes))
		for i, id := range c.Nodes {
			n, _ := v.NodeByID(id)
			mem[i] = n.Key
		}
		out.Cycles = append(out.Cycles, mem)
		edges := make([]cycleEdgeOut, 0, len(c.Edges))
		for _, e := range c.Edges {
			src, _ := v.NodeByID(e.Src)
			dst, _ := v.NodeByID(e.Dst)
			edges = append(edges, cycleEdgeOut{Src: src.Key, Dst: dst.Key, Label: e.Label})
		}
		out.Edges = append(out.Edges, edges)
	}
	return nil, out, nil
}

func (svc *service) lookupProperty(_ context.Context, _ *mcp.CallToolRequest, in lookupIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, listOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	var nodes []vectra.Node
	switch tv := in.PropValue.(type) {
	case string:
		ns, ok := v.LookupByProperty(in.PropKey, tv)
		if !ok {
			return nil, listOut{}, fmt.Errorf("property %q is not indexed (re-seal after it appears in injected data)", in.PropKey)
		}
		nodes = ns
	case float64:
		ns, ok := v.LookupByNumber(in.PropKey, tv)
		if !ok {
			return nil, listOut{}, fmt.Errorf("property %q is not indexed (re-seal after it appears in injected data)", in.PropKey)
		}
		nodes = ns
	case bool:
		ns, ok := v.LookupByBool(in.PropKey, tv)
		if !ok {
			return nil, listOut{}, fmt.Errorf("property %q is not indexed (re-seal after it appears in injected data)", in.PropKey)
		}
		nodes = ns
	default:
		return nil, listOut{}, fmt.Errorf("propValue must be a string, number or boolean, got %T", in.PropValue)
	}
	out := listOut{Total: len(nodes)}
	for i, n := range nodes {
		if i >= limit {
			break
		}
		out.Nodes = append(out.Nodes, n.Key)
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) cypher(_ context.Context, _ *mcp.CallToolRequest, in cypherIn) (*mcp.CallToolResult, cypherOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, cypherOut{}, err
	}
	res, err := v.Cypher(in.Query)
	if err != nil {
		return nil, cypherOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	out := cypherOut{Columns: res.Columns, Total: len(res.Rows)}
	for i, r := range res.Rows {
		if i >= limit {
			break
		}
		out.Rows = append(out.Rows, r)
	}
	out.Count = len(out.Rows)
	return nil, out, nil
}
