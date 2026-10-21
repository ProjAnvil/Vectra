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
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
)

// Version is the implementation version advertised by the MCP service.
// It is a var so release builds can inject the exact tag (the consensus
// hybrid: -ldflags for the semver, source default as fallback for plain
// `go build`/`go install`, which cannot carry flags):
//
//	go build -ldflags "-X github.com/projanvil/vectra/mcp.Version=v0.11.0" ./cmd/vectra-mcp
//
// The Makefile build target injects `git describe --tags --always --dirty`.
// The source default still gets bumped at release (chore(release) commits),
// so unbuilt checkouts and `go install` from a tag report a truthful value.
var Version = "0.11.0"

// defaultLimit is the default cap on nodes returned by bfs/reachable
// (results are capped; the total field still reflects the true size,
// a safe default practice for database MCPs).
const defaultLimit = 1000

// vectorDefaultK / vectorMaxK bound the vector_search tool's k: k <= 0
// selects the default, k above the cap clamps silently (never an error).
// The cap is 1000 (raised from 100 after an industry comparison:
// FalkorDB/Weaviate impose no hard cap and Qdrant's is configurable, so
// silently truncating k>100 requests was a footgun).
const (
	vectorDefaultK = 10
	vectorMaxK     = 1000
)

// Options configures the MCP service.
type Options struct {
	// SnapshotPath, when non-empty: loaded at startup, saved after each seal (single-file snapshot).
	SnapshotPath string

	// WALDir, when non-empty, switches the service to a durable WAL
	// session: the graph is opened with vectra.Open (ModeAlways) using
	// WALDir as the WAL directory. The db/snapshot path is SnapshotPath
	// when set, else <WALDir>/graph.db. seal then checkpoints (merge +
	// watermark snapshot + manifest + WAL reset) and a restart on the same
	// directory recovers every write. WALDir takes precedence over
	// SnapshotPath (the snapshot path names the db file, not a load-and-
	// save target). Empty = the snapshot mode above (v0.6 behavior).
	WALDir string
}

// service holds the embedded engine state: ONE live graph carrying the
// current generation, written by inject_batch/seal under mu and read by
// every query tool lock-free (the engine's generation pointer is atomic —
// v0.7's dissolved gates: queries never require a seal). The live pointer
// itself is assigned only during construction, before the service is
// shared. ONE service backs every session of a server (stdio: the single
// session; HTTP: all connections share it — see Handler).
type service struct {
	mu   sync.Mutex
	live *vectra.Graph

	// openErr is set when a WALDir session failed to open: every tool
	// call then fails loudly instead of silently degrading to a volatile
	// in-memory graph that pretends to be durable.
	openErr error

	propKeys map[string]bool // property keys seen during injection, auto-indexed at seal
	opts     Options
}

// newService builds the embedded state once. WAL mode (Options.WALDir):
// vectra.Open recovers snapshot + WAL replay into a writable session. In
// snapshot mode the loaded graph IS the live graph — v0.7 LoadFile graphs
// are immediately writable (writes open a new generation), so the v0.6
// sealed-view + write-after-seal-clone dance is gone. A missing snapshot
// is a genuine first run (fresh graph; the first seal writes the file); a
// PRESENT but unreadable snapshot sets openErr — every tool then fails
// loudly instead of degrading to an empty graph whose first seal would
// overwrite the (only copy of the) unreadable snapshot.
func newService(g *vectra.Graph, opts Options) *service {
	svc := &service{propKeys: map[string]bool{}, opts: opts}
	switch {
	case opts.WALDir != "":
		dbPath := opts.SnapshotPath
		if dbPath == "" {
			dbPath = filepath.Join(opts.WALDir, "graph.db")
		}
		og, err := vectra.Open(dbPath, vectra.Options{
			WALMode: vectra.ModeAlways,
			WALDir:  opts.WALDir,
		})
		if err != nil {
			svc.openErr = fmt.Errorf("wal session at %s: %w", opts.WALDir, err)
			svc.live = vectra.New()
			return svc
		}
		svc.live = og
	case opts.SnapshotPath != "":
		loaded, err := vectra.LoadFile(opts.SnapshotPath)
		switch {
		case err == nil:
			svc.live = loaded
			for _, k := range loaded.IndexedPropertyKeys() {
				svc.propKeys[k] = true // snapshot's indexes survive the next seal
			}
		case errors.Is(err, fs.ErrNotExist):
			// first run: no snapshot yet — start fresh, first seal creates it
		default:
			svc.openErr = fmt.Errorf("snapshot load %s: %w", opts.SnapshotPath, err)
			svc.live = vectra.New()
			return svc
		}
	}
	if svc.live == nil {
		if g != nil {
			svc.live = g
		} else {
			svc.live = vectra.New()
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
			Instructions: "Vectra in-memory graph engine (dependency graphs). Write with inject_batch — " +
				"writes are immediately visible to every query tool (neighbors: one hop, bfs: multi-hop, " +
				"shortest_path, reachable: impact set, cycles: dependency cycles, cypher: Cypher-subset " +
				"pattern queries, stats, get_node, lookup_property, vector_search: approximate top-k over a vector index). Call seal periodically to merge " +
				"micro-layers and checkpoint (in WAL mode it is the durability boundary); it is " +
				"maintenance, never a query prerequisite.",
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
// serialize writes through the same mutex and read the same live graph
// (per-session services would share the live graph without sharing the lock
// — a data race — and would never observe each other's writes). The SDK asks
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

type vectorSearchIn struct {
	Index     string    `json:"index" jsonschema:"vector index name (declared via the Go API)"`
	Vector    []float64 `json:"query_vector,omitempty" jsonschema:"query embedding (mutually exclusive with similar_to)"`
	SimilarTo string    `json:"similar_to,omitempty" jsonschema:"node key whose indexed vector is the query (mutually exclusive with query_vector)"`
	K         int       `json:"k,omitempty" jsonschema:"neighbors to return, default 10, max 1000"`
}

type vectorHitOut struct {
	Key   string  `json:"key"`
	Score float64 `json:"score" jsonschema:"metric value (cosine/l2/dot as declared); smaller is better"`
}

type vectorSearchOut struct {
	Count int            `json:"count" jsonschema:"neighbors returned"`
	Hits  []vectorHitOut `json:"hits"`
	Note  string         `json:"note,omitempty" jsonschema:"approximation disclosure"`
}

// ---- tool registration ----

func registerTools(s *mcp.Server, svc *service) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "inject_batch",
		Description: "Batch-write and delete nodes/edges (writes are upserts: same key updates props; " +
			"edge identity is (srcKey,dstKey,label) — set allowParallel=true on an edge to append a true " +
			"parallel edge instead of collapsing onto the existing one; deleteEdges removes by " +
			"(srcKey,dstKey,label), deleteNodes removes nodes and cascades their edges). " +
			"Everything applied is immediately visible to query tools.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false},
	}, svc.injectBatch)

	mcp.AddTool(s, &mcp.Tool{
		Name: "seal",
		Description: "Merge the graph's unmerged micro-layers into the sealed base and checkpoint: " +
			"WAL mode writes the watermark snapshot + manifest (the durability boundary); snapshot mode " +
			"saves the snapshot file. Auto-indexes property keys seen during injection, and makes SCC " +
			"structures available to stats. Optional maintenance — queries work without it (the graph " +
			"stays writable afterwards; writes open a new generation). Returns graph stats.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false},
	}, svc.seal)

	mcp.AddTool(s, &mcp.Tool{
		Name: "stats",
		Description: "Graph stats: nodes/edges/strongly connected components/nodes on cycles. " +
			"sealed=true means no unmerged micro-layers (SCC fields are reported once sealed).",
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
		Name:        "cycles",
		Description: "All dependency cycles (strongly connected component member lists, sorted by size descending). The edges field lists each cycle's intra-cycle edges (src/dst/label) for severity triage. No params.",
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

	mcp.AddTool(s, &mcp.Tool{
		Name:        "vector_search",
		Description: "Approximate top-k nearest-neighbor search over a declared vector index. Results are approximate; order across equal scores is not stable. Params: index, exactly one of query_vector or similar_to, k (default 10, max 1000).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.vectorSearch)
}

// ---- tool implementations ----

func (svc *service) injectBatch(_ context.Context, _ *mcp.CallToolRequest, in injectIn) (*mcp.CallToolResult, countsOut, error) {
	if svc.openErr != nil {
		return nil, countsOut{}, svc.openErr
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	out := countsOut{}
	// One Batch boundary per tool call: the whole inject materializes as one
	// micro-layer and — in WAL mode — flushes as one frame (one fsync), not
	// one layer/frame per op (v0.7 per-op writes were an order of magnitude
	// slower). Within-batch visibility is preserved (endpoint validation and
	// upsert folding consult the builder mirrors), and the loops keep their
	// lenient per-op error semantics: a failed item is counted as skipped,
	// never fatal. The boundary's own flush error keeps the same lenient
	// surface — the ops already hit memory and stay buffered in pendOps for
	// the next successful flush (see Batch's direct-mode limitation note).
	_ = svc.live.Batch(func(g *vectra.Graph) {
		for _, n := range in.Nodes {
			if _, err := g.AddNode(n.Key, n.Label, n.Props); err == nil {
				out.NodesApplied++
			}
			for k := range n.Props {
				svc.propKeys[k] = true
			}
		}
		for _, e := range in.Edges {
			var err error
			if e.AllowParallel {
				_, err = g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
			} else {
				_, err = g.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
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
			if n, err := g.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err == nil {
				out.EdgesDeleted += n
			}
		}
		for _, k := range in.DeleteNodes {
			if n, err := g.DeleteNode(k); err == nil {
				out.NodesDeleted++
				out.EdgesDeleted += n
			}
		}
	})
	return nil, out, nil
}

func (svc *service) seal(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, statsOut, error) {
	if svc.openErr != nil {
		return nil, statsOut{}, svc.openErr
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	for k := range svc.propKeys {
		svc.live.IndexProperty(k) // auto-index property keys seen during injection
	}
	// Seal = merge + checkpoint: with a WAL attached the checkpoint (watermark
	// snapshot + manifest + WAL reset) is Seal's own pipeline; snapshot mode
	// persists via the single snapshot file.
	if err := svc.live.Seal(); err != nil {
		return nil, statsOut{}, err
	}
	if svc.opts.SnapshotPath != "" && svc.opts.WALDir == "" {
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

// view returns the live graph for a query tool. svc.live is assigned only
// during construction, so reading it needs no lock; the reads inside the
// Graph are lock-free generation loads (v0.7: sealed queries are gone).
// A failed WAL session makes every query fail loudly instead.
func (svc *service) view() (*vectra.Graph, error) {
	if svc.openErr != nil {
		return nil, svc.openErr
	}
	return svc.live, nil
}

func (svc *service) stats(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, statsOut, error) {
	v, err := svc.view()
	if err != nil {
		return nil, statsOut{}, err
	}
	st := v.Stats()
	return nil, statsOut{
		Nodes: st.Nodes, Edges: st.Edges, SCCs: st.SCCs,
		CyclicNodes: st.CyclicNodes, Sealed: v.Sealed(),
	}, nil
}

func (svc *service) getNode(_ context.Context, _ *mcp.CallToolRequest, in struct {
	Key string `json:"key"`
}) (*mcp.CallToolResult, nodeOut, error) {
	v, err := svc.view()
	if err != nil {
		return nil, nodeOut{}, err
	}
	n, ok := v.NodeByKey(in.Key)
	if !ok {
		return nil, nodeOut{}, fmt.Errorf("node %q does not exist", in.Key)
	}
	return nil, nodeOut{Key: n.Key, Label: n.Label, Props: n.Props}, nil
}

// parseDir maps the tool's direction argument; anything but the exact "in"
// or "out" (or empty) is an error — silently answering out-neighbors for a
// typo'd value would be a wrong answer handed to the agent.
// errPropNotIndexed is the single raise for lookup_property's not-indexed
// misses across the three typed lookups.
var errPropNotIndexed = errors.New("property is not indexed (re-seal after it appears in injected data)")

func notIndexed(key string) error {
	return fmt.Errorf("%w: %q", errPropNotIndexed, key)
}

// parseDir maps the tool's direction argument; anything but the exact "in"
// or "out" (or empty) is an error — silently answering out-neighbors for a
// typo'd value would be a wrong answer handed to the agent.
func parseDir(s string) (vectra.Direction, error) {
	switch s {
	case "", "out":
		return vectra.DirOut, nil
	case "in":
		return vectra.DirIn, nil
	default:
		return 0, fmt.Errorf("invalid direction %q (want \"in\" or \"out\")", s)
	}
}

func (svc *service) neighbors(_ context.Context, _ *mcp.CallToolRequest, in neighborsIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.view()
	if err != nil {
		return nil, listOut{}, err
	}
	var labels []string
	if in.Label != "" {
		labels = []string{in.Label}
	}
	dir, err := parseDir(in.Direction)
	if err != nil {
		return nil, listOut{}, err
	}
	ns, err := v.NeighborsOf(in.Key, dir, labels...)
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
	v, err := svc.view()
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
	dir, err := parseDir(in.Direction)
	if err != nil {
		return nil, depthOut{}, err
	}
	opts := []vectra.BFSOption{vectra.WithDirection(dir)}
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
	v, err := svc.view()
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
	v, err := svc.view()
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
	// Cycles is the query-family exception: with unmerged layers it folds
	// them first (engine-side force merge), which rewrites writer-private
	// state — so it serializes against inject/seal through the write mutex.
	if svc.openErr != nil {
		return nil, cyclesOut{}, svc.openErr
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	v, err := svc.view()
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
	v, err := svc.view()
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
			return nil, listOut{}, notIndexed(in.PropKey)
		}
		nodes = ns
	case float64:
		ns, ok := v.LookupByNumber(in.PropKey, tv)
		if !ok {
			return nil, listOut{}, notIndexed(in.PropKey)
		}
		nodes = ns
	case bool:
		ns, ok := v.LookupByBool(in.PropKey, tv)
		if !ok {
			return nil, listOut{}, notIndexed(in.PropKey)
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
	v, err := svc.view()
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

// vectorSearch is the vector_search tool (spec 9.3): top-k approximate
// neighbors of a query vector or of an existing node's indexed vector.
// k clamps to vectorMaxK; the note discloses approximation (spec D8).
func (svc *service) vectorSearch(_ context.Context, _ *mcp.CallToolRequest, in vectorSearchIn) (*mcp.CallToolResult, vectorSearchOut, error) {
	g, err := svc.view()
	if err != nil {
		return nil, vectorSearchOut{}, err
	}
	k := in.K
	if k <= 0 {
		k = vectorDefaultK
	}
	if k > vectorMaxK {
		k = vectorMaxK
	}
	if (len(in.Vector) == 0) == (in.SimilarTo == "") {
		return nil, vectorSearchOut{}, errors.New("provide exactly one of query_vector or similar_to")
	}
	var q []float32
	if in.SimilarTo != "" {
		if _, ok := g.NodeByKey(in.SimilarTo); !ok {
			return nil, vectorSearchOut{}, fmt.Errorf("node %q not found", in.SimilarTo)
		}
		fv, ok := g.VectorForNode(in.Index, in.SimilarTo)
		if !ok {
			return nil, vectorSearchOut{}, fmt.Errorf("node %q has no valid vector for index %q", in.SimilarTo, in.Index)
		}
		q = fv
	} else {
		q = make([]float32, len(in.Vector))
		for i, x := range in.Vector {
			q[i] = float32(x)
		}
	}
	hits, err := g.QueryVector(in.Index, q, vectra.QueryVectorOptions{K: k})
	if err != nil {
		return nil, vectorSearchOut{}, err
	}
	out := vectorSearchOut{Count: len(hits), Note: "results are approximate (ANN); order across equal scores is not stable"}
	for _, h := range hits {
		out.Hits = append(out.Hits, vectorHitOut{Key: h.Key, Score: h.Score})
	}
	return nil, out, nil
}
