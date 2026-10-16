// vectra-bench is Vectra's stress tool: generate a power-law dependency graph →
// inject (direct engine writes or via HTTP/raft) → parameter curation →
// concurrent stress of neighbors/multi-hop/path/reachability workloads
// (http mode adds interactive-transaction txnsi/txnser) →
// emit a markdown report. Data goes to a local temp directory, deleted after
// the run (--keep retains it).
//
// Usage:
//
//	vectra-bench -size small                      # engine mode, thousand-scale graph
//	vectra-bench -size medium -mode http          # server mode (HTTP+raft)
//	vectra-bench -size large -duration 10s -workers 16
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	gometrics "github.com/hashicorp/go-metrics"
	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/server"
	"github.com/projanvil/vectra/server/internal/bench"
)

type sizeSpec struct {
	name         string
	nodes, edges int
}

var sizes = map[string]sizeSpec{
	"small":  {"S", 1_000, 10_000},
	"medium": {"M", 100_000, 1_000_000},
	"large":  {"L", 1_000_000, 10_000_000},
}

const (
	nodeChunk = 20_000  // Node chunk size for HTTP injection
	edgeChunk = 100_000 // Edge chunk size for HTTP injection
)

func main() {
	size := flag.String("size", "small", "small|medium|large")
	mode := flag.String("mode", "engine", "engine|http")
	workers := flag.Int("workers", 8, "number of concurrent workers")
	duration := flag.Duration("duration", 5*time.Second, "stress duration per workload")
	warmup := flag.Duration("warmup", 500*time.Millisecond, "warmup duration per workload (discarded)")
	seed := flag.Int64("seed", 42, "seed for graph and parameters")
	datadir := flag.String("datadir", "", "data directory (system temp dir by default, deleted at the end)")
	keep := flag.Bool("keep", false, "keep the data directory (debugging)")
	out := flag.String("out", "", "write results to file (stdout by default)")
	sealInterval := flag.Duration("seal-interval", 2*time.Second, "publish throttle interval in http mode")
	workloads := flag.String("workloads", "", "comma-separated workload subset (all by default): neighbors,bfs2,bfs3,bfsall,path,reach; http mode additionally: txnsi,txnser (http-only)")
	flag.Parse()

	spec, ok := sizes[*size]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown size: %s (small|medium|large)\n", *size)
		os.Exit(2)
	}

	dir := *datadir
	cleanup := true
	if dir == "" {
		tmp, err := os.MkdirTemp("", "vectra-bench-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		dir = tmp
	} else {
		cleanup = !*keep
	}
	if *keep {
		cleanup = false
	}
	defer func() {
		if cleanup {
			os.RemoveAll(dir)
		} else {
			fmt.Fprintf(os.Stderr, "data directory kept: %s\n", dir)
		}
	}()

	rep := newReport(spec, *mode, *workers, *duration, *seed)

	// 1. Generate
	t0 := time.Now()
	bg := bench.Generate(bench.Spec{Nodes: spec.nodes, Edges: spec.edges, Seed: *seed})
	rep.GenTime = time.Since(t0)

	// 2. Inject
	var queryGraph *vectra.Graph
	var baseURL string
	var httpc *http.Client
	var node *server.Node
	// Non-nil in http mode: captures hashicorp/go-metrics samples emitted by
	// the raft stack (see the http case below and logsPerBatch usage in the
	// workload loop).
	var sink *gometrics.InmemSink
	switch *mode {
	case "engine":
		queryGraph = injectEngine(rep, bg)
	case "http":
		// hashicorp/raft and raft-boltdb report metrics through the
		// hashicorp/go-metrics global instance, whose default sink discards
		// everything. Install an inmem sink before server.New builds the raft
		// stack so raft.boltdb.logsPerBatch samples are retained.
		sink = gometrics.NewInmemSink(10*time.Second, time.Minute)
		_, _ = gometrics.NewGlobal(&gometrics.Config{EnableHostname: false, FilterDefault: true}, sink)
		var wantNodes, wantEdges int
		baseURL, httpc, node, wantNodes, wantEdges = injectHTTP(rep, bg, dir, *sealInterval, *keep)
		defer node.Close()
		queryGraph = waitPublished(node, wantNodes, wantEdges)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(2)
	}

	// 3. Parameter curation
	t0 = time.Now()
	params := bench.Curate(queryGraph, bg, 64, *seed+1)
	rep.CurateTime = time.Since(t0)

	// 4. Workloads
	ops, txnStats := buildWorkloads(*mode, queryGraph, baseURL, httpc, params)
	rep.Txn = txnStats
	selected := ops
	if *workloads != "" {
		want := map[string]bool{}
		for _, w := range strings.Split(*workloads, ",") {
			want[strings.TrimSpace(w)] = true
		}
		available := map[string]bool{}
		for _, o := range ops {
			available[o.name] = true
		}
		selected = selected[:0]
		for _, o := range ops {
			if want[o.name] {
				selected = append(selected, o)
			}
		}
		// Names requested but unavailable in this mode (e.g. txnsi in engine
		// mode): note and skip rather than failing the whole run.
		for _, w := range strings.Split(*workloads, ",") {
			w = strings.TrimSpace(w)
			if w != "" && want[w] && !available[w] {
				fmt.Fprintf(os.Stderr, "note: workload %q is not available in %s mode — skipped\n", w, *mode)
				want[w] = false
			}
		}
		if len(selected) == 0 {
			fmt.Fprintln(os.Stderr, "note: no requested workloads available in this mode; nothing to run")
		}
	}
	// Two-snapshot diff: the inmem sink cannot be cleared and keeps rolling
	// intervals spanning injection as well, so capture the logsPerBatch
	// aggregates right before the workload loop and again after it; the phase
	// mean is the delta. In http-only-workload runs (e.g. WORKLOADS=txnsi)
	// this isolates the transaction-commit batches from injection.
	count0, sum0 := logsPerBatch(sink)
	timers0 := sinkTimers(sink)
	for _, w := range selected {
		bench.RunWorkload(w.name, w.op, 1, *warmup) // Warmup discarded
		if ts := txnStats[w.name]; ts != nil {
			ts.Reset() // warmup OCC outcomes are discarded too
		}
		res := bench.RunWorkload(w.name, w.op, *workers, *duration)
		fmt.Fprintf(os.Stderr, "  %-10s ops=%-7d p50=%-9s p99=%s\n",
			res.Name, res.Ops, bench.FormatDur(res.P50), bench.FormatDur(res.P99))
		if ts := txnStats[w.name]; ts != nil {
			rd, cm, wt := ts.PhaseMeans()
			fmt.Fprintf(os.Stderr, "  %-10s phases: read=%s commit=%s rywWait=%s (wait per commit, others per attempt)\n",
				res.Name, bench.FormatDur(rd), bench.FormatDur(cm), bench.FormatDur(wt))
		}
		rep.Results = append(rep.Results, res)
	}
	if n, sum := logsPerBatch(sink); n-count0 > 0 {
		fmt.Fprintf(os.Stderr, "raft logsPerBatch (txnsi phase): mean=%.2f samples=%.0f\n",
			(sum-sum0)/(n-count0), n-count0)
	}
	// Server-side pipeline decomposition over the same phase window: timers
	// the raft stack emits through go-metrics (values are milliseconds).
	// dispatchLog covers the leader loop's store-and-dispatch step (bolt
	// StoreLogs + fsync live here); commitTime measures dispatch→committed
	// per log; fsm.enqueue measures FSM handoff. NOTE: raft stamps a zero
	// dispatch time on barrier/config entries, whose elapsed overflows
	// float32 to +Inf — Inf−Inf makes the phase-diff mean NaN; counts stay
	// valid (cross-check: dispatchLog count ≈ StoreLogs batches, commitTime
	// count ≈ logs). NaN means are skipped, not errors.
	timers1 := sinkTimers(sink)
	for _, name := range []string{"raft.leader.dispatchLog", "raft.commitTime", "raft.fsm.enqueue"} {
		a, ok0 := timers0[name]
		b, ok1 := timers1[name]
		if !ok1 {
			continue
		}
		if !ok0 {
			a = sampleAgg{}
		}
		if n := b.count - a.count; n > 0 {
			mean := (b.sum - a.sum) / n
			if math.IsNaN(mean) || math.IsInf(mean, 0) {
				fmt.Fprintf(os.Stderr, "%s (phase): samples=%.0f (mean unavailable: Inf-poisoned samples)\n", name, n)
				continue
			}
			fmt.Fprintf(os.Stderr, "%s (phase): mean=%.2fms samples=%.0f\n", name, mean, n)
		}
	}

	// 5. Output
	md := rep.markdown()
	if *out != "" {
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "results written to %s\n", *out)
	} else {
		fmt.Print(md)
	}
}

// ---- Injection ----

func injectEngine(rep *report, bg *bench.Graph) *vectra.Graph {
	g := vectra.New()
	t0 := time.Now()
	for _, n := range bg.Nodes {
		_, _ = g.AddNode(n.Key, n.Label, n.Props)
	}
	rep.InjectNodesPerS = rate(len(bg.Nodes), time.Since(t0))
	t0 = time.Now()
	for _, e := range bg.Edges {
		_, _ = g.AddEdge(bg.Nodes[e.Src].Key, bg.Nodes[e.Dst].Key, e.Label, nil)
	}
	rep.InjectEdgesPerS = rate(len(bg.Edges), time.Since(t0))
	t0 = time.Now()
	if err := g.Seal(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rep.SealTime = time.Since(t0)
	return g
}

// injectHTTP also returns the publish-wait targets: nodes applied (summed
// from /v1/batch responses) and the unique-edge count. Batch edge writes are
// true upserts collapsing duplicate (src,dst,label) triples, so what the
// publisher can ever seal is the unique-triple count, not the generated one.
func injectHTTP(rep *report, bg *bench.Graph, dir string, sealInterval time.Duration, keep bool) (string, *http.Client, *server.Node, int, int) {
	// Probe a free port and release it for server.New's TCP transport to bind
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raftAddr := probe.Addr().String()
	probe.Close()
	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	n, err := server.New(server.Config{
		NodeID:       "bench-1",
		HTTPAddr:     httpLn.Addr().String(),
		RaftAddr:     raftAddr,
		DataDir:      filepath.Join(dir, "node1"),
		SealInterval: sealInterval,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() {
		if err := http.Serve(httpLn, n.Handler()); err != nil {
			// the API dying mid-run would surface as a wall of confusing
			// worker connection errors — fail the run at the cause instead
			fmt.Fprintf(os.Stderr, "vectra-bench: http serve: %v\n", err)
			os.Exit(1)
		}
	}()
	waitLeader(n)

	client := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	base := "http://" + httpLn.Addr().String()

	// Chunked node injection
	wantNodes := 0
	t0 := time.Now()
	for i := 0; i < len(bg.Nodes); i += nodeChunk {
		end := min(i+nodeChunk, len(bg.Nodes))
		wantNodes += postJSONAndWait(client, base+"/v1/batch", map[string]any{
			"nodes": upsertNodes(bg.Nodes[i:end]),
		}).NodesApplied
	}
	rep.InjectNodesPerS = rate(len(bg.Nodes), time.Since(t0))

	// Chunked edge injection
	wantEdges := uniqueEdgeCount(bg)
	t0 = time.Now()
	chunk := make([]bench.Edge, 0, edgeChunk)
	flush := func() {
		if len(chunk) > 0 {
			postJSONAndWait(client, base+"/v1/batch", map[string]any{
				"edges": upsertEdges(bg, chunk),
			})
			chunk = chunk[:0]
		}
	}
	for _, e := range bg.Edges {
		chunk = append(chunk, e)
		if len(chunk) == edgeChunk {
			flush()
		}
	}
	flush()
	rep.InjectEdgesPerS = rate(len(bg.Edges), time.Since(t0))
	return base, client, n, wantNodes, wantEdges
}

// logsPerBatch sums the raft.boltdb.logsPerBatch sample aggregates retained
// by the inmem sink across all intervals: (count, sum). The sink keeps a
// rolling window and cannot be cleared, so callers isolate a phase by taking
// two snapshots and diffing. Returns (0, 0) in engine mode (no sink
// installed). Do NOT substitute bolt page statistics — bbolt's Stats() has no
// notion of logs per batch; this metric comes from raft-boltdb via
// go-metrics samples.
func logsPerBatch(sink *gometrics.InmemSink) (count, sum float64) {
	if sink == nil {
		return 0, 0
	}
	for _, im := range sink.Data() {
		if sv, ok := im.Samples["raft.boltdb.logsPerBatch"]; ok {
			count += float64(sv.Count)
			sum += sv.Sum
		}
	}
	return count, sum
}

// sampleAgg is the (count, sum) pair of one go-metrics sample series over
// the sink's retained intervals.
type sampleAgg struct {
	count, sum float64
}

// sinkTimers snapshots every sample series the inmem sink retained, keyed by
// metric name (values as emitted: timers are milliseconds, counters/logsPerBatch
// are dimensionless). Two snapshots diff into a phase window.
func sinkTimers(sink *gometrics.InmemSink) map[string]sampleAgg {
	if sink == nil {
		return nil
	}
	out := make(map[string]sampleAgg)
	for _, im := range sink.Data() {
		for name, sv := range im.Samples {
			a := out[name]
			a.count += float64(sv.Count)
			a.sum += sv.Sum
			out[name] = a
		}
	}
	return out
}

func upsertNodes(nodes []bench.Node) []server.NodeUpsert {
	out := make([]server.NodeUpsert, len(nodes))
	for i, n := range nodes {
		out[i] = server.NodeUpsert{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	return out
}

func upsertEdges(bg *bench.Graph, edges []bench.Edge) []server.EdgeUpsert {
	out := make([]server.EdgeUpsert, len(edges))
	for i, e := range edges {
		out[i] = server.EdgeUpsert{
			SrcKey: bg.Nodes[e.Src].Key,
			DstKey: bg.Nodes[e.Dst].Key,
			Label:  e.Label,
		}
	}
	return out
}

// uniqueEdgeCount counts distinct (srcKey, dstKey, label) triples: batch
// edge writes are true upserts that collapse duplicates, so the published
// graph contains one edge per triple (the publish-wait target).
func uniqueEdgeCount(bg *bench.Graph) int {
	type triple struct {
		src, dst int
		label    string
	}
	seen := make(map[triple]struct{}, len(bg.Edges))
	for _, e := range bg.Edges {
		seen[triple{e.Src, e.Dst, e.Label}] = struct{}{}
	}
	return len(seen)
}

// batchResult mirrors the applied counters of a /v1/batch response.
type batchResult struct {
	NodesApplied int `json:"nodesApplied"`
	EdgesApplied int `json:"edgesApplied"`
}

func postJSONAndWait(c *http.Client, url string, body any) batchResult {
	data, _ := json.Marshal(body)
	resp, err := c.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	var res batchResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "injection failed: %s\n", resp.Status)
		os.Exit(1)
	}
	return res
}

func waitLeader(n *server.Node) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "single node not elected leader within the deadline")
	os.Exit(1)
}

// waitPublished blocks until the publisher has sealed a graph containing
// everything injected. Targets are the applied counters from /v1/batch
// responses: edge upserts collapse duplicate (src,dst,label) triples, so
// the generated edge count can exceed what will ever publish.
func waitPublished(n *server.Node, wantNodes, wantEdges int) *vectra.Graph {
	fmt.Fprintf(os.Stderr, "waiting for publish (including large-graph Seal)...\n")
	deadline := time.Now().Add(5 * time.Minute)
	last := time.Now()
	for time.Now().Before(deadline) {
		if p := n.Published(); p != nil {
			st := p.Stats()
			if st.Nodes >= wantNodes && st.Edges >= wantEdges {
				return p
			}
			if time.Since(last) > 10*time.Second {
				fmt.Fprintf(os.Stderr, "  published %d/%d nodes, %d/%d edges (waiting for all injected data to be published)...\n",
					st.Nodes, wantNodes, st.Edges, wantEdges)
				last = time.Now()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "publish timeout")
	os.Exit(1)
	return nil
}

func rate(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

// ---- Workload definitions ----

type workload struct {
	name string
	op   bench.Op
}

// buildWorkloads returns the workloads for mode plus, in http mode, the OCC
// stats per transaction workload (nil in engine mode, which registers no
// transaction workloads).
func buildWorkloads(mode string, g *vectra.Graph, base string, c *http.Client, p bench.QueryParams) ([]workload, map[string]*bench.TxnStats) {
	if mode == "engine" {
		return engineWorkloads(g, p), nil
	}
	return httpWorkloads(base, c, p)
}

func engineWorkloads(g *vectra.Graph, p bench.QueryParams) []workload {
	nextStart, nextPair := rotators(p)
	_ = nextStart
	_ = nextPair
	return []workload{
		{"neighbors", func() error {
			_, err := g.NeighborsOf(nextStart(), vectra.DirOut)
			return err
		}},
		{"bfs2", func() error {
			n, _ := g.NodeByKey(nextStart())
			_, err := g.BFS([]vectra.NodeID{n.ID}, vectra.WithMaxDepth(2))
			return err
		}},
		{"bfs3", func() error {
			n, _ := g.NodeByKey(nextStart())
			_, err := g.BFS([]vectra.NodeID{n.ID}, vectra.WithMaxDepth(3))
			return err
		}},
		{"bfsall", func() error {
			n, _ := g.NodeByKey(nextStart())
			_, err := g.BFS([]vectra.NodeID{n.ID})
			return err
		}},
		{"path", func() error {
			pair := nextPair()
			_, _, err := g.ShortestPath(pair[0], pair[1])
			return err
		}},
		{"reach", func() error {
			_, err := g.Reachable(nextStart())
			return err
		}},
	}
}

func httpWorkloads(base string, c *http.Client, p bench.QueryParams) ([]workload, map[string]*bench.TxnStats) {
	nextStart, nextPair := rotators(p)
	get := func(url string) error {
		resp, err := c.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s: %s", url, resp.Status)
		}
		return nil
	}
	postQ := func(depth int, from string) error {
		body, _ := json.Marshal(map[string]any{
			"from":     []string{from},
			"maxDepth": depth,
		})
		resp, err := c.Post(base+"/v1/query", "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("query: %s", resp.Status)
		}
		return nil
	}
	txnSIStats := &bench.TxnStats{}
	txnSERStats := &bench.TxnStats{}
	return []workload{
		{"neighbors", func() error {
			return get(base + "/v1/neighbors?key=" + nextStart() + "&dir=out")
		}},
		{"bfs2", func() error { return postQ(2, nextStart()) }},
		{"bfs3", func() error { return postQ(3, nextStart()) }},
		{"bfsall", func() error { return postQ(0, nextStart()) }},
		{"path", func() error {
			pair := nextPair()
			return get(base + "/v1/path?from=" + pair[0] + "&to=" + pair[1])
		}},
		{"reach", func() error {
			return get(base + "/v1/reachable?from=" + nextStart())
		}},
		// Interactive-transaction workloads (http only): snapshot read →
		// guarded upsert with OCC retry. SI validates only the write key;
		// SER additionally validates the read set (start + 4 neighbors).
		{"txnsi", bench.NewTxnOp(c, base, nextStart, false, 0, txnSIStats)},
		{"txnser", bench.NewTxnOp(c, base, nextStart, true, 4, txnSERStats)},
	}, map[string]*bench.TxnStats{"txnsi": txnSIStats, "txnser": txnSERStats}
}

// ---- Report ----

type report struct {
	Size    sizeSpec
	Mode    string
	Workers int
	Dur     time.Duration
	Seed    int64

	GenTime         time.Duration
	InjectNodesPerS float64
	InjectEdgesPerS float64
	SealTime        time.Duration
	CurateTime      time.Duration
	Results         []bench.LoadResult
	Txn             map[string]*bench.TxnStats // OCC stats per transaction workload (http mode)
}

func newReport(s sizeSpec, mode string, workers int, dur time.Duration, seed int64) *report {
	return &report{Size: s, Mode: mode, Workers: workers, Dur: dur, Seed: seed}
}

func (r *report) markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "## vectra-bench %s (%s mode, %d workers × %s, seed=%d)\n\n",
		r.Size.name, r.Mode, r.Workers, r.Dur, r.Seed)
	fmt.Fprintf(&b, "- Graph: %s nodes / %s edges (power-law BA + 5%% back edges)\n",
		intComma(r.Size.nodes), intComma(r.Size.edges))
	fmt.Fprintf(&b, "- Generate: %s; inject: %.0f nodes/s, %.0f edges/s; Seal: %s; parameter curation: %s\n\n",
		bench.FormatDur(r.GenTime), r.InjectNodesPerS, r.InjectEdgesPerS,
		bench.FormatDur(r.SealTime), bench.FormatDur(r.CurateTime))
	rows := make([][]string, 0, len(r.Results))
	for _, res := range r.Results {
		rows = append(rows, []string{
			res.Name, intComma(res.Ops), fmt.Sprintf("%.0f", res.QPS),
			bench.FormatDur(res.P50), bench.FormatDur(res.P90), bench.FormatDur(res.P99),
			bench.FormatDur(res.Max), intComma(res.Errors),
		})
	}
	b.WriteString(bench.MarkdownTable(
		[]string{"workload", "ops", "QPS", "p50", "p90", "p99", "max", "err"},
		rows,
	))
	// Transactions section (http mode): OCC outcomes per transaction
	// workload. Latency columns reuse the main table rows; this section
	// focuses on commit/conflict outcomes.
	txnRows := make([][]string, 0, len(r.Txn))
	for _, res := range r.Results {
		ts, ok := r.Txn[res.Name]
		if !ok {
			continue
		}
		txnRows = append(txnRows, []string{
			res.Name,
			intComma(int(ts.Commits.Load())),
			intComma(int(ts.Conflicts.Load())),
			fmt.Sprintf("%.1f%%", ts.ConflictPct()),
			fmt.Sprintf("%.1f", ts.CommitQPS(r.Dur)),
			bench.FormatDur(res.P50),
			bench.FormatDur(res.P99),
		})
	}
	if len(txnRows) > 0 {
		b.WriteString("\n### Transactions\n\n")
		b.WriteString(bench.MarkdownTable(
			[]string{"workload", "commits", "conflicts", "conflict%", "commit QPS", "p50", "p99"},
			txnRows,
		))
	}
	return b.String()
}

func intComma(n int) string {
	s := fmt.Sprint(n)
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	return strings.Join(append([]string{s}, parts...), ",")
}

// rotators returns parameter rotation closures (atomic counter modulo).
func rotators(p bench.QueryParams) (func() string, func() [2]string) {
	var i, j atomic.Int64
	nextStart := func() string { return p.Starts[i.Add(1)%int64(len(p.Starts))] }
	var empty [2]string
	if len(p.Pairs) == 0 {
		return nextStart, func() [2]string { return empty }
	}
	nextPair := func() [2]string { return p.Pairs[j.Add(1)%int64(len(p.Pairs))] }
	return nextStart, nextPair
}
