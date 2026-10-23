package bench

// 1M-scale direct measurement (2026-10-23): replaces the "1M extrapolated"
// caveats with measured lines. Gate 2a's conditional pass and every later
// performance verdict stopped at 100k + extrapolation; the infrastructure
// is now in place (forest persistence, retained-WAL checkpoints, v0.11.3
// tail fast path), so this harness measures the real thing:
//
//   - build: 1,001,471 x 768 (n % 2048 == 2047 - a FULL read-your-write
//     tail) through the real wrapper path (writer.Attach + Submit), with
//     per-seal tier classification (light Rotate vs compact full-snapshot
//     under the 256MiB retained-bytes threshold) and writer stalls;
//   - query: ef 32/64/128 p50/p99 against a same-session directly-built
//     vector.State reference (protocol fix #4), plus the full-tail vs
//     empty-tail pairing for the v0.11.3 tail-scan fast path;
//   - memory: live HeapAlloc after explicit GC (the gate-2a leftover
//     line), graph-alone with fixtures dropped;
//   - restart, three modes: retained-chain replay (base + chain), pure
//     compressed-state forest load (chain empty), and no-forest rebuild.
//
// The library persists across stages in VECTRA_1M_DIR (NOT t.TempDir -
// each stage is a separate process and must see the previous stage's
// closed library):
//
//	VECTRA_1M=1 VECTRA_1M_STAGE=build   ingest + drain + queries + memory
//	VECTRA_1M_STAGE=chain   three scenarios, in order:
//	    B1 reopen the build's light-Closed library (stale forest) and
//	       DRAIN TO CONVERGENCE - the worst-case restart: the forest cache
//	       freezes segments AND overlay at compact time, so a light Close
//	       after a burst build leaves a mid-backlog forest and the reopen
//	       re-pays the whole pack (smoke-verified: overlay-at-open == the
//	       frozen backlog, 85536 at smoke scale);
//	    B2 bulk NON-VECTOR volume (~1MiB-prop plain nodes) until a compact
//	       seal fires - refreshes the forest at a drained state (tail-only
//	       overlay) without adding vector-lane backlog;
//	    B3 ingest ~68k vectors (3 x 64MiB = 3 light rotations, no
//	       compact), Close light, walk-copy, restart the copy: base +
//	       3-segment chain replay, drain to ready.
//	VECTRA_1M_STAGE=compact drain the chain library, bulk-volume to a
//	                       compact seal (fresh forest, chain reset),
//	                       walk-copy, restart the copy: pure forest load
//	                       (overlay < 2048 at Open, chain empty)
//	VECTRA_1M_STAGE=rebuild walk-copy of VECTRA_1M_REBUILD_FROM (default:
//	                       a shell snapshot of the build library taken
//	                       before the chain stage grew it), delete v.vec,
//	                       restart: backfill rebuild (overlay == n at
//	                       open); VECTRA_1M_REBUILD_MAX_SECS > 0 bounds
//	                       the drain window, 0 runs to convergence
//
// Seal tier attribution stays public-behavior only (same discipline as
// vector_restart_test.go): a WALSize drop between submit samples is a
// seal; the seal is compact iff the v-*.db snapshot NAME set changed
// (compact always writes a new LSN-named snapshot and Resets the chain;
// light never touches snapshots). Manifest state is logged per stage via
// internal/wal.ReadManifest for chain-anchoring evidence.

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/vector"
	"github.com/projanvil/vectra/internal/wal"
	"github.com/projanvil/vectra/internal/writer"
)

func TestVector1M(t *testing.T) {
	if os.Getenv("VECTRA_1M") == "" {
		t.Skip("set VECTRA_1M=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}
	switch stage := os.Getenv("VECTRA_1M_STAGE"); stage {
	case "build":
		oneMBuildStage(t)
	case "chain":
		oneMChainStage(t)
	case "compact":
		oneMCompactStage(t)
	case "rebuild":
		oneMRebuildStage(t)
	default:
		t.Fatalf("VECTRA_1M_STAGE=%q: want build|chain|compact|rebuild", stage)
	}
}

// --- shared plumbing -------------------------------------------------

func oneMEnvInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			panic(fmt.Sprintf("%s=%q: not a positive int", name, s))
		}
		return v
	}
	return def
}

// oneMDir is the persistent library directory (VECTRA_1M_DIR, default
// $TMPDIR/vectra-1m). Every stage opens <dir>/v.db.
func oneMDir() string {
	if d := os.Getenv("VECTRA_1M_DIR"); d != "" {
		return d
	}
	return filepath.Join(os.TempDir(), "vectra-1m")
}

func oneMDBPath(dir string) string { return filepath.Join(dir, "v.db") }

// oneMForestPath mirrors the engine's dbPath-scoped rule: v.db -> v.vec.
func oneMForestPath(dir string) string { return filepath.Join(dir, "v.vec") }

// oneMDirState is the observable on-disk state a seal is classified from.
type oneMDirState struct {
	snapNames []string // sorted v-*.db basenames in dir
	segNames  []string // sorted wal-*.log basenames in <dir>/v.db.wal
	vecSize   int64
	vecMod    time.Time
}

func oneMScanDir(dir string) oneMDirState {
	var st oneMDirState
	if names, _ := filepath.Glob(filepath.Join(dir, "v-*.db")); len(names) > 0 {
		for _, p := range names {
			st.snapNames = append(st.snapNames, filepath.Base(p))
		}
		sort.Strings(st.snapNames)
	}
	if names, _ := filepath.Glob(filepath.Join(dir, "v.db.wal", "wal-*.log")); len(names) > 0 {
		for _, p := range names {
			st.segNames = append(st.segNames, filepath.Base(p))
		}
		sort.Strings(st.segNames)
	}
	if fi, err := os.Stat(oneMForestPath(dir)); err == nil {
		st.vecSize, st.vecMod = fi.Size(), fi.ModTime()
	}
	return st
}

func oneMStrEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// oneMSealRec is one observed seal: tier, the submit whose queue wait it
// stalled, and the WAL size drop that detected it.
type oneMSealRec struct {
	submit    int
	tier      string // "light" | "compact"
	at        time.Time
	walBefore int64
	walAfter  int64
}

// oneMIngest submits vecs[keyOff : keyOff+count] through the wrapper in
// batches of 256 (the cadence harness's changeset size), sampling
// g.WALSize() after every Submit. A drop of >= cpBytes/2 between samples
// is a seal; the seal is compact iff the snapshot name set changed across
// it. stop, when non-nil, may end ingestion early (chain/compact stages
// ingest until a tier pattern is seen).
func oneMIngest(t *testing.T, dir string, g *vectra.Graph, h *writer.Handle, vecs [][]float32, keyOff, count int, cpBytes int64, stop func(seals []oneMSealRec, submitted int) bool) (submitted int, wall time.Duration, lat []time.Duration, seals []oneMSealRec) {
	t.Helper()
	const batch = 256
	state := oneMScanDir(dir)
	prev := g.WALSize()
	start := time.Now()
	for lo := 0; lo < count; lo += batch {
		hi := lo + batch
		if hi > count {
			hi = count
		}
		cs := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, hi-lo)}
		for j := lo; j < hi; j++ {
			cs.Nodes[j-lo] = vectra.NodeUpsert{
				Key:   gate2bKey(keyOff + j), // key space continues; vecs is stage-local
				Label: "m",
				Props: map[string]any{"emb": vecs[j]},
			}
		}
		s := time.Now()
		if err := h.Submit(cs); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		lat = append(lat, time.Since(s))
		submitted += hi - lo
		cur := g.WALSize()
		if cur < prev-cpBytes/2 {
			next := oneMScanDir(dir)
			tier := "light"
			if !oneMStrEq(state.snapNames, next.snapNames) {
				tier = "compact"
			}
			seals = append(seals, oneMSealRec{
				submit: len(lat), tier: tier, at: time.Now(),
				walBefore: prev, walAfter: cur,
			})
			state = next
		}
		prev = cur
		if stop != nil && stop(seals, submitted) {
			break
		}
	}
	return submitted, time.Since(start), lat, seals
}

// oneMSummarizeSeals logs tier counts, per-seal WAL drop and stall.
// Stall attribution is the cadence harness's: seal k fires after submit
// k's reply, so submit k+1's wait carries its cost (lat is 1-based here -
// lat[seal.submit-1] is the submit that observed the drop; its own wait
// covered the seal).
func oneMSummarizeSeals(t *testing.T, lat []time.Duration, seals []oneMSealRec) {
	t.Helper()
	sorted := append([]time.Duration(nil), lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	median := sorted[len(sorted)/2]
	var lightStalls, compactStalls []time.Duration
	for i, s := range seals {
		stall := time.Duration(0)
		if s.submit-1 < len(lat) {
			stall = lat[s.submit-1] - median
			if stall < 0 {
				stall = 0
			}
		}
		if s.tier == "compact" {
			compactStalls = append(compactStalls, stall)
		} else {
			lightStalls = append(lightStalls, stall)
		}
		t.Logf("  seal[%d] %s at submit %d: wal %d -> %d, stall %s",
			i, s.tier, s.submit, s.walBefore, s.walAfter, stall.Round(time.Millisecond))
	}
	line := func(label string, ds []time.Duration) {
		if len(ds) == 0 {
			t.Logf("  %s seals: none", label)
			return
		}
		s := append([]time.Duration(nil), ds...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		t.Logf("  %s seals: n=%d stall p50=%s max=%s (submit median %s)",
			label, len(s), s[len(s)/2], s[len(s)-1], median)
	}
	line("light", lightStalls)
	line("compact", compactStalls)
}

// oneMDrain polls the folder lane to convergence (no full chunk pending;
// a sub-2048 tail legitimately stays - D7) and logs the pack-rate curve.
// Returns the drain duration and the overlay length at drain start.
func oneMDrain(t *testing.T, g *vectra.Graph, label string, deadline time.Duration, logEvery time.Duration) (time.Duration, int) {
	t.Helper()
	start := time.Now()
	initial := g.VectorOverlayLen("mem")
	dl := start.Add(deadline)
	last, lastT := initial, start
	for g.VectorOverlayLen("mem") >= 2048 {
		if time.Now().After(dl) {
			t.Fatalf("[%s drain] lane did not converge: overlay=%d", label, g.VectorOverlayLen("mem"))
		}
		time.Sleep(500 * time.Millisecond)
		if time.Since(lastT) >= logEvery {
			cur := g.VectorOverlayLen("mem")
			winRate := float64(last-cur) / time.Since(lastT).Seconds()
			cumRate := float64(initial-cur) / time.Since(start).Seconds()
			t.Logf("[%s drain] +%s overlay=%d (initial %d) window %.1f/s cumulative %.1f/s",
				label, time.Since(start).Round(time.Second), cur, initial, winRate, cumRate)
			last, lastT = cur, time.Now()
		}
	}
	cur := g.VectorOverlayLen("mem")
	t.Logf("[%s drain] converged in %s: overlay %d -> %d (%.1f/s cumulative)",
		label, time.Since(start).Round(time.Millisecond), initial, cur,
		float64(initial-cur)/time.Since(start).Seconds())
	return time.Since(start), initial
}

// oneMOpenTimed opens the library and records the Open wall time and the
// overlay length at Open return (the which-path evidence: a mounted
// forest carries only the sub-2048 tail, a replay/rebuild re-intakes
// everything the chain/backfill covers).
func oneMOpenTimed(t *testing.T, dir string) (*vectra.Graph, time.Duration, int) {
	t.Helper()
	start := time.Now()
	g, err := vectra.Open(oneMDBPath(dir), vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	dur := time.Since(start)
	ov := g.VectorOverlayLen("mem")
	t.Logf("open %s, overlay-at-open=%d", dur.Round(time.Millisecond), ov)
	return g, dur, ov
}

// oneMLogManifest dumps the manifest fields recovery will act on.
func oneMLogManifest(t *testing.T, dir string) {
	t.Helper()
	man, ok, err := wal.ReadManifest(filepath.Join(dir, "v.db.wal"))
	if err != nil {
		t.Logf("manifest: read error %v", err)
		return
	}
	if !ok {
		t.Logf("manifest: none")
		return
	}
	t.Logf("manifest: snapshot=%s lsn=%d segment=%d retainFrom=%d format=%d",
		man.Snapshot, man.LSN, man.Segment, man.RetainFrom, man.Format)
}

// oneMSizes logs every file in the library directory tree.
func oneMSizes(t *testing.T, dir string) {
	t.Helper()
	var total int64
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		total += fi.Size()
		t.Logf("  %s %.1fMB", p, float64(fi.Size())/(1<<20))
		return nil
	})
	t.Logf("library total %.2fGB", float64(total)/(1<<30))
}

// oneMMemLog pins the gate-2a memory口径: live HeapAlloc after explicit
// GC (two collections), HeapInuse alongside for span-level visibility.
func oneMMemLog(t *testing.T, label string) runtime.MemStats {
	t.Helper()
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("[mem %s] HeapAlloc=%.2fGB HeapInuse=%.2fGB HeapSys=%.2fGB NumGC=%d PauseTotal=%s GCCPUFraction=%.4f",
		label, float64(ms.HeapAlloc)/(1<<30), float64(ms.HeapInuse)/(1<<30), float64(ms.HeapSys)/(1<<30),
		ms.NumGC, time.Duration(ms.PauseTotalNs), ms.GCCPUFraction)
	return ms
}

// oneMCopyTree streams the whole library directory (files reach ~4GB at
// 1M; restartCopyTree's whole-file Read would spike RSS 4GB).
func oneMCopyTree(t *testing.T, src, dst string) {
	t.Helper()
	start := time.Now()
	var total int64
	err := filepath.Walk(src, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		outPath := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return err
		}
		out, err := os.Create(outPath)
		if err != nil {
			return err
		}
		n, err := io.Copy(out, in)
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		total += n
		return err
	})
	if err != nil {
		t.Fatalf("copy %s -> %s: %v", src, dst, err)
	}
	t.Logf("copied %.2fGB %s -> %s in %s", float64(total)/(1<<30), src, dst, time.Since(start).Round(time.Millisecond))
}

// oneMBaseVecs regenerates the deterministic build-stage fixture (same
// seed, same n) for the stages that need original vectors back (probes
// must hit keys the build stage created).
func oneMBaseVecs(n int) [][]float32 {
	return gate2bVecs(rand.New(rand.NewSource(20261023)), n, 768)
}

// --- stage: build + queries + memory ---------------------------------

func oneMBuildStage(t *testing.T) {
	dir := oneMDir()
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		t.Fatalf("%s not empty - the build stage needs a fresh library", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	n := oneMEnvInt("VECTRA_1M_N", 1_001_471) // n % 2048 == 2047: full tail
	cpBytes := int64(oneMEnvInt("VECTRA_1M_CP_BYTES", 64<<20))
	t.Logf("env: GOVERSION=%s GOMAXPROCS=%d NumCPU=%d GOGC=%q n=%d cpBytes=%dMiB dir=%s",
		runtime.Version(), runtime.GOMAXPROCS(0), runtime.NumCPU(), os.Getenv("GOGC"), n, cpBytes>>20, dir)

	genStart := time.Now()
	vecs := oneMBaseVecs(n)
	t.Logf("fixture: %d x 768 vectors generated in %s", n, time.Since(genStart).Round(time.Millisecond))

	g, err := vectra.Open(oneMDBPath(dir), vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 768, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{CheckpointBytes: cpBytes})

	// Same-session reference (protocol fix #4): a directly-built State
	// from the same vectors, same seed, same pack cadence - its tail is
	// n%2048 == 2047 too, so no tail re-alignment is needed (the 100k
	// harness re-added the tail after Pack(true); here Pack(true) only
	// runs AFTER the full-tail pairing is measured).
	var st *vector.State
	var stateWG sync.WaitGroup
	refStart := time.Now()
	stateWG.Add(1)
	go func() {
		defer stateWG.Done()
		st = vector.NewState(vector.Cosine, 768, 16, 200, gate2bVecSeed("mem"))
		for i := 0; i < n; i++ {
			st.Add(gate2bKey(i), vecs[i])
			if (i+1)%2048 == 0 {
				st.Pack(false)
			}
		}
	}()
	t.Logf("reference State build started concurrently (single-threaded HNSW, same protocol as the 100k gate)")

	submitted, wall, lat, seals := oneMIngest(t, dir, g, h, vecs, 0, n, cpBytes, nil)
	tp := float64(submitted) / wall.Seconds()
	sortedLat := append([]time.Duration(nil), lat...)
	sort.Slice(sortedLat, func(i, j int) bool { return sortedLat[i] < sortedLat[j] })
	t.Logf("ingest: %d vectors accepted in %s = %.0f/s (submit p50=%s max=%s)",
		submitted, wall.Round(time.Millisecond), tp, sortedLat[len(sortedLat)/2], sortedLat[len(sortedLat)-1])
	oneMSummarizeSeals(t, lat, seals)

	drainDur, initialOverlay := oneMDrain(t, g, "build", 6*time.Hour, time.Minute)
	t.Logf("build totals: ingest=%s drain=%s (initial overlay at drain start %d)",
		wall.Round(time.Millisecond), drainDur.Round(time.Millisecond), initialOverlay)
	stateWG.Wait()
	t.Logf("reference State built in %s (concurrent with the graph lane)", time.Since(refStart).Round(time.Millisecond))
	if tail := g.VectorOverlayLen("mem"); tail != n%2048 {
		t.Errorf("post-drain tail=%d, want n%%2048=%d", tail, n%2048)
	} else {
		t.Logf("converged: tail=%d (full tail by construction)", tail)
	}

	// Queries, protocol fix #4: truth upfront untimed, then timed
	// back-to-back; single source per query (chimeric noise collapsed
	// recall to 0.30 at 10k).
	qrng := rand.New(rand.NewSource(777))
	sigma := 0.35 / math.Sqrt(768)
	queries := make([][]float32, 50)
	for qi := range queries {
		src := vecs[qrng.Intn(n)]
		q := make([]float32, 768)
		for j := range q {
			q[j] = src[j] + float32(qrng.NormFloat64()*sigma)
		}
		norm := 0.0
		for _, x := range q {
			norm += float64(x) * float64(x)
		}
		norm = math.Sqrt(norm)
		for j := range q {
			q[j] = float32(float64(q[j]) / norm)
		}
		queries[qi] = q
	}
	truthStart := time.Now()
	type kd struct {
		k string
		d float64
	}
	truths := make([]map[string]bool, len(queries))
	for qi, q := range queries {
		all := make([]kd, n)
		for i, v := range vecs {
			var dot float64
			for j := range v {
				dot += float64(v[j]) * float64(q[j])
			}
			all[i] = kd{gate2bKey(i), 1 - dot}
		}
		for i := 0; i < 10; i++ {
			for j := i + 1; j < len(all); j++ {
				if all[j].d < all[i].d {
					all[i], all[j] = all[j], all[i]
				}
			}
		}
		want := map[string]bool{}
		for i := 0; i < 10; i++ {
			want[all[i].k] = true
		}
		truths[qi] = want
	}
	t.Logf("truth: %d x top-10 of %d computed untimed in %s", len(queries), n, time.Since(truthStart).Round(time.Millisecond))

	// measure times 50 back-to-back queries through run and returns
	// p50/p99 (p99 is the max of 50 - protocol parity with the 100k run).
	measure := func(run func(q []float32)) (p50, p99 time.Duration) {
		gt := make([]time.Duration, len(queries))
		for qi, q := range queries {
			s := time.Now()
			run(q)
			gt[qi] = time.Since(s)
		}
		sort.Slice(gt, func(i, j int) bool { return gt[i] < gt[j] })
		return gt[len(gt)/2], gt[len(gt)*99/100]
	}

	// Sanity lines, NOT gates: the 1M recall-ef curve sits below 100k's
	// (2026-10-19 direct State measurement: 0.9440/0.9960/0.9980 - logged
	// here as context). The graph rides the same forest algorithm, so a
	// materially lower recall or a >1.5x p50 ratio would be a finding,
	// not a pass/fail.
	for _, ef := range []int{32, 64, 128} {
		gP50, gP99 := measure(func(q []float32) {
			if _, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10, Ef: ef}); err != nil {
				t.Fatal(err)
			}
		})
		var recall float64
		for qi, q := range queries {
			hits, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 10, Ef: ef})
			if err != nil {
				t.Fatal(err)
			}
			hit := 0
			for _, hh := range hits {
				if truths[qi][hh.Key] {
					hit++
				}
			}
			recall += float64(hit) / 10
		}
		recall /= float64(len(queries))
		sP50, sP99 := measure(func(q []float32) {
			st.Search(q, 10, ef)
		})
		t.Logf("ef=%d FULL-TAIL: recall@10=%.4f | graph p50=%s p99(max of %d)=%s | state-ref p50=%s p99=%s | ratio %.2fx | spike-1M context recall 0.9440/0.9960/0.9980",
			ef, recall, gP50, len(queries), gP99, sP50, sP99, float64(gP50)/float64(sP50))
		if ef == 32 && recall < 0.80 {
			t.Errorf("ef=32 recall %.4f < 0.80 sanity line", recall)
		}
		if ef >= 64 && recall < 0.95 {
			t.Errorf("ef=%d recall %.4f < 0.95 sanity line", ef, recall)
		}
		if ratio := float64(gP50) / float64(sP50); ratio > 1.5 {
			t.Errorf("ef=%d graph p50 ratio %.2fx > 1.5 sanity line", ef, ratio)
		}
	}

	// v0.11.3 pairing: the same reference with its tail packed away
	// (tail 0) - against the full-tail lines above, this isolates the
	// marginal tail cost on a 1M index through the fixed scan path.
	st.Pack(true)
	for _, ef := range []int{32, 64, 128} {
		sP50, sP99 := measure(func(q []float32) {
			st.Search(q, 10, ef)
		})
		t.Logf("ef=%d EMPTY-TAIL state-ref p50=%s p99(max of %d)=%s (pairing against the full-tail lines above)",
			ef, sP50, len(queries), sP99)
	}

	// Memory: with everything resident, then graph-alone (fixtures and
	// the reference dropped) - the gate-2a口径.
	oneMMemLog(t, "post-drain+queries (graph+reference+fixtures)")
	vecs = nil
	truths = nil
	st = nil
	oneMMemLog(t, "graph-alone")

	pre := oneMScanDir(dir)
	oneMLogManifest(t, dir)
	s := time.Now()
	if err := h.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}
	hDur := time.Since(s)
	postH := oneMScanDir(dir)
	s = time.Now()
	if err := g.Close(); err != nil {
		t.Fatalf("graph Close: %v", err)
	}
	gDur := time.Since(s)
	closeTier := "light"
	if !oneMStrEq(pre.snapNames, postH.snapNames) || postH.vecSize != pre.vecSize {
		closeTier = "compact"
	}
	t.Logf("close: writer %s (tier=%s), graph %s",
		hDur.Round(time.Millisecond), closeTier, gDur.Round(time.Millisecond))
	oneMLogManifest(t, dir)
	oneMSizes(t, dir)
}

// --- stage: retained-chain restart -----------------------------------

// oneMBulkVolume pushes non-vector WAL volume (plain nodes carrying ~1MiB
// string props) until a COMPACT seal fires - retained bytes reached the
// 256MiB threshold. This refreshes the forest cache at the CURRENT drain
// state without touching the vector lane: a vector burst instead would
// freeze its own backlog into the forest.
//
// Compact detection is SNAPSHOT-SET based, not WALSize-drop based: one
// bulk submit is one seal (64MiB per batch), so pre-seal and post-seal
// WALSize samples both sit at ~0 and the drop is invisible (smoke- proven
// the hard way: the compact fired, wrote its snapshot, and the drop
// detector missed it). The snapshot name set only ever changes on a
// compact tier checkpoint. Returns the light seals seen on the way.
func oneMBulkVolume(t *testing.T, dir string, g *vectra.Graph, h *writer.Handle, cpBytes int64, keyPrefix string) (lights int, bulkBytes int64, wall time.Duration) {
	t.Helper()
	payload := strings.Repeat("b", 1<<20)
	state := oneMScanDir(dir)
	start := time.Now()
	for submitted := 0; ; submitted++ {
		cs := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, 64)} // 64 x 1MiB ~= one seal per submit
		for j := range cs.Nodes {
			cs.Nodes[j] = vectra.NodeUpsert{
				Key:   fmt.Sprintf("%s%08d", keyPrefix, submitted*64+j),
				Label: "bulk",
				Props: map[string]any{"pad": payload},
			}
		}
		if err := h.Submit(cs); err != nil {
			t.Fatalf("bulk Submit: %v", err)
		}
		bulkBytes += int64(len(cs.Nodes)) << 20
		next := oneMScanDir(dir)
		if !oneMStrEq(state.snapNames, next.snapNames) {
			t.Logf("bulk: compact seal fired after %.0fMiB (%s) - forest refreshed at the current drain state",
				float64(bulkBytes)/(1<<20), time.Since(start).Round(time.Millisecond))
			return lights, bulkBytes, time.Since(start)
		}
		if !oneMStrEq(state.segNames, next.segNames) {
			lights++ // a segment rotation landed without a snapshot write: light tier
		}
		state = next
		if bulkBytes > 640<<20 {
			t.Fatalf("bulk: no compact after 640MiB - threshold logic changed?")
		}
	}
}

func oneMChainStage(t *testing.T) {
	dir := oneMDir()
	n := oneMEnvInt("VECTRA_1M_N", 1_001_471)
	cpBytes := int64(oneMEnvInt("VECTRA_1M_CP_BYTES", 64<<20))
	chainVecs := oneMEnvInt("VECTRA_1M_CHAIN_VECS", 68_000) // ~= 3 x 64MiB of vector frames

	// B1: reopen the build's light-Closed library. The forest cache froze
	// segments+overlay at the last mid-build compact, so this reopen
	// restores the backlog and re-pays the pack - drain to convergence is
	// the measurement (overlay-at-open sizes the frozen backlog).
	t.Logf("== B1: reopen after the build's light Close (stale forest, frozen backlog) ==")
	oneMLogManifest(t, dir)
	stale := oneMScanDir(dir)
	g, b1Open, b1Overlay := oneMOpenTimed(t, dir)
	t.Logf("B1 stale-forest restart: open=%s overlay-at-open=%d | v.vec %.2fGB (mtime %s)",
		b1Open.Round(time.Millisecond), b1Overlay,
		float64(stale.vecSize)/(1<<30), stale.vecMod.Format("15:04:05"))
	b1Ready, _ := oneMDrain(t, g, "stale-forest", 6*time.Hour, time.Minute)
	t.Logf("B1 RESULT: open=%s overlay-at-open=%d ready=%s (worst-case restart: light Close after a burst build re-pays the frozen backlog)",
		b1Open.Round(time.Millisecond), b1Overlay, b1Ready.Round(time.Millisecond))
	probes := oneMBaseVecs(n)
	for _, p := range []int{n / 3, 2 * n / 3} {
		restartSelfQuery(t, g, probes, p)
	}

	// B2: bulk-volume to a compact seal - refresh the forest at THIS
	// drained state (tail-only overlay), so the controlled chain below
	// anchors at a clean compact boundary.
	h := writer.Attach(g, writer.Config{CheckpointBytes: cpBytes})
	lights, bulkBytes, bulkWall := oneMBulkVolume(t, dir, g, h, cpBytes, "bulkB")
	t.Logf("B2 bulk: %d light seals, %.0fMiB in %s, then compact", lights, float64(bulkBytes)/(1<<20), bulkWall.Round(time.Millisecond))
	s := time.Now()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	b2CloseDur := time.Since(s)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	oneMLogManifest(t, dir)
	t.Logf("B2 close: %s (light over the fresh compact boundary)", b2CloseDur.Round(time.Millisecond))

	// B3: ingest vectors until the THIRD light rotation, then Close light
	// (retained = 3 x 64MiB + the active segment < 256MiB - strictly under
	// the threshold at any CheckpointBytes). Stopping on the seal count,
	// not a fixed vector budget: residual retained bytes from B2's close
	// shift the threshold crossing (a smoke-scale run crossed it at 64.5k
	// vectors). An unexpected compact downgrades to a warning - the B4
	// restart then anchors at the newer compact boundary and the manifest
	// lines carry the truth; a fatal here would waste B1's hours-long
	// drain on a rerun.
	t.Logf("== B3: controlled 3-segment retained chain ==")
	g2, b3Open, b3Overlay := oneMOpenTimed(t, dir)
	t.Logf("B3 reopen after fresh compact: open=%s overlay-at-open=%d (mini forest-load data point)",
		b3Open.Round(time.Millisecond), b3Overlay)
	oneMDrain(t, g2, "pre-chain", time.Hour, 30*time.Second)
	h2 := writer.Attach(g2, writer.Config{CheckpointBytes: cpBytes})
	extra := gate2bVecs(rand.New(rand.NewSource(31)), chainVecs, 768)
	stop := func(seals []oneMSealRec, submitted int) bool {
		lights := 0
		for _, sl := range seals {
			if sl.tier == "light" {
				lights++
			}
		}
		return lights >= 3
	}
	submitted, wall, lat, seals := oneMIngest(t, dir, g2, h2, extra, n, chainVecs, cpBytes, stop)
	t.Logf("B3 chain ingest: %d vectors in %s", submitted, wall.Round(time.Millisecond))
	oneMSummarizeSeals(t, lat, seals)
	var chainLights int
	for _, sl := range seals {
		if sl.tier == "compact" {
			t.Logf("B3 WARNING: compact seal at submit %d - the chain restart anchors at the newer compact boundary (see manifest lines)", sl.submit)
		} else {
			chainLights++
		}
	}
	if chainLights < 3 {
		t.Errorf("B3: only %d light seals after %d vectors (want 3) - raise VECTRA_1M_CHAIN_VECS", chainLights, submitted)
	}
	extra = nil
	probes = nil

	pre := oneMScanDir(dir)
	s = time.Now()
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}
	hDur := time.Since(s)
	postH := oneMScanDir(dir)
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	closeTier := "light"
	if !oneMStrEq(pre.snapNames, postH.snapNames) || postH.vecSize != pre.vecSize {
		closeTier = "compact"
	}
	t.Logf("B3 close: writer %s tier=%s", hDur.Round(time.Millisecond), closeTier)
	if closeTier != "light" {
		t.Fatalf("B3 needs a light Close (retained < 256MiB); got %s - lower VECTRA_1M_CHAIN_VECS", closeTier)
	}
	oneMLogManifest(t, dir)

	copyDir := dir + "-chain"
	if err := os.MkdirAll(copyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oneMCopyTree(t, dir, copyDir)
	t.Logf("== B4: retained-chain restart (base + %d-segment chain) ==", chainLights)
	oneMLogManifest(t, copyDir)
	g3, b4Open, b4Overlay := oneMOpenTimed(t, copyDir)
	b4Ready, _ := oneMDrain(t, g3, "chain-replay", 2*time.Hour, 30*time.Second)
	probes = oneMBaseVecs(n)
	for _, p := range []int{n / 3, 2 * n / 3} {
		restartSelfQuery(t, g3, probes, p)
	}
	if b4Overlay < 2048 {
		t.Errorf("B4 chain restart opened with overlay=%d (<2048): no chain replayed - check RetainFrom", b4Overlay)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("B4 RESULT (mode B): open=%s overlay-at-open=%d ready=%s (chain bytes re-pack; the drain dominates)",
		b4Open.Round(time.Millisecond), b4Overlay, b4Ready.Round(time.Millisecond))
	oneMSizes(t, copyDir)
}

// --- stage: compressed-state restart (pure forest load) --------------

func oneMCompactStage(t *testing.T) {
	dir := oneMDir()
	n := oneMEnvInt("VECTRA_1M_N", 1_001_471)
	cpBytes := int64(oneMEnvInt("VECTRA_1M_CP_BYTES", 64<<20))

	// C1: the chain stage left the original with the 3-segment chain and
	// the ~68k vector backlog (Close interrupts the lane's drain). Drain
	// it so the coming compact freezes a tail-only overlay.
	t.Logf("== C1: drain the chain library ==")
	g, c1Open, c1Overlay := oneMOpenTimed(t, dir)
	_ = c1Open
	oneMDrain(t, g, "pre-compact", 2*time.Hour, 30*time.Second)
	_ = c1Overlay

	// C2: bulk-volume to a compact seal - full snapshot + fresh forest at
	// the drained state + Reset (chain deleted). The bulk frames are
	// non-vector: the vector lane stays converged.
	t.Logf("== C2: bulk-volume to a compact seal (fresh forest, chain reset) ==")
	h := writer.Attach(g, writer.Config{CheckpointBytes: cpBytes})
	lights, bulkBytes, bulkWall := oneMBulkVolume(t, dir, g, h, cpBytes, "bulkC")
	t.Logf("C2 bulk: %d light seals, %.0fMiB in %s, then compact", lights, float64(bulkBytes)/(1<<20), bulkWall.Round(time.Millisecond))
	pre := oneMScanDir(dir)
	preVec := pre.vecSize
	s := time.Now()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	hDur := time.Since(s)
	postH := oneMScanDir(dir)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	closeTier := "light"
	if !oneMStrEq(pre.snapNames, postH.snapNames) || postH.vecSize != preVec {
		closeTier = "compact"
	}
	t.Logf("C2 close: writer %s tier=%s (v.vec %.1fMB -> %.1fMB)",
		hDur.Round(time.Millisecond), closeTier, float64(preVec)/(1<<20), float64(postH.vecSize)/(1<<20))
	oneMLogManifest(t, dir)

	// C3: restart a walk-copy - the pure forest-load path: chain is empty
	// (Reset), the forest carries the tail-only overlay, so Open mounts
	// prebuilt segments and is converged the moment it returns.
	copyDir := dir + "-cmp"
	if err := os.MkdirAll(copyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oneMCopyTree(t, dir, copyDir)
	t.Logf("== C3: compressed-state restart (pure forest load) ==")
	oneMLogManifest(t, copyDir)
	g2, c3Open, c3Overlay := oneMOpenTimed(t, copyDir)
	c3Ready, _ := oneMDrain(t, g2, "forest-load", time.Hour, 30*time.Second)
	probes := oneMBaseVecs(n)
	for _, p := range []int{n / 3, 2 * n / 3} {
		restartSelfQuery(t, g2, probes, p)
	}
	if c3Overlay >= 2048 {
		t.Errorf("C3 forest-load restart opened with overlay=%d (>=2048): the forest cache did not mount - this was a replay", c3Overlay)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(oneMForestPath(copyDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("C3 RESULT (mode A): open=%s overlay-at-open=%d ready=%s | v.vec %.2fGB | bandwidth ~%.2fGB/s (v.vec bytes / open wall)",
		c3Open.Round(time.Millisecond), c3Overlay, c3Ready.Round(time.Millisecond),
		float64(fi.Size())/(1<<30), float64(fi.Size())/(1<<30)/c3Open.Seconds())
	oneMSizes(t, copyDir)
}

// --- stage: no-forest rebuild ----------------------------------------

func oneMRebuildStage(t *testing.T) {
	dir := oneMDir()
	n := oneMEnvInt("VECTRA_1M_N", 1_001_471)
	maxSecs := oneMEnvInt("VECTRA_1M_REBUILD_MAX_SECS", 0) // 0 = full drain

	// The rebuild's source library: default is this dir, but a snapshot
	// of the build stage's library (taken before the chain/compact stages
	// grew it) pairs the rebuild against the build's own drain at the
	// same n - backfill-from-snapshot vs live-ingest packing.
	src := dir
	if s := os.Getenv("VECTRA_1M_REBUILD_FROM"); s != "" {
		src = s
	}
	copyDir := dir + "-rb"
	if err := os.MkdirAll(copyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oneMCopyTree(t, src, copyDir)
	if err := os.Remove(oneMForestPath(copyDir)); err != nil {
		t.Fatal(err)
	}
	t.Logf("== no-forest rebuild (v.vec deleted) ==")
	oneMLogManifest(t, copyDir)
	g, openDur, overlay := oneMOpenTimed(t, copyDir)
	if overlay < 2048 {
		t.Errorf("rebuild opened with overlay=%d (<2048): nothing re-intaked - v.vec deletion did not force a rebuild", overlay)
	}

	if maxSecs > 0 {
		// Bounded window: log the pack-rate curve, then report rate +
		// arithmetic projection. The projection is a FLOOR: deep-cascade
		// tier merges at 1M are single big rebuilds on top of the
		// per-chunk rate (the 100k->1M spike showed 4.42x write
		// amplification).
		start := time.Now()
		initial := g.VectorOverlayLen("mem")
		last, lastT := initial, start
		for g.VectorOverlayLen("mem") >= 2048 && time.Since(start) < time.Duration(maxSecs)*time.Second {
			time.Sleep(2 * time.Second)
			if time.Since(lastT) >= 30*time.Second {
				cur := g.VectorOverlayLen("mem")
				t.Logf("[rebuild window] +%s overlay=%d/%d window %.1f/s",
					time.Since(start).Round(time.Second), cur, initial, float64(last-cur)/time.Since(lastT).Seconds())
				last, lastT = cur, time.Now()
			}
		}
		cur := g.VectorOverlayLen("mem")
		rate := float64(initial-cur) / time.Since(start).Seconds()
		t.Logf("REBUILD (bounded %ds): open=%s overlay-at-open=%d packed %d in %s = %.1f/s -> projected full convergence >= %.1f min (floor; cascade adds)",
			maxSecs, openDur.Round(time.Millisecond), initial, initial-cur, time.Since(start).Round(time.Millisecond), rate, float64(cur)/rate/60)
	} else {
		readyDur, initial := oneMDrain(t, g, "rebuild", 8*time.Hour, time.Minute)
		t.Logf("REBUILD (full): open=%s overlay-at-open=%d ready=%s (%.1f/s nominal over %d vectors)",
			openDur.Round(time.Millisecond), initial, readyDur.Round(time.Millisecond), float64(initial)/readyDur.Seconds(), initial)
	}
	probes := oneMBaseVecs(n)
	restartSelfQuery(t, g, probes, n/3) // correct mid-drain (overlay is exact-scanned)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	oneMSizes(t, copyDir)
}
