package bench

// Wrapper-mode vector checkpoint cadence (spec §12 deviation item, recorded
// 2026-10-20): the gate-2b write lines ran direct-mode, where no writer is
// attached and CheckpointBytes never fires. This test drives vectors
// through the real wrapper path (writer.Attach + Submit) and measures what
// the default 4MiB volume trigger costs under 768-dim JSON frames:
//
//   - seal cadence (WALSize resets observed live + v-*.db snapshots on
//     disk after the run — the two counts must agree),
//   - per-seal writer stall (a Submit's queue wait while the writer
//     goroutine runs the Seal pipeline; sequential Submits attribute the
//     stall of seal k to the submit that follows it),
//   - empirical frame cost (WAL bytes per vector — spec §12's ~7.2KB),
//   - end-to-end throughput and the checkpoint share of wall time,
//     against the same run with CheckpointBytes raised to 64MiB.
//
// Measurement, not a gate: it asserts only internal consistency (cadence
// counts agree, the raised config seals fewer times). Numbers land in the
// deviation record. Env-gated like the other acceptance runs:
// VECTRA_GATE_CP=1.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

func TestVectorWrapperCheckpoint(t *testing.T) {
	if os.Getenv("VECTRA_GATE_CP") == "" {
		t.Skip("set VECTRA_GATE_CP=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}

	const n = 8192
	const batch = 256 // ~1.9MB of JSON vector frames per changeset

	run := func(cpBytes int64) (seals int, stalls []time.Duration, frameBytesPerVec float64, wall, closeDur time.Duration, tp float64, snapCount int, snapBytes int64) {
		dir := t.TempDir()
		g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 768, Metric: "cosine"}); err != nil {
			t.Fatal(err)
		}
		h := writer.Attach(g, writer.Config{CheckpointBytes: cpBytes})

		rng := rand.New(rand.NewSource(7))
		vecs := gate2bVecs(rng, n, 768)
		lat := make([]time.Duration, 0, n/batch)
		var walSamples []int64
		start := time.Now()
		for b := 0; b < n/batch; b++ {
			cs := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, batch)}
			for j := range cs.Nodes {
				cs.Nodes[j] = vectra.NodeUpsert{
					Key:   fmt.Sprintf("v%06d", b*batch+j),
					Label: "m",
					Props: map[string]any{"emb": vecs[b*batch+j]},
				}
			}
			s := time.Now()
			if err := h.Submit(cs); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			lat = append(lat, time.Since(s))
			walSamples = append(walSamples, g.WALSize())
		}
		wall = time.Since(start)
		s := time.Now()
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		closeDur = time.Since(s)
		g.Close()

		// Seal k fired in afterBatch(k), i.e. after the reply of submit k
		// and before submit k+1 drained; a WALSize drop between samples
		// k and k+1 attributes that seal's stall to submit k+1's wait.
		sorted := append([]time.Duration(nil), lat...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		median := sorted[len(sorted)/2]
		for k := 1; k < len(walSamples); k++ {
			if walSamples[k] < walSamples[k-1]-cpBytes/2 {
				seals++
				stalls = append(stalls, lat[k]-median)
			}
		}
		walGrown := int64(0)
		for k := 1; k < len(walSamples); k++ {
			if d := walSamples[k] - walSamples[k-1]; d > walGrown {
				walGrown = d
			}
		}
		frameBytesPerVec = float64(walGrown) / float64(batch)
		tp = float64(n) / wall.Seconds()

		filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if match, _ := filepath.Match("v-*.db", filepath.Base(p)); match {
				snapCount++
				snapBytes += info.Size()
			}
			return nil
		})
		return
	}

	lastSeals4M, lastStalls4M := 0, 0
	for _, cfg := range []struct {
		label string
		bytes int64
	}{
		{"default 4MiB", 4 << 20},
		{"raised 64MiB", 64 << 20},
	} {
		seals, stalls, perVec, wall, closeDur, tp, snapCount, snapBytes := run(cfg.bytes)
		var sealTime time.Duration
		for _, s := range stalls {
			sealTime += s
		}
		stallStr := "none"
		if len(stalls) > 0 {
			sorted := append([]time.Duration(nil), stalls...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			stallStr = fmt.Sprintf("n=%d p50=%s max=%s", len(stalls), sorted[len(sorted)/2], sorted[len(sorted)-1])
		}
		t.Logf("[%s] %d vecs in %s (close %s) = %.0f/s | seals live=%d disk-retained=%d | stall %s | seal share %.0f%% | frame %.2fKB/vec | snapshot %.1fMB",
			cfg.label, n, wall.Round(time.Millisecond), closeDur.Round(time.Millisecond), tp,
			seals, snapCount, stallStr,
			100*float64(sealTime)/float64(wall), perVec/1024, float64(snapBytes)/(1<<20))
		// cleanupSnapshots retains only the newest snapshot on disk, so
		// the disk count cannot cross-check live seals; the snapshot
		// bytes/log line carries the final size instead.
		if cfg.bytes == 4<<20 {
			lastSeals4M, lastStalls4M = seals, len(stalls)
		}
		if cfg.bytes == 64<<20 && seals >= lastSeals4M {
			t.Errorf("raised 64MiB sealed %d times, expected fewer than default 4MiB's %d", seals, lastSeals4M)
		}
		if cfg.bytes == 4<<20 && lastStalls4M == 0 {
			t.Errorf("default 4MiB: no seal stall observed in %d batches", n/batch)
		}
	}
}
