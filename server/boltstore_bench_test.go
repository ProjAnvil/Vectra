package server

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// BenchmarkBoltStoreLogs isolates the raft log-store write cost — one bolt
// commit + fsync per StoreLogs call — from the rest of the raft pipeline.
// The batch-size sub-benchmarks mirror the drain batches observed in
// production-shaped runs (spec §4.1: ~5.5 logs/StoreLogs). Payload ≈ one
// gob-encoded single-node txn (~300B). Run with -benchtime=200x on slow-disk
// machines; each iteration is one fsync.
func benchBoltStoreLogs(b *testing.B, batchSize int) {
	b.ReportAllocs()
	store, err := raftboltdb.NewBoltStore(b.TempDir() + "/bolt.db")
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	payload := bytes.Repeat([]byte("x"), 300) // ~ one txn gob
	var idx uint64
	logs := make([]*raft.Log, batchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range logs {
			idx++
			logs[j] = &raft.Log{Index: idx, Term: 1, Type: raft.LogCommand, Data: payload}
		}
		if err := store.StoreLogs(logs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBoltStoreLogs1(b *testing.B)   { benchBoltStoreLogs(b, 1) }
func BenchmarkBoltStoreLogs5(b *testing.B)   { benchBoltStoreLogs(b, 5) }
func BenchmarkBoltStoreLogs32(b *testing.B)  { benchBoltStoreLogs(b, 32) }
func BenchmarkBoltStoreLogs256(b *testing.B) { benchBoltStoreLogs(b, 256) }

// TestBoltStoreLogsSmoke keeps the benchmark setup covered by a plain test
// (CI-fast, no timing assertions).
func TestBoltStoreLogsSmoke(t *testing.T) {
	store, err := raftboltdb.NewBoltStore(t.TempDir() + "/bolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.StoreLogs([]*raft.Log{{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreLogs([]*raft.Log{{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("y")}}); err != nil {
		t.Fatal(err)
	}
	first, err := store.FirstIndex()
	if err != nil || first != 1 {
		t.Fatalf("first index = %d, %v", first, err)
	}
	_ = time.Now // keep time import if assertions evolve
}
