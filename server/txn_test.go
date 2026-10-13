package server

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func txnBatch() Batch {
	return Batch{
		Version: 3,
		Nodes:   []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{
			Snapshot:  7,
			SER:       true,
			ReadNodes: []string{"a", "b"},
			ReadEdges: []EdgeRef{{Src: "a", Dst: "b", Label: "CALLS"}},
		},
	}
}

func TestBatchTxnGobRoundtrip(t *testing.T) {
	data, err := encodeBatch(txnBatch())
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn == nil {
		t.Fatal("Txn lost in roundtrip")
	}
	if got.Txn.Snapshot != 7 || !got.Txn.SER || len(got.Txn.ReadNodes) != 2 {
		t.Fatalf("TxnGuard = %+v", got.Txn)
	}
	if got.Txn.ReadEdges[0] != (EdgeRef{Src: "a", Dst: "b", Label: "CALLS"}) {
		t.Fatalf("ReadEdges = %+v", got.Txn.ReadEdges)
	}
}

// legacyBatch mirrors the pre-0.4.0 Batch field set: gob must skip the new
// Txn field when an old binary (or a test-local legacy struct) decodes.
type legacyBatch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
}

func TestBatchGobLegacyCompat(t *testing.T) {
	// New encoder -> legacy struct: unknown Txn field must be ignored.
	data, err := encodeBatch(txnBatch())
	if err != nil {
		t.Fatal(err)
	}
	var old legacyBatch
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&old); err != nil {
		t.Fatalf("legacy decode of new batch: %v", err)
	}
	if len(old.Nodes) != 1 || old.Nodes[0].Key != "a" {
		t.Fatalf("legacy struct = %+v", old)
	}

	// Legacy encoder -> new struct: Txn must stay nil (auto-commit).
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(legacyBatch{Version: 2, Nodes: []NodeUpsert{{Key: "x", Label: "L"}}}); err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn != nil {
		t.Fatalf("Txn = %+v, want nil for legacy payload", got.Txn)
	}
}
