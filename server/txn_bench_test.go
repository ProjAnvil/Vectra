package server

import (
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// benchApply measures the fsm.Apply cost of one batch: auto-commit (nil
// TxnGuard) vs interactive validation. Snapshot is set to ^uint64(0) so the
// guard never conflicts — this isolates the pure validation overhead (write/
// read-set lookups) from conflict handling. The fsm is driven directly via
// encodeBatch + raft.Log to exclude HTTP and raft transport from the number.
func benchApply(b *testing.B, tg *TxnGuard, reads int) {
	f := newFSM(time.Hour)
	keys := make([]string, reads)
	nodes := make([]NodeUpsert, reads+1)
	for i := range keys {
		keys[i] = "r" + strconv.Itoa(i)
		if i < reads {
			nodes[i] = NodeUpsert{Key: keys[i], Label: "S"}
		}
	}
	nodes[reads] = NodeUpsert{Key: "w", Label: "S"}
	if tg != nil {
		tg.ReadNodes = keys[:reads]
		tg.Snapshot = ^uint64(0) // never conflicts: measure pure validation cost
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := encodeBatch(Batch{Version: 3, Nodes: nodes, Txn: tg})
		if err != nil {
			b.Fatal(err)
		}
		f.Apply(&raft.Log{Data: data, Index: uint64(i + 1)})
	}
}

func BenchmarkFSMAutoCommit(b *testing.B) { benchApply(b, nil, 16) }
func BenchmarkFSMTxnSI(b *testing.B)      { benchApply(b, &TxnGuard{}, 0) }
func BenchmarkFSMTxnSER16(b *testing.B)   { benchApply(b, &TxnGuard{SER: true}, 16) }
