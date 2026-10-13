package server

import (
	"bytes"
	"encoding/gob"
)

// batchVersion is the changeset format version. Evolution rule: add fields only (gob compatible).
const batchVersion = 3

// NodeUpsert / EdgeUpsert are the atomic items of a changeset. Semantics:
// an existing node key updates its props; edges upsert by their
// (srcKey, dstKey, label) key — the most recent edge with the same key is
// replaced in place and extra parallels collapse. Multigraph parallel
// appends remain available via the embedded AddEdge API.
type NodeUpsert struct {
	Key   string
	Label string
	Props map[string]any
}

type EdgeUpsert struct {
	SrcKey string
	DstKey string
	Label  string
	Props  map[string]any
}

// EdgeDelete addresses edges by their logical key (srcKey, dstKey, label) —
// EdgeID cannot be used because Seal-time compaction renumbers IDs, and raft
// logs / HTTP requests must not depend on it. Node deletes cascade edges.
type EdgeDelete struct {
	SrcKey string
	DstKey string
	Label  string
}

// EdgeRef addresses an edge by its logical key (mirrors EdgeDelete addressing).
type EdgeRef struct {
	Src   string
	Dst   string
	Label string
}

// TxnGuard carries an interactive transaction's read set and snapshot token.
// Snapshot is the publishedSeq the client's reads were based on (from the
// X-Vectra-Snapshot header); validation rejects the batch when any relevant
// key was modified by a raft entry with a higher index.
//
// gob evolution discipline (see encoding/gob doc): add exported fields only;
// never rename existing fields (gob matches by name, there are no field
// numbers — renaming silently drops data); never change a field's type
// (including signedness — decode errors out). Zero-valued new fields are
// omitted from the wire. A nil Txn means auto-commit; a non-nil TxnGuard
// ALWAYS validates — Snapshot 0 means "read at epoch 0" and conflicts with
// any prior write to the same keys.
type TxnGuard struct {
	Snapshot  uint64    `json:"snapshot"`
	SER       bool      `json:"ser"`
	ReadNodes []string  `json:"readNodes"`
	ReadEdges []EdgeRef `json:"readEdges"`
}

// Batch is a changeset carried by one raft log. Fixed apply order
// (deterministic replay): node upserts → edge upserts → edge deletes →
// node deletes.
type Batch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
	Txn         *TxnGuard // Txn != nil enables optimistic validation (nil = auto-commit, pre-0.4 behavior)
}

func encodeBatch(b Batch) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBatch(data []byte) (Batch, error) {
	var b Batch
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&b); err != nil {
		return Batch{}, err
	}
	return b, nil
}
