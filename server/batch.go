package server

import (
	"bytes"
	"encoding/gob"

	"github.com/projanvil/vectra"
)

// batchVersion is the changeset format version carried in raft logs.
// Evolution rule: add fields only (gob compatible). The item types and the
// changeset shape now live in the core module (vectra); this module's Batch
// uses them directly via type aliases.
const batchVersion = 3

// NodeUpsert / EdgeUpsert / EdgeDelete are the core changeset vocabulary
// (defined in the vectra package since v0.7), aliased here so existing
// references and the JSON request surface stay unchanged. Semantics: an
// existing node key updates its props; edges upsert by their (srcKey,
// dstKey, label) key — the most recent edge with the same key is replaced in
// place and extra parallels collapse (last write wins on props).
// AllowParallel=true opts an edge out of upsert: it is appended as a true
// parallel edge (embedded AddEdge semantics) for pairs carrying several
// semantically distinct relations (e.g. multiple injection points between
// the same components). Multigraph parallel appends are also available via
// the embedded AddEdge API directly.
type (
	NodeUpsert = vectra.NodeUpsert
	EdgeUpsert = vectra.EdgeUpsert
	EdgeDelete = vectra.EdgeDelete
)

// EdgeRef addresses an edge by its logical key (mirrors EdgeDelete addressing).
type EdgeRef struct {
	Src   string `json:"src"`
	Dst   string `json:"dst"`
	Label string `json:"label"`
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
//
// Wire-compatibility note (deviation from the v0.7 spec's stated mechanism,
// same contract): the spec proposed `struct { vectra.Changeset; Txn }`
// embedding, but encoding/gob — unlike encoding/json — does NOT flatten
// embedded structs: the embedded value would go out as a single nested
// field and raft logs would no longer decode across versions. gob matches
// structurally by field name, so Batch keeps the flat legacy field list with
// the item types aliased to the core ones (same shapes, same names) — the
// wire format is identical to pre-0.7 logs in both directions, verified by
// TestBatchEmbedGobLegacyRoundtrip. Converting to/from a core
// vectra.Changeset is field-by-field assignment (see ChangesetOf).
type Batch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
	Txn         *TxnGuard // Txn != nil enables optimistic validation (nil = auto-commit, pre-0.4 behavior)
}

// ChangesetOf returns the batch's core changeset projection (WAL frames,
// writer-wrapper submissions). The slices are shared, not copied.
func (b Batch) ChangesetOf() vectra.Changeset {
	return vectra.Changeset{
		Version:     b.Version,
		Nodes:       b.Nodes,
		Edges:       b.Edges,
		EdgeDeletes: b.EdgeDeletes,
		NodeDeletes: b.NodeDeletes,
	}
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
