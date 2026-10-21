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

// VectorIndexOp is one explicit vector-index declaration change carried in
// a Batch (spec 2026-10-21 §3.4, rulings #6/#7): exactly one of Create/Drop
// is set per op. An op with neither set (or both) is invalid and rejected
// at Apply with a clear error. Create is a pointer so a nil Create is
// distinguishable from a zero-valued declaration on the wire.
//
// This is an op list, NOT a full-list reconciliation: nil/empty
// VectorIndexOps means "no declaration change this batch" — the WAL
// IndexDecl frame's nil-vs-empty (full-list) semantics do NOT apply here.
type VectorIndexOp struct {
	Create *vectra.VectorIndexDecl `json:"create,omitempty"`
	Drop   string                  `json:"drop,omitempty"`
}

// exactlyOneSet reports the op's shape validity: exactly one of Create/Drop
// must be set (Apply rejects entries violating it before any mutation).
func (op VectorIndexOp) exactlyOneSet() bool {
	return (op.Create != nil) != (op.Drop != "")
}

// Batch is a changeset carried by one raft log. Fixed apply order
// (deterministic replay): vector index ops → node upserts → edge upserts →
// edge deletes → node deletes.
//
// Wire-compatibility note (deviation from the v0.7 spec's stated mechanism,
// same contract): the spec proposed `struct { vectra.Changeset; Txn }`
// embedding, but encoding/gob's encoder — unlike encoding/json — does NOT
// flatten embedded structs: the embedded value goes out as a single nested
// field, which would break raft logs for every old binary (the decoder side
// can match flat wire fields into promoted fields recursively, but that
// asymmetry makes any embed shape one-directional at best). gob matches
// structurally by field name, so Batch keeps the flat legacy field list with
// the item types aliased to the core ones (same shapes, same names) — the
// wire format is identical to pre-0.7 logs in both directions, verified by
// TestBatchGobLegacyRoundtrip. Converting to/from a core vectra.Changeset is
// field-by-field assignment (see ChangesetOf).
type Batch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
	Txn         *TxnGuard // Txn != nil enables optimistic validation (nil = auto-commit, pre-0.4 behavior)
	// VectorIndexOps are explicit create/drop declaration ops, applied
	// BEFORE this batch's changesets at replay (ops-first, the writer-lane
	// precedent — see fsm.Apply). Nil/empty = no declaration change.
	// gob add-only evolution: the field is absent from older logs (decodes
	// nil there) and silently ignored by older readers of new logs.
	VectorIndexOps []VectorIndexOp
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
