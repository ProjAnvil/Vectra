package vectra

import (
	"github.com/projanvil/vectra/internal/apply"
	"github.com/projanvil/vectra/internal/wal"
)

// NodeUpsert / EdgeUpsert / EdgeDelete are the atomic items of a Changeset.
// Semantics: an existing node key updates its label/props; edges upsert by
// their (srcKey, dstKey, label) key — the most recent live edge with the same
// key has its props replaced in place and extra parallels collapse (last
// write wins). AllowParallel=true opts an edge out of upsert: it is appended
// as a true parallel edge (AddEdge semantics) for pairs carrying several
// semantically distinct relations.
//
// Edge deletes address edges by logical key, not EdgeID: Seal-time
// compaction renumbers IDs, and persisted logs (WAL frames, raft entries)
// must not depend on them. Node deletes cascade edges.
//
// The struct fields and json tags are a wire contract shared by two formats:
// the embedded WAL's whole-frame JSON, and the server module's raft Batch.
// Wire note (verified empirically): encoding/gob does NOT flatten embedded
// structs on the encoder side — an embedded value goes out as a single
// nested field, which would immediately break old-binary decoders — while
// the decoder side CAN match flat wire fields into promoted (embedded)
// fields recursively. That asymmetry makes any embed shape one-directional
// at best, so the server keeps the flat legacy field list with its item
// types aliased to these; gob matches structurally by field name, which
// keeps raft logs two-directionally compatible (pinned by
// TestBatchGobLegacyRoundtrip). Evolution rule for either format: add
// fields only; never rename or retype existing ones.
//
// Definition-site note: the types are physically defined in internal/wal
// (the dependency leaf) and re-exported here as type aliases, so the public
// API (vectra.Changeset & co.) and the WAL package share one type without an
// import cycle. The aliases are the identity — vectra.NodeUpsert IS
// wal.NodeUpsert.
type (
	NodeUpsert = wal.NodeUpsert
	EdgeUpsert = wal.EdgeUpsert
	EdgeDelete = wal.EdgeDelete
	Changeset  = wal.Changeset
	IndexDecl  = wal.IndexDecl
	Frame      = wal.Frame
)

// changesetVersion is the core-native changeset format version recorded in
// WAL frames written by this package. The server module keeps its own
// batchVersion for raft logs; the versions are independent evolutions of the
// same shape. Evolution rule mirrors gob discipline: add fields only.
const changesetVersion = 1

// applyEngine adapts *Graph to internal/apply.Engine — the bridge that lets
// the apply package drive the engine without importing it (package vectra
// imports internal/apply, so the reverse edge would be a cycle). The ID
// returns are dropped: no apply lane uses them.
type applyEngine struct{ g *Graph }

func (e applyEngine) AddNode(key, label string, props map[string]any) error {
	_, err := e.g.AddNode(key, label, props)
	return err
}

func (e applyEngine) AddEdge(srcKey, dstKey, label string, props map[string]any) error {
	_, err := e.g.AddEdge(srcKey, dstKey, label, props)
	return err
}

func (e applyEngine) UpsertEdge(srcKey, dstKey, label string, props map[string]any) error {
	_, err := e.g.UpsertEdge(srcKey, dstKey, label, props)
	return err
}

func (e applyEngine) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	return e.g.DeleteEdge(srcKey, dstKey, label)
}

func (e applyEngine) DeleteNode(key string) (int, error) {
	return e.g.DeleteNode(key)
}

// ApplyChangeset applies one changeset's four lanes in the fixed order
// (nodes, edges, edge deletes, node deletes) with the shared apply walk
// (audit F-02), under an explicit error policy: the policy decides which
// lane's errors are fatal, observe receives one apply.Op per visited op so
// the caller accumulates its own counters and side-collections, and the
// return value is the fatal op (Err != nil — later ops never ran) or the
// zero Op. See internal/apply for the policy definitions.
//
// The caller owns the Batch boundary: wrap this in its own g.Batch(...)
// (typically via the closure's inner *Graph) to materialize one micro-layer
// — and one WAL frame — per changeset. This method is the single write-side
// used by WAL replay (Open recovery), the writer wrapper, the server FSM
// replay and the MCP inject_batch tool; changes to the walk's order or
// write-method dispatch live in internal/apply.Changeset, once.
//
// Apply-loop coupling surface: internal/writer, server and mcp only — the
// parameter types live in internal/apply, so this is unusable outside the
// repo; not covered by API stability.
func (g *Graph) ApplyChangeset(cs Changeset, p apply.Policy, observe func(apply.Op)) apply.Op {
	return apply.Changeset(applyEngine{g}, cs, p, observe)
}
