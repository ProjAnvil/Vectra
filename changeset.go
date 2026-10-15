package vectra

import "github.com/projanvil/vectra/internal/wal"

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
// the embedded WAL's whole-frame JSON, and the server module's raft Batch —
// server.Batch embeds Changeset, and gob flattens embedded structs to the
// same field names the pre-0.7 flat Batch used, keeping old raft logs
// decodable (gob ignores unknown fields, matches by name). Add fields only;
// never rename or retype existing ones.
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
)

// changesetVersion is the core-native changeset format version recorded in
// WAL frames written by this package. The server module keeps its own
// batchVersion for raft logs; the versions are independent evolutions of the
// same shape. Evolution rule mirrors gob discipline: add fields only.
const changesetVersion = 1
