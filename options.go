package vectra

import "github.com/projanvil/vectra/internal/wal"

// Mode selects the embedded WAL's durability behavior. The zero value is
// ModeOff: New()/Load()/LoadFile() graphs and Open with zero Options keep
// the v0.6 snapshot-only behavior (no WAL directory, no per-write cost).
type Mode = wal.Mode

const (
	ModeOff      = wal.ModeOff      // no WAL (zero value)
	ModeAlways   = wal.ModeAlways   // fsync every frame (one fsync per Batch frame)
	ModeInterval = wal.ModeInterval // fsync at caller boundaries only (Sync/Close/checkpoint)
)

// Options configures Open. The zero value is a v0.6 session: no WAL, LoadFile
// semantics on an existing snapshot path.
type Options struct {
	WALMode Mode
	// WALDir is the WAL directory. Empty means <path>.wal next to the
	// snapshot file. Ignored when WALMode is ModeOff.
	WALDir string
}
