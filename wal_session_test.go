package vectra

import (
	"path/filepath"
	"testing"
)

// TestCloseAfterWALClosed pins the Minor-1 review fix: if the WAL handle dies
// mid-checkpoint (wal.ErrClosed from Sync/Reset/Close — e.g. an fd closed
// underneath the session), Close treats it as benign, completes the
// checkpoint, and leaves the session properly closed — a retry must never
// wedge with an error it can never recover from.
func TestCloseAfterWALClosed(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", nil); err != nil {
		t.Fatal(err)
	}

	// Kill the WAL handle the way a mid-checkpoint death would leave it.
	if err := g.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close with a dead WAL handle must complete the checkpoint: %v", err)
	}
	if _, err := g.AddNode("z", "Z", nil); err != ErrClosed {
		t.Fatalf("write after Close: %v, want ErrClosed", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close retry must be a clean no-op: %v", err)
	}

	// The checkpoint landed: recovery loads the watermark snapshot; the
	// un-reset WAL segments are gated by the manifest LSN.
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if g2.NodeCount() != 1 {
		t.Fatalf("recovered nodes = %d, want 1", g2.NodeCount())
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
}
