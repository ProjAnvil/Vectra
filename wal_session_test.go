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

// TestIndexDeclWALRecovery pins the frame-type-3 durability path end to
// end: index declarations are recorded log-first as IndexDecl frames, so a
// crash between declaration and checkpoint loses nothing (WAL-only
// recovery replays them), and the checkpoint snapshot carries them forward.
func TestIndexDeclWALRecovery(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", map[string]any{"team": "core"}); err != nil {
		t.Fatal(err)
	}
	g.IndexProperty("team")
	if _, err := g.AddNode("b", "Svc", map[string]any{"team": "edge"}); err != nil {
		t.Fatal(err)
	}

	// Crash: no Close. Reopen replays data frames AND the declaration.
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	ns, ok := g2.LookupByProperty("team", "edge")
	if !ok || len(ns) != 1 || ns[0].Key != "b" {
		t.Fatalf("WAL-only recovery lost the index declaration: ok=%v nodes=%v", ok, ns)
	}

	// Checkpoint path: the watermark snapshot must persist it too.
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g3.Close()
	if err := g3.Seal(); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g3.LookupByProperty("team", "core"); !ok || len(ns) != 1 || ns[0].Key != "a" {
		t.Fatalf("checkpoint recovery lost the index declaration: ok=%v nodes=%v", ok, ns)
	}
}
