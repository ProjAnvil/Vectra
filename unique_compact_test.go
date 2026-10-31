package vectra

// Async-compact row for unique constraints (spec §6, Task 3 carry): a
// compact-tier flight over a constrained graph must pin the declaration
// list writer-side at launch (the ucs field — the background flight must
// not read the writer-private registry) and land it in the flight's
// snapshot, so a reopen enforces.

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestUniqueConstraintSurvivesAsyncCompact(t *testing.T) {
	withTinyCompact(t, func() {
		dir := t.TempDir()
		path := filepath.Join(dir, "v.db")
		g, err := Open(path, Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.DeclareUnique("m", "email"); err != nil {
			t.Fatal(err)
		}
		for i, key := range []string{"n1", "n2", "n3"} {
			props := map[string]any{"email": string(rune('a'+i)) + "@x"}
			if _, err := g.AddNode(key, "m", props); err != nil {
				t.Fatal(err)
			}
		}

		if !g.TryAsyncCompact() {
			t.Fatal("flight did not start (compact tier due, none active)")
		}
		// Writer-side completion: PollAsyncCompact returns nil while the
		// background write is still in flight; the flight is done when it
		// stops being active.
		deadline := time.Now().Add(5 * time.Second)
		for g.AsyncCompactActive() {
			if err := g.PollAsyncCompact(); err != nil {
				t.Fatalf("poll: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatal("async compact flight never completed")
			}
			time.Sleep(time.Millisecond)
		}

		// Post-completion writes ride the retained chain past the pin.
		if _, err := g.AddNode("n4", "m", map[string]any{"email": "d@x"}); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}

		g2, err := Open(path, Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		defer g2.Close()
		if cs := g2.UniqueConstraints(); len(cs) != 1 || cs[0].Label != "m" || cs[0].Prop != "email" {
			t.Fatalf("reopened constraints = %+v, want [{m email}] (flight snapshot must carry the pinned declarations)", cs)
		}
		if _, err := g2.AddNode("n5", "m", map[string]any{"email": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("reopened enforcement: %v, want ErrUniqueViolation", err)
		}
	})
}
