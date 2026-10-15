//go:build !release

package writer_test

// Task 5 checklist item 6: wrapper-mode writer discipline (spec §2.3 R7).
// After Attach, write methods from any goroutine other than the writer
// loop return ErrNotWriter; Submit keeps working; after Handle.Close the
// direct-mode writes are legal again. Debug builds only — release builds
// compile the check away and this file with it.

import (
	"errors"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

func TestErrNotWriter(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{})

	// synchronize on the writer loop being up (and the claim recorded)
	if err := h.Submit(nodeCS("w0", "W", nil)); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// this test goroutine is not the writer goroutine
	if _, err := g.AddNode("intruder", "I", nil); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("AddNode from non-writer = %v, want ErrNotWriter", err)
	}
	if _, err := g.AddEdge("w0", "w0", "X", nil); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("AddEdge from non-writer = %v, want ErrNotWriter", err)
	}
	if _, err := g.UpsertEdge("w0", "w0", "X", nil); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("UpsertEdge from non-writer = %v, want ErrNotWriter", err)
	}
	if _, err := g.DeleteEdge("w0", "w0", "X"); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("DeleteEdge from non-writer = %v, want ErrNotWriter", err)
	}
	if _, err := g.DeleteNode("w0"); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("DeleteNode from non-writer = %v, want ErrNotWriter", err)
	}
	if err := g.Batch(func(g *vectra.Graph) {}); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("Batch from non-writer = %v, want ErrNotWriter", err)
	}

	// the writer path itself is unaffected
	if err := h.Submit(nodeCS("w1", "W", nil)); err != nil {
		t.Fatalf("Submit after rejections: %v", err)
	}
	if _, ok := g.NodeByKey("intruder"); ok {
		t.Fatal("rejected write must not be visible")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// yield restored direct mode
	if _, err := g.AddNode("after-close", "D", nil); err != nil {
		t.Fatalf("AddNode after Handle.Close = %v, want direct mode legal", err)
	}
	if got := g.NodeCount(); got != 3 {
		t.Fatalf("NodeCount = %d, want 3", got)
	}
}
