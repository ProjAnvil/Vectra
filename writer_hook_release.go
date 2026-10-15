//go:build release

package vectra

// Wrapper-mode writer discipline (spec §2.3 R7), release implementation:
// zero overhead. The goroutine-token machinery (and its per-write stack
// snapshot) is a debug-build diagnostic; release builds rely on the
// documented contract instead — after internal/writer.Attach, Submit is
// the only legal write path, and concurrent misuse is a data race however
// it is detected. The methods exist in both builds so callers compile
// unchanged.

// ClaimWriter is a no-op in release builds; see writer_hook.go.
func (g *Graph) ClaimWriter() {}

// YieldWriter is a no-op in release builds; see writer_hook.go.
func (g *Graph) YieldWriter() {}

// checkWriter never rejects in release builds; see writer_hook.go.
func (g *Graph) checkWriter() error { return nil }
