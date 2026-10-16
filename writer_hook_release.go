//go:build release

package vectra

import (
	"runtime"
	"strconv"
	"strings"
)

// Wrapper-mode writer discipline (spec §2.3 R7), release implementation.
// The goroutine-token machinery (and its per-write stack snapshot) is a
// debug-build diagnostic; release builds rely on the documented contract
// instead — after internal/writer.Attach, Submit is the only legal write
// path, and concurrent misuse is a data race however it is detected.

// ClaimWriter sets the wrapper-attached flag (needed by the absorption
// window's read-side flush in every build: wrapper-mode readers must skip
// it); the goroutine-token claim itself is a debug-build diagnostic. See
// writer_hook.go.
func (g *Graph) ClaimWriter() { g.writerAttached.Store(true) }

// YieldWriter clears the wrapper-attached flag; see writer_hook.go.
func (g *Graph) YieldWriter() { g.writerAttached.Store(false) }

// checkWriter never rejects in release builds; see writer_hook.go.
func (g *Graph) checkWriter() error { return nil }

// assertWriter is a no-op in release builds; see writer_hook.go.
func (g *Graph) assertWriter() {}

// goroutineToken is kept in release builds for the absorption window: the
// pend's owner check needs the caller's goroutine id to decide whether a
// read API may materialize the window (only the window's writer may). It
// parses the id from a runtime.Stack header line — a fixed 64-byte format,
// far cheaper than the debug build's per-write full check.
func goroutineToken() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := strings.TrimSpace(string(buf[:n]))
	s = strings.TrimPrefix(s, "goroutine ")
	id, _, _ := strings.Cut(s, " ")
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
