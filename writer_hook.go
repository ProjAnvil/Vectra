//go:build !release

package vectra

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// Wrapper-mode writer discipline (spec §2.3 R7), debug implementation.
//
// ClaimWriter records the calling goroutine's token (its runtime goroutine
// id, parsed from a runtime.Stack snapshot — the only portable source).
// Every write-method entry then asserts the caller is either that
// goroutine or that nobody has claimed the graph at all (direct mode).
// The check costs one atomic load plus, on graphs with a live claim, a
// ~300-byte stack snapshot per write call — acceptable for debug builds,
// compiled away entirely under the "release" tag (writer_hook_release.go).
//
// It also sets writerAttached (in BOTH builds — see the release twin): the
// absorption window's read-side flush skips wrapper-mode graphs, whose
// readers are never the writer.
func (g *Graph) ClaimWriter() {
	g.writerTok.Store(goroutineToken())
	g.writerAttached.Store(true)
}

// YieldWriter releases the writer claim: writes from any goroutine are
// legal again (direct mode resumes, e.g. after Handle.Close).
func (g *Graph) YieldWriter() {
	g.writerTok.Store(0)
	g.writerAttached.Store(false)
}

// checkWriter is the write-method entry assertion; see ErrNotWriter.
func (g *Graph) checkWriter() error {
	if tok := g.writerTok.Load(); tok != 0 && tok != goroutineToken() {
		return ErrNotWriter
	}
	return nil
}

// assertWriter is checkWriter's panic variant for write-path methods whose
// signatures cannot carry an error (Cycles): debug builds fail fast instead
// of silently racing the writer's private mirrors. Release builds compile
// it away (writer_hook_release.go) — the documented discipline applies.
func (g *Graph) assertWriter() {
	if err := g.checkWriter(); err != nil {
		panic(fmt.Errorf("%w — this method rewrites writer-private state; run it on the writer goroutine (or with writes quiescent)", err))
	}
}

// goroutineToken parses this goroutine's id from runtime.Stack's header
// line ("goroutine 17 [running]:"). A parse failure returns 0, which the
// claim check reads as "unclaimed" — fail-open, matching release builds.
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
