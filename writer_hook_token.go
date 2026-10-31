package vectra

import (
	"runtime"
	"strconv"
	"strings"
)

// goroutineToken parses this goroutine's id from runtime.Stack's header line
// ("goroutine 17 [running]:") — the only portable source. Built in BOTH
// build flavors (audit F-06, previously duplicated verbatim per tag): debug
// builds use it for the per-write claim check (writer_hook.go), release
// builds keep it solely for the absorption window's pend owner check — only
// the window's writer may materialize it from a read API (vectra.go's
// pendWindow owner semantics).
//
// A parse failure returns 0, which the claim check reads as "unclaimed" —
// fail-open, matching release builds. Theoretical boundary: 0 is also the
// pendWindow owner value stored when the opener's parse failed, so a second
// mis-parsed token could in principle match it and let a foreign goroutine
// trip cutLayer — unreachable in practice, because runtime.Stack's
// header-line format is a guaranteed stable contract.
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
