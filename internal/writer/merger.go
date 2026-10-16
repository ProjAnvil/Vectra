package writer

// The background merger (spec §2.3/§3.1): one goroutine, single-flight
// flights, exponential backoff, and a synchronous writer-side fallback.
//
// Flight lifecycle (the channel under flightMu is the state machine):
//
//	armed    signalMerge creates the channel and pings mergeSig
//	running  the merger captured the prefix and is sealing (possibly
//	         sleeping in MergeDelay or a backoff)
//	resolved close(flight): the rebase was published (or dropped as
//	         stale), or the fold was handed to the writer via needSync
//
// The writer's write stall waits on that channel; every path through
// resolveFlight closes it, so a stalled writer always resumes.

import (
	"errors"
	"runtime/debug"
	"time"

	"github.com/projanvil/vectra"
)

// Backoff ladder for failing background merges: 100ms doubling, capped at
// 2s (spec ruling 4).
const (
	backoffBase = 100 * time.Millisecond
	backoffCap  = 2 * time.Second
	// maxConsecutiveFails hands the fold to the writer's synchronous
	// fallback after this many consecutive background failures.
	maxConsecutiveFails = 3
)

var errMergeInjected = errors.New("vectra/writer: injected merge failure")

// merger is the background merge goroutine. It only observes quit BETWEEN
// flights: an in-flight merge always runs to its resolution, because the
// writer's drain/Close sequence (and any stalled batch) depends on it.
func (h *Handle) merger() {
	defer close(h.mergerDone)
	for {
		select {
		case <-h.quit:
			return
		case <-h.mergeSig:
			h.resolveFlight()
		}
	}
}

// signalMerge arms a flight if none is wanted and returns its completion
// channel; with one already armed it returns that channel unchanged
// (single-flight: the merger skips re-signals, the next batch re-checks
// pressure). Always safe to wait on: every resolve path closes it.
func (h *Handle) signalMerge() chan struct{} {
	h.flightMu.Lock()
	defer h.flightMu.Unlock()
	if h.flight == nil {
		h.flight = make(chan struct{})
		select {
		case h.mergeSig <- struct{}{}:
		default:
			// A stale signal is buffered: the merger will wake, take the
			// current flight, and resolve it. Never dropped on the floor.
		}
	}
	return h.flight
}

// resolveFlight runs one background merge to resolution.
func (h *Handle) resolveFlight() {
	h.flightMu.Lock()
	f := h.flight
	h.flightMu.Unlock()
	if f == nil {
		return // spurious wakeup: nothing wanted
	}
	// Resolve protocol: clear the flight slot (so the next signalMerge arms
	// a fresh flight) BEFORE closing the channel (so anyone already parked
	// on it wakes). Between the two, a racing signalMerge either grabs this
	// f — and wakes at the close below — or arms the next flight, which
	// the merger picks up on its next loop.
	defer func() {
		h.flightMu.Lock()
		if h.flight == f {
			h.flight = nil
		}
		h.flightMu.Unlock()
		close(f)
	}()

	// Re-check pressure: the trigger may already be satisfied by a fold
	// that happened between signal and here (e.g. the sync fallback), and
	// the volume rule is base-proportional (see mergeEdgeThreshold).
	if layers, edges := h.g.LayerPressure(); layers < h.cfg.MaxLayers && edges < h.mergeEdgeThreshold() {
		return
	}

	if h.cfg.GCTune {
		prev := debug.SetGCPercent(50)
		defer debug.SetGCPercent(prev)
	}

	mc := h.g.CaptureMerge() // pin {base, layers[0..n]} — prefix immutable
	if d := h.cfg.MergeDelay; d > 0 {
		time.Sleep(d) // test window: the writer keeps publishing the tail
	}
	for {
		r, err := h.sealAttempt(mc)
		if err == nil {
			h.pubMu.Lock()
			h.g.PublishMerge(mc, r) // stale publishes return false: fine
			h.pubMu.Unlock()
			h.fails.Store(0)
			return
		}
		n := h.fails.Add(1)
		if n >= maxConsecutiveFails {
			// Ruling 4: hand the fold to the writer's synchronous fallback
			// (set BEFORE the close — a stalled writer must observe it).
			h.needSync.Store(true)
			return
		}
		time.Sleep(backoffFor(int(n)))
	}
}

// sealAttempt seals the capture, honoring the FailMerge injection counter
// (background attempts only — the synchronous fallback must be able to
// succeed for the stall to be releasable, so it is never injected).
func (h *Handle) sealAttempt(mc *vectra.MergeCapture) (*vectra.MergeResult, error) {
	if n := h.failN.Add(1); n <= int32(h.cfg.FailMerge) {
		return nil, errMergeInjected
	}
	return mc.Seal()
}

// syncMerge is the writer-goroutine fallback fold: capture + seal +
// publish, synchronously (spec ruling 4 — the guarantee that a stall can
// always be released even when the background merger keeps failing). It
// never touches the writer-private mirrors (a rebase, not a forceMerge),
// so ID ratchets keep their continuity; the next full merge (Close/Seal)
// restores dense renumbering.
func (h *Handle) syncMerge() {
	h.needSync.Store(false)
	if h.cfg.GCTune {
		prev := debug.SetGCPercent(50)
		defer debug.SetGCPercent(prev)
	}
	// A few attempts: a persistently failing seal degrades to unbounded
	// layers (the generation stays readable — ruling 4) but never wedges
	// the writer; the next batch re-triggers and retries.
	const attempts = 3
	for range attempts {
		mc := h.g.CaptureMerge()
		r, err := mc.Seal()
		if err != nil {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		h.pubMu.Lock()
		h.g.PublishMerge(mc, r)
		h.pubMu.Unlock()
		h.fails.Store(0)
		return
	}
}

// backoffFor returns the sleep after the n-th consecutive failure
// (1-based): 100ms, 200ms, 400ms ... capped at 2s.
func backoffFor(n int) time.Duration {
	d := backoffBase
	for i := 1; i < n && d < backoffCap; i++ {
		d *= 2
	}
	if d > backoffCap {
		d = backoffCap
	}
	return d
}
