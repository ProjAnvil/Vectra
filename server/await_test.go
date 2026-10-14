package server

import (
	"context"
	"testing"
	"time"
)

func TestWaitPublishedAlreadySatisfied(t *testing.T) {
	f := newFSM(time.Hour)
	defer f.Close()
	f.dirty = true
	f.sealNow() // publishes seq = appliedIndex = 0 on the empty graph
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan uint64, 1)
	go func() { done <- f.WaitPublished(ctx, 0) }()
	select {
	case <-done: // immediate: minSeq already satisfied
	case <-time.After(time.Second):
		t.Fatal("already-satisfied wait must return immediately")
	}
}

func TestWaitPublishedWakesOnPublish(t *testing.T) {
	f := newFSM(time.Hour)
	defer f.Close()
	const want = 7
	done := make(chan uint64, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- f.WaitPublished(ctx, want)
	}()
	time.Sleep(50 * time.Millisecond) // let the waiter arm; the result assertion below stays deterministic
	f.mu.Lock()
	_, _ = f.live.AddNode("a", "S", nil)
	f.version++ // publishOnce's version gate (Apply/Restore bump it; tests must too)
	f.mu.Unlock()
	f.appliedIndex.Store(want)
	f.dirty = true
	f.sealNow()
	select {
	case got := <-done:
		if got < want {
			t.Fatalf("WaitPublished = %d, want >= %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter was not woken by publish")
	}
}

func TestWaitPublishedTimeoutReturnsCurrentSeq(t *testing.T) {
	f := newFSM(time.Hour)
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	got := f.WaitPublished(ctx, ^uint64(0)) // unreachable
	if got != 0 {
		t.Fatalf("got = %d, want current seq 0", got)
	}
	if elapsed := time.Since(t0); elapsed < 25*time.Millisecond {
		t.Fatalf("returned early: %v", elapsed)
	}
}
