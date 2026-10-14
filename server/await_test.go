package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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

// ---- GET /v1/await ----

func TestAwaitEndpoint(t *testing.T) {
	tc := newTestCluster(t, 1)
	tn := tc.nodes[0]
	// one write so publishedSeq > 0 and stable
	postJSON(t, tn.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	cur := tn.node.fsm.PublishedSeq()
	if cur == 0 {
		t.Fatal("precondition: something published")
	}

	// satisfied: returns immediately
	resp, out := getJSON(t, tn.URL()+"/v1/await?seq="+strconv.FormatUint(cur, 10))
	if resp.StatusCode != http.StatusOK || out["reached"] != true {
		t.Fatalf("satisfied await = %d %v", resp.StatusCode, out)
	}
	if resp.Header.Get("X-Vectra-Snapshot") != strconv.FormatUint(cur, 10) {
		t.Fatalf("X-Vectra-Snapshot = %q, want %d", resp.Header.Get("X-Vectra-Snapshot"), cur)
	}

	// unsatisfied with a short timeout: reached=false, takes >= timeout
	t0 := time.Now()
	resp, out = getJSON(t, tn.URL()+"/v1/await?seq="+strconv.FormatUint(cur+5, 10)+"&timeoutMs=80")
	if resp.StatusCode != http.StatusOK || out["reached"] != false {
		t.Fatalf("unsatisfied await = %d %v", resp.StatusCode, out)
	}
	if time.Since(t0) < 70*time.Millisecond {
		t.Fatalf("await returned early: %v", time.Since(t0))
	}

	// bad params
	resp, _ = getJSON(t, tn.URL()+"/v1/await")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing seq = %d, want 400", resp.StatusCode)
	}
	resp, _ = getJSON(t, tn.URL()+"/v1/await?seq=1&timeoutMs=-1")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("negative timeoutMs = %d, want 400", resp.StatusCode)
	}
}

func TestAwaitWakesOnLateWrite(t *testing.T) {
	tc := newTestCluster(t, 1)
	tn := tc.nodes[0]
	postJSON(t, tn.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	cur := tn.node.fsm.PublishedSeq()

	type out struct {
		Reached      bool    `json:"reached"`
		PublishedSeq float64 `json:"publishedSeq"`
	}
	resCh := make(chan out, 1)
	go func() {
		resp, err := http.Get(tn.URL() + "/v1/await?seq=" + strconv.FormatUint(cur+1, 10) + "&timeoutMs=5000")
		if err != nil {
			resCh <- out{}
			return
		}
		defer resp.Body.Close()
		var o out
		_ = json.NewDecoder(resp.Body).Decode(&o)
		resCh <- o
	}()
	time.Sleep(100 * time.Millisecond)
	postJSON(t, tn.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "b", "label": "S"}},
	})
	select {
	case o := <-resCh:
		if !o.Reached {
			t.Fatalf("await did not wake on publish: %+v", o)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("await never returned")
	}
}
