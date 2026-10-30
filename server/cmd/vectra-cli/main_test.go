package main

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// captureStd is a scoped swap of os.Stdout and os.Stderr for pipes, so a
// test can assert exactly which stream runOneShot wrote to. runOneShot
// writes to the real os.Stdout/os.Stderr, not injected writers.
type captureStd struct {
	outR, outW, errR, errW *os.File
	oldOut, oldErr         *os.File
	collected              bool
}

func newCaptureStd(t *testing.T) *captureStd {
	t.Helper()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	cs := &captureStd{outR: outR, outW: outW, errR: errR, errW: errW}
	cs.oldOut, cs.oldErr = os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outW, errW
	// Restore even when a test fails before calling collect. The double
	// close when collect already ran is harmless (Close just errors).
	t.Cleanup(func() {
		os.Stdout, os.Stderr = cs.oldOut, cs.oldErr
		cs.outW.Close()
		cs.errW.Close()
	})
	return cs
}

// collect restores the real streams, closes the pipe write ends (so the
// readers below see EOF), and drains both pipes. It must be called only
// after the code under test has returned, and at most once per capture.
func (cs *captureStd) collect(t *testing.T) (stdout, stderr string) {
	t.Helper()
	if cs.collected {
		t.Fatal("collect called twice on the same capture")
	}
	cs.collected = true
	os.Stdout, os.Stderr = cs.oldOut, cs.oldErr
	cs.outW.Close()
	cs.errW.Close()
	outB, err := io.ReadAll(cs.outR)
	if err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	errB, err := io.ReadAll(cs.errR)
	if err != nil {
		t.Fatalf("read stderr pipe: %v", err)
	}
	return string(outB), string(errB)
}

func TestRunOneShotSuccessWritesOnlyStdout(t *testing.T) {
	fake := &fakeCypher{
		bodies: []string{`{"columns":["n.key"],"rows":[{"n.key":"a"}],"count":1}`},
	}
	url := newFakeServer(t, fake)
	cs := newCaptureStd(t)

	if code := runOneShot("MATCH (n) RETURN n.key;", testClient(t, url), 5*time.Second); code != 0 {
		t.Fatalf("runOneShot exit = %d, want 0", code)
	}
	stdout, stderr := cs.collect(t)
	if !strings.Contains(stdout, "n.key") || !strings.Contains(stdout, "(1 rows)") {
		t.Errorf("stdout missing table:\n%s", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want empty on success", stderr)
	}
}

// The scripting contract: `vectra-cli -c "BAD" >out 2>err` must land the
// error line in err, not out. The REPL's in-loop error lines stay on its
// output writer (covered by TestRunREPLErrorPrintsAndContinues); only the
// one-shot path redirects.
func TestRunOneShotExecutionErrorGoesToStderr(t *testing.T) {
	fake := &fakeCypher{
		statuses: []int{http.StatusBadRequest},
		bodies:   []string{`{"error":{"code":"PARSE","message":"unparseable statement"}}`},
	}
	url := newFakeServer(t, fake)
	cs := newCaptureStd(t)

	if code := runOneShot("SELECT 1;", testClient(t, url), 5*time.Second); code != 1 {
		t.Fatalf("runOneShot exit = %d, want 1 on execution error", code)
	}
	stdout, stderr := cs.collect(t)
	if stdout != "" {
		t.Errorf("stdout = %q, want empty on execution error (stdout stays data-only)", stdout)
	}
	if !strings.Contains(stderr, "error: PARSE: unparseable statement\n") {
		t.Errorf("stderr missing error line:\n%q", stderr)
	}
}

// A query that is only whitespace/semicolons never reaches the server.
func TestRunOneShotEmptyStatementIsNoop(t *testing.T) {
	fake := &fakeCypher{}
	url := newFakeServer(t, fake)
	cs := newCaptureStd(t)

	if code := runOneShot("  ; ; ", testClient(t, url), 5*time.Second); code != 0 {
		t.Fatalf("runOneShot exit = %d, want 0 for empty statement", code)
	}
	stdout, stderr := cs.collect(t)
	if stdout != "" || stderr != "" {
		t.Errorf("output = %q / %q, want nothing written", stdout, stderr)
	}
	if got := fake.received(); len(got) != 0 {
		t.Errorf("server saw %q, want no queries", got)
	}
}
