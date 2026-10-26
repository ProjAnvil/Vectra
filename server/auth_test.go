package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestLoadAuthTokens(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
		wantErr string // substring expected in the error; empty = success expected
	}{
		{
			name:    "single token with trailing newline",
			content: "tok-1\n",
			want:    []string{"tok-1"},
		},
		{
			name:    "single token without trailing newline",
			content: "tok-1",
			want:    []string{"tok-1"},
		},
		{
			name:    "CRLF line endings",
			content: "tok-1\r\ntok-2\r\n",
			want:    []string{"tok-1", "tok-2"},
		},
		{
			name:    "blank and whitespace-only lines skipped",
			content: "\ntok-1\n\n   \n\t\n\r\ntok-2\n\n",
			want:    []string{"tok-1", "tok-2"},
		},
		{
			name:    "multiple tokens returned in order",
			content: "tok-1\ntok-2\ntok-3",
			want:    []string{"tok-1", "tok-2", "tok-3"},
		},
		{
			name:    "leading and trailing space and tab trimmed",
			content: "  tok-1\t\n\t tok-2  \n",
			want:    []string{"tok-1", "tok-2"},
		},
		{
			// UTF-8-with-BOM editors put \ufeff on line 1; without the
			// strip it becomes part of the first token and every request
			// 401s against a file that looks perfectly valid.
			name:    "UTF-8 BOM on line 1 is stripped",
			content: "\ufefftok-1\ntok-2\n",
			want:    []string{"tok-1", "tok-2"},
		},
		{
			name:    "whitespace-only file",
			content: " \n\t \n\r\n",
			wantErr: "contains no tokens",
		},
		{
			name:    "empty file",
			content: "",
			wantErr: "contains no tokens",
		},
		{
			name:    "internal whitespace names the offending line",
			content: "tok-1\nabc def\ntok-3",
			wantErr: "line 2 contains whitespace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tokens")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := loadAuthTokens(path)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("loadAuthTokens(%q) = %v, want error containing %q", tt.content, got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("loadAuthTokens(%q) error = %q, want containing %q", tt.content, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadAuthTokens(%q): %v", tt.content, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("loadAuthTokens(%q) = %v, want %v", tt.content, got, tt.want)
			}
		})
	}

	t.Run("nonexistent path", func(t *testing.T) {
		_, err := loadAuthTokens(filepath.Join(t.TempDir(), "missing"))
		if err == nil {
			t.Fatal("loadAuthTokens(nonexistent) succeeded, want error")
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("loadAuthTokens(nonexistent) error = %v, want fs.ErrNotExist wrapped", err)
		}
	})
}

// TestNewAuthTokenFailFast pins the security decision: a bad token file must
// abort New before any side effects, while an unset AuthTokenFile keeps
// today's behavior (auth off, New succeeds).
func TestNewAuthTokenFailFast(t *testing.T) {
	dir := t.TempDir()
	badFile := filepath.Join(dir, "bad-tokens") // whitespace-only = no valid tokens
	if err := os.WriteFile(badFile, []byte("  \n\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("bad token file aborts New before touching the data dir", func(t *testing.T) {
		cfg := authTestConfig(t, "auth-bad", filepath.Join(dir, "d-bad"))
		cfg.AuthTokenFile = badFile
		if _, err := New(cfg); err == nil {
			t.Fatal("New with invalid AuthTokenFile succeeded, want fail-fast error")
		} else if !strings.Contains(err.Error(), "no tokens") {
			t.Fatalf("New error = %v, want token-file error", err)
		}
		if _, err := os.Stat(cfg.DataDir); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("data dir %s created despite fail-fast (stat err = %v)", cfg.DataDir, err)
		}
	})

	t.Run("empty AuthTokenFile keeps New working with auth off", func(t *testing.T) {
		n, err := New(authTestConfig(t, "auth-off", filepath.Join(dir, "d-off")))
		if err != nil {
			t.Fatalf("New with empty AuthTokenFile: %v", err)
		}
		defer n.Close()
		if n.authToken != "" || n.authDigests != nil {
			t.Fatalf("auth fields set without AuthTokenFile: token=%q digests=%v", n.authToken, n.authDigests)
		}
	})

	t.Run("valid token file populates digests and first token", func(t *testing.T) {
		tokFile := filepath.Join(dir, "good-tokens")
		if err := os.WriteFile(tokFile, []byte("tok-a\ntok-b\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := authTestConfig(t, "auth-on", filepath.Join(dir, "d-on"))
		cfg.AuthTokenFile = tokFile
		n, err := New(cfg)
		if err != nil {
			t.Fatalf("New with valid AuthTokenFile: %v", err)
		}
		defer n.Close()
		if n.authToken != "tok-a" {
			t.Fatalf("n.authToken = %q, want %q", n.authToken, "tok-a")
		}
		want := authDigestsFrom([]string{"tok-a", "tok-b"})
		if !slices.Equal(n.authDigests, want) {
			t.Fatalf("n.authDigests = %v, want %v", n.authDigests, want)
		}
	})
}

// authTestConfig builds a minimal valid in-process Config (no bolt, no TCP).
func authTestConfig(t *testing.T, id, dataDir string) Config {
	t.Helper()
	transAddr, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	return Config{
		NodeID:        id,
		HTTPAddr:      "127.0.0.1:0",
		RaftAddr:      string(transAddr),
		DataDir:       dataDir,
		SealInterval:  50 * time.Millisecond,
		LogStore:      raft.NewInmemStore(),
		StableStore:   raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(),
		Transport:     trans,
		RaftConfig:    testRaftConfig(id),
	}
}

// ============================================================
// Bearer middleware + Handler wiring (task 2)
// ============================================================

// authReq performs one HTTP request against url, presenting the
// Authorization header verbatim when non-empty, and returns the status,
// headers, and raw body.
func authReq(t *testing.T, method, url, authorization string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, body
}

// startAuthTestNode brings up one in-process node (single-node bootstrap,
// token file optional) served over httptest on n.Handler(), then removes
// startup nondeterminism: wait for leadership, commit one write to force
// the first FSM publish (small graphs seal immediately), wait for Ready.
// Without the forced publish /v1/health answers 503 "starting" and these
// tests would race node startup instead of exercising the middleware.
func startAuthTestNode(t *testing.T, id, dataDir, tokenFile, seedAuth string) string {
	t.Helper()
	cfg := authTestConfig(t, id, dataDir)
	cfg.AuthTokenFile = tokenFile
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(n.Close)
	ts := httptest.NewServer(n.Handler())
	t.Cleanup(ts.Close)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatalf("%s: not elected leader within deadline", id)
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/batch",
		strings.NewReader(`{"nodes":[{"key":"auth-seed","label":"Service"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if seedAuth != "" {
		req.Header.Set("Authorization", seedAuth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed batch: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed batch: status %d, body %s", resp.StatusCode, body)
	}
	select {
	case <-n.Ready():
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: first publish did not complete", id)
	}
	return ts.URL
}

// TestAuthMiddleware pins the middleware's contract through Handler(): one
// exempt route (exact GET /v1/health), everything else — including
// /debug/vars on the same mux — behind a bearer token matched by SHA-256
// digest, with a byte-identical 401 for every rejection (no oracle on
// which token exists).
func TestAuthMiddleware(t *testing.T) {
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "tokens")
	if err := os.WriteFile(tokFile, []byte("tok-a\ntok-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := startAuthTestNode(t, "auth-mw", filepath.Join(dir, "data"), tokFile, "Bearer tok-a")

	t.Run("no Authorization header gets the house 401 shape", func(t *testing.T) {
		status, hdr, body := authReq(t, http.MethodGet, base+"/v1/stats", "")
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
		if got := hdr.Get("WWW-Authenticate"); got != `Bearer realm="vectra"` {
			t.Fatalf("WWW-Authenticate = %q, want %q", got, `Bearer realm="vectra"`)
		}
		var e struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &e); err != nil {
			t.Fatalf("401 body %s: %v", body, err)
		}
		if e.Error.Code != "UNAUTHORIZED" || e.Error.Message != "missing or invalid bearer token" {
			t.Fatalf("401 body = %s, want code UNAUTHORIZED, message \"missing or invalid bearer token\"", body)
		}
	})

	t.Run("wrong token returns the byte-identical 401, no oracle", func(t *testing.T) {
		_, _, noHeader := authReq(t, http.MethodGet, base+"/v1/stats", "")
		status, _, wrongTok := authReq(t, http.MethodGet, base+"/v1/stats", "Bearer not-a-token")
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
		if !bytes.Equal(noHeader, wrongTok) {
			t.Fatalf("401 bodies differ: no-header=%s wrong-token=%s", noHeader, wrongTok)
		}
	})

	t.Run("lowercase bearer scheme is accepted", func(t *testing.T) {
		status, _, _ := authReq(t, http.MethodGet, base+"/v1/stats", "bearer tok-a")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	})

	t.Run("malformed credentials are rejected", func(t *testing.T) {
		for _, auth := range []string{
			"Bearer",             // scheme alone, no token
			"Bearer tok-a extra", // two tokens
			"Basic xyz",          // wrong scheme
		} {
			status, _, _ := authReq(t, http.MethodGet, base+"/v1/stats", auth)
			if status != http.StatusUnauthorized {
				t.Fatalf("Authorization %q: status = %d, want 401", auth, status)
			}
		}
	})

	t.Run("GET /v1/health is exempt without a token", func(t *testing.T) {
		status, _, _ := authReq(t, http.MethodGet, base+"/v1/health", "")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	})

	t.Run("stats and debug endpoints require a token", func(t *testing.T) {
		for _, path := range []string{"/v1/stats", "/debug/vars"} {
			status, _, _ := authReq(t, http.MethodGet, base+path, "")
			if status != http.StatusUnauthorized {
				t.Fatalf("GET %s without token: status = %d, want 401", path, status)
			}
		}
	})

	t.Run("correct token reaches the protected endpoint", func(t *testing.T) {
		status, _, _ := authReq(t, http.MethodGet, base+"/v1/stats", "Bearer tok-a")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	})

	t.Run("second token from the file is also accepted", func(t *testing.T) {
		status, _, _ := authReq(t, http.MethodGet, base+"/v1/stats", "Bearer tok-b")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	})
}

// TestHandlerPassthroughNoAuth pins the zero-churn guarantee: a node built
// without AuthTokenFile serves /v1/stats with no Authorization header at
// all — Handler() returns the bare mux and existing clients see nothing.
func TestHandlerPassthroughNoAuth(t *testing.T) {
	dir := t.TempDir()
	base := startAuthTestNode(t, "auth-off", filepath.Join(dir, "data"), "", "")
	status, _, _ := authReq(t, http.MethodGet, base+"/v1/stats", "")
	if status != http.StatusOK {
		t.Fatalf("GET /v1/stats without Authorization: status = %d, want 200", status)
	}
}
