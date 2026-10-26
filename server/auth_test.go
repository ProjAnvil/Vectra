package server

import (
	"errors"
	"io/fs"
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
