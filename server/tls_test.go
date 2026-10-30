package server

// Task 1: TLS on the HTTP plane. Case groups (per the brief):
//   (a) buildHTTPTLSConfig table test
//   (b) live HTTPS node: trusted client 200 / untrusting client handshake failure
//   (c) mTLS: client leaf accepted, no-cert client gets a connection error
//   (d) no-flags parity: plain HTTP still serves /v1/health
//   (e) fail-fast: server.New rejects a malformed cert, naming the file

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ============================================================
// (a) buildHTTPTLSConfig table
// ============================================================

func TestBuildHTTPTLSConfig(t *testing.T) {
	p := genTLSCerts(t)
	p2 := genTLSCerts(t) // independent pair for the mismatch case

	dir := t.TempDir()
	malformedCert := filepath.Join(dir, "malformed-cert.pem")
	if err := os.WriteFile(malformedCert, []byte("this is not a PEM file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A directory as the CA path: os.ReadFile fails deterministically on every
	// platform (EISDIR), unlike chmod 000 which root would still read.
	unreadableCA := filepath.Join(dir, "unreadable-ca.pem")
	if err := os.Mkdir(unreadableCA, 0o755); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		cert     string
		key      string
		clientCA string
		wantErr  string // non-empty: error must contain this substring (the offending path)
	}{
		{name: "valid pair", cert: p.ServerCert, key: p.ServerKey},
		{name: "missing cert file", cert: filepath.Join(dir, "missing-cert.pem"), key: p.ServerKey,
			wantErr: "missing-cert.pem"},
		{name: "malformed cert PEM", cert: malformedCert, key: p.ServerKey,
			wantErr: "malformed-cert.pem"},
		{name: "missing key file", cert: p.ServerCert, key: filepath.Join(dir, "missing-key.pem"),
			wantErr: "missing-key.pem"},
		{name: "key cert mismatch", cert: p.ServerCert, key: p2.ServerKey,
			wantErr: p2.ServerKey},
		{name: "unreadable client CA", cert: p.ServerCert, key: p.ServerKey, clientCA: unreadableCA,
			wantErr: "unreadable-ca.pem"},
		{name: "valid pair with client CA (mTLS)", cert: p.ServerCert, key: p.ServerKey, clientCA: p.CA},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildHTTPTLSConfig(tt.cert, tt.key, tt.clientCA)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("buildHTTPTLSConfig(%q,%q,%q) = %+v, want error containing %q",
						tt.cert, tt.key, tt.clientCA, cfg, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("buildHTTPTLSConfig error = %q, want it to name %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildHTTPTLSConfig(%q,%q,%q): %v", tt.cert, tt.key, tt.clientCA, err)
			}
			if cfg == nil {
				t.Fatal("buildHTTPTLSConfig = nil config, want non-nil")
			}
			if len(cfg.Certificates) != 1 {
				t.Fatalf("len(Certificates) = %d, want 1", len(cfg.Certificates))
			}
			if cfg.MinVersion != tls.VersionTLS12 {
				t.Fatalf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
			}
			wantAuth := tls.NoClientCert
			if tt.clientCA != "" {
				wantAuth = tls.RequireAndVerifyClientCert
			}
			if cfg.ClientAuth != wantAuth {
				t.Fatalf("ClientAuth = %v, want %v", cfg.ClientAuth, wantAuth)
			}
		})
	}
}

// ============================================================
// Live HTTPS node bootstrap (mirrors main.go's listener path)
// ============================================================

// bootTLSNode assembles a real node (in-memory raft stores, temp data dir —
// the shared test bootstrap pattern) with the given HTTP TLS files and serves
// n.Handler() through tls.NewListener + http.Server.Serve, exactly like
// main.go. Returns the https base URL.
func bootTLSNode(t *testing.T, id, certFile, keyFile, clientCA string) (*Node, string) {
	t.Helper()
	transAddr, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	n, err := New(Config{
		NodeID:        id,
		HTTPAddr:      "127.0.0.1:0",
		RaftAddr:      string(transAddr),
		DataDir:       t.TempDir(),
		SealInterval:  50 * time.Millisecond,
		LogStore:      raft.NewInmemStore(),
		StableStore:   raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(),
		Transport:     trans,
		RaftConfig:    testRaftConfig(id),
		HTTPCert:      certFile,
		HTTPKey:       keyFile,
		HTTPClientCA:  clientCA,
	})
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	tlsCfg := n.HTTPTLSConfig()
	if tlsCfg == nil {
		t.Fatalf("HTTPTLSConfig() = nil, want non-nil (HTTPCert=%q)", certFile)
	}
	// Free port: the listener itself binds 127.0.0.1:0 (8080 is occupied here).
	l, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	srv := &http.Server{Handler: n.Handler(), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(l) }()
	t.Cleanup(func() {
		_ = srv.Close()
		n.Close()
	})
	return n, "https://" + l.Addr().String()
}

// tlsTestClient builds a client trusting genTLSCerts' CA, optionally
// presenting the client leaf (mTLS).
func tlsTestClient(t *testing.T, p certPaths, withClientCert bool) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(p.CA)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("AppendCertsFromPEM(ca.pem) failed")
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if withClientCert {
		cert, err := tls.LoadX509KeyPair(p.ClientCert, p.ClientKey)
		if err != nil {
			t.Fatalf("load client pair: %v", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
}

// awaitHealthReady drives the node to a 200 /v1/health: a single node must
// first win its election (~hundreds of ms) and commit one write to open the
// first-publish readiness gate, so both the POST and the health check retry.
func awaitHealthReady(t *testing.T, client *http.Client, base string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "tls-probe", "label": "S"}},
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := client.Post(base+"/v1/batch", "application/json", bytes.NewReader(body)); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		if resp, err := client.Get(base + "/v1/health"); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("/v1/health never returned 200")
}

// ============================================================
// (b) HTTPS on: trusted client 200, untrusting client handshake failure
// ============================================================

func TestHTTPSNodeTrustedAndUntrustingClients(t *testing.T) {
	p := genTLSCerts(t)
	_, base := bootTLSNode(t, "tls-https", p.ServerCert, p.ServerKey, "")

	// Trusted client: handshake + a real round trip.
	trusted := tlsTestClient(t, p, false)
	awaitHealthReady(t, trusted, base)
	resp, err := trusted.Get(base + "/v1/health")
	if err != nil {
		t.Fatalf("trusted GET /v1/health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("trusted /v1/health = %d, want 200", resp.StatusCode)
	}

	// Same client WITHOUT the test CA in its roots: the handshake itself must
	// fail — no request ever reaches the handler.
	untrusting := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, // system roots only
	}}
	if resp, err := untrusting.Get(base + "/v1/health"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("untrusting client: expected handshake failure, got %d %s", resp.StatusCode, body)
	}
}

// ============================================================
// (c) mTLS: client cert required
// ============================================================

func TestHTTPSNodeMTLS(t *testing.T) {
	p := genTLSCerts(t)
	_, base := bootTLSNode(t, "tls-mtls", p.ServerCert, p.ServerKey, p.CA)

	// Client presenting genTLSCerts' client leaf: 200.
	withCert := tlsTestClient(t, p, true)
	awaitHealthReady(t, withCert, base)
	resp, err := withCert.Get(base + "/v1/health")
	if err != nil {
		t.Fatalf("mTLS GET /v1/health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mTLS /v1/health = %d, want 200", resp.StatusCode)
	}

	// Client with no certificate: a connection error, never an HTTP status.
	noCert := tlsTestClient(t, p, false)
	if resp, err := noCert.Get(base + "/v1/health"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("no-cert client: expected connection error, got %d %s", resp.StatusCode, body)
	}
}

// ============================================================
// (d) No-flags parity: plain HTTP path unchanged
// ============================================================

func TestNoTLSFlagsPlainHTTPParity(t *testing.T) {
	transAddr, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	n, err := New(Config{
		NodeID:        "plain",
		HTTPAddr:      "127.0.0.1:0",
		RaftAddr:      string(transAddr),
		DataDir:       t.TempDir(),
		SealInterval:  50 * time.Millisecond,
		LogStore:      raft.NewInmemStore(),
		StableStore:   raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(),
		Transport:     trans,
		RaftConfig:    testRaftConfig("plain"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := n.HTTPTLSConfig(); got != nil {
		t.Fatalf("HTTPTLSConfig() = %+v, want nil when no TLS flags are set", got)
	}
	// Same mounting the existing suite uses (httptest over n.Handler()).
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(func() {
		srv.Close()
		n.Close()
	})
	if !strings.HasPrefix(srv.URL, "http://") {
		t.Fatalf("server URL = %q, want plain http://", srv.URL)
	}
	// Same leader wait the shared cluster bootstrap does: a fresh single node
	// must win its election before it accepts writes.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("plain node not elected leader")
	}
	resp, out := postJSON(t, srv.URL+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plain POST /v1/batch = %d %v", resp.StatusCode, out)
	}
	for time.Now().Before(deadline) {
		resp, _ := getJSON(t, srv.URL+"/v1/health")
		if resp.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("plain /v1/health never returned 200")
}

// ============================================================
// (e) Fail-fast in New
// ============================================================

func TestNewFailFastOnBadCertFiles(t *testing.T) {
	newCfg := func(cert, key, ca string) Config {
		return Config{
			NodeID:       "tls-bad",
			HTTPAddr:     "127.0.0.1:0",
			RaftAddr:     "127.0.0.1:0",
			DataDir:      t.TempDir(),
			HTTPCert:     cert,
			HTTPKey:      key,
			HTTPClientCA: ca,
		}
	}
	dir := t.TempDir()
	badCert := filepath.Join(dir, "bad-cert.pem")
	if err := os.WriteFile(badCert, []byte("garbage, not PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := genTLSCerts(t)

	t.Run("malformed cert file", func(t *testing.T) {
		_, err := New(newCfg(badCert, p.ServerKey, ""))
		if err == nil {
			t.Fatal("New: want error for malformed cert file")
		}
		if !strings.Contains(err.Error(), badCert) {
			t.Fatalf("New error = %q, want it to name %q", err.Error(), badCert)
		}
	})
	t.Run("cert without key", func(t *testing.T) {
		if _, err := New(newCfg(p.ServerCert, "", "")); err == nil {
			t.Fatal("New: want error when HTTPCert is set without HTTPKey")
		}
	})
	t.Run("key without cert", func(t *testing.T) {
		if _, err := New(newCfg("", p.ServerKey, "")); err == nil {
			t.Fatal("New: want error when HTTPKey is set without HTTPCert")
		}
	})
}
