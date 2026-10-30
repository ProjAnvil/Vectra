package server

// Task 2: mutual X.509 TLS on the raft plane (node-to-node). Case groups
// (per the brief):
//   (a) buildNodeTLSConfig table
//   (b) two-node TLS-raft cluster: join over plain HTTP, replicate over TLS
//   (c) rejection: plain-raft joiner and foreign-CA joiner never converge
//   (d) parity: no node-TLS flags keeps the plain TCP transport default
//   (e) HTTPS-seed join: joining node trusts the seed CA via SSL_CERT_FILE
// plus Config field validation (all-or-nothing, fail-fast on bad files).

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ============================================================
// (a) buildNodeTLSConfig table
// ============================================================

func TestBuildNodeTLSConfig(t *testing.T) {
	p := genTLSCerts(t)
	p2 := genTLSCerts(t) // independent pair for the mismatch case

	dir := t.TempDir()
	malformedCert := filepath.Join(dir, "malformed-cert.pem")
	if err := os.WriteFile(malformedCert, []byte("this is not a PEM file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	unreadableCA := filepath.Join(dir, "unreadable-ca.pem")
	if err := os.Mkdir(unreadableCA, 0o755); err != nil {
		t.Fatal(err)
	}
	notPEMCA := filepath.Join(dir, "not-pem-ca.pem")
	if err := os.WriteFile(notPEMCA, []byte("also not PEM\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		cert    string
		key     string
		ca      string
		wantErr string // non-empty: error must contain this substring (the offending path)
	}{
		{name: "valid triple", cert: p.ServerCert, key: p.ServerKey, ca: p.CA},
		{name: "missing cert file", cert: filepath.Join(dir, "missing-cert.pem"), key: p.ServerKey, ca: p.CA,
			wantErr: "missing-cert.pem"},
		{name: "malformed cert PEM", cert: malformedCert, key: p.ServerKey, ca: p.CA,
			wantErr: "malformed-cert.pem"},
		{name: "missing key file", cert: p.ServerCert, key: filepath.Join(dir, "missing-key.pem"), ca: p.CA,
			wantErr: "missing-key.pem"},
		{name: "key cert mismatch", cert: p.ServerCert, key: p2.ServerKey, ca: p.CA,
			wantErr: p2.ServerKey},
		{name: "missing CA file", cert: p.ServerCert, key: p.ServerKey, ca: filepath.Join(dir, "missing-ca.pem"),
			wantErr: "missing-ca.pem"},
		{name: "unreadable CA", cert: p.ServerCert, key: p.ServerKey, ca: unreadableCA,
			wantErr: "unreadable-ca.pem"},
		{name: "CA without PEM certificates", cert: p.ServerCert, key: p.ServerKey, ca: notPEMCA,
			wantErr: "not-pem-ca.pem"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildNodeTLSConfig(tt.cert, tt.key, tt.ca)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("buildNodeTLSConfig(%q,%q,%q) = %+v, want error containing %q",
						tt.cert, tt.key, tt.ca, cfg, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("buildNodeTLSConfig error = %q, want it to name %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildNodeTLSConfig(%q,%q,%q): %v", tt.cert, tt.key, tt.ca, err)
			}
			if cfg == nil {
				t.Fatal("buildNodeTLSConfig = nil config, want non-nil")
			}
			if len(cfg.Certificates) != 1 {
				t.Fatalf("len(Certificates) = %d, want 1", len(cfg.Certificates))
			}
			// Dual role: the same config serves (ClientCAs/ClientAuth) and
			// dials (RootCAs), so both pools must be populated.
			if cfg.RootCAs == nil {
				t.Fatal("RootCAs = nil, want the CA pool (peers dial with this config)")
			}
			if cfg.ClientCAs == nil {
				t.Fatal("ClientCAs = nil, want the CA pool (peer certs are verified with it)")
			}
			if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
				t.Fatalf("ClientAuth = %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
			}
			if cfg.MinVersion != tls.VersionTLS12 {
				t.Fatalf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
			}
		})
	}
}

// ============================================================
// Production-shaped node harness for the raft-TLS tests
// ============================================================

// freeLocalAddrs reserves n free 127.0.0.1 ports and releases them again so
// listeners created moments later can bind deterministic addresses (raft peers
// must know each other's address before the transport exists). All ports are
// held open until all are reserved, guaranteeing they are pairwise distinct.
func freeLocalAddrs(t *testing.T, n int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("freeLocalAddrs: %v", err)
		}
		listeners = append(listeners, l)
		addrs = append(addrs, l.Addr().String())
	}
	for _, l := range listeners {
		if err := l.Close(); err != nil {
			t.Fatalf("freeLocalAddrs: close: %v", err)
		}
	}
	return addrs
}

// genNodeLeaf issues one leaf under p's CA with BOTH ServerAuth and ClientAuth
// EKUs plus 127.0.0.1/localhost SANs — the certificate shape a raft node needs
// when the same pair serves (listener role) and dials (peer role) on the
// mutual-TLS plane. genTLSCerts' server leaf is ServerAuth-only, so it cannot
// be presented by a dialing peer.
func genNodeLeaf(t *testing.T, p certPaths) (certFile, keyFile string) {
	t.Helper()
	caPEM, err := os.ReadFile(p.CA)
	if err != nil {
		t.Fatalf("genNodeLeaf: read CA: %v", err)
	}
	caBlock, _ := pem.Decode(caPEM)
	if caBlock == nil {
		t.Fatal("genNodeLeaf: CA file contains no PEM block")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("genNodeLeaf: parse CA: %v", err)
	}
	caKeyPEM, err := os.ReadFile(p.CAKey)
	if err != nil {
		t.Fatalf("genNodeLeaf: read CA key: %v", err)
	}
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		t.Fatal("genNodeLeaf: CA key file contains no PEM block")
	}
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("genNodeLeaf: parse CA key: %v", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genNodeLeaf: generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject:      pkix.Name{CommonName: "vectra-test-node"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("genNodeLeaf: create leaf: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "node.pem")
	keyFile = filepath.Join(dir, "node-key.pem")
	writeTestPEM(t, certFile, "CERTIFICATE", der)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("genNodeLeaf: marshal key: %v", err)
	}
	writeTestPEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

// prodNode is a production-shaped test node: real (pre-reserved) addresses on
// both planes so registry hints and raft dials carry usable addresses, real
// BoltDB stores in a temp dir, and the real transport paths from New.
type prodNode struct {
	node     *Node
	url      string // real base URL of the HTTP API (http:// or https://)
	raftAddr string
}

// nodeSpec selects which planes run TLS for a prodNode. Empty cert fields keep
// that plane on today's plain path.
type nodeSpec struct {
	id   string
	join string

	// Raft plane mutual TLS (all three or none).
	raftCert, raftKey, raftCA string

	// HTTP plane TLS (cert+key or none).
	httpCert, httpKey string
}

// newProdNode assembles the node and serves its HTTP API on a pre-reserved
// address (plain or TLS listener, mirroring main.go). Port 8080 is occupied in
// this environment, so every listener binds a reserved free port instead.
func newProdNode(t *testing.T, s nodeSpec) *prodNode {
	t.Helper()
	ports := freeLocalAddrs(t, 2)
	raftAddr, httpAddr := ports[0], ports[1]
	n, err := New(Config{
		NodeID:       s.id,
		HTTPAddr:     httpAddr,
		RaftAddr:     raftAddr,
		DataDir:      t.TempDir(),
		Join:         s.join,
		SealInterval: 50 * time.Millisecond,
		NodeCert:     s.raftCert,
		NodeKey:      s.raftKey,
		NodeCACert:   s.raftCA,
		HTTPCert:     s.httpCert,
		HTTPKey:      s.httpKey,
		RaftConfig:   testRaftConfig(s.id),
	})
	if err != nil {
		t.Fatalf("New(%s): %v", s.id, err)
	}
	scheme := "http"
	var l net.Listener
	if httpTLS := n.HTTPTLSConfig(); httpTLS != nil {
		l, err = tls.Listen("tcp", httpAddr, httpTLS)
		scheme = "https"
	} else {
		l, err = net.Listen("tcp", httpAddr)
	}
	if err != nil {
		n.Close()
		t.Fatalf("listen %s: %v", httpAddr, err)
	}
	srv := &http.Server{Handler: n.Handler(), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(l) }()
	t.Cleanup(func() {
		_ = srv.Close()
		n.Close()
	})
	return &prodNode{node: n, url: scheme + "://" + l.Addr().String(), raftAddr: raftAddr}
}

// waitLeader blocks until pn's node wins its election (single-node bootstrap).
func waitLeader(t *testing.T, pn *prodNode, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) && !pn.node.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !pn.node.IsLeader() {
		t.Fatalf("%s not elected leader", pn.node.cfg.NodeID)
	}
}

// waitVoter blocks until id is a voter in n's view of the configuration.
func waitVoter(t *testing.T, n *Node, id string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) && !hasVoter(n, id) {
		time.Sleep(20 * time.Millisecond)
	}
	if !hasVoter(n, id) {
		t.Fatalf("%s never became a voter in %s's configuration", id, n.cfg.NodeID)
	}
}

// assertNeverVoterOnNode fails if id becomes a voter in n's configuration, n
// elects itself, or n ever publishes — the observable ways a rejected joiner
// could "converge" into a cluster it cannot actually talk to.
func assertNeverConverges(t *testing.T, n *Node, id string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if hasVoter(n, id) || n.IsLeader() || n.Published() != nil {
			t.Fatalf("%s converged into the cluster (self-voter=%v leader=%v published=%v)",
				n.cfg.NodeID, hasVoter(n, id), n.IsLeader(), n.Published() != nil)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// writeViaNode writes a batch via base's HTTP API, following the
// X-Vectra-Leader hint on 503 NOT_LEADER — the documented client behavior —
// so the test literally writes through a follower's API.
func writeViaNode(t *testing.T, base, key string) {
	t.Helper()
	target := base
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := json.Marshal(map[string]any{
			"nodes": []map[string]any{{"key": key, "label": "Service"}},
		})
		resp, err := http.Post(target+"/v1/batch", "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("POST %s/batch: %v", target, err)
		}
		_, _ = io.ReadAll(resp.Body) // drain for keep-alive reuse
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}
		if resp.StatusCode == http.StatusServiceUnavailable {
			if hint := resp.Header.Get("X-Vectra-Leader"); hint != "" {
				target = "http://" + hint
				if strings.HasPrefix(hint, "http") {
					target = hint
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("write via %s never returned 200", base)
}

// readNodeVia polls GET base/v1/nodes/{key} until 200 (bounded-stale reads:
// the node must publish first).
func readNodeVia(t *testing.T, base, key string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/v1/nodes/" + key)
		if err != nil {
			t.Fatalf("GET %s/nodes/%s: %v", base, key, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && strings.Contains(string(raw), key) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("read via %s never returned node %q", base, key)
}

// ============================================================
// (b) Two-node cluster, raft plane TLS / HTTP plane plain
// ============================================================

func TestTLSRaftTwoNodeCluster(t *testing.T) {
	p := genTLSCerts(t)
	nodeCert, nodeKey := genNodeLeaf(t, p)

	a := newProdNode(t, nodeSpec{id: "tls-a", raftCert: nodeCert, raftKey: nodeKey, raftCA: p.CA})
	waitLeader(t, a, 10*time.Second)

	// B joins over the still-plain HTTP plane; its raft plane is TLS too.
	b := newProdNode(t, nodeSpec{id: "tls-b", join: a.url, raftCert: nodeCert, raftKey: nodeKey, raftCA: p.CA})

	// Both nodes see each other (the membership/registry assertions of the
	// shared cluster-test pattern): each configuration holds both voters.
	waitVoter(t, a.node, "tls-b", 10*time.Second)
	waitVoter(t, b.node, "tls-a", 10*time.Second)
	waitVoter(t, b.node, "tls-b", 10*time.Second)

	// Replication proof over TLS: write via B's HTTP API (B is a follower;
	// its 503 carries the leader hint, which writeViaNode follows), then read
	// via A — and confirm B's own FSM received the write, which is the entry
	// that actually traveled the TLS raft plane.
	writeViaNode(t, b.url, "svc-tls")
	readNodeVia(t, a.url, "svc-tls")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if g := b.node.Published(); g != nil && g.Stats().Nodes == 1 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("B never published the write replicated over the TLS raft plane")
}

// ============================================================
// (c) Rejection: plain-raft joiner and foreign-CA joiner never converge
// ============================================================

func TestTLSRaftRejectsPlainTransportJoiner(t *testing.T) {
	p := genTLSCerts(t)
	nodeCert, nodeKey := genNodeLeaf(t, p)

	a := newProdNode(t, nodeSpec{id: "tls-a", raftCert: nodeCert, raftKey: nodeKey, raftCA: p.CA})
	waitLeader(t, a, 10*time.Second)
	b := newProdNode(t, nodeSpec{id: "tls-b", join: a.url, raftCert: nodeCert, raftKey: nodeKey, raftCA: p.CA})
	waitVoter(t, a.node, "tls-b", 10*time.Second)

	// C speaks plain TCP on the raft plane (no node-TLS flags) and tries to
	// join the TLS pair. The join POST may even return 200 (AddVoter commits
	// under the old configuration), but the leader can never replicate to C:
	// its TLS dials hit a plaintext listener. C must never converge.
	c := newProdNode(t, nodeSpec{id: "plain-c", join: a.url})
	assertNeverConverges(t, c.node, "plain-c", 3*time.Second)

	// The TLS pair is unaffected: both remain voters and still replicate.
	if !hasVoter(a.node, "tls-a") || !hasVoter(a.node, "tls-b") {
		t.Fatal("TLS pair lost voters after the rejected join")
	}
	writeViaNode(t, a.url, "svc-after-reject")
	readNodeVia(t, b.url, "svc-after-reject")
}

func TestTLSRaftRejectsForeignCAJoiner(t *testing.T) {
	p := genTLSCerts(t)     // the cluster's CA
	rogue := genTLSCerts(t) // a different throwaway CA
	nodeCert, nodeKey := genNodeLeaf(t, p)
	rogueCert, rogueKey := genNodeLeaf(t, rogue)

	a := newProdNode(t, nodeSpec{id: "tls-a", raftCert: nodeCert, raftKey: nodeKey, raftCA: p.CA})
	waitLeader(t, a, 10*time.Second)
	b := newProdNode(t, nodeSpec{id: "tls-b", join: a.url, raftCert: nodeCert, raftKey: nodeKey, raftCA: p.CA})
	waitVoter(t, a.node, "tls-b", 10*time.Second)

	// D runs TLS on the raft plane but presents a leaf signed by another CA:
	// neither side of the handshake trusts the other. Same negative: never
	// converges within a bounded wait.
	d := newProdNode(t, nodeSpec{id: "rogue-d", join: a.url,
		raftCert: rogueCert, raftKey: rogueKey, raftCA: rogue.CA})
	assertNeverConverges(t, d.node, "rogue-d", 3*time.Second)

	// The TLS pair is unaffected: both remain voters and still replicate.
	if !hasVoter(a.node, "tls-a") || !hasVoter(a.node, "tls-b") {
		t.Fatal("TLS pair lost voters after the foreign-CA join attempt")
	}
	writeViaNode(t, a.url, "svc-after-rogue")
	readNodeVia(t, b.url, "svc-after-rogue")
}

// ============================================================
// (d) Parity: no node-TLS flags keeps the plain TCP transport
// ============================================================

func TestTLSRaftPlainTCPParity(t *testing.T) {
	// No NodeCert/NodeKey/NodeCACert and no injected Transport: New must take
	// today's plain TCP path (raft.NewTCPTransport), byte-for-byte.
	pn := newProdNode(t, nodeSpec{id: "plain-tcp"})
	if _, ok := pn.node.trans.(*raft.NetworkTransport); !ok {
		t.Fatalf("default transport = %T, want *raft.NetworkTransport (plain TCP)", pn.node.trans)
	}
	waitLeader(t, pn, 10*time.Second)
	writeViaNode(t, pn.url, "svc-plain")
	readNodeVia(t, pn.url, "svc-plain")
}

// ============================================================
// Config field validation (all-or-nothing, fail-fast on bad files)
// ============================================================

func TestTLSRaftNewFailFastOnNodeTLSFields(t *testing.T) {
	p := genTLSCerts(t)
	nodeCert, nodeKey := genNodeLeaf(t, p)
	dir := t.TempDir()
	badCert := filepath.Join(dir, "node-bad-cert.pem")
	if err := os.WriteFile(badCert, []byte("garbage, not PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	badCA := filepath.Join(dir, "node-bad-ca.pem")
	if err := os.Mkdir(badCA, 0o755); err != nil {
		t.Fatal(err)
	}

	cfgWith := func(cert, key, ca string) Config {
		return Config{
			NodeID:     "node-tls-bad",
			HTTPAddr:   "127.0.0.1:0",
			RaftAddr:   "127.0.0.1:0",
			DataDir:    t.TempDir(),
			NodeCert:   cert,
			NodeKey:    key,
			NodeCACert: ca,
		}
	}

	// Any one or two of the three without the rest: error naming the missing
	// field(s). RaftAddr :0 with a nil Transport never gets that far.
	tests := []struct {
		name        string
		cert, key   string
		ca          string
		wantMissing []string
	}{
		{name: "cert only", cert: nodeCert, wantMissing: []string{"NodeKey", "NodeCACert"}},
		{name: "key only", key: nodeKey, wantMissing: []string{"NodeCert", "NodeCACert"}},
		{name: "ca only", ca: p.CA, wantMissing: []string{"NodeCert", "NodeKey"}},
		{name: "cert and key without ca", cert: nodeCert, key: nodeKey, wantMissing: []string{"NodeCACert"}},
		{name: "cert and ca without key", cert: nodeCert, ca: p.CA, wantMissing: []string{"NodeKey"}},
		{name: "key and ca without cert", key: nodeKey, ca: p.CA, wantMissing: []string{"NodeCert"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(cfgWith(tt.cert, tt.key, tt.ca))
			if err == nil {
				t.Fatal("New: want all-or-nothing error")
			}
			for _, m := range tt.wantMissing {
				if !strings.Contains(err.Error(), m) {
					t.Fatalf("New error = %q, want it to name missing field %q", err.Error(), m)
				}
			}
		})
	}

	t.Run("malformed cert file", func(t *testing.T) {
		_, err := New(cfgWith(badCert, nodeKey, p.CA))
		if err == nil {
			t.Fatal("New: want error for malformed node cert file")
		}
		if !strings.Contains(err.Error(), badCert) {
			t.Fatalf("New error = %q, want it to name %q", err.Error(), badCert)
		}
	})
	t.Run("unreadable CA file", func(t *testing.T) {
		_, err := New(cfgWith(nodeCert, nodeKey, badCA))
		if err == nil {
			t.Fatal("New: want error for unreadable node CA file")
		}
		if !strings.Contains(err.Error(), badCA) {
			t.Fatalf("New error = %q, want it to name %q", err.Error(), badCA)
		}
	})
}

// ============================================================
// (e) HTTPS-seed join: SSL_CERT_FILE makes the joiner trust the seed
// ============================================================

// Child-process markers for the join mechanism test below.
const (
	joinChildEnv = "VECTRA_TLS_JOIN_CHILD"
	seedURLEnv   = "VECTRA_TLS_SEED_URL"
	seedRaftEnv  = "VECTRA_TLS_SEED_RAFT"
	joinerNodeID = "https-joiner"
)

// TestTLSRaftJoinHTTPSSeed pins the documented self-signed join mechanism:
// a joining node whose Go x509 client trusts the seed's CA via SSL_CERT_FILE
// can join an HTTPS seed.
//
// The join itself runs in a re-exec'd child test process, not in-process:
// crypto/x509 loads the system root pool at most once per process, and on
// darwin ANY earlier certificate verification (the raft-TLS clusters in the
// groups above) loads it without the env var — after which SSL_CERT_FILE is
// ignored. A fresh process with the env var set from the start is also the
// production shape of the mechanism, and the child inherits the value this
// test sets via t.Setenv.
func TestTLSRaftJoinHTTPSSeed(t *testing.T) {
	if os.Getenv(joinChildEnv) == "1" {
		joinHTTPSSeedChild(t)
		return
	}
	p := genTLSCerts(t)

	// Seed with TLS on the HTTP plane (self-signed test CA); raft plane stays
	// plain so the focus is the join's client trust.
	seed := newProdNode(t, nodeSpec{id: "https-seed", httpCert: p.ServerCert, httpKey: p.ServerKey})
	waitLeader(t, seed, 10*time.Second)

	// In-process negative: a client that does not carry the CA cannot even
	// complete the handshake. The explicit empty root pool keeps the parent
	// process's system-root state untouched (it only matters for the parent,
	// but stays correct by construction).
	untrusting := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12},
	}}
	if resp, err := untrusting.Get(seed.url + "/v1/stats"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("untrusting joiner client: expected handshake failure, got %d %s", resp.StatusCode, body)
	}

	t.Setenv("SSL_CERT_FILE", p.CA)

	// Without the env var the join POST cannot even handshake.
	if _, stderr, err := runJoinChild(t, false, seed.url, seed.raftAddr); err == nil {
		t.Fatalf("child join without SSL_CERT_FILE unexpectedly succeeded: %s", stderr)
	}

	// With it, the production Join path succeeds end to end.
	if _, stderr, err := runJoinChild(t, true, seed.url, seed.raftAddr); err != nil {
		t.Fatalf("child join with SSL_CERT_FILE set: %v\n%s", err, stderr)
	}

	// The seed actually absorbed the joiner (its AddVoter committed).
	waitVoter(t, seed.node, joinerNodeID, 10*time.Second)
}

// runJoinChild re-exec's the test binary as the joiner node. withEnv=false
// scrubs SSL_CERT_FILE from the inherited environment; withEnv=true relies on
// t.Setenv having set it in the parent.
func runJoinChild(t *testing.T, withEnv bool, seedURL, seedRaft string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestTLSRaftJoinHTTPSSeed")
	env := os.Environ()
	if !withEnv {
		filtered := make([]string, 0, len(env))
		for _, kv := range env {
			if !strings.HasPrefix(kv, "SSL_CERT_FILE=") {
				filtered = append(filtered, kv)
			}
		}
		env = filtered
	}
	env = append(env,
		joinChildEnv+"=1",
		seedURLEnv+"="+seedURL,
		seedRaftEnv+"="+seedRaft,
	)
	cmd.Env = env
	var out, errBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errBuf
	err = cmd.Run()
	return out.String(), errBuf.String(), err
}

// joinHTTPSSeedChild runs inside the re-exec'd process: a full node (in-memory
// raft stores, real Join over HTTPS) joins the seed started by the parent.
func joinHTTPSSeedChild(t *testing.T) {
	seedURL := os.Getenv(seedURLEnv)
	if seedURL == "" {
		fmt.Fprintln(os.Stderr, "child: no seed URL in env")
		os.Exit(2)
	}
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "child: "+format+"\n", args...)
		os.Exit(1)
	}
	transAddr, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	n, err := New(Config{
		NodeID:        joinerNodeID,
		HTTPAddr:      "127.0.0.1:0",
		RaftAddr:      string(transAddr),
		DataDir:       t.TempDir(),
		Join:          seedURL,
		SealInterval:  50 * time.Millisecond,
		LogStore:      raft.NewInmemStore(),
		StableStore:   raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(),
		Transport:     trans,
		RaftConfig:    testRaftConfig(joinerNodeID),
	})
	if err != nil {
		fail("New: %v", err)
	}
	// New's join is non-fatal on failure, so pin the mechanism with an
	// explicit Join call plus the registry round trip from the response.
	if err := n.Join(seedURL); err != nil {
		n.Close()
		fail("join %s: %v", seedURL, err)
	}
	if seedRaft := os.Getenv(seedRaftEnv); seedRaft != "" {
		n.regMu.Lock()
		httpAddr := n.registry[seedRaft]
		n.regMu.Unlock()
		if httpAddr == "" {
			n.Close()
			fail("registry has no entry for the seed's raft address %s", seedRaft)
		}
	}
	n.Close()
	fmt.Println("JOIN_OK")
	os.Exit(0)
}
