# P2 (part 1): TLS + Backup Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the server-layer half of the P2 spec item: TLS on both the HTTP API and raft node-to-node planes (rqlite-style static certs, std-lib only), plus an online `GET /v1/backup` endpoint that streams a consistent snapshot archive (raft snapshot + node identity) restorable into a fresh data dir.

**Architecture:** TLS is pure configuration surface — `server.Config` gains cert-path fields loaded fail-fast in `server.New`; the HTTP plane switches `main.go` to `tls.Listen` + `srv.Serve` when configured (mTLS via optional client CA), and the raft plane injects a TLS `StreamLayer` transport at the existing `Config.Transport` seam. Backup reuses raft's own consistency machinery: `raft.Snapshot()` materializes an FSM snapshot on disk (the FSM is in-memory in server mode; raft snapshots ARE the persistence), the handler streams `tar.gz(meta.json + snapshots/<id>/…)`; restore = untar into an empty data dir and boot with the same node ID. No engine-core changes.

**Tech Stack:** Go 1.27, std-lib `crypto/tls`/`crypto/x509` only (no new dependencies anywhere); `hashicorp/raft` v1.8.0 (`NewNetworkTransport` over a TLS listener); test fixtures generate throwaway CAs/certs with `crypto/x509` + `crypto/ecdsa`.

**Spec:** `docs/research/2026-10-29-lightweight-adoption-assessment.md` §4 item 4 (P2) with the rqlite benchmark from `docs/research/2026-10-26-gap-fill-feasibility.md` §1 (external reference: `-http-cert/-http-key` optional mTLS + separate node-to-node X.509 `-node-cert/-node-key/-node-ca-cert`, all std-lib). Scope rulings (controller, from that reference): TLS covers BOTH planes; unique constraints are deliberately OUT of this arc (separate spec-level arc next); cert hot-reload is OUT (restart to rotate — document it).

## Global Constraints

- English only for comments, docs, commit messages.
- Zero new dependencies; std-lib only. Root engine module untouched.
- Work on branch `feat/p2-tls-backup` from `main@1bf90af`. No push. ff-merge to local `main` at arc end.
- With no TLS flags set, behavior must be byte-for-byte identical to today (plain HTTP, plain raft TCP) — the whole existing suite staying green is part of the gate.
- Cert/key/CA load failures must fail-fast in `server.New` (return error → `vectra-server` exits 1); never silently fall back to plaintext.
- Port 8080 on this machine is occupied — all test boots use free ports.
- Server API wire shapes of existing endpoints are untouchable. New endpoint error faces follow the existing `{"error":{"code","message"}}` envelope (`writeErr`).
- CHANGELOG gets a new `## [0.16.0] - Unreleased` section on top of `[0.15.0] - Unreleased`.
- Backup semantics warning that must appear in docs (Task 4): a restored backup must never run concurrently with the original cluster (same node ID, empty raft log = fresh term) — restore is for disaster recovery / new clusters, not fan-out.

## Code Facts (pinned — do not re-derive)

- HTTP startup: `server/cmd/vectra-server/main.go:51-71` builds `http.Server` + `ListenAndServe`; `server.Config` is in `server/node.go:43` (string fields, `New` returns `(*Node, error)`).
- Raft transport seam: `server/node.go:152-158` — `if cfg.Transport == nil { cfg.Transport, err = raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr) }`. Pool 3 / timeout 10s must be preserved for the TLS variant (`raft.NewNetworkTransport(listener, 3, 10*time.Second, os.Stderr)`).
- Join flow: `Node.Join(seed)` POSTs to the seed's HTTP address via the package's `httpPost` with a default-verification `http.Client`; auth bearer attaches when set (`server/api.go:26-49`).
- Auth middleware (`server/auth.go`) guards `/v1/*` per its existing rules — the new backup endpoint inherits them automatically; check how `/v1/health` is exempted and do not change it.
- FSM is in-memory in server mode (`server/fsm.go` `newFSM`: `live: vectra.New()`); persistence = raft log (`raft.db`, boltdb) + `raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)` snapshots dir + `meta.json` (node identity). FSM.Snapshot serializes the full `store.Data` (nodes, edges, IndexKeys, VectorDecls, FTSKeys).
- hashicorp/raft v1.8.0 `FileSnapshotStore` on-disk layout: each finalized snapshot lives under `snapshots/<id>/` with a meta file and a state file — CONFIRM exact filenames from the vendored source in the module cache (`$(go env GOMODCACHE)/github.com/hashicorp/raft@v1.8.0/file_snapshot.go`) before writing the tar code; `raft.Snapshot()` returns `*SnapshotFuture` whose `.Meta()` gives the ID; triggering it also runs the store's retain-2 cleanup (existing behavior, fine).

## Review Focus

1. **No-flags parity:** with no cert flags, plain HTTP + plain raft TCP, existing logs and wire behavior unchanged. — pinned by the whole existing suite staying green (Task 1/2 verification steps) plus an explicit test that boots a no-TLS node and asserts a plain `http://` client works (`Task 1 Step 1d`).
2. **Fail-fast on bad certs:** unreadable/malformed cert, key, or CA file → `server.New` returns an error naming the file. — Task 1/2 table tests.
3. **mTLS actually rejects:** HTTP plane with client CA: request without a client cert fails the TLS handshake (no 200/401 — connection error); raft plane with node CA: a plaintext (or wrong-CA) peer cannot join or talk to the cluster. — Task 1 Step 1c, Task 2 Step 1c.
4. **Backup round-trip completeness:** nodes, edges, typed-index declarations, FTS keys, and vector index declarations all survive backup → restore → reboot, and the restored node accepts new writes. — Task 3 Step 1b.
5. **Backup edge faces:** non-leader returns 503 `NOT_LEADER` with `X-Vectra-Leader` hint (same shape as `/v1/join`); with auth enabled and no token → 401 before any snapshot work. — Task 3 Step 1c.

---

### Task 1: TLS on the HTTP plane (HTTPS + optional client-cert mTLS)

**Files:**
- Create: `server/tls.go`
- Create: `server/testcert_test.go` (shared throwaway-cert helper for Tasks 1–2)
- Modify: `server/node.go` (Config fields + load in `New`)
- Modify: `server/cmd/vectra-server/main.go` (flags + `tls.Listen`/`srv.Serve` path)
- Test: `server/tls_test.go`

**Interfaces:**
- Consumes: `server.Config` (node.go:43), `New(...) (*Node, error)` error contract, main.go's `http.Server` block.
- Produces (Task 2 and 4 depend on these):

```go
// server.Config gains (empty string = off, byte-for-byte legacy behavior):
    HTTPCert    string // PEM cert file for the HTTP API; requires HTTPKey
    HTTPKey     string // PEM key file for the HTTP API
    HTTPClientCA string // optional PEM CA; when set, clients must present a cert (mTLS)

// server/tls.go
// buildHTTPTLSConfig returns a *tls.Config with the leaf pair loaded and
// (when clientCA != "") ClientCAs populated + ClientAuth = RequireAndVerifyClientCert.
// MinVersion: TLS 1.2. Any unreadable/malformed file → error naming the path.
func buildHTTPTLSConfig(certFile, keyFile, clientCA string) (*tls.Config, error)
// buildNodeTLSConfig returns a *tls.Config usable as BOTH server and client
// (raft peers dial each other): leaf pair + RootCAs=ClientCAs=ca +
// ClientAuth=RequireAndVerifyClientCert. MinVersion: TLS 1.2.
func buildNodeTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error)

// Node exposes the parsed HTTP TLS config so main.go can build its listener:
func (n *Node) HTTPTLSConfig() *tls.Config // nil when HTTP TLS is off

// server/testcert_test.go (test-only, reused by Task 2):
// genTLSCerts generates into t.TempDir(): a CA (ca.pem/ca-key.pem), a server
// leaf (server.pem/server-key.pem, SANs: 127.0.0.1, localhost), and a client
// leaf (client.pem/client-key.pem); returns the paths struct.
type certPaths struct{ CA, CAKey, ServerCert, ServerKey, ClientCert, ClientKey string }
func genTLSCerts(t *testing.T) certPaths // crypto/ecdsa P-256, 1h validity, t.Cleanup nothing
```

main.go behavior pin: when `HTTPTLSConfig() != nil`, serve via `tls.Listen("tcp", httpAddr, cfg)` + `srv.Serve(l)`; otherwise today's `ListenAndServe` unchanged. Startup log line gains `tls=http` when on (stderr, same format family as the existing banner).

Flags: `-http-cert`, `-http-key`, `-http-client-ca` (defaults empty; `-http-cert` without `-http-key` or vice versa is a startup error).

- [ ] **Step 1: Write failing tests** — `server/tls_test.go`: (a) `buildHTTPTLSConfig` table test: valid pair → non-nil config with 1 certificate; each of {missing cert file, malformed PEM cert, missing key, key/cert mismatch, unreadable CA} → error containing the offending path; clientCA set → `ClientAuth == tls.RequireAndVerifyClientCert`. (b) boot a real `server.New` node (existing test bootstrap pattern, free ports, temp dir) with TLS: an `https://` client using the CA connects and `/v1/health` returns 200; the same client WITHOUT trusting the CA fails the handshake. (c) mTLS: with `HTTPClientCA` set, a client presenting `genTLSCerts`' client leaf gets 200; a client with no cert gets a connection error (not an HTTP status). (d) no-flags parity: boot without TLS, plain `http://` health check 200 (guards the legacy path). (e) fail-fast: `server.New` with `-http-cert` pointing at a malformed file returns an error naming the file. Also `server/testcert_test.go` with `genTLSCerts`.
- [ ] **Step 2: Run, verify FAIL** — `cd server && go test -run 'TLS' ./... ` → build failure (no tls.go).
- [ ] **Step 3: Implement** `server/tls.go`, Config fields + load in `New`, `HTTPTLSConfig`, main.go flags + listener switch, `testcert_test.go`.
- [ ] **Step 4: Run, verify PASS** — `cd server && go test ./...` (full module: new tests green, everything existing green = parity evidence).
- [ ] **Step 5: Commit** — `git commit -am "feat(server): HTTPS with optional client-cert mTLS on the HTTP API"`

### Task 2: TLS on the raft plane (node-to-node X.509, mutual)

**Files:**
- Modify: `server/tls.go` (transport wiring helper)
- Modify: `server/node.go` (Config fields + Transport injection at node.go:152-158)
- Modify: `server/cmd/vectra-server/main.go` (flags `-node-cert`, `-node-key`, `-node-ca-cert`)
- Test: `server/tls_raft_test.go`

**Interfaces:**
- Consumes: `buildNodeTLSConfig` (Task 1), `genTLSCerts` (`testcert_test.go`), the existing two-node cluster test bootstrap pattern (grep `server/cluster_test.go` / `auth_cluster_test.go` for how nodes are joined).
- Produces: `func newTLSTransport(addr string, tc *tls.Config) (raft.Transport, error)` — wraps `tls.Listen("tcp", addr, tc)` in `raft.NewNetworkTransport(l, 3, 10*time.Second, os.Stderr)` (same pool/timeout as the TCP default it replaces). Task 4 documents the flags.

`server.Config` gains `NodeCert, NodeKey, NodeCACert string` (empty = plain TCP transport, legacy). All-or-nothing: any one set without the other two → startup error naming what's missing. When set, `New` uses `newTLSTransport` instead of `NewTCPTransport` (the `cfg.Transport == nil` injection point stays overridable for tests).

- [ ] **Step 1: Write failing tests** — `server/tls_raft_test.go`: (a) `buildNodeTLSConfig` table: valid triple → config with leaf + RootCAs + `ClientAuth == RequireAndVerifyClientCert`; bad file → error naming path. (b) two-node TLS cluster: node A (seed) and node B with node-cert flags from `genTLSCerts`, join over the (still-plain) HTTP plane, wait for both to see each other (`alreadyMember`/registry assertions per cluster_test.go pattern), then write via B's HTTP API → read via A (replication proof over TLS). (c) rejection: a third node configured with PLAIN raft transport cannot join the TLS pair (join fails or membership never converges — assert the negative within a bounded wait), and a node presenting a leaf signed by a different throwaway CA is likewise rejected. (d) parity: existing cluster tests (plain) still pass untouched. (e) HTTPS-seed join: seed with HTTP TLS on; joining node's client trusts the CA via `t.Setenv("SSL_CERT_FILE", paths.CA)` → join succeeds (pins the documented self-signed join mechanism).
- [ ] **Step 2: Run, verify FAIL** — `cd server && go test -run 'TLSRaft|TLS' ./...` → FAIL (no transport wiring).
- [ ] **Step 3: Implement** per Interfaces. Note: `tls.Listen` needs `cfg.RaftAddr` as-is (`:9000` style works — listener binds all interfaces; dial addresses come from raft server config).
- [ ] **Step 4: Run, verify PASS** — `cd server && go test ./...` full module green (cluster tests included).
- [ ] **Step 5: Commit** — `git commit -am "feat(server): X.509 mutual TLS for raft node-to-node traffic"`

### Task 3: `GET /v1/backup` online snapshot stream

**Files:**
- Create: `server/backup.go`
- Test: `server/backup_test.go`

**Interfaces:**
- Consumes: `n.r *raft.Raft` (Snapshot), the FileSnapshotStore layout (see Code Facts — confirm filenames from module-cache source first), `writeErr`, auth middleware behavior, `n.IsLeader()`/`n.LeaderHint()` (same faces as handleJoin).
- Produces (Task 4 documents):

```go
// server/backup.go
// handleBackup streams a tar.gz of the node's latest raft snapshot plus its
// meta.json identity: { "meta.json": <node meta bytes>, "snapshots/<id>/<meta>", "snapshots/<id>/<state>" }.
// Non-leader → 503 NOT_LEADER + X-Vectra-Leader hint (join's shape).
// raft.Snapshot() error → 500 INTERNAL. Auth applies per existing middleware.
// Response headers: Content-Type: application/gzip,
// Content-Disposition: attachment; filename="vectra-backup-<snapshotID>.tar.gz".
func (n *Node) handleBackup(w http.ResponseWriter, r *http.Request)
// mux registration in api.go next to the other GET /v1 routes.
```

Tar writing detail: after `fut := n.r.Snapshot(); fut.Error()`, read the snapshot dir under `<DataDir>/snapshots/<fut.Meta().ID>/`; stream files with `gzip.Writer`→`tar.Writer` (no buffering whole archive in memory; set `Content-Length` only if trivially known — streaming without it is fine). Restore contract for docs/tests: untar into an empty dir, start `vectra-server -id <same-ID> -data <dir>` — raft finds the snapshot, FSM restores, node serves fresh-term.

- [ ] **Step 1: Write failing tests** — `server/backup_test.go`: (a) happy path: single node, write nodes+edges via `/v1/batch`, declare a typed index + FTS key + vector index, `GET /v1/backup` → 200, `application/gzip`, body gunzips to a tar containing exactly the pinned entries; (b) round-trip: untar into a temp dir (add `meta.json` from the archive), boot a NEW `server.New` with the same NodeID on that dir + free ports, assert via `/v1/cypher` reads that nodes/edges survive AND the index/FTS/vector declarations survive (their behavior — typed lookup, fulltext hit, vector search — not just files), then write NEW data through the restored node and read it back; (c) faces: non-leader (second node of a 2-node cluster, or assert via a node that hasn't won election within a bound) → 503 NOT_LEADER with hint header; auth-enabled node without token → 401; (d) header check: Content-Disposition contains the snapshot ID.
- [ ] **Step 2: Run, verify FAIL** — `cd server && go test -run Backup ./...` → FAIL (no backup.go).
- [ ] **Step 3: Implement** `server/backup.go` + mux line in `api.go`.
- [ ] **Step 4: Run, verify PASS** — `cd server && go test ./...`.
- [ ] **Step 5: Commit** — `git commit -am "feat(server): GET /v1/backup online raft-snapshot stream (tar.gz)"`

### Task 4: Docs — TLS configuration, backup/restore runbook, CHANGELOG

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md` (parity)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: Tasks 1–3 flag names and endpoint, the SSL_CERT_FILE / NODE_EXTRA_CA_CERTS / `ssl` std-lib facts below.
- Produces: user-facing docs; Task 5 verifies links.

Client-trust facts to state (no code changes anywhere — this is the ruling): Go clients (`vectra-cli`, `vectra-mcp`) honor `SSL_CERT_FILE` for custom CAs on Unix; Node (`sdk/ts`, `vectra-mcp` TS flavor) honors `NODE_EXTRA_CA_CERTS`; Python honors `SSL_CERT_FILE`/`SSL_CERT_DIR`.

- [ ] **Step 1: README.md** — inside "Server mode (optional module)": a **TLS** subsection (three-plane picture: HTTP API `-http-cert/-http-key[-http-client-ca]`, raft `-node-cert/-node-key/-node-ca-cert`, all std-lib, restart-to-rotate; client trust table per the facts above; self-signed join via `SSL_CERT_FILE` on the joining node) and a **Backup & restore** subsection (`curl -f localhost:8080/v1/backup -o b.tar.gz` with optional `-H 'Authorization: Bearer …'`; restore = `mkdir newdir && tar -xzf b.tar.gz -C newdir && vectra-server -id <same-ID> -data newdir`; the never-run-concurrently-with-original-cluster warning verbatim from Global Constraints; note backup requires the leader). Repository layout untouched (no new top-level dirs). Benchmarks/other sections untouched.
- [ ] **Step 2: README.zh-CN.md** — parity translation (flags/endpoints/paths stay English).
- [ ] **Step 3: CHANGELOG.md** — new top `## [0.16.0] - Unreleased`: Added (HTTPS + optional mTLS on the HTTP API; X.509 mutual TLS node-to-node; `GET /v1/backup` streaming tar.gz raft snapshot + restore runbook), voice matching 0.15.0.
- [ ] **Step 4: Link + claim check** — every relative link resolves; every flag/endpoint string in the docs greps true against `server/` code.
- [ ] **Step 5: Commit** — `git commit -am "docs(readme): TLS configuration and backup/restore runbook; changelog 0.16.0"`

### Task 5: Full-branch verification

**Files:** none (verification only; fixes fold back into the owning task)

- [ ] **Step 1: Suites** — `make test vet` (three modules); root `go test -race ./... -count=1 -timeout 40m`; root `go test -run 'TestCypher.*Allocs' -count=1 .` (TLS/backup touch no read path — alloc gates must be untouched).
- [ ] **Step 2: Arc-specific** — the CI `examples` job block still passes (rebuild server binary with new flags available, boot, run examples verbatim); `git log --oneline main..HEAD` shows only planned commits.
- [ ] **Step 3: Record** — failures loop back to the owning task; all-green is the merge precondition.

---

## Execution Notes

- Final review: code-reviewer subagent over the whole branch, one fix wave, ff-merge `feat/p2-tls-backup` → local `main` (no push), per the established process.
- The unique-constraint half of P2 is a separate later arc and MUST get its own spec-level design doc first (gap-fill report §5 ruling) — do not let it leak into this branch.
