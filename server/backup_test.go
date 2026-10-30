package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ============================================================
// GET /v1/backup (Task 3): online raft-snapshot stream.
//
// These tests boot production-shaped nodes: the nil LogStore /
// SnapshotStore / Transport Config selects the real defaults (bolt
// raft.db + FileSnapshotStore + TCP transport), because the endpoint
// streams the FileSnapshotStore's on-disk layout — an InmemSnapshotStore
// node has nothing on disk to stream.
// ============================================================

// backupNode is a production-shaped node for backup tests: real free
// addresses on both planes (registry hints carry usable addresses), real
// BoltDB + FileSnapshotStore persistence under the caller's data dir.
type backupNode struct {
	node     *Node
	url      string
	raftAddr string
}

// bootBackupNode reserves two free ports (raft + HTTP), runs New with the
// production persistence defaults and serves the HTTP API on the second
// port. Empty join = single-node bootstrap; empty tokenFile = auth off.
// dataDir is caller-owned: the round-trip test points it at an untarred
// backup so raft boots from the streamed snapshot.
func bootBackupNode(t *testing.T, id, dataDir, join, tokenFile string) *backupNode {
	t.Helper()
	ports := freeLocalAddrs(t, 2)
	// HTTPAddr is advertised verbatim in the node registry, so it carries
	// the scheme — that is what makes X-Vectra-Leader hints equal the
	// joined peer's real base URL in the non-leader face test.
	n, err := New(Config{
		NodeID:        id,
		HTTPAddr:      "http://" + ports[1],
		RaftAddr:      ports[0],
		DataDir:       dataDir,
		Join:          join,
		SealInterval:  50 * time.Millisecond,
		RaftConfig:    testRaftConfig(id),
		AuthTokenFile: tokenFile,
	})
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	l, err := net.Listen("tcp", ports[1])
	if err != nil {
		n.Close()
		t.Fatalf("listen %s: %v", ports[1], err)
	}
	srv := &http.Server{Handler: n.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() {
		_ = srv.Close()
		n.Close()
	})
	return &backupNode{node: n, url: "http://" + l.Addr().String(), raftAddr: ports[0]}
}

// waitBackupLeader blocks until bn's node wins its election.
func waitBackupLeader(t *testing.T, bn *backupNode, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) && !bn.node.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !bn.node.IsLeader() {
		t.Fatalf("%s not elected leader within %s", bn.node.cfg.NodeID, within)
	}
}

// waitBackupPublished blocks until the published graph carries at least the
// wanted node/edge counts (reads serve from the published clone).
func waitBackupPublished(t *testing.T, bn *backupNode, nodes, edges int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if p := bn.node.Published(); p != nil &&
			p.NodeCount() >= nodes && p.EdgeCount() >= edges {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("published graph never reached %d nodes / %d edges within %s", nodes, edges, within)
}

// seedBackupGraph writes the fixture the happy-path tests share: nodes +
// edge via /v1/batch, a typed index + an FTS key via cypher DDL, and a dim-4
// vector index whose vectors ride the batch nodes' "emb" prop (declared
// first so the batch's own nodes ingest through it, the api_vector_test
// ordering). bearer rides every call ("" is harmless when auth is off).
func seedBackupGraph(t *testing.T, base, bearer string) {
	t.Helper()
	resp, out := postJSONAuth(t, base+"/v1/vector/indexes", bearer, map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create vector index: %d %v", resp.StatusCode, out)
	}
	resp, out = postJSONAuth(t, base+"/v1/batch", bearer, map[string]any{
		"nodes": []map[string]any{
			{"key": "svc", "label": "Service", "props": map[string]any{
				"env":  "prod",
				"desc": "payment gateway",
				"emb":  []float32{1, 0, 0, 0},
			}},
			{"key": "db", "label": "Store", "props": map[string]any{
				"env":  "dev",
				"desc": "ledger store",
				"emb":  []float32{0, 1, 0, 0},
			}},
		},
		"edges": []map[string]any{{"srcKey": "svc", "dstKey": "db", "label": "DEPENDS_ON"}},
	})
	if resp.StatusCode != http.StatusOK || out["nodesApplied"] != float64(2) {
		t.Fatalf("seed batch: %d %v", resp.StatusCode, out)
	}
	for _, ddl := range []string{
		`CREATE INDEX ON (n.env)`,
		`CREATE FULLTEXT INDEX ON (n.desc)`,
	} {
		resp, out = postJSONAuth(t, base+"/v1/cypher", bearer, map[string]any{"query": ddl})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("seed DDL %q: %d %v", ddl, resp.StatusCode, out)
		}
	}
}

// fetchBackup GETs /v1/backup (optionally with a bearer token) and returns
// the response with the full body read.
func fetchBackup(t *testing.T, base, bearer string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/v1/backup", nil)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s/v1/backup: %v", base, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read backup body: %v", err)
	}
	return resp, body
}

// snapshotIDFromDisposition extracts <id> from
// `attachment; filename="vectra-backup-<id>.tar.gz"`.
func snapshotIDFromDisposition(t *testing.T, cd string) string {
	t.Helper()
	const pre, suf = `attachment; filename="vectra-backup-`, `.tar.gz"`
	if !strings.HasPrefix(cd, pre) || !strings.HasSuffix(cd, suf) {
		t.Fatalf("Content-Disposition = %q, want %s<id>%s", cd, pre, suf)
	}
	id := strings.TrimSuffix(strings.TrimPrefix(cd, pre), suf)
	if id == "" {
		t.Fatalf("Content-Disposition = %q carries an empty snapshot id", cd)
	}
	return id
}

// backupTarEntries gunzips the body and returns the ordered regular-file
// entry names, failing on directory or non-regular records (the archive is
// pinned to exactly the three file entries, no dir records).
func backupTarEntries(t *testing.T, body []byte) []string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("backup body is not valid gzip: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar walk: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			t.Fatalf("archive holds a non-regular entry %q (type %q)", hdr.Name, string(hdr.Typeflag))
		}
		names = append(names, hdr.Name)
	}
	return names
}

// untarBackupInto expands the backup body's regular-file entries under dir
// (creating parent directories), the documented restore contract's first
// half: untar into an empty dir, then start with the same NodeID.
func untarBackupInto(t *testing.T, body []byte, dir string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("backup body is not valid gzip: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatalf("tar walk: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			t.Fatalf("archive holds a non-regular entry %q", hdr.Name)
		}
		target := filepath.Join(dir, filepath.FromSlash(hdr.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, hdr.FileInfo().Mode())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			t.Fatalf("untar %s: %v", hdr.Name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// cypherRows runs one read statement through /v1/cypher and returns the
// rows, failing on any non-200 or parse face.
func cypherRows(t *testing.T, base, query string) []map[string]any {
	t.Helper()
	resp, out := postJSON(t, base+"/v1/cypher", map[string]any{"query": query})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cypher %q: %d %v", query, resp.StatusCode, out)
	}
	raw, ok := out["rows"].([]any)
	if !ok {
		t.Fatalf("cypher %q response lacks rows: %v", query, out)
	}
	rows := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		m, ok := r.(map[string]any)
		if !ok {
			t.Fatalf("cypher %q row is not an object: %v", query, r)
		}
		rows = append(rows, m)
	}
	return rows
}

// (a) Happy path: 200, application/gzip, streamed (no Content-Length), and
// the tar holds exactly the three pinned entries — node identity plus the
// materialized FileSnapshotStore snapshot, nothing else (no raft.db, no
// directory records).
func TestBackupHappyPathTarEntries(t *testing.T) {
	bn := bootBackupNode(t, "bk-a", t.TempDir(), "", "")
	waitBackupLeader(t, bn, 10*time.Second)
	seedBackupGraph(t, bn.url, "")
	waitBackupPublished(t, bn, 2, 1, 5*time.Second)

	resp, body := fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/backup = %d (%s)", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("Content-Type = %q, want application/gzip", ct)
	}
	// No Content-Length assertion: net/http computes one itself when a
	// small handler body completes before the buffer flushes. The
	// no-full-buffer property is structural in the handler (io.Copy from
	// each file into the tar/gzip writers, never into memory).
	id := snapshotIDFromDisposition(t, resp.Header.Get("Content-Disposition"))
	want := []string{
		"meta.json",
		"snapshots/" + id + "/meta.json",
		"snapshots/" + id + "/state.bin",
	}
	if got := backupTarEntries(t, body); !slices.Equal(got, want) {
		t.Fatalf("tar entries = %v, want exactly %v", got, want)
	}
}

// (d) The Content-Disposition filename carries the real snapshot ID: the ID
// it names is the very directory the snapshot entries live under.
func TestBackupDispositionCarriesSnapshotID(t *testing.T) {
	bn := bootBackupNode(t, "bk-d", t.TempDir(), "", "")
	waitBackupLeader(t, bn, 10*time.Second)
	seedBackupGraph(t, bn.url, "")
	waitBackupPublished(t, bn, 2, 1, 5*time.Second)

	resp, body := fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/backup = %d", resp.StatusCode)
	}
	id := snapshotIDFromDisposition(t, resp.Header.Get("Content-Disposition"))
	// The disposition's ID must be the archived snapshot's directory: the
	// only snapshots/<...> prefix in the entry set.
	sawSnap := false
	for _, name := range backupTarEntries(t, body) {
		if rest, ok := strings.CutPrefix(name, "snapshots/"); ok {
			sawSnap = true
			dir, _, found := strings.Cut(rest, "/")
			if !found {
				t.Fatalf("entry %q has no file under snapshots/<id>/", name)
			}
			if dir != id {
				t.Fatalf("entry %q lives under snapshot dir %q, want the disposition id %q", name, dir, id)
			}
		}
	}
	if !sawSnap {
		t.Fatalf("archive holds no snapshots/<id>/ entries for disposition id %q", id)
	}
}

// (b) Round-trip: untar the stream into an empty dir, boot a NEW server.New
// with the SAME NodeID on it (+ free ports, no join) — raft finds the
// snapshot, the FSM restores — then assert by behavior through /v1/cypher
// and the vector endpoint: nodes, edges, typed lookup, fulltext hit, vector
// top-1 all still work, and the restored node accepts NEW writes.
func TestBackupRestoreRoundTrip(t *testing.T) {
	const nodeID = "bk-rt"
	src := bootBackupNode(t, nodeID, t.TempDir(), "", "")
	waitBackupLeader(t, src, 10*time.Second)
	seedBackupGraph(t, src.url, "")
	waitBackupPublished(t, src, 2, 1, 5*time.Second)

	_, body := fetchBackup(t, src.url, "")
	restoreDir := t.TempDir()
	untarBackupInto(t, body, restoreDir)
	if st, err := os.Stat(filepath.Join(restoreDir, "meta.json")); err != nil || st.Size() == 0 {
		t.Fatalf("untarred meta.json missing or empty: %v", err)
	}

	rn := bootBackupNode(t, nodeID, restoreDir, "", "")
	waitBackupLeader(t, rn, 10*time.Second)
	waitBackupPublished(t, rn, 2, 1, 5*time.Second)

	// Nodes survive.
	var keys []string
	for _, row := range cypherRows(t, rn.url, `MATCH (n) RETURN n.key`) {
		keys = append(keys, row["n.key"].(string))
	}
	if len(keys) != 2 || !slices.Contains(keys, "svc") || !slices.Contains(keys, "db") {
		t.Fatalf("restored node keys = %v, want [svc db]", keys)
	}
	// Edges survive.
	edges := cypherRows(t, rn.url, `MATCH (a)-[:DEPENDS_ON]->(b) RETURN a.key, b.key`)
	if len(edges) != 1 || edges[0]["a.key"] != "svc" || edges[0]["b.key"] != "db" {
		t.Fatalf("restored DEPENDS_ON edge = %v, want svc->db", edges)
	}
	// Typed index declaration survives (behavior, not files).
	typed := cypherRows(t, rn.url, `MATCH (n {env: 'prod'}) RETURN n.key`)
	if len(typed) != 1 || typed[0]["n.key"] != "svc" {
		t.Fatalf("typed lookup env='prod' = %v, want [svc]", typed)
	}
	// Fulltext declaration survives.
	fts := cypherRows(t, rn.url, `MATCH (n) WHERE n.desc CONTAINS 'payment' RETURN n.key`)
	if len(fts) != 1 || fts[0]["n.key"] != "svc" {
		t.Fatalf("fulltext CONTAINS 'payment' = %v, want [svc]", fts)
	}
	// Vector index declaration + vectors survive: top-1 for the exact match.
	resp, out := postJSON(t, rn.url+"/v1/vector/search", map[string]any{
		"index": "mem", "query_vector": []float32{1, 0, 0, 0}, "k": 1,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("restored vector search: %d %v", resp.StatusCode, out)
	}
	if hits := hitKeys(t, out); len(hits) != 1 || hits[0] != "svc" {
		t.Fatalf("restored vector top-1 = %v, want [svc]", hits)
	}

	// The restored node is a full member: NEW writes commit and read back.
	resp, out = postJSON(t, rn.url+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "cache", "label": "Cache"}},
		"edges": []map[string]any{{"srcKey": "svc", "dstKey": "cache", "label": "DEPENDS_ON"}},
	})
	if resp.StatusCode != http.StatusOK || out["nodesApplied"] != float64(1) {
		t.Fatalf("post-restore write: %d %v", resp.StatusCode, out)
	}
	waitBackupPublished(t, rn, 3, 2, 5*time.Second)
	resp, out = getJSON(t, rn.url+"/v1/nodes/cache")
	if resp.StatusCode != http.StatusOK || out["key"] != "cache" {
		t.Fatalf("post-restore read of new node: %d %v", resp.StatusCode, out)
	}
}

// (c1) Non-leader face: the second node of a two-node cluster answers 503
// NOT_LEADER with the leader-hint header (join's shape), once it has
// observed the leader so the hint is populated.
func TestBackupNonLeaderFace(t *testing.T) {
	n1 := bootBackupNode(t, "bk-l1", t.TempDir(), "", "")
	waitBackupLeader(t, n1, 10*time.Second)
	n2 := bootBackupNode(t, "bk-l2", t.TempDir(), n1.url, "")

	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, body := fetchBackup(t, n2.url, "")
		if resp.StatusCode == http.StatusServiceUnavailable &&
			resp.Header.Get("X-Vectra-Leader") == n1.url {
			if ra := resp.Header.Get("Retry-After"); ra != "1" {
				t.Fatalf("Retry-After = %q, want 1", ra)
			}
			var out struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("503 body: %v (%s)", err, body)
			}
			if out.Error.Code != "NOT_LEADER" {
				t.Fatalf("503 code = %q, want NOT_LEADER", out.Error.Code)
			}
			return
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("follower GET /v1/backup = %d (%s), want 503", resp.StatusCode, body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never surfaced the leader hint (last hint %q)",
				resp.Header.Get("X-Vectra-Leader"))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// (c2) Auth face: on an auth-enabled node /v1/backup inherits the bearer
// middleware — no token is the standard 401; the valid token streams.
func TestBackupAuthFace(t *testing.T) {
	tf := writeTokenFile(t, "tok-backup\n")
	bn := bootBackupNode(t, "bk-auth", t.TempDir(), "", tf)
	waitBackupLeader(t, bn, 10*time.Second)
	// One write, so a snapshot exists to stream (see
	// TestBackupEmptyLeaderInternalFace for the zero-write edge).
	seedBackupGraph(t, bn.url, "tok-backup")
	waitBackupPublished(t, bn, 2, 1, 5*time.Second)

	resp, body := fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-token GET /v1/backup = %d (%s), want 401", resp.StatusCode, body)
	}
	var out struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.Error.Code != "UNAUTHORIZED" {
		t.Fatalf("401 body = %s, want code UNAUTHORIZED", body)
	}

	resp, body = fetchBackup(t, bn.url, "tok-backup")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token GET /v1/backup = %d (%s), want 200", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("Content-Type = %q, want application/gzip", ct)
	}
}

// (c3) Dataless-leader edge, pinned as found: a leader that has never
// applied an entry has no snapshot on disk, and raft v1.8.0's user-snapshot
// path keeps answering ErrNothingNewToSnapshot (the election no-op alone
// never advances the FSM watermark on a peerless node). The endpoint
// surfaces the pinned 500 INTERNAL face; the remedy is to write once, after
// which backups succeed.
func TestBackupEmptyLeaderInternalFace(t *testing.T) {
	bn := bootBackupNode(t, "bk-empty", t.TempDir(), "", "")
	waitBackupLeader(t, bn, 10*time.Second)

	resp, body := fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("dataless GET /v1/backup = %d (%s), want 500", resp.StatusCode, body)
	}
	var out struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil ||
		out.Error.Code != "INTERNAL" || !strings.Contains(out.Error.Message, "nothing new to snapshot") {
		t.Fatalf("500 body = %s, want INTERNAL / nothing new to snapshot", body)
	}

	// Remedy check: one committed write flips the endpoint to streaming.
	seedBackupGraph(t, bn.url, "")
	waitBackupPublished(t, bn, 2, 1, 5*time.Second)
	resp, body = fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-write GET /v1/backup = %d (%s), want 200", resp.StatusCode, body)
	}
}

// backupErrBody decodes a writeErr JSON body into code + message.
func backupErrBody(t *testing.T, body []byte) (code, msg string) {
	t.Helper()
	var out struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("error body %s: %v", body, err)
	}
	return out.Error.Code, out.Error.Message
}

// (c4) Snapshot-materialization 500 stays path-free: with the snapshots
// store directory unwritable, raft's Create fails with a *os.PathError over
// the absolute DataDir path — the response body must collapse that to the
// generic reason and echo neither the DataDir nor the snapshot dir layout.
func TestBackupCreateFailureFaceScrubbed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits are advisory for root; the scrub cannot be forced")
	}
	dataDir := t.TempDir()
	bn := bootBackupNode(t, "bk-creatfail", dataDir, "", "")
	waitBackupLeader(t, bn, 10*time.Second)
	seedBackupGraph(t, bn.url, "")
	waitBackupPublished(t, bn, 2, 1, 5*time.Second)

	snapDir := filepath.Join(dataDir, "snapshots")
	if err := os.Chmod(snapDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(snapDir, 0o755) })

	resp, body := fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unwritable-store GET /v1/backup = %d (%s), want 500", resp.StatusCode, body)
	}
	code, msg := backupErrBody(t, body)
	if code != "INTERNAL" || msg != "snapshot: failed" {
		t.Fatalf("500 face = %s %q, want INTERNAL \"snapshot: failed\"", code, msg)
	}
	if strings.Contains(msg, dataDir) || strings.Contains(msg, snapDir) {
		t.Fatalf("500 message %q leaks the server-side path", msg)
	}
	// Remedy check: writable store restores the streaming face.
	if err := os.Chmod(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	resp, body = fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recovered GET /v1/backup = %d (%s), want 200", resp.StatusCode, body)
	}
}

// (c5) Member-open 500 names only the archive-relative member: deleting the
// node-identity member after boot makes os.Open fail with a *os.PathError
// carrying the absolute DataDir path; the body must keep the member name
// plus a generic errno-class reason and no path.
func TestBackupMemberOpenFailureFaceScrubbed(t *testing.T) {
	dataDir := t.TempDir()
	bn := bootBackupNode(t, "bk-memfail", dataDir, "", "")
	waitBackupLeader(t, bn, 10*time.Second)
	seedBackupGraph(t, bn.url, "")
	waitBackupPublished(t, bn, 2, 1, 5*time.Second)

	if err := os.Remove(filepath.Join(dataDir, "meta.json")); err != nil {
		t.Fatal(err)
	}
	resp, body := fetchBackup(t, bn.url, "")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("missing-member GET /v1/backup = %d (%s), want 500", resp.StatusCode, body)
	}
	code, msg := backupErrBody(t, body)
	if code != "INTERNAL" || msg != "open meta.json: missing" {
		t.Fatalf("500 face = %s %q, want INTERNAL \"open meta.json: missing\"", code, msg)
	}
	if strings.Contains(msg, dataDir) {
		t.Fatalf("500 message %q leaks the server-side path", msg)
	}
}

// (c6) Snapshot-failure face mapping, unit level. Forcing the race for real
// is impractical on two counts: n.r is a concrete *raft.Raft (no seam to
// fail Snapshot()), and raft v1.8.0 answers the user-snapshot future from
// the state-agnostic runSnapshots loop, so a live stepdown never yields
// ErrNotLeader from it — the handler branch guards the gate race for
// wrapped/future variants. The classifier is therefore pinned directly:
// leader-loss errors take the 503 NOT_LEADER face (wrapped errors
// included), ErrNothingNewToSnapshot keeps the runbook's exact message,
// and no failure's message may echo a server-side path.
func TestBackupSnapshotFailureFace(t *testing.T) {
	for _, tc := range []struct {
		name              string
		err               error
		wantStatus        int
		wantCode, wantMsg string
	}{
		{
			name:       "leader lost between gate and snapshot",
			err:        raft.ErrNotLeader,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "NOT_LEADER",
			wantMsg:    "send backup to the leader",
		},
		{
			name:       "wrapped ErrNotLeader still unwraps to the race face",
			err:        fmt.Errorf("runSnapshot failed: %w", raft.ErrNotLeader),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "NOT_LEADER",
			wantMsg:    "send backup to the leader",
		},
		{
			name:       "leadership lost one hop later",
			err:        raft.ErrLeadershipLost,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "NOT_LEADER",
			wantMsg:    "send backup to the leader",
		},
		{
			name:       "dataless leader keeps the documented message",
			err:        raft.ErrNothingNewToSnapshot,
			wantStatus: http.StatusInternalServerError,
			wantCode:   "INTERNAL",
			wantMsg:    "snapshot: nothing new to snapshot",
		},
		{
			name:       "store failure collapses to a path-free generic",
			err:        &fs.PathError{Op: "mkdir", Path: "/srv/vectra/data/snapshots/3-9-824.tmp", Err: fs.ErrPermission},
			wantStatus: http.StatusInternalServerError,
			wantCode:   "INTERNAL",
			wantMsg:    "snapshot: failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := snapshotFailureFace(tc.err)
			if status != tc.wantStatus || code != tc.wantCode || msg != tc.wantMsg {
				t.Fatalf("snapshotFailureFace(%v) = %d %s %q, want %d %s %q",
					tc.err, status, code, msg, tc.wantStatus, tc.wantCode, tc.wantMsg)
			}
			if strings.Contains(msg, "/srv/vectra") {
				t.Fatalf("message %q echoes the server-side path", msg)
			}
		})
	}
}
