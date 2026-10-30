package server

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
)

// snapshotFailureFace maps a failed n.r.Snapshot() onto the endpoint's wire
// face. Leadership slipping between the IsLeader gate and the snapshot
// pipeline answering is the gate's own redirect-me race, not a server fault:
// raft.ErrNotLeader (and ErrLeadershipLost, the same race one hop later —
// Apply maps the pair identically) takes the 503 NOT_LEADER face. Everything
// else is a 500: ErrNothingNewToSnapshot keeps its documented message
// verbatim (pinned by tests and the runbook), while every other failure
// collapses to a generic reason — the FileSnapshotStore's errors embed the
// absolute DataDir layout (e.g. "failed to create snapshot: mkdir
// <DataDir>/snapshots/..."), which must not reach a response body.
func snapshotFailureFace(err error) (status int, code, msg string) {
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
		return http.StatusServiceUnavailable, "NOT_LEADER", "send backup to the leader"
	case errors.Is(err, raft.ErrNothingNewToSnapshot):
		return http.StatusInternalServerError, "INTERNAL", "snapshot: " + err.Error()
	default:
		return http.StatusInternalServerError, "INTERNAL", "snapshot: failed"
	}
}

// memberErrReason collapses a member open/stat failure into a generic,
// client-safe reason: the raw *os.PathError renders the server's absolute
// DataDir member path into the response body, so only the errno class is
// relayed, and the archive-relative member name is added by the caller.
func memberErrReason(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "missing"
	case errors.Is(err, fs.ErrPermission):
		return "permission denied"
	default:
		return "unreadable"
	}
}

// handleBackup streams GET /v1/backup: a tar.gz of this node's materialized
// raft snapshot plus its meta.json identity, so an operator can untar the
// body into an empty directory and start a node with the same NodeID on it —
// raft finds the snapshot, the FSM restores, the node serves a fresh term.
//
// Archive entries (exactly these three, no directory records):
//
//	meta.json                    — node identity (nodeID/httpAddr/raftAddr)
//	snapshots/<id>/meta.json     — raft FileSnapshotStore snapshot meta
//	snapshots/<id>/state.bin     — FSM payload (graph + VCTX version tables)
//
// The literal layout is hashicorp/raft v1.8.0's FileSnapshotStore contract
// (file_snapshot.go): snapshots live under <DataDir>/snapshots/<id>/ holding
// exactly meta.json + state.bin, where <id> is the directory name
// snapshotName(term, index) builds ("<term>-<index>-<msec>"). raft.db is
// deliberately NOT archived — the snapshot plus the node ID is the whole
// restore contract.
//
// Faces (the endpoint's own, all via writeErr): non-leader → 503 NOT_LEADER
// with the X-Vectra-Leader hint + Retry-After (handleJoin's shape) — that is
// both the IsLeader gate and a leadership loss that lands after it, while
// n.r.Snapshot() is in flight; a failed snapshot materialization or
// unreadable member file → 500 INTERNAL whose message names only the
// archive-relative member and a generic reason, never the server's absolute
// DataDir layout (authenticated clients still must not see it). Auth applies
// per the middleware wrapping the whole mux.
//
// The archive is never buffered in memory: entries stream gzip-over-tar
// straight onto the response writer as each file is read, so no
// Content-Length is set (the response is chunked).
func (n *Node) handleBackup(w http.ResponseWriter, r *http.Request) {
	// Backups run on the leader only: a follower's local FSM may lag the
	// committed frontier, and the leader check is what gives clients the
	// redirect face instead of a silently stale archive.
	if !n.IsLeader() {
		if hint := n.LeaderHint(); hint != "" {
			w.Header().Set("X-Vectra-Leader", hint)
		}
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "NOT_LEADER", "send backup to the leader")
		return
	}
	// Materialize an FSM snapshot. The future resolves only after the
	// FileSnapshotStore has fully persisted and renamed the snapshot
	// directory, so reading its files immediately after Error() is
	// race-free. Open() yields the snapshot's metadata (its ID is the
	// on-disk directory name) plus a reader over the state payload; it is
	// only legal after a nil error.
	//
	// One documented wart: a leader that has never applied an entry (fresh
	// single-node bootstrap, zero writes) fails with
	// ErrNothingNewToSnapshot — there is genuinely no snapshot on disk to
	// stream. Any committed write makes the next call succeed.
	fut := n.r.Snapshot()
	if err := fut.Error(); err != nil {
		status, code, msg := snapshotFailureFace(err)
		if status == http.StatusServiceUnavailable {
			// Leadership slipped between the gate above and the snapshot
			// pipeline answering: the gate's own face, headers included.
			if hint := n.LeaderHint(); hint != "" {
				w.Header().Set("X-Vectra-Leader", hint)
			}
			w.Header().Set("Retry-After", "1")
		}
		writeErr(w, status, code, msg)
		return
	}
	meta, state, err := fut.Open()
	if err != nil {
		// The store's Open fails with *os.PathError over the absolute
		// DataDir path (the concurrent-pull reap race documented in the
		// README lands here) — keep the body path-free.
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "open snapshot: unreadable")
		return
	}
	snapID := meta.ID
	_ = state.Close()

	// Open and stat every archive member before touching the response:
	// a missing or unreadable file then still gets the 500 JSON face,
	// never a half-written gzip body.
	type member struct {
		hdr  tar.Header
		file *os.File
	}
	names := [3]string{
		"meta.json",
		"snapshots/" + snapID + "/meta.json",
		"snapshots/" + snapID + "/state.bin",
	}
	paths := [3]string{
		filepath.Join(n.cfg.DataDir, "meta.json"),
		filepath.Join(n.cfg.DataDir, "snapshots", snapID, "meta.json"),
		filepath.Join(n.cfg.DataDir, "snapshots", snapID, "state.bin"),
	}
	members := make([]member, 0, len(paths))
	defer func() {
		for _, m := range members {
			_ = m.file.Close()
		}
	}()
	for i, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			// err is a *os.PathError over the absolute DataDir member
			// path; surface only the archive-relative name (names[i])
			// plus a generic reason, never the server-side layout.
			writeErr(w, http.StatusInternalServerError, "INTERNAL",
				fmt.Sprintf("open %s: %s", names[i], memberErrReason(err)))
			return
		}
		st, err := f.Stat()
		if err != nil || st.IsDir() {
			// Same scrub as open: stat's PathError carries the absolute
			// path, and the IsDir branch has no error at all.
			reason := "not a regular file"
			if err != nil {
				reason = memberErrReason(err)
			}
			writeErr(w, http.StatusInternalServerError, "INTERNAL",
				fmt.Sprintf("stat %s: %s", names[i], reason))
			return
		}
		members = append(members, member{
			hdr: tar.Header{
				Name:     names[i],
				Mode:     int64(st.Mode().Perm()),
				Size:     st.Size(),
				Typeflag: tar.TypeReg,
				ModTime:  st.ModTime(),
			},
			file: f,
		})
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=\"vectra-backup-%s.tar.gz\"", snapID))
	w.WriteHeader(http.StatusOK)

	// Stream: file → tar → gzip → response. After the first byte any I/O
	// failure can only abort the connection — writing an error envelope
	// mid-gzip would corrupt the body instead.
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, m := range members {
		if err := tw.WriteHeader(&m.hdr); err != nil {
			return
		}
		if _, err := io.Copy(tw, m.file); err != nil {
			return
		}
	}
	if err := tw.Close(); err != nil {
		return
	}
	if err := gz.Close(); err != nil {
		return
	}
}
