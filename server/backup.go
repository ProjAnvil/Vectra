package server

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

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
// with the X-Vectra-Leader hint + Retry-After (handleJoin's shape); a failed
// snapshot materialization or unreadable member file → 500 INTERNAL. Auth
// applies per the middleware wrapping the whole mux.
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
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "snapshot: "+err.Error())
		return
	}
	meta, state, err := fut.Open()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "open snapshot: "+err.Error())
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
			writeErr(w, http.StatusInternalServerError, "INTERNAL",
				fmt.Sprintf("open %s: %v", names[i], err))
			return
		}
		st, err := f.Stat()
		if err != nil || st.IsDir() {
			writeErr(w, http.StatusInternalServerError, "INTERNAL",
				fmt.Sprintf("stat %s: %v", names[i], err))
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
