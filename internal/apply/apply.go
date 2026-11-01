// Package apply single-sources the changeset apply loop (audit F-02).
//
// The four write lanes — WAL replay (package vectra's Open recovery), the
// writer wrapper (internal/writer), the replicated FSM replay (server) and
// the MCP inject_batch tool — all walk one Changeset in the same fixed lane
// order (node upserts → edge upserts → edge deletes → node deletes, the
// deterministic-replay contract) with the same write-method dispatch
// (AddNode; AddEdge for AllowParallel else UpsertEdge; DeleteEdge;
// DeleteNode). Before F-02 that loop was written four times and had already
// drifted once (the unique-constraint abort semantic was hand-applied per
// lane and missed one — the fixed F-03). This package owns the walk once;
// each lane supplies its error POLICY and accumulates its own counters.
//
// Layering: this package deliberately depends only on internal/wal (the
// changeset vocabulary). It cannot import the engine package (that would be
// an import cycle — package vectra imports this package), so the engine
// surface is the Engine interface below; *vectra.Graph bridges it through a
// thin adapter (see Graph.ApplyChangeset).
package apply

import (
	"fmt"

	"github.com/projanvil/vectra/internal/wal"
)

// Kind identifies which of the four changeset lanes an op came from.
type Kind uint8

const (
	KindNodes       Kind = iota // wal.Changeset.Nodes
	KindEdges                   // wal.Changeset.Edges
	KindEdgeDeletes             // wal.Changeset.EdgeDeletes
	KindNodeDeletes             // wal.Changeset.NodeDeletes
)

// String names the lane (KindNodes & co.) so diagnostics — the stops() panic,
// test failure output, caller error strings — read names, not ints.
func (k Kind) String() string {
	switch k {
	case KindNodes:
		return "KindNodes"
	case KindEdges:
		return "KindEdges"
	case KindEdgeDeletes:
		return "KindEdgeDeletes"
	case KindNodeDeletes:
		return "KindNodeDeletes"
	}
	panic(fmt.Sprintf("apply: unknown kind %d", k))
}

// Policy is the explicit error strategy of one apply: for each lane, whether
// an engine error is FATAL (the walk stops there; later ops never run) or is
// TOLERATED (the walk continues; the lane's observer decides how to count
// it). The policies are named for what they are — the set of lanes that
// stop.
type Policy uint8

const (
	// StopAtFirst makes every lane first-error-stops: the first op any lane
	// rejects aborts the apply and is returned. The WAL replay path (package
	// vectra Open recovery) and the writer wrapper's Submit use this: a
	// changeset the engine rejects mid-way means the recorded stream did not
	// rebuild identically — surface it, never skip.
	StopAtFirst Policy = iota
	// NodesStopRestSkip makes node upserts first-error-stops while the edge
	// and delete lanes tolerate errors (the lane counts them — e.g. the
	// server's EdgesSkipped/NodesSkipped). The replicated FSM replay
	// (server/fsm.go) uses this: node rejections are log-content-derived
	// (unique-constraint violations, spec 2026-10-30 §2.4) so every replica
	// aborts identically, while edge/deletes references to missing endpoints
	// stay deterministic skip-counts. Node deletes are tolerated (missing
	// keys count as NodesSkipped).
	NodesStopRestSkip
	// NodesAndDeletesStop is NodesStopRestSkip plus fatal node deletes: the
	// MCP inject_batch tool uses this — a caller always learns a delete
	// missed, deliberately stricter than the replicated server lane (which
	// skip-counts missing delete keys). Edge lanes stay tolerated.
	NodesAndDeletesStop
)

// stops reports whether an error in lane k is fatal under p.
func (p Policy) stops(k Kind) bool {
	switch p {
	case StopAtFirst:
		return true
	case NodesStopRestSkip:
		return k == KindNodes
	case NodesAndDeletesStop:
		return k == KindNodes || k == KindNodeDeletes
	}
	panic(fmt.Sprintf("apply: unknown policy %d", p))
}

// String names the policy (StopAtFirst & co.) — same role as Kind.String,
// same panic-default discipline as stops.
func (p Policy) String() string {
	switch p {
	case StopAtFirst:
		return "StopAtFirst"
	case NodesStopRestSkip:
		return "NodesStopRestSkip"
	case NodesAndDeletesStop:
		return "NodesAndDeletesStop"
	}
	panic(fmt.Sprintf("apply: unknown policy %d", p))
}

// Engine is the graph write surface the apply walk drives. Satisfied by an
// adapter over *vectra.Graph (Graph.ApplyChangeset) — the ID returns are
// dropped because no apply lane uses them; signature types are builtins and
// wal types only, so this package needs no engine import.
type Engine interface {
	AddNode(key, label string, props map[string]any) error
	AddEdge(srcKey, dstKey, label string, props map[string]any) error
	UpsertEdge(srcKey, dstKey, label string, props map[string]any) error
	DeleteEdge(srcKey, dstKey, label string) (int, error)
	DeleteNode(key string) (int, error)
}

// Op is one visited op's identity and outcome, reported through the observer
// callback. Err == nil means the op applied; Err != nil means the engine
// rejected it — tolerated per policy (the walk continued) or the fatal op
// itself (see Changeset's return value). The identity fields let each lane
// build its own error strings and counters without re-walking the changeset:
// Index addresses the lane's slice (Op.Index into cs.Nodes / cs.Edges /
// cs.EdgeDeletes / cs.NodeDeletes), Key is the node key on the node lanes,
// SrcKey/DstKey/Label address edge lanes. N carries the delete lanes' engine
// counts (edges removed by DeleteEdge — parallel matches — and edges cascaded
// by DeleteNode); it is meaningless on the upsert lanes.
type Op struct {
	Kind           Kind
	Index          int
	Key            string // node lanes
	SrcKey, DstKey string // edge lanes
	Label          string // edge lanes
	Err            error
	N              int
}

// Changeset applies cs's four lanes in the fixed order under policy p,
// calling observe once per VISITED op, in visit order (nil observe is
// allowed — lanes with no per-op accounting skip it). It returns the op the
// policy treats as fatal — the walk stopped there, everything after it never
// ran — or the zero Op (Err == nil) when the whole changeset applied or every
// error was tolerated. The fatal op is also passed to observe before the
// walk stops, so observers see a complete visitation record.
//
// Changeset does NOT own a Batch boundary: every caller keeps its own
// g.Batch(...) discipline around the call (one micro-layer — and, on a
// WAL-attached graph, one frame — per changeset, not per op).
func Changeset(e Engine, cs wal.Changeset, p Policy, observe func(Op)) Op {
	for i, n := range cs.Nodes {
		op := Op{Kind: KindNodes, Index: i, Key: n.Key, Err: e.AddNode(n.Key, n.Label, n.Props)}
		if observe != nil {
			observe(op)
		}
		if op.Err != nil && p.stops(KindNodes) {
			return op
		}
	}
	for i, ed := range cs.Edges {
		var err error
		if ed.AllowParallel {
			err = e.AddEdge(ed.SrcKey, ed.DstKey, ed.Label, ed.Props)
		} else {
			err = e.UpsertEdge(ed.SrcKey, ed.DstKey, ed.Label, ed.Props)
		}
		op := Op{Kind: KindEdges, Index: i, SrcKey: ed.SrcKey, DstKey: ed.DstKey, Label: ed.Label, Err: err}
		if observe != nil {
			observe(op)
		}
		if op.Err != nil && p.stops(KindEdges) {
			return op
		}
	}
	for i, d := range cs.EdgeDeletes {
		n, err := e.DeleteEdge(d.SrcKey, d.DstKey, d.Label)
		op := Op{Kind: KindEdgeDeletes, Index: i, SrcKey: d.SrcKey, DstKey: d.DstKey, Label: d.Label, Err: err, N: n}
		if observe != nil {
			observe(op)
		}
		if op.Err != nil && p.stops(KindEdgeDeletes) {
			return op
		}
	}
	for i, key := range cs.NodeDeletes {
		n, err := e.DeleteNode(key)
		op := Op{Kind: KindNodeDeletes, Index: i, Key: key, Err: err, N: n}
		if observe != nil {
			observe(op)
		}
		if op.Err != nil && p.stops(KindNodeDeletes) {
			return op
		}
	}
	return Op{}
}
