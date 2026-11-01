package apply

import (
	"errors"
	"testing"

	"github.com/projanvil/vectra/internal/wal"
)

// fakeEngine answers from scripts so the walk's policy behavior is
// observable without the engine: errs maps an op's stable identity to the
// error it must return; deleteN is the count every successful delete
// returns. calls records the visit order with the dispatch verbs
// (edgeUpsert vs edgeAdd — the AllowParallel branch — kept distinct).
type fakeEngine struct {
	errs    map[string]error
	deleteN int
	calls   []string
}

func (f *fakeEngine) AddNode(key, label string, props map[string]any) error {
	f.calls = append(f.calls, "node:"+key)
	return f.errs["nodes:"+key]
}

func (f *fakeEngine) AddEdge(srcKey, dstKey, label string, props map[string]any) error {
	f.calls = append(f.calls, "edgeAdd:"+srcKey)
	return f.errs["edges:"+srcKey+","+dstKey+","+label]
}

func (f *fakeEngine) UpsertEdge(srcKey, dstKey, label string, props map[string]any) error {
	f.calls = append(f.calls, "edgeUpsert:"+srcKey)
	return f.errs["edges:"+srcKey+","+dstKey+","+label]
}

func (f *fakeEngine) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	f.calls = append(f.calls, "edgeDelete:"+srcKey)
	if err := f.errs["edgeDeletes:"+srcKey+","+dstKey+","+label]; err != nil {
		return 0, err
	}
	return f.deleteN, nil
}

func (f *fakeEngine) DeleteNode(key string) (int, error) {
	f.calls = append(f.calls, "nodeDelete:"+key)
	if err := f.errs["nodeDeletes:"+key]; err != nil {
		return 0, err
	}
	return f.deleteN, nil
}

func fullChangeset() wal.Changeset {
	return wal.Changeset{
		Nodes: []wal.NodeUpsert{
			{Key: "n1", Label: "Svc"},
			{Key: "n2", Label: "Svc"},
		},
		Edges: []wal.EdgeUpsert{
			{SrcKey: "n1", DstKey: "n2", Label: "E"},
			{SrcKey: "n2", DstKey: "n1", Label: "P", AllowParallel: true},
		},
		EdgeDeletes: []wal.EdgeDelete{{SrcKey: "n1", DstKey: "n2", Label: "E"}},
		NodeDeletes: []string{"n2"},
	}
}

var errFake = errors.New("fake rejection")

// observed renders the observer's visitation as tokens — the per-op report
// the lanes build counters from ("!" marks an errored op).
func observed(ops []Op) []string {
	var out []string
	for _, op := range ops {
		tok := "nodes:" + op.Key
		switch op.Kind {
		case KindEdges:
			tok = "edges:" + op.SrcKey
		case KindEdgeDeletes:
			tok = "edgeDeletes:" + op.SrcKey
		case KindNodeDeletes:
			tok = "nodeDeletes:" + op.Key
		}
		if op.Err != nil {
			tok += "!"
		}
		out = append(out, tok)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestChangesetWalkOrderAndDispatch pins the shared walk's fixed lane order
// and write-method dispatch (identical in every lane pre-F-02 — the point
// of single-sourcing it).
func TestChangesetWalkOrderAndDispatch(t *testing.T) {
	for _, p := range []Policy{StopAtFirst, NodesStopRestSkip, NodesAndDeletesStop} {
		e := &fakeEngine{deleteN: 3}
		var seen []Op
		if fatal := Changeset(e, fullChangeset(), p, func(op Op) { seen = append(seen, op) }); fatal.Err != nil {
			t.Fatalf("policy %d: unexpected fatal op %+v", p, fatal)
		}
		wantCalls := []string{"node:n1", "node:n2", "edgeUpsert:n1", "edgeAdd:n2", "edgeDelete:n1", "nodeDelete:n2"}
		if !equal(e.calls, wantCalls) {
			t.Fatalf("policy %d engine calls = %v, want %v (lane order + AllowParallel dispatch)", p, e.calls, wantCalls)
		}
		wantSeen := []string{"nodes:n1", "nodes:n2", "edges:n1", "edges:n2", "edgeDeletes:n1", "nodeDeletes:n2"}
		if got := observed(seen); !equal(got, wantSeen) {
			t.Fatalf("policy %d visitation = %v, want %v", p, got, wantSeen)
		}
	}
}

// TestChangesetPolicyStopSets pins which lanes each policy treats as fatal.
func TestChangesetPolicyStopSets(t *testing.T) {
	// StopAtFirst: an edges-lane failure stops before the delete lanes run.
	e := &fakeEngine{errs: map[string]error{"edges:n1,n2,E": errFake}}
	fatal := Changeset(e, fullChangeset(), StopAtFirst, nil)
	if !errors.Is(fatal.Err, errFake) || fatal.Kind != KindEdges || fatal.Index != 0 || fatal.SrcKey != "n1" {
		t.Fatalf("StopAtFirst fatal = %+v, want the n1 edge error", fatal)
	}
	if got, want := e.calls, []string{"node:n1", "node:n2", "edgeUpsert:n1"}; !equal(got, want) {
		t.Fatalf("StopAtFirst calls = %v, want %v (delete lanes never run)", got, want)
	}

	// NodesStopRestSkip: node errors fatal...
	e = &fakeEngine{errs: map[string]error{"nodes:n2": errFake}}
	fatal = Changeset(e, fullChangeset(), NodesStopRestSkip, nil)
	if fatal.Kind != KindNodes || fatal.Index != 1 || fatal.Key != "n2" || !errors.Is(fatal.Err, errFake) {
		t.Fatalf("NodesStopRestSkip node fatal = %+v", fatal)
	}
	if got, want := e.calls, []string{"node:n1", "node:n2"}; !equal(got, want) {
		t.Fatalf("NodesStopRestSkip node-abort calls = %v, want %v", got, want)
	}
	// ...edge, edge-delete and node-delete errors tolerated.
	e = &fakeEngine{errs: map[string]error{
		"edges:n1,n2,E":       errFake,
		"edgeDeletes:n1,n2,E": errFake,
		"nodeDeletes:n2":      errFake,
	}}
	var seen []Op
	if fatal := Changeset(e, fullChangeset(), NodesStopRestSkip, func(op Op) { seen = append(seen, op) }); fatal.Err != nil {
		t.Fatalf("NodesStopRestSkip: edge/delete errors must be tolerated, got fatal %+v", fatal)
	}
	want := []string{"nodes:n1", "nodes:n2", "edges:n1!", "edges:n2", "edgeDeletes:n1!", "nodeDeletes:n2!"}
	if got := observed(seen); !equal(got, want) {
		t.Fatalf("NodesStopRestSkip visitation = %v, want %v", got, want)
	}

	// NodesAndDeletesStop: node deletes are fatal too (the MCP lane's
	// stricter delete discipline)...
	e = &fakeEngine{errs: map[string]error{"nodeDeletes:n2": errFake}}
	fatal = Changeset(e, fullChangeset(), NodesAndDeletesStop, nil)
	if fatal.Kind != KindNodeDeletes || fatal.Index != 0 || fatal.Key != "n2" || !errors.Is(fatal.Err, errFake) {
		t.Fatalf("NodesAndDeletesStop delete fatal = %+v", fatal)
	}
	// ...while edge and edge-delete errors stay tolerated.
	e = &fakeEngine{errs: map[string]error{
		"edges:n2,n1,P":       errFake,
		"edgeDeletes:n1,n2,E": errFake,
	}}
	if fatal := Changeset(e, fullChangeset(), NodesAndDeletesStop, nil); fatal.Err != nil {
		t.Fatalf("NodesAndDeletesStop: edge errors must be tolerated, got fatal %+v", fatal)
	}
}

// TestChangesetDeleteCounts checks the N channel: DeleteEdge's parallel
// matches and DeleteNode's cascade reach the observer on success (0 on the
// errored op).
func TestChangesetDeleteCounts(t *testing.T) {
	e := &fakeEngine{deleteN: 4}
	var ns []int
	Changeset(e, fullChangeset(), StopAtFirst, func(op Op) {
		if op.Kind == KindEdgeDeletes || op.Kind == KindNodeDeletes {
			ns = append(ns, op.N)
		}
	})
	if len(ns) != 2 || ns[0] != 4 || ns[1] != 4 {
		t.Fatalf("delete Ns = %v, want [4 4]", ns)
	}

	e = &fakeEngine{deleteN: 4, errs: map[string]error{"nodeDeletes:n2": errFake}}
	ns = nil
	fatal := Changeset(e, fullChangeset(), StopAtFirst, func(op Op) {
		if op.Kind == KindNodeDeletes {
			ns = append(ns, op.N)
		}
	})
	if fatal.Err == nil || len(ns) != 1 || ns[0] != 0 {
		t.Fatalf("failed delete must report N=0 and be fatal, got ns=%v fatal=%+v", ns, fatal)
	}
}
