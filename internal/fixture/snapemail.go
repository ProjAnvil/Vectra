// Package fixture loads the shared SNAP email-Eu-core dataset used by the
// reference-correctness tests (root package) and the LDBC-style benchmarks
// (bench package). The dataset is embedded, so every consumer is independent
// of the working directory it happens to run from.
//
// Provenance and the published ground-truth statistics (1005 nodes, 25571
// edges, 203 SCCs, largest SCC 803) are documented in testdata/README.md.
package fixture

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"fmt"
	"strconv"

	"github.com/projanvil/vectra"
)

//go:embed testdata/email-Eu-core.txt.gz
var snapEmailGZ []byte

// EdgeList is the raw directed edge list of a dataset (adjacency views are
// built by callers).
type EdgeList struct {
	N        int
	Src, Dst []int
}

// TB is the testing-surface fixture helpers need (satisfied by *testing.T
// and *testing.B); keeps this package free of a testing import.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// SNAPEmail decodes the embedded dataset and builds the sealed graph
// (nodes labeled "Person", edges labeled "EMAIL"); fails t on any decode
// or shape deviation.
func SNAPEmail(t TB) (*vectra.Graph, EdgeList) {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(snapEmailGZ))
	if err != nil {
		t.Fatalf("fixture: snap email gzip: %v", err)
	}
	defer zr.Close()

	el := EdgeList{N: 1005}
	dec := make(map[string]vectra.NodeID, 1005)
	g := vectra.New()
	ensure := func(id int) vectra.NodeID {
		key := strconv.Itoa(id)
		if nid, ok := dec[key]; ok {
			return nid
		}
		nid, err := g.AddNode(key, "Person", nil)
		if err != nil {
			t.Fatalf("fixture: add node: %v", err)
		}
		dec[key] = nid
		return nid
	}
	line := 0
	for {
		var u, v int
		if _, err := fmt.Fscan(zr, &u, &v); err != nil {
			break
		}
		line++
		ensure(u)
		ensure(v)
		if _, err := g.AddEdge(strconv.Itoa(u), strconv.Itoa(v), "EMAIL", nil); err != nil {
			t.Fatalf("fixture: add edge: %v", err)
		}
		el.Src = append(el.Src, u)
		el.Dst = append(el.Dst, v)
	}
	if err := g.Seal(); err != nil {
		t.Fatalf("fixture: seal: %v", err)
	}
	if line != 25571 {
		t.Fatalf("fixture: read %d lines, want 25571", line)
	}
	return g, el
}
