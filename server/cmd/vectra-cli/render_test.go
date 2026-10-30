package main

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestFormatValue(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, "null"},
		{"hello", "hello"},
		{"with spaces", "with spaces"},
		{true, "true"},
		{false, "false"},
		{float64(3), "3"},
		{float64(0), "0"},
		{float64(3.5), "3.5"},
		{float64(-0.25), "-0.25"},
		// Integral floats outside int64 range must render via %g, not the
		// implementation-defined int64 conversion (amd64 yields
		// -9223372036854775808 for 1e300).
		{1e300, "1e+300"},
		{-1e300, "-1e+300"},
		// Boundaries: MinInt64 is exactly representable and still formats
		// as an integer; MaxInt64 rounds up to 2^63 as a float64, which no
		// longer fits, so it takes the %g path.
		{float64(math.MinInt64), "-9223372036854775808"},
		{float64(math.MaxInt64), "9.223372036854776e+18"},
		{[]any{float64(1), float64(2)}, "[1,2]"},
		{map[string]any{"key": "v"}, `{"key":"v"}`},
	}
	for _, tc := range cases {
		if got := formatValue(tc.in); got != tc.want {
			t.Errorf("formatValue(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRenderTable(t *testing.T) {
	columns := []string{"name", "age"}
	rows := []map[string]any{
		{"name": "alice", "age": float64(30)},
		{"name": "bob", "age": float64(25)},
		{}, // missing keys render as null
	}
	// Each cell is padded to the column width then followed by two spaces
	// (including the last column); spelled cell-by-cell so the trailing
	// spaces are explicit and survive whitespace-stripping editors.
	want := "name " + "  " + "age " + "  " + "\n" +
		"alice" + "  " + "30  " + "  " + "\n" +
		"bob  " + "  " + "25  " + "  " + "\n" +
		"null " + "  " + "null" + "  " + "\n"

	var buf bytes.Buffer
	if err := renderTable(&buf, columns, rows); err != nil {
		t.Fatalf("renderTable: %v", err)
	}
	if buf.String() != want {
		t.Errorf("renderTable output = %q, want %q", buf.String(), want)
	}
}

func TestRenderTableEmptyColumns(t *testing.T) {
	var buf bytes.Buffer
	if err := renderTable(&buf, nil, []map[string]any{{"a": "x"}}); err != nil {
		t.Fatalf("renderTable: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("renderTable with no columns wrote %q", buf.String())
	}
}

// errWriter fails every write, to prove renderTable propagates output errors.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

func TestRenderTableWriteError(t *testing.T) {
	if err := renderTable(errWriter{}, []string{"a"}, []map[string]any{{"a": 1}}); err == nil {
		t.Fatal("renderTable swallowed a write error")
	}
}

func TestRenderSummary(t *testing.T) {
	cases := []struct {
		name string
		sum  *cypherSummary
		want string
	}{
		{"all zero", &cypherSummary{}, "Summary: no changes\n"},
		{"nil summary", nil, "Summary: no changes\n"},
		{"created nodes", &cypherSummary{CreatedNodes: 2}, "Summary: created 2 nodes\n"},
		{"created edges", &cypherSummary{CreatedEdges: 1}, "Summary: created 1 edges\n"},
		{"deleted nodes", &cypherSummary{DeletedNodes: 3}, "Summary: deleted 3 nodes\n"},
		{"deleted edges", &cypherSummary{DeletedEdges: 4}, "Summary: deleted 4 edges\n"},
		{"set properties", &cypherSummary{PropertiesSet: 5}, "Summary: set 5 properties\n"},
		{"created indexes", &cypherSummary{IndexesCreated: 6}, "Summary: created 6 indexes\n"},
		{"multiple counters keep order", &cypherSummary{CreatedNodes: 1, DeletedEdges: 2, PropertiesSet: 3},
			"Summary: created 1 nodes, deleted 2 edges, set 3 properties\n"},
		{"all six counters", &cypherSummary{CreatedNodes: 1, CreatedEdges: 2, DeletedNodes: 3, DeletedEdges: 4, PropertiesSet: 5, IndexesCreated: 6},
			"Summary: created 1 nodes, created 2 edges, deleted 3 nodes, deleted 4 edges, set 5 properties, created 6 indexes\n"},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		renderSummary(&buf, tc.sum)
		if buf.String() != tc.want {
			t.Errorf("%s: renderSummary = %q, want %q", tc.name, buf.String(), tc.want)
		}
	}
	// renderSummary writes exactly one line.
	var buf bytes.Buffer
	renderSummary(&buf, &cypherSummary{CreatedNodes: 1})
	if got := buf.String(); !bytes.HasSuffix([]byte(got), []byte("\n")) || bytes.Count([]byte(got), []byte("\n")) != 1 {
		t.Errorf("renderSummary wrote %q, want exactly one newline-terminated line", got)
	}
}
