package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// formatValue renders one table cell: nil becomes "null", strings stay
// unquoted, bools render as true/false, float64 (what encoding/json turns
// every JSON number into) renders integral values without a decimal point,
// and anything else (maps, slices) renders as compact JSON.
func formatValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		// Integral values render without a decimal point — but only when
		// they fit int64. Outside that range (1e300) the float-to-int
		// conversion is implementation-defined garbage, so fall through to
		// %g. The upper bound must be strict: math.MaxInt64 rounds up to
		// 2^63 as a float64, which does not fit.
		if math.Trunc(t) == t && t >= math.MinInt64 && t < math.MaxInt64 {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	default:
		b, err := json.Marshal(v)
		if err != nil { // unmarshalable value (chan, func, ...): best effort
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// renderTable writes an aligned table to w: a header row of the column
// names followed by one row per result, every cell left-padded to the
// widest cell of its column and followed by two spaces. Nothing is ever
// truncated. Empty columns write nothing.
func renderTable(w io.Writer, columns []string, rows []map[string]any) error {
	if len(columns) == 0 {
		return nil
	}
	cells := make([][]string, 0, len(rows)+1)
	cells = append(cells, columns)
	for _, row := range rows {
		r := make([]string, len(columns))
		for i, col := range columns {
			r[i] = formatValue(row[col])
		}
		cells = append(cells, r)
	}
	widths := make([]int, len(columns))
	for _, r := range cells {
		for i, cell := range r {
			if n := len(cell); n > widths[i] {
				widths[i] = n
			}
		}
	}
	var firstErr error
	for _, r := range cells {
		for i, cell := range r {
			if _, err := fmt.Fprintf(w, "%-*s  ", widths[i], cell); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// renderSummary writes the one-line summary of a WRITE response: non-zero
// counters joined by ", " in fixed order; all-zero (or nil) summaries
// render as "Summary: no changes".
func renderSummary(w io.Writer, s *cypherSummary) {
	if s == nil {
		s = &cypherSummary{}
	}
	var parts []string
	if s.CreatedNodes > 0 {
		parts = append(parts, fmt.Sprintf("created %d nodes", s.CreatedNodes))
	}
	if s.CreatedEdges > 0 {
		parts = append(parts, fmt.Sprintf("created %d edges", s.CreatedEdges))
	}
	if s.DeletedNodes > 0 {
		parts = append(parts, fmt.Sprintf("deleted %d nodes", s.DeletedNodes))
	}
	if s.DeletedEdges > 0 {
		parts = append(parts, fmt.Sprintf("deleted %d edges", s.DeletedEdges))
	}
	if s.PropertiesSet > 0 {
		parts = append(parts, fmt.Sprintf("set %d properties", s.PropertiesSet))
	}
	if s.IndexesCreated > 0 {
		parts = append(parts, fmt.Sprintf("created %d indexes", s.IndexesCreated))
	}
	if len(parts) == 0 {
		fmt.Fprintln(w, "Summary: no changes")
		return
	}
	fmt.Fprintf(w, "Summary: %s\n", strings.Join(parts, ", "))
}
