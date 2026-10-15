package cypher

import (
	"strings"
	"testing"
)

// aggRow 取唯一一行（聚合无分组键时恰一行）。
func aggRow(t *testing.T, q string) map[string]any {
	t.Helper()
	res := run(t, q)
	if len(res.Rows) != 1 {
		t.Fatalf("Run(%q): want 1 row, got %d: %+v", q, len(res.Rows), res.Rows)
	}
	return res.Rows[0]
}

func aggNum(t *testing.T, r map[string]any, col string) float64 {
	t.Helper()
	v, ok := r[col].(float64)
	if !ok {
		t.Fatalf("column %q: want float64, got %T (%v)", col, r[col], r[col])
	}
	return v
}

// TestAggFunctionSet 覆盖全部六个聚合函数（:Service 三行 w=5,3,7）。
func TestAggFunctionSet(t *testing.T) {
	// 隐式分组：非聚合项 a.env 是分组键
	res := run(t, `MATCH (a:Service) RETURN a.env, count(*)`)
	want := map[string]float64{"prod": 2, "dev": 1}
	if len(res.Rows) != len(want) {
		t.Fatalf("groups: %d rows %+v", len(res.Rows), res.Rows)
	}
	seen := map[string]bool{}
	for _, r := range res.Rows {
		env, _ := r["a.env"].(string)
		if _, dup := want[env]; !dup || seen[env] {
			t.Fatalf("unexpected group env=%v rows=%+v", env, res.Rows)
		}
		seen[env] = true
		if aggNum(t, r, "count(*)") != want[env] {
			t.Fatalf("count(*) for %q: %v, want %v", env, r["count(*)"], want[env])
		}
	}

	// Service（5+3+7=15, avg 5, min 3, max 7）
	r := aggRow(t, `MATCH (a:Service) RETURN sum(a.w), avg(a.w), min(a.w), max(a.w), collect(a.key)`)
	if aggNum(t, r, "sum(a.w)") != 15 {
		t.Fatalf("sum: %v", r["sum(a.w)"])
	}
	if aggNum(t, r, "avg(a.w)") != 5 {
		t.Fatalf("avg: %v", r["avg(a.w)"])
	}
	if aggNum(t, r, "min(a.w)") != 3 {
		t.Fatalf("min: %v", r["min(a.w)"])
	}
	if aggNum(t, r, "max(a.w)") != 7 {
		t.Fatalf("max: %v", r["max(a.w)"])
	}
	keys, ok := r["collect(a.key)"].([]any)
	if !ok || len(keys) != 3 || keys[0] != "gw" || keys[1] != "order" || keys[2] != "pay" {
		t.Fatalf("collect: %#v", r["collect(a.key)"])
	}

	// 全图（+store w=2 → sum 17, avg 4.25, min 2, max 7）
	r = aggRow(t, `MATCH (a) RETURN sum(a.w), avg(a.w), min(a.w), max(a.w)`)
	if aggNum(t, r, "sum(a.w)") != 17 || aggNum(t, r, "avg(a.w)") != 4.25 ||
		aggNum(t, r, "min(a.w)") != 2 || aggNum(t, r, "max(a.w)") != 7 {
		t.Fatalf("all-nodes aggregates: %+v", r)
	}

	// min/max 跳过 null，按 cmpVals 字符串序
	if v := aggRow(t, `MATCH (a) RETURN min(a.env)`)["min(a.env)"]; v != "dev" {
		t.Fatalf("min(a.env): %#v", v)
	}
}

// TestAggCountNullDistinct：store 无 env（null）。
func TestAggCountNullDistinct(t *testing.T) {
	r := aggRow(t, `MATCH (a) RETURN count(*), count(a.env), count(DISTINCT a.env)`)
	if aggNum(t, r, "count(*)") != 4 {
		t.Fatalf("count(*): %v", r["count(*)"])
	}
	if aggNum(t, r, "count(a.env)") != 3 { // null 跳过
		t.Fatalf("count(a.env): %v", r["count(a.env)"])
	}
	if aggNum(t, r, "count(DISTINCT a.env)") != 2 { // prod, dev
		t.Fatalf("count(DISTINCT a.env): %v", r["count(DISTINCT a.env)"])
	}
	lst, ok := aggRow(t, `MATCH (a) RETURN collect(DISTINCT a.env)`)["collect(DISTINCT a.env)"].([]any)
	if !ok || len(lst) != 2 || lst[0] != "prod" || lst[1] != "dev" {
		t.Fatalf("collect(DISTINCT a.env): %#v", lst)
	}
	// null 分组键自成一组的独立编码
	res := run(t, `MATCH (a) RETURN a.env, count(*)`)
	if len(res.Rows) != 3 {
		t.Fatalf("env groups: %d rows %+v", len(res.Rows), res.Rows)
	}
	nulls := 0
	for _, row := range res.Rows {
		if row["a.env"] == nil {
			nulls++
			if aggNum(t, row, "count(*)") != 1 {
				t.Fatalf("null group count: %+v", row)
			}
		}
	}
	if nulls != 1 {
		t.Fatalf("null group rows: %d", nulls)
	}
}

func TestAggMultiKeyGrouping(t *testing.T) {
	res := run(t, `MATCH (a) RETURN a.label, a.type, count(*)`)
	want := map[string]float64{"Service|controller": 1, "Service|service": 2, "Store|store": 1}
	if len(res.Rows) != len(want) {
		t.Fatalf("groups: %d rows %+v", len(res.Rows), res.Rows)
	}
	for _, r := range res.Rows {
		k := r["a.label"].(string) + "|" + r["a.type"].(string)
		if aggNum(t, r, "count(*)") != want[k] {
			t.Fatalf("group %q: %+v", k, r)
		}
	}
}

// TestAggNoKeysEmptyInput：无分组键 + 零输入行 → 恰一行默认值。
func TestAggNoKeysEmptyInput(t *testing.T) {
	r := aggRow(t, `MATCH (a:Missing) RETURN count(*), sum(a.w), avg(a.w), min(a.w), max(a.w), collect(a.key)`)
	if aggNum(t, r, "count(*)") != 0 {
		t.Fatalf("count: %v", r["count(*)"])
	}
	if aggNum(t, r, "sum(a.w)") != 0 {
		t.Fatalf("sum: %v", r["sum(a.w)"])
	}
	for _, col := range []string{"avg(a.w)", "min(a.w)", "max(a.w)"} {
		if r[col] != nil {
			t.Fatalf("%s: %#v, want nil", col, r[col])
		}
	}
	lst, ok := r["collect(a.key)"].([]any)
	if !ok || len(lst) != 0 {
		t.Fatalf("collect: %#v, want empty list", r["collect(a.key)"])
	}
	// 有分组键 + 零输入行 → 零行
	if res := run(t, `MATCH (a:Missing) RETURN a.env, count(*)`); len(res.Rows) != 0 {
		t.Fatalf("keyed empty input: %d rows", len(res.Rows))
	}
}

func TestAggAliasOrderLimit(t *testing.T) {
	res := run(t, `MATCH (a:Service) RETURN a.env AS env, sum(a.w) AS total ORDER BY total DESC`)
	if len(res.Columns) != 2 || res.Columns[0] != "env" || res.Columns[1] != "total" {
		t.Fatalf("columns: %v", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
	if res.Rows[0]["env"] != "prod" || aggNum(t, res.Rows[0], "total") != 8 {
		t.Fatalf("top group: %+v", res.Rows[0])
	}
	if res.Rows[1]["env"] != "dev" || aggNum(t, res.Rows[1], "total") != 7 {
		t.Fatalf("second group: %+v", res.Rows[1])
	}
	// LIMIT 作用于聚合后的组行
	res = run(t, `MATCH (a:Service) RETURN a.env AS env, sum(a.w) AS total ORDER BY total DESC LIMIT 1`)
	if len(res.Rows) != 1 || res.Rows[0]["env"] != "prod" {
		t.Fatalf("limit: %+v", res.Rows)
	}
	// 非聚合项的 AS 别名同样可进 ORDER BY（存储 prop 原生类型为 int）
	res = run(t, `MATCH (a:Service) RETURN a.w AS w ORDER BY w DESC LIMIT 1`)
	if len(res.Rows) != 1 || res.Rows[0]["w"] != 7 {
		t.Fatalf("non-agg alias order: %+v", res.Rows)
	}
}

func TestAggParseErrors(t *testing.T) {
	cases := []struct {
		src  string
		want string
	}{
		{"MATCH (a) RETURN *, count(*)", "RETURN *"},
		{"MATCH (a) RETURN sum(count(a))", "nested"},
		{"MATCH (a) RETURN foo(a)", "unknown function"},
		{"MATCH (a) RETURN a.key + count(*)", "unexpected character"},
		{"MATCH (a) RETURN count(*) ORDER BY count(*)", "ORDER BY"},
		{"MATCH (a) RETURN count(*) ORDER BY a.w", "RETURN"}, // 聚合时 ORDER BY 必须命中 RETURN 名/别名
		{"MATCH (a) RETURN count(z.w)", `variable "z" is not declared`},
		{"MATCH (a) RETURN count(DISTINCT *)", "not supported"},
		{"MATCH (a) RETURN sum(*)", "not supported"},
		{"MATCH (a) RETURN count(*) AS", "alias"},
	}
	for _, c := range cases {
		_, err := Parse(c.src)
		if err == nil {
			t.Fatalf("Parse(%q): expected error", c.src)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Parse(%q): got %q, want substring %q", c.src, err.Error(), c.want)
		}
		if _, ok := err.(*Error); !ok {
			t.Fatalf("Parse(%q): error must be *Error, got %T", c.src, err)
		}
	}
}

// TestAggLimitDoesNotShortCircuit：LIMIT 在聚合之后才生效。
func TestAggLimitDoesNotShortCircuit(t *testing.T) {
	r := aggRow(t, `MATCH (a) RETURN count(*) LIMIT 1`)
	if aggNum(t, r, "count(*)") != 4 {
		t.Fatalf("count with LIMIT 1: %v", r["count(*)"])
	}
}

func TestAggGroupCap(t *testing.T) {
	old := maxGroups
	maxGroups = 2
	defer func() { maxGroups = old }()
	_, err := Run(execSnap(t), `MATCH (a) RETURN a.key, count(*)`) // 4 组 > 2
	if err == nil || !strings.Contains(err.Error(), "too many groups") {
		t.Fatalf("group cap: %v", err)
	}
}

// TestAggMixedItems：聚合项与多个非聚合项混排，输出列序 = 声明序。
func TestAggMixedItems(t *testing.T) {
	res := run(t, `MATCH (a:Service) RETURN count(*), a.env, collect(a.key)`)
	if len(res.Columns) != 3 || res.Columns[0] != "count(*)" || res.Columns[1] != "a.env" || res.Columns[2] != "collect(a.key)" {
		t.Fatalf("columns: %v", res.Columns)
	}
	for _, r := range res.Rows {
		keys := r["collect(a.key)"].([]any)
		if int(aggNum(t, r, "count(*)")) != len(keys) {
			t.Fatalf("count vs collect: %+v", r)
		}
	}
}
