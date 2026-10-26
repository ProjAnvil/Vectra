package cypher

import (
	"strings"
	"testing"
)

// Scope-chain tests for the WITH/UNWIND rules. The T2 parser does not
// accept those keywords yet (their clause-loop arms land in T4/T5), so the
// clause lists are constructed directly in-package and driven through
// validate(); the parse-path variants of these tests land with their
// parser arms.

// ---- small AST builders ----

// matchNodes builds a MATCH clause of one single-node pattern per var.
func matchNodes(vars ...string) *MatchClause {
	m := &MatchClause{}
	for _, v := range vars {
		m.Patterns = append(m.Patterns, &PathPattern{
			Nodes: []*NodePattern{{Var: v}},
		})
	}
	return m
}

// matchEdge builds a MATCH clause with one (a)-[e]->(b) pattern; hops
// customizes the edge (1,1 = fixed single hop).
func matchEdge(a, e, b string, minH, maxH int) *MatchClause {
	return &MatchClause{Patterns: []*PathPattern{{
		Nodes: []*NodePattern{{Var: a}, {Var: b}},
		Edges: []*EdgePattern{{Var: e, MinHops: minH, MaxHops: maxH}},
	}}}
}

// bareItem is one var item ("a"), optionally aliased ("a AS x").
func bareItem(v, alias string) *ReturnItem {
	it := &ReturnItem{Var: v, Name: v}
	if alias != "" {
		it.Alias, it.Name = alias, alias
	}
	return it
}

// propItem is one var.prop item, optionally aliased.
func propItem(v, p, alias string) *ReturnItem {
	it := &ReturnItem{Var: v, Prop: p, Name: v + "." + p}
	if alias != "" {
		it.Alias, it.Name = alias, alias
	}
	return it
}

// aggStarItem is one count(*) item, optionally aliased.
func aggStarItem(alias string) *ReturnItem {
	it := &ReturnItem{Agg: AggCall{Func: "count", Star: true}, Name: "count(*)"}
	if alias != "" {
		it.Alias, it.Name = alias, alias
	}
	return it
}

func returnOf(items ...*ReturnItem) *ReturnClause {
	return &ReturnClause{Limit: -1, Skip: -1, Items: items}
}

func withOf(items ...*ReturnItem) *WithClause {
	return &WithClause{Items: items}
}

func validateClauses(t *testing.T, clauses ...Clause) error {
	t.Helper()
	return (&Query{Clauses: clauses}).validate()
}

func wantErr(t *testing.T, clauses []Clause, want string) {
	t.Helper()
	err := validateClauses(t, clauses...)
	if err == nil {
		t.Fatalf("validate(%+v): expected error %q, got nil", clauses, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("validate: got %q, want substring %q", err.Error(), want)
	}
	if _, ok := err.(*Error); !ok {
		t.Fatalf("validate: error must be *Error, got %T", err)
	}
}

func wantOK(t *testing.T, clauses ...Clause) {
	t.Helper()
	if err := validateClauses(t, clauses...); err != nil {
		t.Fatalf("validate(%+v): unexpected error: %v", clauses, err)
	}
}

// Rule 2: WITH starts a new scope — non-bare items REQUIRE an alias.
func TestScopeWithAliasRequired(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a"),
		withOf(propItem("a", "x", "")),
		returnOf(bareItem("a", "")),
	}, "expression in WITH must be aliased (use AS)")

	wantErr(t, []Clause{
		matchNodes("a"),
		withOf(aggStarItem("")),
		returnOf(bareItem("a", "")),
	}, "expression in WITH must be aliased (use AS)")

	wantErr(t, []Clause{
		matchNodes("a"),
		&WithClause{}, // a projection with no items is meaningless
		returnOf(bareItem("a", "")),
	}, "WITH requires at least one item")

	// bare variables and properly aliased expressions pass
	wantOK(t,
		matchNodes("a"),
		withOf(bareItem("a", ""), propItem("a", "x", "k"), aggStarItem("c")),
		returnOf(bareItem("a", ""), bareItem("k", ""), bareItem("c", "")),
	)
}

// Rule 2: only projected names survive a WITH.
func TestScopeWithSurvival(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a", "b"),
		withOf(bareItem("a", "")),
		returnOf(bareItem("b", "")),
	}, `variable "b" is not defined in this scope (WITH starts a new scope; only projected variables survive)`)

	// bare items pass under their own name; aliases rename
	wantOK(t, matchNodes("a"), withOf(bareItem("a", "")), returnOf(bareItem("a", "")))
	wantOK(t, matchNodes("a"), withOf(bareItem("a", "x")), returnOf(bareItem("x", "")))
	wantErr(t, []Clause{
		matchNodes("a"),
		withOf(bareItem("a", "x")),
		returnOf(bareItem("a", "")),
	}, `variable "a" is not defined in this scope`)
}

// Rule 3: UNWIND requires an alias and declares it as a scalar; the prior
// scope survives UNWIND (only WITH resets it).
func TestScopeUnwind(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: ""},
		returnOf(bareItem("a", "")),
	}, "UNWIND expression must be aliased (use AS)")

	wantOK(t,
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: "k"},
		returnOf(bareItem("a", ""), bareItem("k", "")),
	)

	// the unwound expression is reference-checked too
	wantErr(t, []Clause{
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "q", Name: "x"}, Alias: "k"},
		returnOf(bareItem("a", "")),
	}, `variable "q" is not defined in this scope`)
}

// Rule 4: WHERE after WITH references projected names.
func TestScopeWhereAfterWith(t *testing.T) {
	wantOK(t,
		matchNodes("a"),
		&WithClause{Items: []*ReturnItem{bareItem("a", "")}, Where: &PropRef{Var: "a", Name: "x"}},
		returnOf(bareItem("a", "")),
	)
	wantErr(t, []Clause{
		matchNodes("a", "b"),
		&WithClause{Items: []*ReturnItem{bareItem("a", "")}, Where: &PropRef{Var: "b", Name: "x"}},
		returnOf(bareItem("a", "")),
	}, `variable "b" is not defined in this scope`)
}

// Rule 5: ORDER BY after WITH references projected names only.
func TestScopeOrderAfterWith(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a", "b"),
		withOf(bareItem("a", "")),
		&ReturnClause{Limit: -1, Skip: -1, Items: []*ReturnItem{bareItem("a", "")},
			OrderBy: []OrderKey{{Name: "b.w"}}},
	}, `ORDER BY variable "b" is not defined in this scope`)

	wantErr(t, []Clause{
		matchNodes("a", "b"),
		&WithClause{Items: []*ReturnItem{bareItem("a", "")},
			OrderBy: []OrderKey{{Name: "b.w"}}},
		returnOf(bareItem("a", "")),
	}, `ORDER BY variable "b" is not defined in this scope`)

	// projected names (and their properties) are fine
	wantOK(t,
		matchNodes("a"),
		withOf(bareItem("a", "")),
		&ReturnClause{Limit: -1, Skip: -1, Items: []*ReturnItem{bareItem("a", "")},
			OrderBy: []OrderKey{{Name: "a.w"}}},
	)
	wantOK(t,
		matchNodes("a"),
		&WithClause{Items: []*ReturnItem{bareItem("a", "")},
			OrderBy: []OrderKey{{Name: "a.w"}}},
		returnOf(bareItem("a", "")),
	)
}

// Rule 6: error-string families. Pure-MATCH queries keep the historical
// strings byte-identical; once WITH/UNWIND has been seen the scope-chain
// family applies.
func TestScopeErrorFamilies(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a"),
		returnOf(bareItem("b", "")),
	}, `variable "b" is not declared in the pattern`)

	wantErr(t, []Clause{
		matchNodes("a"),
		&ReturnClause{Limit: -1, Skip: -1, Items: []*ReturnItem{bareItem("a", "")},
			OrderBy: []OrderKey{{Name: "b.w"}}},
	}, `ORDER BY variable "b" is not declared in the pattern`)

	// the variable-length edge reference strings are family-independent
	wantErr(t, []Clause{
		matchEdge("a", "r", "b", 1, 2),
		returnOf(bareItem("r", "")),
	}, `edge variable "r" is on a variable-length edge and cannot be referenced (node-pair semantics)`)
	wantErr(t, []Clause{
		matchEdge("a", "r", "b", 1, 2),
		&MatchClause{Patterns: []*PathPattern{{Nodes: []*NodePattern{{Var: "c"}}}},
			Where: &PropRef{Var: "r", Name: "x"}},
		returnOf(bareItem("c", "")),
	}, `edge variable "r" is on a variable-length edge and cannot be referenced (node-pair semantics)`)
}

// Rule 7: a scalar variable cannot be (re)bound by a pattern.
func TestScopeScalarAsPatternVar(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: "k"},
		matchNodes("k"),
		returnOf(bareItem("k", "")),
	}, `variable "k" is a scalar; pattern variables must be nodes or edges`)

	// scalars projected by WITH hit the same check
	wantErr(t, []Clause{
		matchNodes("a"),
		withOf(aggStarItem("c")),
		matchNodes("c"),
		returnOf(bareItem("c", "")),
	}, `variable "c" is a scalar; pattern variables must be nodes or edges`)

	// node/edge reuse across patterns is a join, not an error
	wantOK(t, matchNodes("a"), matchNodes("a"), returnOf(bareItem("a", "")))
}

// Rule 8: property access on a scalar variable is a kind error (the
// null-instead-of-error path remains only for unbound OPTIONAL vars).
func TestScopePropRefOnScalarVar(t *testing.T) {
	wantErr(t, []Clause{
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: "k"},
		&MatchClause{Patterns: []*PathPattern{{Nodes: []*NodePattern{{Var: "b"}}}},
			Where: &PropRef{Var: "k", Name: "foo"}},
		returnOf(bareItem("b", "")),
	}, `variable "k" is a scalar; property access requires a node or edge`)

	wantErr(t, []Clause{
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: "k"},
		returnOf(propItem("k", "foo", "")),
	}, `variable "k" is a scalar; property access requires a node or edge`)

	// returning the scalar itself is fine
	wantOK(t,
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: "k"},
		returnOf(bareItem("k", "")),
	)
}

// RETURN * is valid only for the degenerate single-MATCH query.
func TestScopeReturnStarDegenerateOnly(t *testing.T) {
	star := &ReturnClause{Limit: -1, Skip: -1, Star: true}
	wantErr(t, []Clause{matchNodes("a"), withOf(bareItem("a", "")), star},
		"RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses")
	wantErr(t, []Clause{matchNodes("a"), matchNodes("b"), star},
		"RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses")
	wantErr(t, []Clause{
		matchNodes("a"),
		&UnwindClause{Expr: &PropRef{Var: "a", Name: "x"}, Alias: "k"},
		star,
	}, "RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses")

	// the degenerate form keeps its historical checks
	wantOK(t, matchNodes("a"), star)
	wantErr(t, []Clause{matchNodes(""), star},
		"RETURN * with no named variables in the pattern")
	wantErr(t, []Clause{matchNodes("a"),
		&ReturnClause{Limit: -1, Skip: -1, Star: true, Items: []*ReturnItem{bareItem("a", "")}}},
		"RETURN * cannot be combined with other RETURN items")
	wantErr(t, []Clause{matchNodes("a"),
		&ReturnClause{Limit: -1, Skip: -1, Star: true, Items: []*ReturnItem{aggStarItem("")}}},
		"RETURN * cannot be combined with aggregation")
}
