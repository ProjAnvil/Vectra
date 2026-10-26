package cypher

// Query is one parsed statement: a sequence of clauses ending in RETURN.
// The v0.13 clause pipeline: MATCH may repeat (disjoint variables produce
// a cartesian product, shared variables join — matching Neo4j); WITH and
// UNWIND clauses are scope-validated here and parsed by their own arms.
// Positions live on the expression nodes for error reports.
//
// Clauses is backed by the inline clauses array for the first four
// clauses: every realistic MATCH/WITH/UNWIND pipeline parses with ZERO
// clause-slice allocations (the executor's alloc budget includes none),
// and the fifth clause spills to a heap slice through append's natural
// growth. Parse seeds the slice via newQuery; hand-built Queries assign
// Clauses directly and ignore the array. Do not copy a Query by value
// while it is being parsed (the slice would alias the original's array).
type Query struct {
	clauses [4]Clause
	Clauses []Clause
}

// newQuery returns a Query whose Clauses slice is backed by the inline
// array: appends 1..4 allocate nothing.
func newQuery() *Query {
	q := &Query{}
	q.Clauses = q.clauses[:0]
	return q
}

// Clause is the clause sum type; the marker-method idiom follows Expr.
type Clause interface{ isClause() }

// MatchClause is one MATCH clause: comma-separated path patterns plus an
// optional WHERE filter. Optional marks the OPTIONAL MATCH spelling:
// optionality covers the whole clause (every comma pattern is
// all-or-nothing per incoming row — see optionalRows).
type MatchClause struct {
	Patterns []*PathPattern
	Where    Expr // nil when absent
	Optional bool // OPTIONAL MATCH: an empty expansion keeps the incoming rows
}

// WithClause is the WITH clause: it projects its items into a new scope
// (only projected names survive) with an optional DISTINCT dedup, an ORDER
// BY/SKIP/LIMIT trailer, and a WHERE filter over the projected rows.
type WithClause struct {
	Items    []*ReturnItem
	OrderBy  []OrderKey
	Skip     int  // <0 = none (after ORDER BY, before LIMIT)
	Limit    int  // <0 = none
	Distinct bool // WITH DISTINCT: dedup the projected rows
	Where    Expr // nil when absent; filters AFTER the trailer
}

// UnwindClause is the UNWIND clause: it declares its (mandatory) alias as
// a scalar bound to each element of the unwound expression — a list
// literal, a bare variable naming a list-valued scalar (typically a
// collect() alias), or var.prop.
type UnwindClause struct {
	Expr  Expr
	Alias string // mandatory: UNWIND expr AS alias
}

// PathPattern is one comma-separated path of the MATCH clause:
// Nodes[i] --Edges[i]--> Nodes[i+1].
type PathPattern struct {
	Nodes []*NodePattern
	Edges []*EdgePattern
}

type NodePattern struct {
	Var   string         // "" = anonymous
	Label string         // "" = any
	Props map[string]any // literal equality conditions; nil when absent
}

// EdgePattern directions.
const (
	DirOut = iota
	DirIn
	DirBoth
)

// EdgePattern is one pattern element between two nodes. A fixed single hop
// is {MinHops:1, MaxHops:1}; MaxHops<0 means unbounded. Variable-length
// (Min!=1 || Max!=1) follows node-pair semantics and cannot bind edges.
type EdgePattern struct {
	Var     string
	Labels  []string // nil = any label
	MinHops int
	MaxHops int
	Dir     int
}

type ReturnClause struct {
	Star    bool
	Items   []*ReturnItem
	OrderBy []OrderKey
	Limit   int // <0 = none
	Skip    int // <0 = none (SKIP n, after ORDER BY, before LIMIT)
}

type ReturnItem struct {
	Var   string
	Prop  string  // "" = the whole variable; "key"/"label"/"id" are pseudo-props
	Name  string  // display name: "a" or "a.key", the alias when AS is given, or the aggregation text
	Alias string  // the "AS n" name; "" = no alias
	Agg   AggCall // non-zero Func marks an aggregation item
}

// AggCall is one aggregation in a RETURN item: count/sum/avg/min/max/collect.
// The zero value (Func "") marks a plain non-aggregating item. Star is only
// valid for count(*), whose Arg is nil.
type AggCall struct {
	Func     string // "count", "sum", "avg", "min", "max", "collect"
	Star     bool   // count(*): count rows, not values
	Distinct bool   // count/collect(... DISTINCT ...) — dedupe within the group
	Arg      *ReturnItem
}

type OrderKey struct {
	Name string // must equal a ReturnItem.Name
	Desc bool
}

// Expr is a WHERE expression tree. The Binary ops: = <> < <= > >= AND OR
// STARTS WITH ENDS WITH CONTAINS IN. Not is unary negation. PropRef reads
// var.prop (or var's pseudo-props). Lit carries a string/float64/bool/nil
// literal; ListLit the right side of IN.
type Expr interface{ pos() (line, col int) }

type Binary struct {
	Op        string
	L, R      Expr
	Line, Col int
}

type Not struct {
	X         Expr
	Line, Col int
}

type PropRef struct {
	Var, Name string
	Line, Col int
}

// VarRef is a bare variable reference used as a WHERE operand (the
// WHERE-after-WITH extension): it names a scalar binding (a WITH/UNWIND
// projection) whose value is the operand itself. Node and edge variables
// are rejected at validation — only scalars may be referenced bare.
type VarRef struct {
	Var       string
	Line, Col int
}

type Lit struct {
	Val       any
	Line, Col int
}

type ListLit struct {
	Vals      []any
	Line, Col int
}

func (e *Binary) pos() (int, int)  { return e.Line, e.Col }
func (e *Not) pos() (int, int)     { return e.Line, e.Col }
func (e *PropRef) pos() (int, int) { return e.Line, e.Col }
func (e *VarRef) pos() (int, int)  { return e.Line, e.Col }
func (e *Lit) pos() (int, int)     { return e.Line, e.Col }
func (e *ListLit) pos() (int, int) { return e.Line, e.Col }

func (*MatchClause) isClause()  {}
func (*WithClause) isClause()   {}
func (*UnwindClause) isClause() {}
func (*ReturnClause) isClause() {}
