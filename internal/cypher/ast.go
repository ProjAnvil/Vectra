package cypher

// Query is one parsed statement: a sequence of clauses ending in RETURN.
// The v0.13 clause pipeline: MATCH may repeat (disjoint variables produce
// a cartesian product, shared variables join — matching Neo4j); WITH and
// UNWIND clauses are scope-validated here, and their parser arms land in
// T4/T5. Positions live on the expression nodes for error reports.
type Query struct {
	Clauses []Clause
}

// Clause is the clause sum type; the marker-method idiom follows Expr.
type Clause interface{ isClause() }

// MatchClause is one MATCH clause: comma-separated path patterns plus an
// optional WHERE filter. (An Optional flag arrives with T6.)
type MatchClause struct {
	Patterns []*PathPattern
	Where    Expr // nil when absent
}

// WithClause is the WITH clause: it projects its items into a new scope
// (only projected names survive) with an optional ORDER BY trailer and
// WHERE filter. The data type and scope validation are load-bearing from
// T2 (the scope-chain validator type-switches on it); the parser arm,
// SKIP/LIMIT/DISTINCT trailer fields and execution land in T4.
type WithClause struct {
	Items   []*ReturnItem
	OrderBy []OrderKey
	Where   Expr // nil when absent
}

// UnwindClause is the UNWIND clause: it declares its (mandatory) alias as
// a scalar bound to each element of the unwound expression. Data type and
// scope validation from T2; parser arm and execution land in T5.
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
func (e *Lit) pos() (int, int)     { return e.Line, e.Col }
func (e *ListLit) pos() (int, int) { return e.Line, e.Col }

func (*MatchClause) isClause()  {}
func (*WithClause) isClause()   {}
func (*UnwindClause) isClause() {}
func (*ReturnClause) isClause() {}
