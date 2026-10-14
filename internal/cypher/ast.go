package cypher

// Query is one parsed statement: MATCH ... [WHERE ...] RETURN ... [ORDER BY
// ...] [LIMIT n]. Positions live on the expression nodes for error reports.
type Query struct {
	Match  *MatchClause
	Return *ReturnClause
}

type MatchClause struct {
	Patterns []*PathPattern
	Where    Expr // nil when absent
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
}

type ReturnItem struct {
	Var  string
	Prop string // "" = the whole variable; "key"/"label"/"id" are pseudo-props
	Name string // display name: "a" or "a.key"
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
