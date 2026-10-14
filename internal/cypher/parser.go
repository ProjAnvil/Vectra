package cypher

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse lexes and parses one statement, then runs the static checks
// (declared-variable references, variable-length edge bindings, ORDER BY
// coverage).
func Parse(src string) (*Query, error) {
	toks, err := Lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	if err := q.validate(); err != nil {
		return nil, err
	}
	return q, nil
}

type parser struct {
	toks []Token
	pos  int
}

func (p *parser) cur() Token { return p.toks[p.pos] }

func (p *parser) at(k TokenKind) bool { return p.cur().Kind == k }

func (p *parser) kw(word string) bool {
	t := p.cur()
	return t.Kind == TIdent && strings.EqualFold(t.Text, word)
}

func (p *parser) next() Token { t := p.cur(); p.pos++; return t }

func (p *parser) errf(format string, args ...any) error {
	t := p.cur()
	return &Error{Line: t.Line, Col: t.Col, Msg: fmt.Sprintf(format, args...)}
}

func describe(t Token) string {
	switch t.Kind {
	case TEOF:
		return "end of input"
	case TString:
		return fmt.Sprintf("string %q", t.Text)
	default:
		return fmt.Sprintf("%q", t.Text)
	}
}

func (p *parser) expect(k TokenKind, what string) (Token, error) {
	if !p.at(k) {
		return Token{}, p.errf("expected %s, got %s", what, describe(p.cur()))
	}
	return p.next(), nil
}

func (p *parser) expectKw(word string) error {
	if !p.kw(word) {
		return p.errf("expected %s, got %s", word, describe(p.cur()))
	}
	p.next()
	return nil
}

func (p *parser) parseQuery() (*Query, error) {
	if err := p.expectKw("MATCH"); err != nil {
		return nil, err
	}
	q := &Query{Match: &MatchClause{}, Return: &ReturnClause{Limit: -1}}
	pat, err := p.parsePath()
	if err != nil {
		return nil, err
	}
	q.Match.Patterns = append(q.Match.Patterns, pat)
	for p.at(TComma) {
		p.next()
		pat, err := p.parsePath()
		if err != nil {
			return nil, err
		}
		q.Match.Patterns = append(q.Match.Patterns, pat)
	}
	if p.kw("WHERE") {
		p.next()
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		q.Match.Where = e
	}
	if err := p.expectKw("RETURN"); err != nil {
		return nil, err
	}
	if p.at(TStar) {
		p.next()
		q.Return.Star = true
	} else {
		item, err := p.parseReturnItem()
		if err != nil {
			return nil, err
		}
		q.Return.Items = append(q.Return.Items, item)
		for p.at(TComma) {
			p.next()
			item, err := p.parseReturnItem()
			if err != nil {
				return nil, err
			}
			q.Return.Items = append(q.Return.Items, item)
		}
	}
	if p.kw("ORDER") {
		p.next()
		if err := p.expectKw("BY"); err != nil {
			return nil, err
		}
		key, err := p.parseOrderKey()
		if err != nil {
			return nil, err
		}
		q.Return.OrderBy = append(q.Return.OrderBy, key)
		for p.at(TComma) {
			p.next()
			key, err := p.parseOrderKey()
			if err != nil {
				return nil, err
			}
			q.Return.OrderBy = append(q.Return.OrderBy, key)
		}
	}
	if p.kw("LIMIT") {
		p.next()
		t, err := p.expect(TNumber, "number")
		if err != nil {
			return nil, err
		}
		n, convErr := strconv.Atoi(t.Text)
		if convErr != nil || n < 0 {
			return nil, &Error{Line: t.Line, Col: t.Col, Msg: fmt.Sprintf("bad LIMIT %q", t.Text)}
		}
		q.Return.Limit = n
	}
	if !p.at(TEOF) {
		return nil, p.errf("expected end of input, got %s", describe(p.cur()))
	}
	return q, nil
}

// parsePath parses nodePat (edgePat nodePat)*. Edge forms, by first token:
// TDMinus (`--`, `-->`), TLt (`<--`, `<-[...]-`), TMinus (`-[...]-`, `-[...]->`).
func (p *parser) parsePath() (*PathPattern, error) {
	path := &PathPattern{}
	n, err := p.parseNodePattern()
	if err != nil {
		return nil, err
	}
	path.Nodes = append(path.Nodes, n)
	for {
		var ep *EdgePattern
		switch {
		case p.at(TDMinus): // -- / -->
			p.next()
			ep = &EdgePattern{MinHops: 1, MaxHops: 1, Dir: DirBoth}
			if p.at(TGt) {
				p.next()
				ep.Dir = DirOut
			}
		case p.at(TLt): // <-- / <-[...]-
			p.next()
			if p.at(TDMinus) {
				p.next()
				ep = &EdgePattern{MinHops: 1, MaxHops: 1, Dir: DirIn}
			} else {
				if _, err := p.expect(TMinus, "\"-\""); err != nil {
					return nil, err
				}
				inner, err := p.parseBracketEdge()
				if err != nil {
					return nil, err
				}
				ep = inner
				ep.Dir = DirIn
				if _, err := p.expect(TMinus, "\"-\""); err != nil {
					return nil, err
				}
				if p.at(TGt) {
					return nil, p.errf("conflicting edge direction: both < and >")
				}
			}
		case p.at(TMinus): // -[...]-> / -[...]-
			p.next()
			inner, err := p.parseBracketEdge()
			if err != nil {
				return nil, err
			}
			ep = inner
			if p.at(TArrowR) { // ]->
				p.next()
				ep.Dir = DirOut
			} else if p.at(TMinus) { // ]- or ]-> (the lexer may also split "->")
				p.next()
				if p.at(TGt) {
					p.next()
					ep.Dir = DirOut
				}
				// else: undirected -[...]-(b), Dir stays Both
			}
		default:
			return path, nil
		}
		path.Edges = append(path.Edges, ep)
		n, err := p.parseNodePattern()
		if err != nil {
			return nil, err
		}
		path.Nodes = append(path.Nodes, n)
	}
}

// parseBracketEdge parses [var][:A|B][hops] between brackets (brackets
// themselves included); direction is set by the caller.
func (p *parser) parseBracketEdge() (*EdgePattern, error) {
	if _, err := p.expect(TBracketL, "\"[\""); err != nil {
		return nil, err
	}
	ep := &EdgePattern{MinHops: 1, MaxHops: 1, Dir: DirBoth}
	if p.at(TIdent) && !p.kw("TRUE") && !p.kw("FALSE") && !p.kw("NULL") {
		ep.Var = p.next().Text
	}
	if p.at(TColon) {
		p.next()
		l, err := p.expect(TIdent, "label name")
		if err != nil {
			return nil, err
		}
		ep.Labels = append(ep.Labels, l.Text)
		for p.at(TPipe) {
			p.next()
			l, err := p.expect(TIdent, "label name")
			if err != nil {
				return nil, err
			}
			ep.Labels = append(ep.Labels, l.Text)
		}
	}
	if p.at(TStar) {
		p.next()
		switch {
		case p.at(TNumber): // *n | *n.. | *n..m
			n := p.mustNum()
			if p.at(TDot) {
				p.next()
				if _, err := p.expect(TDot, "\".\""); err != nil {
					return nil, err
				}
				if p.at(TNumber) {
					ep.MinHops, ep.MaxHops = n, p.mustNum()
				} else {
					ep.MinHops, ep.MaxHops = n, -1
				}
			} else {
				ep.MinHops, ep.MaxHops = n, n
			}
		case p.at(TDot): // *..m (lower bound defaults to 1)
			p.next()
			if _, err := p.expect(TDot, "\".\""); err != nil {
				return nil, err
			}
			ep.MinHops, ep.MaxHops = 1, p.mustNum()
		default: // bare * = 1..∞
			ep.MinHops, ep.MaxHops = 1, -1
		}
	}
	if _, err := p.expect(TBracketR, "\"]\""); err != nil {
		return nil, err
	}
	return ep, nil
}

func (p *parser) mustNum() int {
	t := p.next()
	n, err := strconv.Atoi(t.Text)
	if err != nil {
		panic(fmt.Sprintf("lexer produced non-numeric TNumber %q", t.Text))
	}
	return n
}

func (p *parser) parseNodePattern() (*NodePattern, error) {
	if _, err := p.expect(TParenL, "\"(\""); err != nil {
		return nil, err
	}
	np := &NodePattern{}
	if p.at(TIdent) && !p.kw("TRUE") && !p.kw("FALSE") && !p.kw("NULL") {
		np.Var = p.next().Text
	}
	if p.at(TColon) {
		p.next()
		l, err := p.expect(TIdent, "label name")
		if err != nil {
			return nil, err
		}
		np.Label = l.Text
	}
	if p.at(TBraceL) {
		p.next()
		np.Props = map[string]any{}
		for {
			k, err := p.expect(TIdent, "property name")
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TColon, "\":\""); err != nil {
				return nil, err
			}
			v, err := p.parseLiteral()
			if err != nil {
				return nil, err
			}
			np.Props[k.Text] = v
			if p.at(TComma) {
				p.next()
				continue
			}
			break
		}
		if _, err := p.expect(TBraceR, "\"}\""); err != nil {
			return nil, err
		}
	}
	if _, err := p.expect(TParenR, "\")\""); err != nil {
		return nil, err
	}
	return np, nil
}

// parseLiteral parses a scalar literal (string, number — optionally
// negated, true/false/null keywords).
func (p *parser) parseLiteral() (any, error) {
	t := p.cur()
	switch {
	case t.Kind == TString:
		p.next()
		return t.Text, nil
	case t.Kind == TNumber:
		p.next()
		f, err := strconv.ParseFloat(t.Text, 64)
		if err != nil {
			return nil, &Error{t.Line, t.Col, fmt.Sprintf("bad number %q", t.Text)}
		}
		return f, nil
	case p.at(TMinus) && p.toks[p.pos+1].Kind == TNumber:
		p.next()
		n := p.next()
		f, err := strconv.ParseFloat(n.Text, 64)
		if err != nil {
			return nil, &Error{t.Line, t.Col, fmt.Sprintf("bad number %q", n.Text)}
		}
		return -f, nil
	case p.kw("TRUE"):
		p.next()
		return true, nil
	case p.kw("FALSE"):
		p.next()
		return false, nil
	case p.kw("NULL"):
		p.next()
		return nil, nil
	}
	return nil, p.errf("expected literal, got %s", describe(t))
}

func (p *parser) parseReturnItem() (*ReturnItem, error) {
	v, err := p.expect(TIdent, "variable")
	if err != nil {
		return nil, err
	}
	item := &ReturnItem{Var: v.Text, Name: v.Text}
	if p.at(TDot) {
		p.next()
		prop, err := p.expect(TIdent, "property name")
		if err != nil {
			return nil, err
		}
		item.Prop = prop.Text
		item.Name = v.Text + "." + prop.Text
	}
	return item, nil
}

func (p *parser) parseOrderKey() (OrderKey, error) {
	v, err := p.expect(TIdent, "variable")
	if err != nil {
		return OrderKey{}, err
	}
	name := v.Text
	if p.at(TDot) {
		p.next()
		prop, err := p.expect(TIdent, "property name")
		if err != nil {
			return OrderKey{}, err
		}
		name = v.Text + "." + prop.Text
	}
	key := OrderKey{Name: name}
	if p.kw("ASC") {
		p.next()
	} else if p.kw("DESC") {
		p.next()
		key.Desc = true
	}
	return key, nil
}

// ---- WHERE expression grammar: OR < AND < NOT < comparison ----

func (p *parser) parseOr() (Expr, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.kw("OR") {
		t := p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "OR", L: l, R: r, Line: t.Line, Col: t.Col}
	}
	return l, nil
}

func (p *parser) parseAnd() (Expr, error) {
	l, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.kw("AND") {
		t := p.next()
		r, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l = &Binary{Op: "AND", L: l, R: r, Line: t.Line, Col: t.Col}
	}
	return l, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.kw("NOT") {
		t := p.next()
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &Not{X: x, Line: t.Line, Col: t.Col}, nil
	}
	return p.parseCmp()
}

func (p *parser) parseCmp() (Expr, error) {
	l, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	t := p.cur()
	switch {
	case t.Kind == TEq || t.Kind == TNe || t.Kind == TLt || t.Kind == TLe || t.Kind == TGt || t.Kind == TGe:
		p.next()
		r, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: t.Text, L: l, R: r, Line: t.Line, Col: t.Col}, nil
	case p.kw("STARTS"):
		p.next()
		if err := p.expectKw("WITH"); err != nil {
			return nil, err
		}
		r, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: "STARTS WITH", L: l, R: r, Line: t.Line, Col: t.Col}, nil
	case p.kw("ENDS"):
		p.next()
		if err := p.expectKw("WITH"); err != nil {
			return nil, err
		}
		r, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: "ENDS WITH", L: l, R: r, Line: t.Line, Col: t.Col}, nil
	case p.kw("CONTAINS"):
		p.next()
		r, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: "CONTAINS", L: l, R: r, Line: t.Line, Col: t.Col}, nil
	case p.kw("IN"):
		p.next()
		r, err := p.parseListLit()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: "IN", L: l, R: r, Line: t.Line, Col: t.Col}, nil
	}
	return l, nil
}

func (p *parser) parseOperand() (Expr, error) {
	t := p.cur()
	switch {
	case p.at(TParenL): // parenthesized subexpression
		p.next()
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TParenR, "\")\""); err != nil {
			return nil, err
		}
		return e, nil
	case t.Kind == TString || t.Kind == TNumber || p.at(TMinus) ||
		p.kw("TRUE") || p.kw("FALSE") || p.kw("NULL"):
		v, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		return &Lit{Val: v, Line: t.Line, Col: t.Col}, nil
	case t.Kind == TIdent: // var.prop
		p.next()
		if _, err := p.expect(TDot, "\".\""); err != nil {
			return nil, err
		}
		prop, err := p.expect(TIdent, "property name")
		if err != nil {
			return nil, err
		}
		return &PropRef{Var: t.Text, Name: prop.Text, Line: t.Line, Col: t.Col}, nil
	}
	return nil, p.errf("expected property reference or literal, got %s", describe(t))
}

func (p *parser) parseListLit() (Expr, error) {
	t := p.cur()
	if _, err := p.expect(TBracketL, "\"[\""); err != nil {
		return nil, err
	}
	lit := &ListLit{Line: t.Line, Col: t.Col}
	for {
		v, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		lit.Vals = append(lit.Vals, v)
		if p.at(TComma) {
			p.next()
			continue
		}
		break
	}
	if _, err := p.expect(TBracketR, "\"]\""); err != nil {
		return nil, err
	}
	return lit, nil
}

// ---- static validation ----

// validate enforces: referenced variables are declared; variable-length
// edge variables are never referenced; ORDER BY names appear in RETURN
// items; RETURN * has at least one named variable.
func (q *Query) validate() error {
	nodeVars := map[string]bool{}
	edgeVars := map[string]*EdgePattern{}
	for _, pat := range q.Match.Patterns {
		for _, n := range pat.Nodes {
			if n.Var != "" {
				nodeVars[n.Var] = true
			}
		}
		for _, e := range pat.Edges {
			if e.Var != "" {
				edgeVars[e.Var] = e
			}
		}
	}
	declared := func(v string) bool { return nodeVars[v] || edgeVars[v] != nil }

	var checkPropRef func(Expr) error
	checkPropRef = func(e Expr) error {
		switch x := e.(type) {
		case *PropRef:
			if !declared(x.Var) {
				return &Error{x.Line, x.Col, fmt.Sprintf("variable %q is not declared in the pattern", x.Var)}
			}
			if ep := edgeVars[x.Var]; ep != nil && (ep.MinHops != 1 || ep.MaxHops != 1) {
				return &Error{x.Line, x.Col, fmt.Sprintf("edge variable %q is on a variable-length edge and cannot be referenced (node-pair semantics)", x.Var)}
			}
			return nil
		case *Binary:
			if err := checkPropRef(x.L); err != nil {
				return err
			}
			return checkPropRef(x.R)
		case *Not:
			return checkPropRef(x.X)
		}
		return nil
	}
	if q.Match.Where != nil {
		if err := checkPropRef(q.Match.Where); err != nil {
			return err
		}
	}

	items := q.Return.Items
	if q.Return.Star {
		if len(nodeVars)+len(edgeVars) == 0 {
			return &Error{1, 1, "RETURN * with no named variables in the pattern"}
		}
		return nil // ORDER BY together with * is rejected below
	}
	if len(items) == 0 {
		return &Error{1, 1, "RETURN requires at least one item"}
	}
	names := map[string]bool{}
	for _, it := range items {
		if !declared(it.Var) {
			return &Error{1, 1, fmt.Sprintf("variable %q is not declared in the pattern", it.Var)}
		}
		if ep := edgeVars[it.Var]; ep != nil && (ep.MinHops != 1 || ep.MaxHops != 1) {
			return &Error{1, 1, fmt.Sprintf("edge variable %q is on a variable-length edge and cannot be referenced (node-pair semantics)", it.Var)}
		}
		names[it.Name] = true
	}
	for _, k := range q.Return.OrderBy {
		varPart := k.Name
		if i := strings.Index(varPart, "."); i >= 0 {
			varPart = varPart[:i]
		}
		if !declared(varPart) {
			return &Error{1, 1, fmt.Sprintf("ORDER BY variable %q is not declared in the pattern", varPart)}
		}
		if ep := edgeVars[varPart]; ep != nil && (ep.MinHops != 1 || ep.MaxHops != 1) {
			return &Error{1, 1, fmt.Sprintf("edge variable %q is on a variable-length edge and cannot be referenced (node-pair semantics)", varPart)}
		}
	}
	return nil
}
