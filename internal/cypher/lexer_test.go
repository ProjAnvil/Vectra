package cypher

import "testing"

func tk(k TokenKind, text string) Token { return Token{Kind: k, Text: text} }

func TestLexSnippets(t *testing.T) {
	cases := []struct {
		src  string
		want []Token
	}{
		{"(a:Bean)", []Token{tk(TParenL, "("), tk(TIdent, "a"), tk(TColon, ":"), tk(TIdent, "Bean"), tk(TParenR, ")")}},
		{"--> b", []Token{tk(TDMinus, "--"), tk(TGt, ">"), tk(TIdent, "b")}},
		{"<-- b", []Token{tk(TLt, "<"), tk(TDMinus, "--"), tk(TIdent, "b")}},
		{"-[v:A|B*1..3]->", []Token{tk(TMinus, "-"), tk(TBracketL, "["), tk(TIdent, "v"), tk(TColon, ":"), tk(TIdent, "A"), tk(TPipe, "|"), tk(TIdent, "B"), tk(TStar, "*"), tk(TNumber, "1"), tk(TDot, "."), tk(TDot, "."), tk(TNumber, "3"), tk(TBracketR, "]"), tk(TArrowR, "->")}},
		{"*..3", []Token{tk(TStar, "*"), tk(TDot, "."), tk(TDot, "."), tk(TNumber, "3")}},
		{"*2..", []Token{tk(TStar, "*"), tk(TNumber, "2"), tk(TDot, "."), tk(TDot, ".")}},
		{"'it\\'s'", []Token{tk(TString, "it's")}},
		{`"a\"b\n"`, []Token{tk(TString, "a\"b\n")}},
		{"1.5", []Token{tk(TNumber, "1.5")}},
		{"a.x <> 3", []Token{tk(TIdent, "a"), tk(TDot, "."), tk(TIdent, "x"), tk(TNe, "<>"), tk(TNumber, "3")}},
		{"w >= 2 AND w <= 4 OR w = 5", []Token{tk(TIdent, "w"), tk(TGe, ">="), tk(TNumber, "2"), tk(TIdent, "AND"), tk(TIdent, "w"), tk(TLe, "<="), tk(TNumber, "4"), tk(TIdent, "OR"), tk(TIdent, "w"), tk(TEq, "="), tk(TNumber, "5")}},
		{"{type: 'controller', n: 3}", []Token{tk(TBraceL, "{"), tk(TIdent, "type"), tk(TColon, ":"), tk(TString, "controller"), tk(TComma, ","), tk(TIdent, "n"), tk(TColon, ":"), tk(TNumber, "3"), tk(TBraceR, "}")}},
		{"match RETURN", []Token{tk(TIdent, "match"), tk(TIdent, "RETURN")}}, // 关键词大小写不敏感，原文保留
		{"// comment\nMATCH", []Token{tk(TIdent, "MATCH")}},
	}
	for _, c := range cases {
		got, err := Lex(c.src)
		if err != nil {
			t.Fatalf("Lex(%q): %v", c.src, err)
		}
		want := append(c.want, tk(TEOF, ""))
		if len(got) != len(want) {
			t.Fatalf("Lex(%q): got %d tokens, want %d", c.src, len(got), len(want))
		}
		for i := range want {
			if got[i].Kind != want[i].Kind || got[i].Text != want[i].Text {
				t.Fatalf("Lex(%q) token %d: got {%v %q}, want {%v %q}", c.src, i, got[i].Kind, got[i].Text, want[i].Kind, want[i].Text)
			}
		}
	}
}

func TestLexPositionsAndErrors(t *testing.T) {
	toks, err := Lex("(a)\n-b")
	if err != nil {
		t.Fatal(err)
	}
	if toks[3].Line != 2 || toks[3].Col != 1 || toks[3].Kind != TMinus { // ( a ) 换行后 - b：'-' 是第 4 个 token
		t.Fatalf("line/col: %+v", toks[3])
	}
	if _, err := Lex("(a) @"); err == nil {
		t.Fatal("illegal char must error")
	} else if e, ok := err.(*Error); !ok || e.Line != 1 || e.Col != 5 {
		t.Fatalf("error position: %v", err)
	}
	if _, err := Lex("'unterminated"); err == nil {
		t.Fatal("unterminated string must error")
	}
}
