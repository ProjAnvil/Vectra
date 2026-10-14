// Package cypher implements the Cypher-subset query language of Vectra:
// lexer, recursive-descent parser, and a row-based executor running directly
// on an engine.Snapshot. The supported grammar is an openCypher/GQL-compatible
// core — see the spec (docs/superpowers/specs/2026-10-14-typed-index-cypher.md).
package cypher

import (
	"fmt"
	"strings"
)

// TokenKind enumerates lexical classes. Words (including keywords) lex as
// TIdent with their raw text; the parser matches keywords case-insensitively.
type TokenKind int

const (
	TEOF TokenKind = iota
	TIdent
	TString
	TNumber

	TParenL // (
	TParenR // )
	TBracketL
	TBracketR
	TBraceL
	TBraceR
	TColon
	TComma
	TDot
	TStar
	TPipe // |

	TMinus // -
	TArrowR
	TDMinus // --

	TLt // <
	TGt // >
	TLe // <=
	TGe // >=
	TNe // <>
	TEq // =
)

// Token is one lexical token with 1-based source position.
type Token struct {
	Kind      TokenKind
	Text      string
	Line, Col int
}

// Error is a positioned lexer/parser/executor error.
type Error struct {
	Line, Col int
	Msg       string
}

func (e *Error) Error() string { return fmt.Sprintf("cypher: %d:%d: %s", e.Line, e.Col, e.Msg) }

// Lex tokenizes src. Multi-char operators are lexed greedily (->, --, <=,
// >=, <>); `1..3` style ranges stay three tokens (Number, Dot, Dot, Number)
// because the fraction dot is only consumed when a digit follows.
func Lex(src string) ([]Token, error) {
	var toks []Token
	line, col := 1, 1
	i := 0
	next := func() (byte, bool) { // current byte advanced past
		if i >= len(src) {
			return 0, false
		}
		c := src[i]
		return c, true
	}
	peek := func(off int) (byte, bool) {
		if i+off >= len(src) {
			return 0, false
		}
		return src[i+off], true
	}
	adv := func() byte {
		c := src[i]
		i++
		if c == '\n' {
			line++
			col = 1
		} else {
			col++
		}
		return c
	}
	emit := func(k TokenKind, sl, sc int, text string) {
		toks = append(toks, Token{Kind: k, Text: text, Line: sl, Col: sc})
	}
	for {
		// skip whitespace and // comments
		for {
			c, ok := next()
			if !ok {
				break
			}
			if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
				adv()
				continue
			}
			if c == '/' {
				if p, _ := peek(1); p == '/' {
					for {
						c2, ok2 := next()
						if !ok2 || c2 == '\n' {
							break
						}
						adv()
					}
					continue
				}
			}
			break
		}
		sl, sc := line, col
		c, ok := next()
		if !ok {
			emit(TEOF, sl, sc, "")
			break
		}
		switch {
		case isIdentStart(c):
			var sb strings.Builder
			for {
				c2, ok2 := next()
				if !ok2 || !isIdentPart(c2) {
					break
				}
				sb.WriteByte(adv())
			}
			emit(TIdent, sl, sc, sb.String())
		case c >= '0' && c <= '9':
			var sb strings.Builder
			for {
				c2, ok2 := next()
				if !ok2 || c2 < '0' || c2 > '9' {
					break
				}
				sb.WriteByte(adv())
			}
			if p, okp := peek(0); okp && p == '.' { // fraction only when a digit follows
				if p2, okp2 := peek(1); okp2 && p2 >= '0' && p2 <= '9' {
					sb.WriteByte(adv()) // '.'
					for {
						c2, ok2 := next()
						if !ok2 || c2 < '0' || c2 > '9' {
							break
						}
						sb.WriteByte(adv())
					}
				}
			}
			emit(TNumber, sl, sc, sb.String())
		case c == '\'' || c == '"':
			quote := adv()
			var sb strings.Builder
			closed := false
			for {
				c2, ok2 := next()
				if !ok2 {
					break
				}
				if c2 == '\n' {
					break // strings do not span lines
				}
				ch := adv()
				if ch == quote {
					closed = true
					break
				}
				if ch == '\\' {
					if _, okE := next(); !okE {
						break
					}
					e := adv()
					switch e {
					case 'n':
						sb.WriteByte('\n')
					case 't':
						sb.WriteByte('\t')
					case 'r':
						sb.WriteByte('\r')
					case '\\', '\'', '"':
						sb.WriteByte(e)
					default:
						return nil, &Error{sl, sc, fmt.Sprintf("unknown escape \\%c", e)}
					}
					continue
				}
				sb.WriteByte(ch)
			}
			if !closed {
				return nil, &Error{sl, sc, "unterminated string"}
			}
			emit(TString, sl, sc, sb.String())
		default:
			adv()
			switch c {
			case '(':
				emit(TParenL, sl, sc, "(")
			case ')':
				emit(TParenR, sl, sc, ")")
			case '[':
				emit(TBracketL, sl, sc, "[")
			case ']':
				emit(TBracketR, sl, sc, "]")
			case '{':
				emit(TBraceL, sl, sc, "{")
			case '}':
				emit(TBraceR, sl, sc, "}")
			case ':':
				emit(TColon, sl, sc, ":")
			case ',':
				emit(TComma, sl, sc, ",")
			case '.':
				emit(TDot, sl, sc, ".")
			case '*':
				emit(TStar, sl, sc, "*")
			case '|':
				emit(TPipe, sl, sc, "|")
			case '-':
				if p, _ := peek(0); p == '>' {
					adv()
					emit(TArrowR, sl, sc, "->")
				} else if p == '-' {
					adv()
					emit(TDMinus, sl, sc, "--")
				} else {
					emit(TMinus, sl, sc, "-")
				}
			case '<':
				if p, _ := peek(0); p == '=' {
					adv()
					emit(TLe, sl, sc, "<=")
				} else if p == '>' {
					adv()
					emit(TNe, sl, sc, "<>")
				} else {
					emit(TLt, sl, sc, "<")
				}
			case '>':
				if p, _ := peek(0); p == '=' {
					adv()
					emit(TGe, sl, sc, ">=")
				} else {
					emit(TGt, sl, sc, ">")
				}
			case '=':
				emit(TEq, sl, sc, "=")
			default:
				return nil, &Error{sl, sc, fmt.Sprintf("unexpected character %q", string(rune(c)))}
			}
		}
	}
	return toks, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}
