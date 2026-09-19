// SPDX-License-Identifier: MIT

package sql

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The SQL-standard spellings whose grammar is their own — a call written with
// keywords rather than commas — rewritten into the calls this engine answers
// (#1169). Each rewrite is enumerated in ADR-0038's table
// (physical.TestEveryGrammarRewriteProducesACallItsSignatureAccepts), which is
// what keeps a minted name and arity from outrunning the signature the
// registry declares.

// parseSubstringExpr reads the SQL-standard SUBSTRING spellings and rewrites
// each into the `substring` call this engine already answers (#1169):
//
//	SUBSTRING(s FROM n FOR m)  → substring(s, n, m)
//	SUBSTRING(s FROM n)        → substring(s, n)
//	SUBSTRING(s FOR m)         → substring(s, 1, m)
//	SUBSTRING(s FROM pattern)  → substring(s, pattern), the REGEX reading
//	SUBSTRING(s, n[, m])       → the plain call, unchanged
//
// PostgreSQL chooses between the positional and the regex reading by the
// second operand's TYPE — `substring('abcdef', '2')` is the regex reading and
// answers NULL, not `bcdef` — which is a decision no parser can make. It is
// made in the body instead (expr.fnSubstr), by the operand's runtime type, so
// both spellings reach one rule rather than two.
//
// Operands parse at `parseBitwise` precedence, the level a comparison's own
// operands take: anything looser and FROM/FOR would be swallowed.
func (p *selectParser) parseSubstringExpr() (Node, error) {
	p.advance() // consume SUBSTRING
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after SUBSTRING")
	}
	src, err := p.parseBitwise()
	if err != nil {
		return nil, fmt.Errorf("parsing SUBSTRING source: %w", err)
	}
	// SUBSTRING(s SIMILAR p ESCAPE e) is the standard's OTHER spelling: it
	// returns the part of s between the pattern's `#"` markers. It is refused
	// by name rather than read as something else — the markers are not
	// expressible through the SIMILAR TO translation this engine does
	// (expr.SimilarToRegexp), which has no notion of a returned portion.
	if p.isBareWord(0, "SIMILAR") {
		return nil, sqlerr.New("0A000",
			"SUBSTRING(text SIMILAR pattern ESCAPE escape) is not supported: this engine "+
				"has no capture-marker form of the SIMILAR TO pattern language. Write "+
				"SUBSTRING(text FROM regexp) with a regular expression, or "+
				"REGEXP_EXTRACT(text, regexp, group)")
	}
	switch {
	case p.isKeyword(TokenKWFrom):
		p.advance()
		from, err := p.parseBitwise()
		if err != nil {
			return nil, fmt.Errorf("parsing SUBSTRING FROM: %w", err)
		}
		args := []Node{src, from}
		if p.isBareWord(0, "FOR") {
			p.advance()
			count, err := p.parseBitwise()
			if err != nil {
				return nil, fmt.Errorf("parsing SUBSTRING FOR: %w", err)
			}
			args = append(args, count)
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after SUBSTRING")
		}
		return &FuncCallNode{Name: "substring", Args: args}, nil
	case p.isBareWord(0, "FOR"):
		// `SUBSTRING(s FOR m)` starts at 1 — the standard's default — which
		// is substring(s, 1, m).
		p.advance()
		count, err := p.parseBitwise()
		if err != nil {
			return nil, fmt.Errorf("parsing SUBSTRING FOR: %w", err)
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after SUBSTRING")
		}
		return &FuncCallNode{Name: "substring",
			Args: []Node{src, &Lit{Value: "1", Kind: LitNumber}, count}}, nil
	}
	// The comma spelling: an ordinary call, read here rather than handed back
	// so the source expression already parsed is not read twice.
	args := []Node{src}
	for p.peek() == TokenComma {
		p.advance()
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after SUBSTRING")
	}
	return &FuncCallNode{Name: "substring", Args: args}, nil
}

// parseOverlayExpr reads OVERLAY(s PLACING r FROM n [FOR m]) and rewrites it
// to overlay(s, r, n[, m]) — the call whose body is PostgreSQL's own
// definition of the operator, boundaries included (expr.fnOverlay, #1169).
//
// The comma spelling `overlay(s, r, n[, m])` is PostgreSQL's too and reaches
// the same call through the generic path.
func (p *selectParser) parseOverlayExpr() (Node, error) {
	p.advance() // consume OVERLAY
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after OVERLAY")
	}
	src, err := p.parseBitwise()
	if err != nil {
		return nil, fmt.Errorf("parsing OVERLAY source: %w", err)
	}
	if p.peek() == TokenComma {
		args := []Node{src}
		for p.peek() == TokenComma {
			p.advance()
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after OVERLAY")
		}
		return &FuncCallNode{Name: "overlay", Args: args}, nil
	}
	if !p.isBareWord(0, "PLACING") {
		return nil, fmt.Errorf("expected PLACING in OVERLAY(s PLACING r FROM n [FOR m])")
	}
	p.advance()
	placing, err := p.parseBitwise()
	if err != nil {
		return nil, fmt.Errorf("parsing OVERLAY PLACING: %w", err)
	}
	if _, err := p.expect(TokenKWFrom); err != nil {
		return nil, fmt.Errorf("expected FROM in OVERLAY(s PLACING r FROM n [FOR m])")
	}
	from, err := p.parseBitwise()
	if err != nil {
		return nil, fmt.Errorf("parsing OVERLAY FROM: %w", err)
	}
	args := []Node{src, placing, from}
	if p.isBareWord(0, "FOR") {
		p.advance()
		count, err := p.parseBitwise()
		if err != nil {
			return nil, fmt.Errorf("parsing OVERLAY FOR: %w", err)
		}
		args = append(args, count)
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after OVERLAY")
	}
	return &FuncCallNode{Name: "overlay", Args: args}, nil
}

// parseNormalizeExpr reads NORMALIZE(text [, NFC|NFD|NFKC|NFKD]).
//
// The form is a bare keyword in the standard's grammar and on the server —
// `normalize('abc', 'NFC')` with a QUOTED form is a syntax error on 17.11 —
// so it is read here as a word and lowered to the string argument the
// `normalize` call takes. An unknown word is refused at the spelling, with
// the four forms named, which is the server's class for it.
func (p *selectParser) parseNormalizeExpr() (Node, error) {
	p.advance() // consume NORMALIZE
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after NORMALIZE")
	}
	src, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("parsing NORMALIZE source: %w", err)
	}
	args := []Node{src}
	if p.peek() == TokenComma {
		p.advance()
		formTok := p.cur
		if formTok.typ != TokenIdent || formTok.quoted {
			// A quoted or computed form: this engine's own superset, read as
			// an ordinary second argument rather than refused, because the
			// body validates it (ADR-0012's "PostgreSQL rejects, we answer"
			// class).
			form, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, form)
		} else {
			switch strings.ToUpper(formTok.val) {
			case "NFC", "NFD", "NFKC", "NFKD":
				p.advance()
				args = append(args, &Lit{Value: strings.ToUpper(formTok.val), Kind: LitString})
			default:
				return nil, sqlerr.New("42601",
					"normalization form %s is not one of NFC, NFD, NFKC, NFKD",
					sqlerr.Quote(formTok.val))
			}
		}
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after NORMALIZE")
	}
	return &FuncCallNode{Name: "normalize", Args: args}, nil
}

// parseLocalTimestamp reads LOCALTIMESTAMP and LOCALTIMESTAMP(precision).
//
// PostgreSQL declares it `timestamp without time zone` — CURRENT_TIMESTAMP is
// `timestamp with time zone` — and that declaration is the reason the
// spelling exists. The precision is parsed and dropped: this engine renders
// an instant to milliseconds and has no per-call precision, which is a
// recorded boundary rather than a silent truncation of a value the client
// asked to be narrower.
func (p *selectParser) parseLocalTimestamp() (Node, error) {
	p.advance() // consume LOCALTIMESTAMP
	if p.peek() == TokenLParen {
		p.advance()
		// The EMPTY parentheses are this engine's own rendering of the
		// niladic call, and the rendering has to re-read: a worker fragment
		// re-parses the filter text the planner rendered
		// (`localtimestamp() >= localtimestamp()`), so a spelling the
		// renderer emits and the parser refuses fails the query on the DAG
		// arms and answers on the single one. Found by the five-arm gate.
		if p.peek() != TokenRParen {
			if _, err := p.expect(TokenNumber); err != nil {
				return nil, fmt.Errorf("expected a precision in LOCALTIMESTAMP(p)")
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected ) after LOCALTIMESTAMP(p)")
		}
	}
	return &FuncCallNode{Name: "localtimestamp"}, nil
}
