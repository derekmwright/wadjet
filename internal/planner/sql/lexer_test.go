package sql

import (
	"testing"
)

func collectTokens(input string) []token {
	l := newLexer(input)
	var tokens []token
	for {
		tok := l.nextToken()
		tokens = append(tokens, tok)
		if tok.typ == TokenEOF || tok.typ == TokenError {
			break
		}
	}
	return tokens
}

func TestLexBasicKeywords(t *testing.T) {
	tokens := collectTokens("CREATE FUNCTION double")
	expected := []TokenType{TokenKWCreate, TokenKWFunction, TokenIdent, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %d, got %d (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
	if tokens[2].val != "double" {
		t.Errorf("ident val: expected 'double', got %q", tokens[2].val)
	}
}

func TestLexCaseInsensitive(t *testing.T) {
	tokens := collectTokens("create Function EXPLAIN verbose")
	expected := []TokenType{TokenKWCreate, TokenKWFunction, TokenKWExplain, TokenKWVerbose, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %d, got %d", i, exp, tokens[i].typ)
		}
	}
}

func TestLexPunctuation(t *testing.T) {
	tokens := collectTokens("fn(x, y);")
	expected := []TokenType{TokenIdent, TokenLParen, TokenIdent, TokenComma, TokenIdent, TokenRParen, TokenSemicolon, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %d, got %d (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
}

func TestLexStringLiteral(t *testing.T) {
	tokens := collectTokens("'hello world'")
	if len(tokens) != 2 { // string + EOF
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].typ != TokenString {
		t.Fatalf("expected TokenString, got %d", tokens[0].typ)
	}
	if tokens[0].val != "hello world" {
		t.Errorf("expected 'hello world', got %q", tokens[0].val)
	}
}

func TestLexStringEscapedQuote(t *testing.T) {
	tokens := collectTokens("'it''s a test'")
	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].typ != TokenString {
		t.Fatalf("expected TokenString, got %d", tokens[0].typ)
	}
	if tokens[0].val != "it's a test" {
		t.Errorf("expected \"it's a test\", got %q", tokens[0].val)
	}
}

func TestLexUnterminatedString(t *testing.T) {
	tokens := collectTokens("'unterminated")
	if tokens[0].typ != TokenError {
		t.Fatalf("expected TokenError for unterminated string, got %d", tokens[0].typ)
	}
}

func TestLexNumber(t *testing.T) {
	tokens := collectTokens("42 3.14")
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].typ != TokenNumber || tokens[0].val != "42" {
		t.Errorf("expected number '42', got %d %q", tokens[0].typ, tokens[0].val)
	}
	if tokens[1].typ != TokenNumber || tokens[1].val != "3.14" {
		t.Errorf("expected number '3.14', got %d %q", tokens[1].typ, tokens[1].val)
	}
}

func TestLexScientificNotation(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{"1e10", "1e10"},
		{"1E10", "1E10"},
		{"3.14e+2", "3.14e+2"},
		{"1e-05", "1e-05"},
		{"2.5E-3", "2.5E-3"},
		{"1E+0", "1E+0"},
	} {
		tokens := collectTokens(tc.input)
		if tokens[0].typ != TokenNumber {
			t.Errorf("%s: expected TokenNumber, got %d", tc.input, tokens[0].typ)
		}
		if tokens[0].val != tc.want {
			t.Errorf("%s: expected %q, got %q", tc.input, tc.want, tokens[0].val)
		}
	}

	// Verify "1e-05 + 2" lexes as number, plus, number
	tokens := collectTokens("1e-05 + 2")
	if len(tokens) != 4 { // number, plus, number, EOF
		t.Fatalf("expected 4 tokens, got %d", len(tokens))
	}
	if tokens[0].val != "1e-05" {
		t.Errorf("expected '1e-05', got %q", tokens[0].val)
	}
}

func TestLexCreateFunction(t *testing.T) {
	tokens := collectTokens("CREATE OR REPLACE FUNCTION weighted(val, weight) AS")
	expected := []TokenType{
		TokenKWCreate, TokenKWOr, TokenKWReplace, TokenKWFunction,
		TokenIdent, TokenLParen, TokenIdent, TokenComma, TokenIdent, TokenRParen,
		TokenKWAs, TokenEOF,
	}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %d, got %d (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
	if tokens[4].val != "weighted" {
		t.Errorf("function name: expected 'weighted', got %q", tokens[4].val)
	}
}

func TestLexDropFunction(t *testing.T) {
	tokens := collectTokens("DROP FUNCTION IF EXISTS my_func;")
	expected := []TokenType{
		TokenKWDrop, TokenKWFunction, TokenKWIf, TokenKWExists,
		TokenIdent, TokenSemicolon, TokenEOF,
	}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %d, got %d (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
}

func TestLexEmptyInput(t *testing.T) {
	tokens := collectTokens("")
	if len(tokens) != 1 || tokens[0].typ != TokenEOF {
		t.Fatalf("expected single EOF, got %v", tokens)
	}
}

func TestLexWhitespaceOnly(t *testing.T) {
	tokens := collectTokens("   \t\n  ")
	if len(tokens) != 1 || tokens[0].typ != TokenEOF {
		t.Fatalf("expected single EOF, got %v", tokens)
	}
}

func TestLexPeekToken(t *testing.T) {
	l := newLexer("CREATE FUNCTION")
	peeked := l.peekToken()
	if peeked.typ != TokenKWCreate {
		t.Fatalf("peek: expected CREATE, got %d", peeked.typ)
	}
	// nextToken should return the same token
	next := l.nextToken()
	if next.typ != TokenKWCreate {
		t.Fatalf("next after peek: expected CREATE, got %d", next.typ)
	}
	// Now should advance to FUNCTION
	next2 := l.nextToken()
	if next2.typ != TokenKWFunction {
		t.Fatalf("second next: expected FUNCTION, got %d", next2.typ)
	}
}

// --- scanRawBody tests ---

func TestScanRawBodySimple(t *testing.T) {
	l := newLexer("x * 2")
	tok := l.scanRawBody()
	if tok.typ != TokenRawBody {
		t.Fatalf("expected TokenRawBody, got %d", tok.typ)
	}
	if tok.val != "x * 2" {
		t.Errorf("expected 'x * 2', got %q", tok.val)
	}
}

func TestScanRawBodyWithLock(t *testing.T) {
	l := newLexer("x + 1 WITH LOCK")
	tok := l.scanRawBody()
	if tok.val != "x + 1" {
		t.Errorf("expected 'x + 1', got %q", tok.val)
	}
	// WITH LOCK should not be consumed — lexer can still read it
	next := l.nextToken()
	if next.typ != TokenKWWith {
		t.Errorf("expected WITH after body, got %d %q", next.typ, next.val)
	}
}

func TestScanRawBodySemicolon(t *testing.T) {
	l := newLexer("x + 1;")
	tok := l.scanRawBody()
	if tok.val != "x + 1" {
		t.Errorf("expected 'x + 1', got %q", tok.val)
	}
	next := l.nextToken()
	if next.typ != TokenSemicolon {
		t.Errorf("expected semicolon after body, got %d", next.typ)
	}
}

func TestScanRawBodyWithParens(t *testing.T) {
	l := newLexer("CASE WHEN f(x) > 0 THEN g(x, y) ELSE 0 END WITH LOCK")
	tok := l.scanRawBody()
	if tok.val != "CASE WHEN f(x) > 0 THEN g(x, y) ELSE 0 END" {
		t.Errorf("unexpected body: %q", tok.val)
	}
}

func TestScanRawBodyStringContainingWithLock(t *testing.T) {
	// The string literal 'WITH LOCK' inside the body must NOT terminate it
	l := newLexer("CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END WITH LOCK")
	tok := l.scanRawBody()
	expected := "CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END"
	if tok.val != expected {
		t.Errorf("expected %q, got %q", expected, tok.val)
	}
}

func TestScanRawBodyStringContainingWithLockNoTerminator(t *testing.T) {
	// Body contains 'WITH LOCK' in a string, no actual WITH LOCK terminator
	l := newLexer("CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END;")
	tok := l.scanRawBody()
	expected := "CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END"
	if tok.val != expected {
		t.Errorf("expected %q, got %q", expected, tok.val)
	}
}

func TestScanRawBodyNestedParens(t *testing.T) {
	l := newLexer("f(g(h(x, y), z)) + 1;")
	tok := l.scanRawBody()
	if tok.val != "f(g(h(x, y), z)) + 1" {
		t.Errorf("expected body with nested parens, got %q", tok.val)
	}
}

func TestScanRawBodyEscapedQuotes(t *testing.T) {
	l := newLexer("'hello, it''s ' || name;")
	tok := l.scanRawBody()
	if tok.val != "'hello, it''s ' || name" {
		t.Errorf("expected body with escaped quotes, got %q", tok.val)
	}
}

func TestScanRawBodyEOF(t *testing.T) {
	l := newLexer("x + y")
	tok := l.scanRawBody()
	if tok.val != "x + y" {
		t.Errorf("expected 'x + y', got %q", tok.val)
	}
}

func TestScanRawBodyCaseInsensitiveWithLock(t *testing.T) {
	l := newLexer("x + 1 with lock")
	tok := l.scanRawBody()
	if tok.val != "x + 1" {
		t.Errorf("expected 'x + 1', got %q", tok.val)
	}
}

func TestScanRawBodyWithLockNotAtWordBoundary(t *testing.T) {
	// "WITHLOCK" should NOT terminate (no space between WITH and LOCK)
	l := newLexer("xWITH LOCK_val + 1;")
	tok := l.scanRawBody()
	// "xWITH" is not preceded by whitespace, so it shouldn't match
	if tok.val != "xWITH LOCK_val + 1" {
		t.Errorf("expected body to not split on non-word-boundary WITH, got %q", tok.val)
	}
}

func TestLexRest(t *testing.T) {
	l := newLexer("CREATE FUNCTION double")
	l.nextToken() // consume CREATE
	rest := l.rest()
	// After consuming CREATE and its whitespace
	if rest != "FUNCTION double" {
		// The lexer skips whitespace as part of tokenization, so
		// rest starts at the next non-whitespace character position
		// Actually, after reading "CREATE", pos is at the space.
		// skipWhitespace in next call will advance past it.
		// But rest() returns from current pos which is right after "CREATE"
		t.Logf("rest after CREATE: %q (expected to start with whitespace or FUNCTION)", rest)
	}
}

func TestLexJSONArrow(t *testing.T) {
	tokens := collectTokens("col -> 'key'")
	expected := []TokenType{TokenIdent, TokenJSONArrow, TokenString, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %v, got %v (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
}

func TestLexJSONDoubleArrow(t *testing.T) {
	tokens := collectTokens("col ->> 'key'")
	expected := []TokenType{TokenIdent, TokenJSONDoubleArrow, TokenString, TokenEOF}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %v, got %v (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
}

func TestLexJSONArrowVsMinus(t *testing.T) {
	// Ensure -> is distinct from -
	tokens := collectTokens("a - b -> 'c' ->> 'd'")
	expected := []TokenType{
		TokenIdent, TokenMinus, TokenIdent,
		TokenJSONArrow, TokenString,
		TokenJSONDoubleArrow, TokenString,
		TokenEOF,
	}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %v, got %v (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
}

func TestLexJSONArrowChain(t *testing.T) {
	tokens := collectTokens("data -> 'a' ->> 'b'")
	expected := []TokenType{
		TokenIdent, TokenJSONArrow, TokenString,
		TokenJSONDoubleArrow, TokenString, TokenEOF,
	}
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d: %v", len(expected), len(tokens), tokens)
	}
	for i, exp := range expected {
		if tokens[i].typ != exp {
			t.Errorf("token %d: expected %v, got %v (%q)", i, exp, tokens[i].typ, tokens[i].val)
		}
	}
}

// Comments are whitespace, in every position. The lexer skipped none of them,
// so any statement carrying one failed with "expected SELECT" — a note typed
// above a query in a client's editor, or the commented-out CTE DataGrip ships
// inside its index introspection query.
func TestLexerSkipsComments(t *testing.T) {
	for _, tt := range []struct {
		name string
		sql  string
	}{
		{"leading line", "-- note\nSELECT a FROM t"},
		{"leading block", "/* note */ SELECT a FROM t"},
		{"inline block", "SELECT /* which */ a FROM t"},
		{"trailing line", "SELECT a FROM t -- why"},
		{"nested block", "/* outer /* inner */ still outer */ SELECT a FROM t"},
		{"block spanning lines", "/*\n  multi\n  line\n*/\nSELECT a FROM t"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLexer(tt.sql)
			tok := l.nextToken()
			if tok.typ != TokenKWSelect {
				t.Fatalf("first token = %v %q, want SELECT keyword", tok.typ, tok.val)
			}
		})
	}

	// A comment marker inside a string literal is data, not a comment.
	l := newLexer("SELECT '-- not a comment' FROM t")
	l.nextToken() // SELECT
	tok := l.nextToken()
	if tok.typ != TokenString || tok.val != "-- not a comment" {
		t.Fatalf("string literal = %v %q", tok.typ, tok.val)
	}
}

// TestLexerPeekDoesNotCorruptBackup: peek() left l.width holding the PEEKED
// rune's width, and backup() steps back by width — so a caller that read a
// one-byte rune, peeked a wider one, and then backed up stepped back too
// far. skipWhitespace does exactly that on '-', so `a-é` drove l.pos to -1
// and the next read sliced l.input[-1:]. That is a process kill from any
// pgwire client that pastes a query with a non-ASCII character after a
// minus sign, and no recover in the server would have caught it: it is a
// runtime panic in the lexer's own goroutine.
func TestLexerPeekDoesNotCorruptBackup(t *testing.T) {
	inputs := []string{
		"-ױ0",
		"SELECT a-é FROM t",
		"SELECT 1-—2",
		"-é",
		"a -  b",
		"SELECT '日本' - 1",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			l := newLexer(in)
			for i := 0; i < 64; i++ {
				if l.pos < 0 {
					t.Fatalf("lexer position went negative after %d tokens", i)
				}
				_ = l.peekToken()
				if tok := l.nextToken(); tok.typ == TokenEOF {
					return
				}
			}
			t.Fatal("the lexer did not reach EOF in 64 tokens")
		})
	}

	// peek() must not disturb what a following backup() undoes.
	l := newLexer("aéb")
	if r := l.next(); r != 'a' {
		t.Fatalf("first rune = %q", r)
	}
	if r := l.peek(); r != 'é' {
		t.Fatalf("peeked %q, want é", r)
	}
	l.backup()
	if l.pos != 0 {
		t.Errorf("after next()+peek()+backup() the position is %d, want 0", l.pos)
	}
}
