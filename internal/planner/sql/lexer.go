package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType identifies the kind of lexical token.
type TokenType int

const (
	// Special
	TokenError TokenType = iota // lexing error (val contains message)
	TokenEOF                    // end of input

	// Literals
	TokenIdent  // identifier — unquoted, or double-quoted (token.quoted is set)
	TokenString // single-quoted string literal (val has quotes stripped, '' unescaped)
	TokenNumber // integer or decimal

	// Punctuation
	TokenLParen    // (
	TokenRParen    // )
	TokenComma     // ,
	TokenSemicolon // ;
	TokenStar      // *
	TokenDot       // .
	TokenLBracket  // [
	TokenRBracket  // ]
	TokenLBrace    // {
	TokenRBrace    // }

	// Operators
	TokenPlus            // +
	TokenMinus           // -
	TokenSlash           // /
	TokenPercent         // %
	TokenConcat          // ||
	TokenDoubleColon     // ::
	TokenJSONArrow       // ->
	TokenJSONDoubleArrow // ->>
	TokenEq              // =
	TokenNotEq           // != or <>
	TokenLT              // <
	TokenLTEq            // <=
	TokenGT              // >
	TokenGTEq            // >=

	// Keywords (case-insensitive, val is always uppercase)
	TokenKWCreate
	TokenKWOr
	TokenKWReplace
	TokenKWFunction
	TokenKWAs
	TokenKWDrop
	TokenKWIf
	TokenKWExists
	TokenKWShow
	TokenKWFunctions
	TokenKWColumns
	TokenKWFrom
	TokenKWExplain
	TokenKWVerbose
	TokenKWAnalyze
	TokenKWDescribe
	TokenKWDesc
	TokenKWWith
	TokenKWLock
	TokenKWTable
	TokenKWTables
	TokenKWNot
	TokenKWNull
	TokenKWPartition
	TokenKWBy

	// SQL query keywords
	TokenKWSelect
	TokenKWWhere
	TokenKWGroup
	TokenKWHaving
	TokenKWOrder
	TokenKWLimit
	TokenKWOffset
	TokenKWAsc
	TokenKWDistinct
	TokenKWAll
	TokenKWUnion
	TokenKWIntersect
	TokenKWExcept
	TokenKWAnd
	TokenKWIn
	TokenKWBetween
	TokenKWLike
	TokenKWILike
	TokenKWIs
	TokenKWTrue
	TokenKWFalse
	TokenKWCase
	TokenKWWhen
	TokenKWThen
	TokenKWElse
	TokenKWEnd
	TokenKWCast
	TokenKWJoin
	TokenKWOn
	TokenKWInner
	TokenKWLeft
	TokenKWRight
	TokenKWOuter
	TokenKWFull
	TokenKWCross
	TokenKWNatural
	TokenKWOver
	TokenKWNulls
	TokenKWFirst
	TokenKWLast
	TokenKWRows
	TokenKWRange
	TokenKWUnbounded
	TokenKWPreceding
	TokenKWFollowing
	TokenKWCurrent
	TokenKWRow
	TokenKWCube
	TokenKWRollup
	TokenKWGrouping
	TokenKWSets

	// Clause keywords
	TokenKWFetch
	TokenKWView
	TokenKWAlter
	TokenKWAdd
	TokenKWColumn
	TokenKWRename
	TokenKWTo

	// DML keywords
	TokenKWUpdate
	TokenKWSet
	TokenKWDelete
	TokenKWInsert
	TokenKWInto
	TokenKWValues
	TokenKWMerge
	TokenKWUsing
	TokenKWMatched

	// Alert DDL keywords
	TokenKWAlert
	TokenKWEvery
	TokenKWWebhook
	TokenKWHeaders
	TokenKWEnable
	TokenKWDisable
	TokenKWSeconds
	TokenKWMinutes
	TokenKWHours

	// Snapshot keywords
	TokenKWSnapshot

	// Raw capture
	TokenRawBody // everything after AS until terminator
)

const eof = -1

var keywords = map[string]TokenType{
	"CREATE":    TokenKWCreate,
	"OR":        TokenKWOr,
	"REPLACE":   TokenKWReplace,
	"FUNCTION":  TokenKWFunction,
	"AS":        TokenKWAs,
	"DROP":      TokenKWDrop,
	"IF":        TokenKWIf,
	"EXISTS":    TokenKWExists,
	"SHOW":      TokenKWShow,
	"FUNCTIONS": TokenKWFunctions,
	"COLUMNS":   TokenKWColumns,
	"FROM":      TokenKWFrom,
	"EXPLAIN":   TokenKWExplain,
	"VERBOSE":   TokenKWVerbose,
	"ANALYZE":   TokenKWAnalyze,
	"DESCRIBE":  TokenKWDescribe,
	"DESC":      TokenKWDesc,
	"WITH":      TokenKWWith,
	"LOCK":      TokenKWLock,
	"TABLE":     TokenKWTable,
	"TABLES":    TokenKWTables,
	"NOT":       TokenKWNot,
	"NULL":      TokenKWNull,
	"PARTITION": TokenKWPartition,
	"BY":        TokenKWBy,
	"SELECT":    TokenKWSelect,
	"WHERE":     TokenKWWhere,
	"GROUP":     TokenKWGroup,
	"HAVING":    TokenKWHaving,
	"ORDER":     TokenKWOrder,
	"LIMIT":     TokenKWLimit,
	"OFFSET":    TokenKWOffset,
	"ASC":       TokenKWAsc,
	"DISTINCT":  TokenKWDistinct,
	"ALL":       TokenKWAll,
	"UNION":     TokenKWUnion,
	"INTERSECT": TokenKWIntersect,
	"EXCEPT":    TokenKWExcept,
	"AND":       TokenKWAnd,
	"IN":        TokenKWIn,
	"BETWEEN":   TokenKWBetween,
	"LIKE":      TokenKWLike,
	"ILIKE":     TokenKWILike,
	"IS":        TokenKWIs,
	"TRUE":      TokenKWTrue,
	"FALSE":     TokenKWFalse,
	"CASE":      TokenKWCase,
	"WHEN":      TokenKWWhen,
	"THEN":      TokenKWThen,
	"ELSE":      TokenKWElse,
	"END":       TokenKWEnd,
	"CAST":      TokenKWCast,
	"JOIN":      TokenKWJoin,
	"ON":        TokenKWOn,
	"INNER":     TokenKWInner,
	"LEFT":      TokenKWLeft,
	"RIGHT":     TokenKWRight,
	"OUTER":     TokenKWOuter,
	"FULL":      TokenKWFull,
	"CROSS":     TokenKWCross,
	"NATURAL":   TokenKWNatural,
	"OVER":      TokenKWOver,
	"NULLS":     TokenKWNulls,
	"FIRST":     TokenKWFirst,
	"LAST":      TokenKWLast,
	"ROWS":      TokenKWRows,
	"RANGE":     TokenKWRange,
	"UNBOUNDED": TokenKWUnbounded,
	"PRECEDING": TokenKWPreceding,
	"FOLLOWING": TokenKWFollowing,
	"CURRENT":   TokenKWCurrent,
	"ROW":       TokenKWRow,
	"CUBE":      TokenKWCube,
	"ROLLUP":    TokenKWRollup,
	"GROUPING":  TokenKWGrouping,
	"SETS":      TokenKWSets,
	"FETCH":     TokenKWFetch,
	"VIEW":      TokenKWView,
	"ALTER":     TokenKWAlter,
	"ADD":       TokenKWAdd,
	"COLUMN":    TokenKWColumn,
	"RENAME":    TokenKWRename,
	"TO":        TokenKWTo,
	"UPDATE":    TokenKWUpdate,
	"SET":       TokenKWSet,
	"DELETE":    TokenKWDelete,
	"INSERT":    TokenKWInsert,
	"INTO":      TokenKWInto,
	"VALUES":    TokenKWValues,
	"MERGE":     TokenKWMerge,
	"USING":     TokenKWUsing,
	"MATCHED":   TokenKWMatched,
	"ALERT":     TokenKWAlert,
	"EVERY":     TokenKWEvery,
	"WEBHOOK":   TokenKWWebhook,
	"HEADERS":   TokenKWHeaders,
	"ENABLE":    TokenKWEnable,
	"DISABLE":   TokenKWDisable,
	"SECONDS":   TokenKWSeconds,
	"MINUTES":   TokenKWMinutes,
	"HOURS":     TokenKWHours,
	"SNAPSHOT":  TokenKWSnapshot,
}

// token is a lexical token produced by the lexer.
type token struct {
	typ TokenType
	val string
	pos int // byte offset in original input
	// raw is the ORIGINAL spelling of a token whose val was normalized: a
	// keyword (val uppercased for comparison) or an unquoted identifier
	// (val folded to lower case, #731). Empty for every other token, where
	// val is already verbatim. It is the spelling to ECHO — a syntax error
	// names the text the client sent, and a type name is not an identifier
	// reference — never the spelling to RESOLVE.
	raw string
	// quoted marks a TokenIdent that came from a double-quoted
	// ("delimited") identifier. Such a token is always exactly one
	// identifier: its value is taken verbatim between the quotes, so any
	// dots, spaces, or keyword spellings inside it are part of the name
	// rather than syntax.
	quoted bool
}

// source is the spelling the client actually sent, for a message that echoes
// the input rather than naming a resolved object.
func (t token) source() string {
	if t.raw != "" {
		return t.raw
	}
	return t.val
}

// stateFn is a state function in the Pike lexer pattern.
// Each state function processes the current input position, emits a token,
// and returns the next state function (or nil to stop).
type stateFn func(*lexer) stateFn

// lexer implements a Rob Pike-style lexical scanner.
// Pull-based: call nextToken() to drive the state machine synchronously.
type lexer struct {
	input   string
	pos     int    // current byte position in input
	start   int    // start byte position of current token
	width   int    // width of last rune read (for backup)
	pending *token // pending token to return
	// verbatim suppresses the unquoted-identifier fold. It is for the
	// callers that lex a NAME rather than a query — SplitIdentRef and the
	// helpers built on it are asked to split `WatchID` or `"my tbl"."c"`
	// into its parts, not to bind it, and folding a schema-derived name
	// there would rename the column the caller is asking about.
	verbatim bool
}

// newLexer creates a new lexer for the given input.
func newLexer(input string) *lexer {
	return &lexer{input: input}
}

// newVerbatimLexer creates a lexer that does NOT fold unquoted identifiers.
// See lexer.verbatim.
func newVerbatimLexer(input string) *lexer {
	return &lexer{input: input, verbatim: true}
}

// --- Character-level operations ---

// next returns the next rune and advances the position.
func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += w
	return r
}

// backup steps back one rune. Can only be called once per next().
func (l *lexer) backup() {
	l.pos -= l.width
	if l.pos < 0 {
		// Unreachable while every caller honours the once-per-next()
		// contract; a floor rather than a negative index, because the
		// alternative is l.input[-1:] and a dead process.
		l.pos = 0
	}
}

// peek returns the next rune without advancing.
//
// It restores l.width as well as l.pos, because backup() steps back by
// WIDTH and the caller's next backup() is meant to undo the rune it read
// BEFORE this peek. Leaving the peeked rune's width behind made that step
// too large by however much wider the peeked rune was: skipWhitespace reads
// one byte ('-'), peeks a two-byte rune, then backs up by two and lands at
// position -1, so `SELECT a-é` — a minus followed by any non-ASCII
// character — took the process down inside the lexer. Reachable from any
// pgwire client that pastes a query with an accent in it.
func (l *lexer) peek() rune {
	savedWidth := l.width
	r := l.next()
	l.backup()
	l.width = savedWidth
	return r
}

// peekAt returns the BYTE n positions ahead of the current one, or 0 past the
// end. Byte, not rune: its callers ask whether the next character is an ASCII
// digit, and a multi-byte rune is not one.
func (l *lexer) peekAt(n int) byte {
	if l.pos+n >= len(l.input) {
		return 0
	}
	return l.input[l.pos+n]
}

// emit produces a token from start..pos with the given type.
func (l *lexer) emit(typ TokenType) {
	l.pending = &token{
		typ: typ,
		val: l.input[l.start:l.pos],
		pos: l.start,
	}
	l.start = l.pos
}

// emitVal produces a token with a custom value (e.g., unquoted strings).
func (l *lexer) emitVal(typ TokenType, val string) {
	l.pending = &token{
		typ: typ,
		val: val,
		pos: l.start,
	}
	l.start = l.pos
}

// errorf emits an error token, formatting the message with fmt.Sprintf so
// that verbs such as %c and %q render the offending input.
func (l *lexer) errorf(format string, args ...any) stateFn {
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	l.pending = &token{
		typ: TokenError,
		val: msg,
		pos: l.start,
	}
	return nil
}

// skipWhitespace advances past any whitespace and resets start.
// Comments are lexical whitespace: `-- to end of line` and `/* block */`,
// the latter nesting the way PostgreSQL nests it. Skipping them here rather
// than in the parser means every construct accepts them, in every position,
// because no parse rule ever sees one.
//
// Without this the parser rejected a statement with any comment at all —
// "expected SELECT" for a leading `--` note typed above a query in a client's
// editor, and for the commented-out CTE DataGrip ships inside its index
// introspection query.
func (l *lexer) skipWhitespace() {
	for {
		r := l.next()
		if r == eof {
			l.start = l.pos
			return
		}
		if unicode.IsSpace(r) {
			continue
		}
		if r == '-' && l.peek() == '-' {
			for {
				c := l.next()
				if c == eof || c == '\n' {
					break
				}
			}
			continue
		}
		if r == '/' && l.peek() == '*' {
			l.next() // consume the '*'
			for depth := 1; depth > 0; {
				c := l.next()
				if c == eof {
					break // unterminated comment: consume to end of input
				}
				if c == '/' && l.peek() == '*' {
					l.next()
					depth++
				} else if c == '*' && l.peek() == '/' {
					l.next()
					depth--
				}
			}
			continue
		}
		l.backup()
		l.start = l.pos
		return
	}
}

// --- Public API ---

// nextToken returns the next token from the input.
func (l *lexer) nextToken() token {
	l.pending = nil
	state := lexStart
	for state != nil && l.pending == nil {
		state = state(l)
	}
	if l.pending != nil {
		return *l.pending
	}
	return token{typ: TokenEOF, pos: l.pos}
}

// peekToken returns the next token without consuming it.
func (l *lexer) peekToken() token {
	savedPos := l.pos
	savedStart := l.start
	savedWidth := l.width
	tok := l.nextToken()
	l.pos = savedPos
	l.start = savedStart
	l.width = savedWidth
	return tok
}

// rest returns the unconsumed input from the current position.
func (l *lexer) rest() string {
	return l.input[l.pos:]
}

// scanRawBody captures the function body — everything from the current
// position until an unquoted WITH LOCK, unquoted semicolon, or EOF.
// String literals and nested parentheses are respected so that keywords
// inside strings or parens don't terminate the body prematurely.
// The terminator (WITH LOCK, semicolon) is NOT consumed.
func (l *lexer) scanRawBody() token {
	l.skipWhitespace()
	l.start = l.pos

	bodyStart := l.pos
	parenDepth := 0

	for l.pos < len(l.input) {
		r, w := utf8.DecodeRuneInString(l.input[l.pos:])

		// Handle string literals — skip their contents entirely
		if r == '\'' {
			l.pos += w
			for l.pos < len(l.input) {
				c, cw := utf8.DecodeRuneInString(l.input[l.pos:])
				l.pos += cw
				if c == '\'' {
					// Check for escaped quote ('')
					if l.pos < len(l.input) && l.input[l.pos] == '\'' {
						l.pos++ // skip the second quote
						continue
					}
					break // end of string literal
				}
			}
			continue
		}

		// Track parentheses depth
		if r == '(' {
			parenDepth++
			l.pos += w
			continue
		}
		if r == ')' {
			parenDepth--
			l.pos += w
			continue
		}

		// Only check for terminators at paren depth 0
		if parenDepth == 0 {
			// Semicolon terminates
			if r == ';' {
				break
			}

			// Check for WITH LOCK (case-insensitive) at a word boundary
			if (r == 'W' || r == 'w') && l.matchesWithLock() {
				break
			}
		}

		l.pos += w
	}

	body := strings.TrimSpace(l.input[bodyStart:l.pos])
	l.start = l.pos
	return token{typ: TokenRawBody, val: body, pos: bodyStart}
}

// matchesWithLock checks if the input at the current position is
// "WITH" followed by whitespace and "LOCK" (case-insensitive),
// preceded by a word boundary (start of input or whitespace).
func (l *lexer) matchesWithLock() bool {
	remaining := l.input[l.pos:]
	if len(remaining) < 9 { // "WITH LOCK" = 9 chars minimum
		return false
	}

	// Must be at a word boundary — preceded by whitespace or start
	if l.pos > 0 {
		prev, _ := utf8.DecodeLastRuneInString(l.input[:l.pos])
		if !unicode.IsSpace(prev) {
			return false
		}
	}

	upper := strings.ToUpper(remaining[:9])
	if !strings.HasPrefix(upper, "WITH") {
		return false
	}

	// After "WITH" must be whitespace
	afterWith := remaining[4:]
	r, w := utf8.DecodeRuneInString(afterWith)
	if !unicode.IsSpace(r) {
		return false
	}

	// Skip whitespace, then expect "LOCK"
	rest := strings.TrimLeftFunc(afterWith[w:], unicode.IsSpace)
	if len(rest) < 4 {
		return false
	}
	if !strings.EqualFold(rest[:4], "LOCK") {
		return false
	}

	// "LOCK" must be followed by whitespace, semicolon, or EOF
	if len(rest) > 4 {
		after, _ := utf8.DecodeRuneInString(rest[4:])
		if after != ';' && !unicode.IsSpace(after) {
			return false
		}
	}

	return true
}

// --- State functions (Pike pattern) ---

// lexStart is the initial state. Dispatches based on the next character.
func lexStart(l *lexer) stateFn {
	l.skipWhitespace()
	l.start = l.pos

	r := l.next()
	switch {
	case r == eof:
		l.emit(TokenEOF)
		return nil
	case r == '(':
		l.emit(TokenLParen)
		return nil
	case r == ')':
		l.emit(TokenRParen)
		return nil
	case r == ',':
		l.emit(TokenComma)
		return nil
	case r == ';':
		l.emit(TokenSemicolon)
		return nil
	case r == '*':
		l.emit(TokenStar)
		return nil
	case r == '.':
		// `.5` is a numeric literal, not a dot followed by a 5 (#655).
		// PostgreSQL, DuckDB and the SQL standard all accept the leading-dot
		// form; wadjet reported `unexpected token "."` at the literal's own
		// position for `SELECT .5`, `WHERE m1 > .5` and `SELECT .5 + 1`.
		// Only when a DIGIT follows: a bare dot is still the qualifier
		// separator in `t.c`, and a dot at the end of input is still an
		// error.
		if d := l.peek(); d >= '0' && d <= '9' {
			return lexNumber
		}
		l.emit(TokenDot)
		return nil
	case r == '+':
		l.emit(TokenPlus)
		return nil
	case r == '-':
		if l.peek() == '>' {
			l.next()
			if l.peek() == '>' {
				l.next()
				l.emit(TokenJSONDoubleArrow) // ->>
			} else {
				l.emit(TokenJSONArrow) // ->
			}
		} else {
			l.emit(TokenMinus)
		}
		return nil
	case r == '/':
		l.emit(TokenSlash)
		return nil
	case r == '%':
		l.emit(TokenPercent)
		return nil
	case r == '=':
		l.emit(TokenEq)
		return nil
	case r == '!':
		if l.peek() == '=' {
			l.next()
			l.emit(TokenNotEq)
			return nil
		}
		return l.errorf("unexpected character: !")
	case r == '<':
		p := l.peek()
		if p == '=' {
			l.next()
			l.emit(TokenLTEq)
			return nil
		}
		if p == '>' {
			l.next()
			l.emit(TokenNotEq)
			return nil
		}
		l.emit(TokenLT)
		return nil
	case r == '>':
		if l.peek() == '=' {
			l.next()
			l.emit(TokenGTEq)
			return nil
		}
		l.emit(TokenGT)
		return nil
	case r == '|':
		if l.peek() == '|' {
			l.next()
			l.emit(TokenConcat)
			return nil
		}
		return l.errorf("unexpected character: |")
	case r == ':':
		if l.peek() == ':' {
			l.next()
			l.emit(TokenDoubleColon)
			return nil
		}
		return l.errorf("unexpected character: :")
	case r == '$':
		if l.peek() == '$' {
			return lexDollarString
		}
		return l.errorf("unexpected character: $")
	case r == '[':
		l.emit(TokenLBracket)
		return nil
	case r == ']':
		l.emit(TokenRBracket)
		return nil
	case r == '{':
		l.emit(TokenLBrace)
		return nil
	case r == '}':
		l.emit(TokenRBrace)
		return nil
	case r == '\'':
		return lexString
	case r == '"':
		return lexQuotedIdent
	case r >= '0' && r <= '9':
		return lexNumber
	case r == '_' || unicode.IsLetter(r):
		return lexIdentOrKeyword
	default:
		return l.errorf("unexpected character: %c", r)
	}
}

// lexString scans a single-quoted string literal.
// The opening quote has already been consumed by lexStart.
//
// The body is copied BYTE for byte, not rune by rune. next() decodes UTF-8,
// and utf8.DecodeRuneInString answers (RuneError, 1) for a byte that starts
// no valid sequence — so sb.WriteRune(r) wrote the three-byte replacement
// character U+FFFD in its place and the literal came out holding different
// bytes than the client sent. That is silent corruption for any literal at
// all, and it made the one carrier the extended protocol has for a bytea
// parameter unusable: pgwire's Bind renders each parameter as a literal
// (there is no bound-parameter path below the parser), so `WHERE b = $1`
// bound with 0xff 0xfe 0x00 0x41 compared the column against three
// replacement characters and matched nothing (#570). A NUL was never the
// problem — eof is -1 here, so a zero byte is an ordinary one.
//
// Slicing the input keeps every other property: l.width is the width next()
// just consumed, so l.input[l.pos-l.width : l.pos] is exactly those bytes,
// and a valid multi-byte rune round-trips as itself.
func lexString(l *lexer) stateFn {
	var sb strings.Builder

	for {
		r := l.next()
		switch {
		case r == eof:
			return l.errorf("unterminated string literal")
		case r == '\'':
			// Check for escaped quote ('')
			if l.peek() == '\'' {
				l.next() // consume the second quote
				sb.WriteByte('\'')
				continue
			}
			// End of string
			l.emitVal(TokenString, sb.String())
			return nil
		default:
			sb.WriteString(l.input[l.pos-l.width : l.pos])
		}
	}
}

// lexQuotedIdent scans a double-quoted ("delimited") identifier.
// The opening quote has already been consumed by lexStart.
//
// The value is the text between the quotes, taken verbatim: case is
// preserved, keyword spellings are not recognised, and dots are ordinary
// characters — so "id.orig_h" is one column name, not a qualified
// reference. A doubled quote ("") inside the identifier is an escaped
// quote character.
func lexQuotedIdent(l *lexer) stateFn {
	var sb strings.Builder

	for {
		r := l.next()
		switch {
		case r == eof:
			return l.errorf("unterminated quoted identifier")
		case r == '"':
			// Check for escaped quote ("")
			if l.peek() == '"' {
				l.next() // consume the second quote
				sb.WriteByte('"')
				continue
			}
			// End of identifier
			if sb.Len() == 0 {
				return l.errorf("zero-length quoted identifier")
			}
			l.pending = &token{
				typ:    TokenIdent,
				val:    sb.String(),
				pos:    l.start,
				quoted: true,
			}
			l.start = l.pos
			return nil
		default:
			sb.WriteRune(r)
		}
	}
}

// lexNumber scans an integer, decimal, or scientific notation number.
// The first digit — or a leading `.` immediately followed by one — has
// already been consumed by lexStart.
// Accepts: 123, 3.14, .5, 1e10, 1E-5, 3.14e+2, .5e3
func lexNumber(l *lexer) stateFn {
	// A leading `.` is already consumed and IS the decimal point, so a second
	// one ends the number: `.5.5` lexes as `.5` then `.5`, which the parser
	// then refuses as it refuses `1.5.5`.
	seenDot := l.pos > l.start && l.input[l.start] == '.'
	// A RADIX PREFIX — 0x, 0o, 0b, either case. PostgreSQL 16+ reads these in
	// its lexer, not only in the integer type's input function, so `SELECT
	// 0x1A` is 26 there and was a syntax error here (#634). The digits are
	// scanned in the prefix's own base; anything else ends the token and the
	// parser refuses what follows.
	if !seenDot && l.input[l.start] == '0' && l.pos == l.start+1 {
		if base := radixBase(l.peek()); base > 0 {
			l.next() // consume x/o/b
			for {
				r := l.peek()
				if r == '_' || (r < 0x80 && digitInBase(byte(r), base)) {
					l.next()
					continue
				}
				break
			}
			return emitRadixNumber(l, base)
		}
	}
	for {
		r := l.peek()
		if r >= '0' && r <= '9' {
			l.next()
			continue
		}
		// An UNDERSCORE separator, PostgreSQL's own rule: it must sit
		// BETWEEN digits, so the next character decides whether it is part of
		// this number or the start of an identifier.
		if r == '_' && l.pos > l.start && isDigitByte(l.peekAt(1)) {
			l.next()
			continue
		}
		if r == '.' && !seenDot {
			seenDot = true
			l.next()
			continue
		}
		break
	}
	// Scientific notation: e/E followed by optional +/- and digits
	if r := l.peek(); r == 'e' || r == 'E' {
		l.next() // consume e/E
		if r := l.peek(); r == '+' || r == '-' {
			l.next() // consume sign
		}
		for l.peek() >= '0' && l.peek() <= '9' {
			l.next()
		}
	}
	// The token's VALUE carries no separators. Every consumer of a numeric
	// literal downstream reads the text — the DECIMAL path reads its exact
	// digits, ADR-0024 item 6 — and teaching each of them the separator rule
	// is how they would come to disagree about it.
	text := l.input[l.start:l.pos]
	if strings.ContainsRune(text, '_') {
		l.emitVal(TokenNumber, strings.ReplaceAll(text, "_", ""))
		return nil
	}
	l.emit(TokenNumber)
	return nil
}

// radixBase maps a prefix letter to its base, or 0 for anything else.
func radixBase(r rune) int {
	switch r {
	case 'x', 'X':
		return 16
	case 'o', 'O':
		return 8
	case 'b', 'B':
		return 2
	}
	return 0
}

func isDigitByte(c byte) bool { return c >= '0' && c <= '9' }

func digitInBase(c byte, base int) bool {
	var v int
	switch {
	case c >= '0' && c <= '9':
		v = int(c - '0')
	case c >= 'a' && c <= 'f':
		v = int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		v = int(c-'A') + 10
	default:
		return false
	}
	return v < base
}

// emitRadixNumber converts a radix-prefixed literal to the DECIMAL text every
// downstream consumer already reads. PostgreSQL's own answer for `0x1A` is the
// integer 26, and normalizing here means the parser, the planner, the DECIMAL
// path and the wire all keep exactly one numeric-literal grammar.
func emitRadixNumber(l *lexer, base int) stateFn {
	raw := l.input[l.start:l.pos]
	digits := strings.ReplaceAll(raw[2:], "_", "")
	if digits == "" {
		return l.errorf("invalid integer literal %q: no digits after the radix prefix", raw)
	}
	// An underscore must be FOLLOWED by a digit, as it must in the decimal
	// form. It MAY come first here — `0x_1A` is 26 in PostgreSQL — because the
	// prefix already stands in front of it.
	body := raw[2:]
	for i := 0; i < len(body); i++ {
		if body[i] == '_' && (i+1 >= len(body) || !digitInBase(body[i+1], base)) {
			return l.errorf("invalid integer literal %q: an underscore must separate digits", raw)
		}
	}
	var acc int64
	for i := 0; i < len(digits); i++ {
		c := digits[i]
		var v int64
		switch {
		case c >= '0' && c <= '9':
			v = int64(c - '0')
		case c >= 'a' && c <= 'f':
			v = int64(c-'a') + 10
		default:
			v = int64(c-'A') + 10
		}
		if acc > (math.MaxInt64-v)/int64(base) {
			return l.errorf("value %q is out of range for type bigint", raw)
		}
		acc = acc*int64(base) + v
	}
	l.emitVal(TokenNumber, strconv.FormatInt(acc, 10))
	return nil
}

// lexIdentOrKeyword scans an identifier or keyword.
// The first letter/underscore has already been consumed by lexStart.
func lexIdentOrKeyword(l *lexer) stateFn {
	for {
		r := l.peek()
		if r == '_' || unicode.IsLetter(r) || (r >= '0' && r <= '9') {
			l.next()
			continue
		}
		break
	}

	word := l.input[l.start:l.pos]
	upper := strings.ToUpper(word)

	if kwType, ok := keywords[upper]; ok {
		// val is uppercased so keyword comparisons can be exact; raw keeps
		// the spelling the user wrote, which matters where a keyword is
		// accepted as a NAME (an alias after AS) and the name is echoed back
		// to the client as a result column — folded there, since an unquoted
		// keyword spelling is an unquoted identifier.
		l.emitVal(kwType, upper)
		l.pending.raw = word
	} else if folded := FoldIdent(word); l.verbatim || folded == word {
		l.emit(TokenIdent)
	} else {
		// An UNQUOTED identifier folds, here, once — PostgreSQL's rule and
		// the reason `SELECT G` reads the column `g`. Everything downstream
		// then sees ONE spelling for one name, which is what lets the
		// engine's byte-exact `batch.RecordBatch.ColumnIndex` stay
		// byte-exact (#731).
		l.emitVal(TokenIdent, folded)
		l.pending.raw = word
	}
	return nil
}

// lexDollarString scans a dollar-quoted string literal ($$...$$).
// The first $ has already been consumed by lexStart.
func lexDollarString(l *lexer) stateFn {
	l.next() // consume second opening $
	contentStart := l.pos
	for {
		r := l.next()
		if r == eof {
			return l.errorf("unterminated dollar-quoted string")
		}
		if r == '$' && l.peek() == '$' {
			contentEnd := l.pos - l.width
			l.next() // consume second closing $
			l.emitVal(TokenString, l.input[contentStart:contentEnd])
			return nil
		}
	}
}
