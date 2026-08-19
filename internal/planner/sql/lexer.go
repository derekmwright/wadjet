package sql

import (
	"fmt"
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
	TokenPlus    // +
	TokenMinus   // -
	TokenSlash   // /
	TokenPercent // %
	TokenConcat      // ||
	TokenDoubleColon // ::
	TokenJSONArrow       // ->
	TokenJSONDoubleArrow // ->>
	TokenEq          // =
	TokenNotEq   // != or <>
	TokenLT      // <
	TokenLTEq    // <=
	TokenGT      // >
	TokenGTEq    // >=

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
	"OVER":       TokenKWOver,
	"NULLS":      TokenKWNulls,
	"FIRST":      TokenKWFirst,
	"LAST":       TokenKWLast,
	"ROWS":       TokenKWRows,
	"RANGE":      TokenKWRange,
	"UNBOUNDED":  TokenKWUnbounded,
	"PRECEDING":  TokenKWPreceding,
	"FOLLOWING":  TokenKWFollowing,
	"CURRENT":    TokenKWCurrent,
	"ROW":        TokenKWRow,
	"CUBE":       TokenKWCube,
	"ROLLUP":     TokenKWRollup,
	"GROUPING":   TokenKWGrouping,
	"SETS":       TokenKWSets,
	"FETCH":      TokenKWFetch,
	"VIEW":       TokenKWView,
	"ALTER":      TokenKWAlter,
	"ADD":        TokenKWAdd,
	"COLUMN":     TokenKWColumn,
	"RENAME":     TokenKWRename,
	"TO":         TokenKWTo,
	"UPDATE":     TokenKWUpdate,
	"SET":        TokenKWSet,
	"DELETE":     TokenKWDelete,
	"INSERT":     TokenKWInsert,
	"INTO":       TokenKWInto,
	"VALUES":     TokenKWValues,
	"MERGE":      TokenKWMerge,
	"USING":      TokenKWUsing,
	"MATCHED":    TokenKWMatched,
	"ALERT":      TokenKWAlert,
	"EVERY":      TokenKWEvery,
	"WEBHOOK":    TokenKWWebhook,
	"HEADERS":    TokenKWHeaders,
	"ENABLE":     TokenKWEnable,
	"DISABLE":    TokenKWDisable,
	"SECONDS":    TokenKWSeconds,
	"MINUTES":    TokenKWMinutes,
	"HOURS":      TokenKWHours,
	"SNAPSHOT":   TokenKWSnapshot,
}

// token is a lexical token produced by the lexer.
type token struct {
	typ TokenType
	val string
	pos int // byte offset in original input
	// raw is the ORIGINAL spelling of a keyword token, whose val is
	// uppercased for comparison. Empty for every other token, where val is
	// already verbatim.
	raw string
	// quoted marks a TokenIdent that came from a double-quoted
	// ("delimited") identifier. Such a token is always exactly one
	// identifier: its value is taken verbatim between the quotes, so any
	// dots, spaces, or keyword spellings inside it are part of the name
	// rather than syntax.
	quoted bool
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
}

// newLexer creates a new lexer for the given input.
func newLexer(input string) *lexer {
	return &lexer{input: input}
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
}

// peek returns the next rune without advancing.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
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
			sb.WriteRune(r)
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
// The first digit has already been consumed by lexStart.
// Accepts: 123, 3.14, 1e10, 1E-5, 3.14e+2
func lexNumber(l *lexer) stateFn {
	seenDot := false
	for {
		r := l.peek()
		if r >= '0' && r <= '9' {
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
	l.emit(TokenNumber)
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
		// to the client as a result column.
		l.emitVal(kwType, upper)
		l.pending.raw = word
	} else {
		l.emit(TokenIdent)
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
