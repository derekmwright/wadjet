package expr

import (
	"sync/atomic"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// `CAST(x AS VARCHAR(n))` and `CAST(x AS CHAR(n))` — the VALUE half of #838.
//
// `castDestType` maps CHAR / VARCHAR / TEXT / STRING onto one unparameterized
// `batch.TypeString`, so `n` was parsed by the SQL parser and then dropped.
// Cast.Eval's string arm never even saw the parameterized spelling: its switch
// matches the lowered type name exactly, and `varchar(4)` matches no case
// label, so the whole cast fell to `default: return v` and returned its
// operand untouched. A client casting to bound a column's width got a longer
// string than PostgreSQL gives it — a wrong VALUE, not just wrong metadata,
// and ADR-0012 item 5 fixes the order: the length is ENFORCED before it is
// DECLARED, because declaring a bound nothing enforces is the first of two
// lies rather than the end of one.
//
// Measured live on postgres:17.11:
//
//	CAST('abcdef' AS VARCHAR(4))    abcd
//	CAST('abcdef' AS CHAR(4))       abcd
//	CAST('éàüxyz' AS VARCHAR(3))    éàü      -- CHARACTERS, six octets
//	CAST(12345 AS VARCHAR(3))       123      -- the rendering is truncated
//	CAST('abcdef' AS VARCHAR(0))    22023 length for type varchar must be at least 1
//
// CHAR(n) is TRUNCATED and not PADDED here, and that is a decision rather than
// an omission. PostgreSQL's bpchar pads the stored value to n but strips
// trailing blanks for `length()`, for `||`, and for every comparison —
// `CAST('ab' AS CHAR(4)) = 'ab'` is true there, `length` is 2 and `… || 'x'`
// is `abx`, all verified live. This engine has one TypeString and no bpchar,
// so padding would leak blanks into GROUP BY keys, join keys and equality,
// where PostgreSQL strips them: it would turn a rendering divergence into a
// WRONG ROW SET. Truncating alone keeps every one of those three agreeing with
// the server and leaves one residual — the rendered value of a SHORT CHAR(n)
// is `ab` where PostgreSQL prints `ab  ` — which ADR-0012's list records.

// castStringState caches the parsed string destination, for the reason
// castDecimalState exists: `varchar(4)` is fixed for the query and re-parsing
// the type name per row costs a string walk on every value.
type castStringState struct {
	ready atomic.Bool
	limit int
	is    bool
}

// stringDestination resolves this cast's length-carrying string destination
// once. ok=false for every other destination, including the unparameterized
// CHAR / VARCHAR / TEXT / STRING spellings, which impose nothing.
func (e *Cast) stringDestination() (int, bool) {
	if e.strDest.ready.Load() {
		return e.strDest.limit, e.strDest.is
	}
	n, ok := parseStringDest(e.DestType)
	e.strDest.limit, e.strDest.is = n, ok
	e.strDest.ready.Store(true)
	return n, ok
}

// parseStringDest resolves a length-carrying string destination through
// `parquet.StringTypeLength`, the ONE reading of a parameterized string type
// name that the DDL door reads too.
//
// It used to be a second, narrower reading, and that is exactly the defect
// this arc keeps closing elsewhere: it had the `n < 1` refusal that
// `ParseTypeID` lacked (so `CAST(x AS VARCHAR(0))` raised while
// `CREATE TABLE t (a VARCHAR(0))` was accepted), it said `character` where
// PostgreSQL says `char`, and it silently PASSED THROUGH `VARCHAR(abc)` and
// `TEXT(5)`, which the server refuses with 42601. One function, both doors,
// one disposition per type name.
func parseStringDest(dest string) (int, bool) {
	n, err, ok := parquet.StringTypeLength(dest)
	if !ok {
		return 0, false
	}
	if err != nil {
		panic(fatalEval{err})
	}
	return n, true
}

// truncateToChars cuts s to n CHARACTERS. PostgreSQL counts characters, not
// bytes: `CAST('éàüxyz' AS VARCHAR(3))` is `éàü`, three characters and six
// octets. Counting bytes would split a multi-byte rune and put invalid UTF-8
// on the wire.
func truncateToChars(s string, n int) string {
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	if count <= n {
		return s
	}
	// Unreachable for well-formed UTF-8 (the range loop above returns), kept
	// so a byte slice that is not valid UTF-8 still gets a bounded result
	// rather than a panic.
	return s[:utf8.RuneCountInString(s)]
}
