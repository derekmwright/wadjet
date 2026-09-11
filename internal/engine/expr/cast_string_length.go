package expr

import (
	"sync/atomic"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// CAST AS VARCHAR(n) / CHAR(n) truncates the rendered value to n CHARACTERS,
// not bytes; enforce the bound before declaring it (#838, ADR-0012 item 5).
// The shared type parser rejects n < 1 with 22023.
// CHAR(n) truncates without padding: TypeString has no bpchar semantics,
// and padding would leak blanks into grouping, join keys and equality.
// Short CHAR rendering therefore differs from PostgreSQL's padded output,
// as recorded in ADR-0012; unparameterized string casts impose no bound.
// See docs/internals/bounded-string-cast-character-contract.md for the design.

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
