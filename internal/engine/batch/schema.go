package batch

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// Column-name resolution against a batch schema.
//
// The SCHEMA is byte-exact — `ColumnIndex` compares `col.Name == name` and
// nothing here changes that. A column called `WatchID` is called `WatchID`,
// and every producer that writes a batch, a shuffle file or a parquet file
// writes the name it was given. What folds is the RESOLVER: the step that
// takes a reference the user wrote and decides which column of this batch it
// names.
//
// The rule is PostgreSQL's, plus one recorded divergence (ADR-0012):
//
//  1. An UNQUOTED identifier folds to lower case at the lexer
//     (`plansql.FoldIdent`), so by the time a reference reaches a batch it is
//     already the folded spelling. A DELIMITED identifier keeps its bytes.
//     The two are therefore distinguishable from the name alone: a reference
//     carrying an ASCII upper-case letter can only have been delimited (or
//     minted by the planner from a schema, which byte-matches anyway).
//  2. PostgreSQL then matches the folded name EXACTLY against the catalog, so
//     a column stored as `"WatchID"` is unreachable as `watchid`. Wadjet's
//     tables come from parquet and ingest, where CamelCase column names are
//     ordinary — ClickBench's `hits` has `WatchID`, `UserID`, `EventTime` —
//     and refusing to resolve them would make those tables unqueryable
//     without quoting every reference. So a FOLDED reference that misses
//     byte-exact resolves case-insensitively when exactly one column matches.
//  3. Two columns matching is ambiguous and resolves to nothing, which the
//     caller reports as the miss it is. Within one table this cannot happen:
//     `catalog.checkDistinctColumnNames` already refuses a schema whose
//     columns collide under `parquet.FoldName`. Across relations it can, and
//     the planner refuses it earlier with 42702.
//  4. A reference carrying an upper-case letter is delimited and resolves
//     byte-exact ONLY. That is what keeps `SELECT "G"` over a column `g` a
//     miss — PostgreSQL's 42703 — rather than a silent read of `g`.

// FoldIdent is the identifier fold, ASCII A-Z only — the same rule
// `plansql.FoldIdent` applies at the lexer, restated here because the engine
// cannot import the planner. Anything that has to decide whether two column
// names are ONE name uses it, so the resolver and the join's duplicate
// detector cannot disagree about that (#731).
func FoldIdent(s string) string {
	if asciiFolded(s) {
		return s
	}
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// IsFoldedIdent reports whether s carries no ASCII upper-case letter, i.e.
// whether it is already in the form the lexer's identifier fold produces —
// which is how a resolver tells an unquoted reference from a delimited one
// with nothing but the name.
func IsFoldedIdent(s string) bool { return asciiFolded(s) }

// EqualFoldIdent reports whether two identifiers are the same name under the
// identifier fold. Exported for the resolvers that match a SUFFIX rather than
// a whole name.
func EqualFoldIdent(a, b string) bool { return asciiEqualFold(a, b) }

// asciiFolded reports whether s carries no ASCII upper-case letter, i.e.
// whether it is already in the form the lexer's identifier fold produces.
func asciiFolded(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return false
		}
	}
	return true
}

// asciiEqualFold reports whether a and b are equal ignoring ASCII case only.
// The ASCII restriction is PostgreSQL's own identifier fold (see
// plansql.FoldIdent): in a UTF8 database `Ä` is not folded, so treating it as
// equal to `ä` here would resolve a name PostgreSQL keeps distinct.
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// ResolveColumnIndex resolves a column REFERENCE to an index in b's schema,
// or -1. See the rule at the top of this file: byte-exact first, then a
// unique ASCII-case-insensitive match when the reference is itself folded.
//
// Callers that hold a NAME rather than a reference — a producer writing its
// own output schema, a stage matching the name it just emitted — keep using
// ColumnIndex.
func (b *RecordBatch) ResolveColumnIndex(name string) int {
	if i := b.ColumnIndex(name); i >= 0 {
		return i
	}
	return resolveFoldedIndex(b.Schema, name)
}

// ResolveColumnByName is ResolveColumnIndex's vector-returning form.
func (b *RecordBatch) ResolveColumnByName(name string) *Vector {
	if i := b.ResolveColumnIndex(name); i >= 0 {
		return b.Columns[i]
	}
	return nil
}

// ResolveSchemaIndex is ResolveColumnIndex over a bare schema, for the
// resolvers that hold one without a batch.
func ResolveSchemaIndex(schema []parquet.Column, name string) int {
	for i, col := range schema {
		if col.Name == name {
			return i
		}
	}
	return resolveFoldedIndex(schema, name)
}

func resolveFoldedIndex(schema []parquet.Column, name string) int {
	if !asciiFolded(name) {
		// A delimited identifier. Byte-exact only — item 4.
		return -1
	}
	found := -1
	for i, col := range schema {
		if foldedNameMatches(col.Name, name) {
			if found >= 0 {
				return -1 // ambiguous — item 3
			}
			found = i
		}
	}
	return found
}

// foldedNameMatches reports whether a folded REFERENCE names a schema column,
// judging the two parts of a qualified name by different rules: the COLUMN
// folds (item 2), the QUALIFIER is byte-exact.
//
// A qualifier is a RELATION's name or alias, and PostgreSQL matches those
// byte-exactly against what the FROM clause declared — a delimited alias `"T"`
// is not reachable as `t` there, and `SELECT "T".x FROM t` is 42P01. Folding
// the whole string bound a reference to the WRONG RELATION: over `FROM rvc t,
// rvd2 "T"` the join emits `g` and `T.g`, and `t.g` fold-matched `T.g` and
// answered rvd2's row (5 → 7). The column half keeps the concession, which is
// what a CamelCase schema needs; the qualifier half never had one.
func foldedNameMatches(schemaName, ref string) bool {
	sq, sc := splitQualifier(schemaName)
	rq, rc := splitQualifier(ref)
	if sq != rq {
		return false
	}
	return asciiEqualFold(sc, rc)
}

// splitQualifier splits at the FIRST dot, which is what every other resolver
// in the engine does (exec.columnIndexFallback). A name with no dot is all
// column and no qualifier.
func splitQualifier(name string) (qualifier, column string) {
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return name[:i], name[i+1:]
		}
	}
	return "", name
}
