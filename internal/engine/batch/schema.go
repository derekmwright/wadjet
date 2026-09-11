package batch

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// Schema names and ColumnIndex remain byte-exact; producers preserve names.
// The resolver follows lexer ASCII folding (ADR-0012): uppercase in a reference
// means delimited/schema-minted and permits only a byte-exact match.
// A folded name first matches exactly, then uniquely case-insensitively; this
// fallback for parquet/ingest CamelCase is a recorded PostgreSQL divergence.
// Multiple folded matches resolve to nothing. Catalog checks forbid within-table
// fold collisions; the planner rejects cross-relation ambiguity with 42702.
// An uppercase delimited miss remains a miss (42703), never a folded read.
// See docs/internals/batch-column-name-resolution.md for the design.

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

// NameSetNames reports whether a set of column REFERENCES names the schema
// column spelled schemaName — the resolver's rule read in the other direction,
// for the consumers that hold a SET of wanted names and walk the schema rather
// than the reverse (a read-set projection, a keep-set prune).
//
// The rule is the same one `ResolveSchemaIndex` applies: byte-exact, or a
// reference that is ITSELF folded matching case-insensitively. Ambiguity
// cannot arise within one schema — `catalog.checkDistinctColumnNames` refuses
// a schema whose columns collide under the fold — so a folded reference names
// at most one column of it.
func NameSetNames(set map[string]bool, schemaName string) bool {
	if set[schemaName] {
		return true
	}
	for ref := range set {
		if asciiFolded(ref) && asciiEqualFold(ref, schemaName) {
			return true
		}
	}
	return false
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

// RowFieldPath resolves a qualifier naming a ROW that DECLARES the field
// (ADR-0022, #769). Consumers ask before stripping the qualifier, so another
// relation's same-named column cannot replace the ROW field.
// A literal flat dotted column wins first; undeclared fields decline (#604).
// Resolve the parent normally, then as the ONE <qualifier>.<name> suffix;
// two matching parents decline. Declining alone does not refuse ambiguity:
// physical.colScope.check must raise 42702 before later resolver fallbacks bind.
// See docs/internals/batch-row-field-path-precedence.md for the design.
func (b *RecordBatch) RowFieldPath(name string) (parent, field int, ok bool) {
	dot := -1
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			dot = i
			break
		}
	}
	if dot <= 0 || dot == len(name)-1 {
		return -1, -1, false
	}
	// A flat column literally called `a.b` (a Zeek `id.orig_h`) is that
	// column and not a path into anything.
	if b.ResolveColumnIndex(name) >= 0 {
		return -1, -1, false
	}
	qual, fieldName := name[:dot], name[dot+1:]
	pi := b.ResolveColumnIndex(qual)
	if pi < 0 {
		pi = b.uniqueQualifiedColumn(qual)
	}
	if pi < 0 || pi >= len(b.Columns) || b.Columns[pi] == nil || b.Columns[pi].Type != TypeRow {
		return -1, -1, false
	}
	v := b.Columns[pi]
	for j, fn := range v.FieldNames {
		if j < len(v.Children) && asciiEqualFold(fn, fieldName) {
			return pi, j, true
		}
	}
	return -1, -1, false
}

// uniqueQualifiedColumn returns the index of the ONE column of b spelled
// `<qualifier>.<bare>`, or -1 when none or more than one matches.
func (b *RecordBatch) uniqueQualifiedColumn(bare string) int {
	found := -1
	for i := range b.Schema {
		_, c := splitQualifier(b.Schema[i].Name)
		if c == b.Schema[i].Name || !asciiEqualFold(c, bare) {
			continue
		}
		if found >= 0 {
			return -1
		}
		found = i
	}
	return found
}
