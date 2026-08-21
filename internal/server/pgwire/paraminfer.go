package pgwire

// Parameter type inference for placeholders the client did NOT declare.
//
// The protocol allows a Parse message to declare no parameter types (or OID 0
// for some of them) and leave the choice to the server — pgJDBC, psycopg and
// DataGrip all do this for values they have as text. Wadjet answered
// ParameterDescription with OID 0, which is legal, and then Bind rendered the
// undeclared parameter's text bytes as a QUOTED string literal, so
// `WHERE n_nationkey = $1` bound with 7 became `n_nationkey = '7'` — an
// int/string comparison the engine coerces to 0, silently matching the WRONG
// row (#365). The declared-OID path beside it was correct, which localizes
// the defect to inference, not to binding.
//
// The inference here is deliberately narrow and lexical: a placeholder that
// stands directly against a column in a comparison ($N <op> col or
// col <op> $N) takes that column's wire type, resolved from the schemas of
// the tables the statement references. Anything else keeps OID 0 and the old
// quoted-literal rendering. PostgreSQL's inference is the full type-checking
// pass; this covers the shape every driver actually sends — a filter on a
// column — without a planner round-trip.

import (
	"context"
	"strings"
	"time"
)

// cmpOperators are the comparison spellings a placeholder can stand against.
// Longest first, so "<=" is not read as "<".
var cmpOperators = []string{"<=", ">=", "!=", "<>", "=", "<", ">"}

// inferParamOIDs returns declared with every OID-0 entry (and every entry
// past the declared list, up to the statement's placeholder count) filled by
// comparison-context inference where possible. The declared entries are never
// overridden: the client's word wins.
func (c *pgConn) inferParamOIDs(sql string, declared []uint32) []uint32 {
	n := countParamPlaceholders(sql)
	if n == 0 {
		return declared
	}
	oids := make([]uint32, n)
	copy(oids, declared)
	missing := false
	for _, oid := range oids {
		if oid == 0 {
			missing = true
			break
		}
	}
	if !missing {
		return oids
	}

	// One resolution per statement text per connection: Bind runs per
	// execution, and the schema lookup behind columnParamOIDs is a catalog
	// round-trip.
	if cached, ok := c.paramOIDCache[sql]; ok {
		return cached
	}

	var colOIDs map[string]uint32 // lazily resolved on the first hit
	for _, ref := range scanParamRefs(sql) {
		if ref.n > n || oids[ref.n-1] != 0 {
			continue
		}
		col := comparisonColumn(sql, ref)
		if col == "" {
			continue
		}
		if colOIDs == nil {
			colOIDs = c.columnParamOIDs(sql)
		}
		if oid, ok := colOIDs[col]; ok {
			oids[ref.n-1] = oid
		}
	}

	if c.paramOIDCache == nil {
		c.paramOIDCache = make(map[string][]uint32)
	}
	c.paramOIDCache[sql] = oids
	return oids
}

// comparisonColumn returns the lower-cased, unqualified column name that ref
// stands directly against in a comparison, or "" when the placeholder's
// context is not `col <op> $N` / `$N <op> col`.
func comparisonColumn(sql string, ref paramRef) string {
	// Backward: col <op> $N
	i := ref.start
	i = skipSpacesBack(sql, i)
	if op := opEndingAt(sql, i); op != "" {
		i = skipSpacesBack(sql, i-len(op))
		if col := identEndingAt(sql, i); col != "" {
			return col
		}
	}
	// Forward: $N <op> col
	j := ref.end
	j = skipSpaces(sql, j)
	for _, op := range cmpOperators {
		if strings.HasPrefix(sql[j:], op) {
			j = skipSpaces(sql, j+len(op))
			if col := identStartingAt(sql, j); col != "" {
				return col
			}
			break
		}
	}
	return ""
}

func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func skipSpacesBack(s string, i int) int {
	for i > 0 && (s[i-1] == ' ' || s[i-1] == '\t' || s[i-1] == '\n' || s[i-1] == '\r') {
		i--
	}
	return i
}

// opEndingAt reports the comparison operator whose last byte is at i-1, or "".
func opEndingAt(s string, i int) string {
	for _, op := range cmpOperators {
		if i >= len(op) && s[i-len(op):i] == op {
			return op
		}
	}
	return ""
}

func isIdentByte(b byte) bool {
	return b == '_' || b == '.' || b == '"' ||
		(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// identEndingAt reads the identifier whose last byte is at i-1, backwards.
func identEndingAt(s string, i int) string {
	j := i
	for j > 0 && isIdentByte(s[j-1]) {
		j--
	}
	return normalizeIdent(s[j:i])
}

// identStartingAt reads the identifier beginning at i, forwards.
func identStartingAt(s string, i int) string {
	j := i
	for j < len(s) && isIdentByte(s[j]) {
		j++
	}
	return normalizeIdent(s[i:j])
}

// normalizeIdent strips a table qualifier and quoting, lower-cases, and
// refuses anything that does not look like a plain column reference (a
// numeric literal, a keyword-shaped operand like NULL, the empty string).
func normalizeIdent(ident string) string {
	if ident == "" {
		return ""
	}
	// A delimited identifier keeps its dots ("id.orig_h" is one name); only
	// an unquoted qualifier is stripped.
	if !strings.Contains(ident, `"`) {
		if dot := strings.LastIndexByte(ident, '.'); dot >= 0 {
			ident = ident[dot+1:]
		}
	}
	ident = strings.Trim(ident, `"`)
	if ident == "" {
		return ""
	}
	if c := ident[0]; c >= '0' && c <= '9' {
		return "" // a literal, not a column
	}
	lower := strings.ToLower(ident)
	switch lower {
	case "null", "true", "false", "and", "or", "not":
		return ""
	}
	return lower
}

// columnParamOIDs resolves the wire type OID of every column of every catalog
// table the statement mentions, keyed by lower-cased column name. A name two
// tables carry at DIFFERENT types is dropped: a wrong confident answer would
// re-create the very defect this exists to fix.
func (c *pgConn) columnParamOIDs(sql string) map[string]uint32 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return nil
	}
	lowerSQL := strings.ToLower(sql)
	out := make(map[string]uint32)
	conflicting := make(map[string]bool)
	for _, table := range tables {
		if !containsIdentWord(lowerSQL, strings.ToLower(table)) {
			continue
		}
		res, err := c.db.Query(ctx, "DESCRIBE "+table)
		if err != nil {
			continue
		}
		for _, row := range res.Rows {
			colName, _ := row["column_name"].(string)
			colType, _ := row["type"].(string)
			if colName == "" || colType == "" {
				continue
			}
			key := strings.ToLower(colName)
			oid := uint32(pgTypeOID(colType))
			if prev, dup := out[key]; dup && prev != oid {
				conflicting[key] = true
				continue
			}
			out[key] = oid
		}
	}
	for key := range conflicting {
		delete(out, key)
	}
	return out
}

// containsIdentWord reports whether word appears in s bounded by
// non-identifier bytes — `nation` must not match `nation_region`.
func containsIdentWord(s, word string) bool {
	for from := 0; ; {
		i := strings.Index(s[from:], word)
		if i < 0 {
			return false
		}
		i += from
		before := i == 0 || !isIdentByte(s[i-1])
		afterIdx := i + len(word)
		after := afterIdx >= len(s) || !isIdentByte(s[afterIdx])
		if before && after {
			return true
		}
		from = i + 1
	}
}
