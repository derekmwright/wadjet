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

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
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

// inferenceContext is the context the two schema-reading inference passes below
// run under: a short timeout, carrying THIS CONNECTION'S IDENTITY.
//
// The identity is the load-bearing half. Both passes used a bare
// context.Background(), so the table list they drew from was unfiltered and a
// statement that merely MENTIONED a relation — a string literal is enough,
// containsIdentWord reads the raw SQL — folded that relation's column types
// into the ParameterDescription. A denied relation's declared type reached the
// wire on the extended-protocol path every JDBC and pgx driver prepares
// through. Without the identity, `visibleCatalogTables` would instead refuse
// EVERY relation under auth (a nil identity is a refusal), which would silently
// un-type every parameter for every caller.
func (c *pgConn) inferenceContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if c.identity != nil {
		ctx = auth.ContextWithIdentity(ctx, c.identity)
	}
	// The TRUSTED environment, as well as the identity: a policy conditioned
	// on `env.source_ip` or `env.protocol` decides nothing without it, so a
	// deny that names one would not have matched here and the denied
	// relation's column types would have gone back on the wire anyway
	// (round-1 review P8).
	//
	// The SAME expression `queryContext` attaches for a statement — one
	// builder, one spelling, so the inference path and the statement path
	// cannot come to describe different environments for one connection.
	// `Time` is left for the decision to stamp (auth.DecisionEnvironment), and
	// the port is stripped where every door's is, in auth.
	ctx = auth.ContextWithEnvironment(ctx, auth.Environment{
		SourceIP: peerAddr(c.conn), Protocol: "pgwire",
	})
	return context.WithTimeout(ctx, 5*time.Second)
}

// columnParamOIDs resolves the wire type OID of every column of every catalog
// table the statement mentions, keyed by lower-cased column name. A name two
// tables carry at DIFFERENT types is dropped: a wrong confident answer would
// re-create the very defect this exists to fix.
func (c *pgConn) columnParamOIDs(sql string) map[string]uint32 {
	ctx, cancel := c.inferenceContext()
	defer cancel()

	tables, err := c.visibleCatalogTables(ctx)
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

// isNestedTypeID reports whether t is one of the container types whose text
// rendering needs the column's DECLARED structure — ROW's field order,
// ARRAY's element type, MAP's key/value field names — because the Go value
// GetValue hands back carries none of that on its own: a ROW is a
// map[string]any with no remembered order, an ARRAY and a MAP are both a
// bare []any indistinguishable from each other without it (#471).
func isNestedTypeID(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeRow, parquet.TypeArray, parquet.TypeMap:
		return true
	}
	return false
}

// nestedColumnSchemas first uses the result metas' own declared nested shape,
// then falls back to catalog columns matched by name for the legacy path.
// Conflicting top-level types drop the name rather than confidently apply a
// wrong structure and lose fields. Skip all lookup if no output is nested.
// Only result-derived declarations may supply ordered positional fallback;
// catalog guesses return byName only. Coordinator output schema is authoritative.
// See docs/internals/pgwire-legacy-nested-schema-resolution.md for the design.
func (c *pgConn) nestedColumnSchemas(sql string, metas []wadjet.ColumnMeta) *nestedFieldSchema {
	needed := false
	for _, m := range metas {
		if isNestedTypeID(m.TypeID) {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}
	// The RESULT's own declaration first. A ROW the plan declared carries its
	// field list on the meta (wadjet.ColumnMeta.Fields), and that beats every
	// guess below it: it is per-result rather than a cross-table name match,
	// it is POSITIONAL, and it is the only source at all for a ROW no catalog
	// describes — which is what an aggregate that CONSTRUCTS one produces
	// (ohlcv's bar, #965). Before it, such a column reached
	// formatPgComposite with no declaration and rendered in SORTED-KEY order:
	// a well-formed DataRow carrying the right numbers in the wrong places.
	if ns := nestedSchemaFromMetas(metas); ns != nil {
		return ns
	}

	ctx, cancel := c.inferenceContext()
	defer cancel()

	tables, err := c.visibleCatalogTables(ctx)
	if err != nil {
		return nil
	}
	lowerSQL := strings.ToLower(sql)
	out := make(map[string]parquet.Column)
	conflicting := make(map[string]bool)
	for _, table := range tables {
		if !containsIdentWord(lowerSQL, strings.ToLower(table)) {
			continue
		}
		tm, err := c.db.Catalog().GetTable(ctx, c.db.Catalog().ResolveTableName(table))
		if err != nil || tm == nil {
			continue
		}
		for _, col := range tm.Schema.Columns {
			if prev, dup := out[col.Name]; dup && prev.Type != col.Type {
				conflicting[col.Name] = true
				continue
			}
			out[col.Name] = col
		}
	}
	for name := range conflicting {
		delete(out, name)
	}
	if len(out) == 0 {
		return nil
	}
	// Publish unique folded aliases for catalog names so unquoted output references
	// find their nested declaration (#731), without shadowing byte-exact entries.
	// Two catalog names folding to one alias are ambiguous: drop that alias,
	// matching batch/schema.go item 3; never let map iteration select field order
	// (ADR-0013). Top-level-type conflicts alone cannot detect this case.
	// These catalog guesses have no positional fallback; a miss renders through
	// the deterministic declaration-less path.
	// See docs/internals/pgwire-folded-nested-schema-aliases.md for the design.
	aliases := make(map[string]parquet.Column)
	ambiguous := make(map[string]bool)
	for name, col := range out {
		f := batch.FoldIdent(name)
		if f == name {
			continue
		}
		if _, taken := out[f]; taken {
			continue
		}
		if _, dup := aliases[f]; dup {
			ambiguous[f] = true
			continue
		}
		aliases[f] = col
	}
	for f := range ambiguous {
		delete(aliases, f)
	}
	for f, col := range aliases {
		out[f] = col
	}
	return &nestedFieldSchema{byName: out}
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

// nestedSchemaFromMetas builds the declaration map out of the result's own
// column metadata. It answers only when EVERY nested-typed column has a
// declaration, so a result that mixes a declared ROW with an undeclared one
// still falls through to the catalog walk rather than half-answering.
//
// `ordered` is set, because these entries ARE the output column list in
// order — the positional fallback nestedColumnFor offers is exact here, which
// is what makes a renamed or duplicated ROW column resolve.
func nestedSchemaFromMetas(metas []wadjet.ColumnMeta) *nestedFieldSchema {
	any := false
	for _, m := range metas {
		if m.TypeID != parquet.TypeRow {
			continue
		}
		if len(m.Fields) == 0 {
			return nil
		}
		any = true
	}
	if !any {
		return nil
	}
	byName := make(map[string]parquet.Column, len(metas))
	ordered := make([]parquet.Column, len(metas))
	for i, m := range metas {
		col := parquet.Column{Name: m.Name, Type: m.TypeID,
			Precision: m.Precision, Scale: m.Scale, Fields: m.Fields}
		ordered[i] = col
		byName[m.Name] = col
	}
	return &nestedFieldSchema{byName: byName, ordered: ordered}
}
