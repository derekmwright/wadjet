package pgwire

// Single-row catalog relations: pg_database and pg_namespace.
//
// A client's database and schema pickers are populated by listing these two.
// pg_database queries reached the pg_catalog intercept, matched no branch of
// it, and fell through to the generic answer — the SELECT list's column names
// with no rows under them — so the picker rendered an empty database list and
// nothing downstream could be selected (issue #305 item 6).
//
// Both relations have exactly one row here: this server is one database
// ("wadjet") with one schema ("public"), the same constants the session
// functions current_database() and current_schema() report. What varies is
// which of the relation's columns a client asks for and what it labels them,
// and the answer has to be built from that — the columns declared in the
// RowDescription are the columns the DataRow carries, one decision, per the
// coherence invariant in describe_execute_test.go.

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// pgDatabaseAttrs is the one pg_database row, keyed by column name.
//
// Values carry their real Go types — bool, int, string — because the declared
// column OID is inferred from them (synthAnswer.colOID) and typed clients pick
// their accessor from that OID: a bool declared as text sent DataGrip's reader
// through getInt("f"). The wire text a bool renders to is still t/f
// (formatPgValue). The encoding is 6 (UTF8) and the collation names match a
// UTF-8 cluster, because that is what this server is. Columns of pg_database
// that carry no meaning here — the transaction-id horizons, the ACL — are
// absent, and a client asking for one gets NULL rather than a number this
// server made up.
func pgDatabaseAttrs() map[string]any {
	return map[string]any{
		"oid":            tableOID(expr.SessionCatalog),
		"datname":        expr.SessionCatalog,
		"datdba":         10,
		"encoding":       6,
		"datcollate":     "en_US.UTF-8",
		"datctype":       "en_US.UTF-8",
		"datlocprovider": "c",
		"datistemplate":  false,
		"datallowconn":   true,
		"datconnlimit":   -1,
		"dattablespace":  1663,
		"datacl":         nil,
		// pg_shdescription joined alongside: the database has no comment.
		"description": nil,
	}
}

// pgUserAttrs is the one pg_user row: the identity on this connection.
//
// usesuper is false because this server has no superuser: authorization is
// the RBAC/ABAC policy engine, not a cluster-wide bit, and a client that asks
// whether it may do anything it likes is owed the honest no. Clients use the
// answer to decide which admin-only nodes to offer, and PostgreSQL clients
// introspect perfectly well as ordinary users.
func pgUserAttrs(user string) map[string]any {
	return map[string]any{
		"usename":      user,
		"usesysid":     10,
		"usecreatedb":  false,
		"usesuper":     false,
		"userepl":      false,
		"usebypassrls": false,
		"passwd":       "********",
		"valuntil":     nil,
		"useconfig":    nil,
	}
}

// pgNamespaceAttrs is the one pg_namespace row.
func pgNamespaceAttrs() map[string]any {
	return map[string]any{
		"oid":      tableOID(expr.SessionSchema),
		"nspname":  expr.SessionSchema,
		"nspowner": 10,
		"nspacl":   nil,
	}
}

// matchPgDatabase answers a pg_database listing with the single database this
// server is.
func (c *pgConn) matchPgDatabase(sql string) *synthAnswer {
	return catalogRowAnswer(sql, pgDatabaseAttrs(), []string{
		"oid", "datname", "datdba", "encoding", "datcollate", "datctype",
		"datistemplate", "datallowconn",
	})
}

// catalogRowAnswer builds a one-row answer for a catalog relation whose whole
// contents this server knows, shaped by the statement's own SELECT list: the
// answer's columns are the labels the client wrote, and each value is the
// attribute that column names, or NULL for one this server does not model.
// `SELECT *` gets fallbackCols.
//
// It returns nil when the SELECT list names none of the relation's columns —
// an aggregate, a projection of something else — leaving the caller's
// empty-but-coherent fallback in place rather than answering a question that
// was not asked.
func catalogRowAnswer(sql string, attrs map[string]any, fallbackCols []string) *synthAnswer {
	return catalogRowsAnswer(sql, attrs, []map[string]any{attrs}, fallbackCols)
}

// cteAliases maps the column names a statement's CTEs expose to the catalog
// attributes they read, by running the CTE body's own SELECT list through the
// same scanner the outer list uses.
//
// DataGrip reads columns through a CTE over pg_class that renames as it goes —
// `T.oid as oid, T.relkind as kind, T.relnamespace as schemaId` — and the outer
// query then selects T.kind and T.schemaId. Those names appear in no catalog,
// so without this the outer statement asked for columns that resolved to
// nothing. Resolving them is what lets one answer carry both relations of the
// join the client wrote.
func cteAliases(sql string) map[string]string {
	if len(sql) < 5 || !strings.EqualFold(strings.TrimSpace(sql)[:5], "WITH ") {
		return nil
	}
	aliases := make(map[string]string)
	// Walk each `... AS ( body )` at paren depth zero.
	depth, inSingle, inDouble := 0, false, false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			if depth == 0 {
				// Body runs to the matching close paren.
				body, end := balancedBody(sql, i)
				if body != "" {
					for _, it := range selectItems(strings.TrimSpace(body)) {
						if it.label != "" && it.expr != "" {
							aliases[strings.ToLower(it.label)] = it.expr
						}
					}
				}
				i = end
				continue
			}
			depth++
		case ch == ')':
			if depth > 0 {
				depth--
			}
		}
	}
	return aliases
}

// balancedBody returns the text between the paren at open and its match, plus
// the index of that closing paren.
func balancedBody(s string, open int) (string, int) {
	depth, inSingle, inDouble := 0, false, false
	for i := open; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			depth--
			if depth == 0 {
				return s[open+1 : i], i
			}
		}
	}
	return "", len(s) - 1
}

// evalCatalogExpr computes the value of a SELECT-list expression that wraps a
// single catalog attribute in one of the text functions clients use to shape
// catalog values, and reports whether it could.
//
// DataGrip derives an object's kind with
// `pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS')`. Mapping only bare
// attribute names left that column NULL, and an object with no kind is one the
// client cannot place: "Unsupported object kind ” of object 'customer'".
// These functions are pure and tiny, so the honest answer is to compute what
// PostgreSQL would compute rather than to guess or to return nothing.
func evalCatalogExpr(expr string, attrs map[string]any) (any, bool) {
	expr = strings.TrimSpace(expr)

	// `not C.attislocal` — a negated attribute, which the column queries use
	// to report inheritance.
	if len(expr) > 4 && strings.EqualFold(expr[:4], "NOT ") {
		v, ok := attrs[strings.ToLower(bareAttr(expr[4:]))]
		if b, isBool := v.(bool); ok && isBool {
			return !b, true
		}
		return nil, false
	}

	open := strings.IndexByte(expr, '(')
	if open < 0 || !strings.HasSuffix(expr, ")") {
		return nil, false
	}
	fn := strings.ToLower(strings.TrimSpace(expr[:open]))
	fn = strings.TrimPrefix(fn, "pg_catalog.")
	args := splitArgs(expr[open+1 : len(expr)-1])
	if len(args) == 0 {
		return nil, false
	}

	// format_type(atttypid, atttypmod) renders a column's SQL type. Its
	// argument is an OID rather than the text the rest of these functions
	// take, and the row already carries the rendering — computed from the
	// column's real type — under format_type.
	if fn == "format_type" {
		ft, ok := attrs["format_type"]
		return ft, ok
	}

	// The first argument names the attribute; the rest must be literals.
	attr := strings.ToLower(strings.Trim(bareAttr(args[0]), `"`))
	val, ok := attrs[attr]
	if !ok {
		return nil, false
	}
	s, isStr := val.(string)
	if !isStr {
		return nil, false
	}

	switch fn {
	case "upper":
		if len(args) != 1 {
			return nil, false
		}
		return strings.ToUpper(s), true
	case "lower":
		if len(args) != 1 {
			return nil, false
		}
		return strings.ToLower(s), true
	case "translate":
		if len(args) != 3 {
			return nil, false
		}
		from, ok1 := literalString(args[1])
		to, ok2 := literalString(args[2])
		if !ok1 || !ok2 {
			return nil, false
		}
		return translateChars(s, from, to), true
	}
	return nil, false
}

// translateChars is PostgreSQL's translate(): each character of s that appears
// in from is replaced by the character at the same position in to, or dropped
// when to is shorter.
func translateChars(s, from, to string) string {
	fromRunes, toRunes := []rune(from), []rune(to)
	var b strings.Builder
	for _, r := range s {
		idx := -1
		for i, f := range fromRunes {
			if f == r {
				idx = i
				break
			}
		}
		switch {
		case idx < 0:
			b.WriteRune(r)
		case idx < len(toRunes):
			b.WriteRune(toRunes[idx])
		}
	}
	return b.String()
}

// literalString returns the contents of a single-quoted SQL literal.
func literalString(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), true
}

// bareAttr strips a cast and a table qualifier from an expression, leaving the
// attribute name it reads: `T.relkind::char` → `relkind`.
func bareAttr(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.Index(s, "::"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	if i := strings.LastIndex(s, "."); i >= 0 {
		s = strings.TrimSpace(s[i+1:])
	}
	return s
}

// splitArgs splits a function argument list on top-level commas.
func splitArgs(s string) []string {
	var args []string
	depth := 0
	inSingle, inDouble := false, false
	start := 0
	for i := 0; i < len(s); i++ {
		switch ch := s[i]; {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			depth--
		case ch == ',' && depth == 0:
			args = append(args, s[start:i])
			start = i + 1
		}
	}
	return append(args, s[start:])
}

// catalogRowsAnswer is catalogRowAnswer over many rows: pg_class lists one row
// per table, pg_database one row per database. shape names the relation's
// columns (which of them the SELECT list may read); rows carry the values, and
// may be empty — a listing narrowed to nothing still has to answer with the
// columns the client asked for, not with a different shape.
func catalogRowsAnswer(sql string, shape map[string]any, rows []map[string]any, fallbackCols []string) *synthAnswer {
	items := selectItems(sql)
	if len(items) == 0 {
		return nil
	}

	// SELECT * — the client wants the relation's columns, so name them.
	if len(items) == 1 && items[0].expr == "*" {
		items = items[:0]
		for _, col := range fallbackCols {
			items = append(items, selectItem{expr: col, label: col, raw: col})
		}
	}

	cols := make([]string, 0, len(items))
	picks := make([]selectItem, 0, len(items))
	seen := make(map[string]bool, len(items))
	known := false
	for _, it := range items {
		if _, ok := shape[it.expr]; ok {
			known = true
		}
		// Duplicate labels would collapse in the row map; the value under a
		// repeated label is the same either way, so only the first is kept.
		if seen[it.label] {
			continue
		}
		seen[it.label] = true
		cols = append(cols, it.label)
		picks = append(picks, it)
	}
	if !known {
		return nil
	}

	ans := &synthAnswer{cols: cols}
	for _, attrs := range rows {
		row := make(map[string]any, len(picks))
		for _, it := range picks {
			if v, ok := attrs[it.expr]; ok {
				row[it.label] = v
				continue
			}
			// Not a bare attribute: a function over one may still be
			// computable (translate/upper/lower over relkind and friends).
			if v, ok := evalCatalogExpr(it.raw, attrs); ok {
				row[it.label] = v
				continue
			}
			row[it.label] = nil
		}
		ans.rows = append(ans.rows, row)
	}
	return ans
}

// pgClassAttrs is the pg_class row for one table. Everything this server can
// answer honestly is here; what it does not model (ACLs, index/trigger flags
// it has no concept of) is either its truthful constant or absent, so a client
// asking for it reads NULL rather than an invention.
//
// relkind is 'r' (ordinary table) for every entry: the catalog holds tables,
// and a client that reads relkind is deciding which node type to draw.
func pgClassAttrs(name string) map[string]any {
	return map[string]any{
		"oid":            tableOID(name),
		"relname":        name,
		"relnamespace":   tableOID(expr.SessionSchema),
		"relkind":        "r",
		"relowner":       10,
		"reltuples":      float64(-1), // PostgreSQL's "not yet vacuumed/analyzed"
		"relhasindex":    false,
		"relpersistence": "p",
		"relispartition": false,
		"relhasrules":    false,
		"relhastriggers": false,
		"relhassubclass": false,
		"reltablespace":  0,
		"reloptions":     nil,
		"relacl":         nil,
		"description":    nil,
	}
}

// pgTablesAttrs is the pg_tables row for one table: the user-facing listing
// view over pg_class. Everything lives in the one schema, owned by the
// connection's identity; the flag columns are the honest false — this server
// has no indexes, rules, triggers or row security.
func pgTablesAttrs(name, owner string) map[string]any {
	return map[string]any{
		"schemaname":  expr.SessionSchema,
		"tablename":   name,
		"tableowner":  owner,
		"tablespace":  nil,
		"hasindexes":  false,
		"hasrules":    false,
		"hastriggers": false,
		"rowsecurity": false,
	}
}

// stripSQLComments removes -- line comments and /* block */ comments (nested,
// as PostgreSQL nests them), leaving a space where each stood so tokens do not
// fuse. String literals and quoted identifiers are respected.
//
// This layer reasons about statement text — which relation is the subject,
// which columns the client asked for — so it has to reason about the statement
// the server will run, not the commentary around it. DataGrip ships a
// commented-out CTE at the top of its index query:
//
//	/* with T as ( select T.oid from pg_catalog.pg_class T ... ) */
//	select ind_head.indexrelid ...
//
// Read literally that text names pg_class, so the statement looked like a
// table listing and would have been answered as one.
func stripSQLComments(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))
	inSingle, inDouble := false, false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			out.WriteByte(' ')
			if i < len(sql) {
				out.WriteByte('\n')
			}
			continue
		case ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			depth, j := 1, i+2
			for j < len(sql) && depth > 0 {
				if sql[j] == '/' && j+1 < len(sql) && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if sql[j] == '*' && j+1 < len(sql) && sql[j+1] == '/' {
					depth--
					j += 2
					continue
				}
				j++
			}
			i = j - 1
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
}

// selectItem is one entry of a SELECT list: the attribute it reads, lowercased
// and stripped of any table qualifier, and the label the client will read the
// value back under.
type selectItem struct {
	expr  string // the attribute it reads, lowercased and unqualified
	label string // what the client will read the value back under
	raw   string // the entry's own text, for expressions expr cannot name
}

// selectItems splits a SELECT list into its entries. It is the same rough
// comma split extractSelectColumns does — the labels it produces match, so a
// statement this layer answers is labelled the way the rest of this layer
// labels one — but it keeps the expression alongside the label, which is what
// tells `datname AS TABLE_CAT` to read datname and report TABLE_CAT.
//
// Anything more structured than `[table.]column [AS label]` comes back with
// its whole text as the expression, which matches no attribute and so becomes
// NULL (or, if nothing in the list matches, declines the statement).
func selectItems(sql string) []selectItem {
	sql = outerSelect(sql)
	if len(sql) < 7 || !strings.EqualFold(sql[:7], "SELECT ") {
		return nil
	}
	entries := splitSelectList(sql[7:])
	if len(entries) > 0 {
		if t := strings.TrimSpace(entries[0]); len(t) > 9 && strings.EqualFold(t[:9], "DISTINCT ") {
			// DISTINCT belongs to the statement, not to the first column.
			entries[0] = t[9:]
		}
	}

	var items []selectItem
	for _, p := range entries {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		exprPart, label := p, ""
		if asIdx := lastTopLevelAS(p); asIdx >= 0 {
			exprPart = strings.TrimSpace(p[:asIdx])
			label = strings.Trim(strings.TrimSpace(p[asIdx+4:]), `"`)
		} else if fields := strings.Fields(p); len(fields) == 2 &&
			balancedParens(fields[0]) && isPlainIdent(fields[1]) {
			// SQL's implicit alias: `rolsuper is_super` labels the column
			// is_super with no AS. Without this the whole two-word text
			// became the column name, and a client reading its own label
			// found nothing under it.
			//
			// Both guards matter: `format_type(a.atttypid, a.atttypmod)`
			// splits into two whitespace tokens too, and reading the second
			// as an alias turned one expression into a column labelled
			// "a.atttypmod)".
			exprPart, label = fields[0], strings.Trim(fields[1], `"`)
		}
		// A negated attribute is not that attribute. Stripping the qualifier
		// off `not C.attislocal` leaves "attislocal", which resolves to the
		// attribute itself and reports the opposite of what was asked — the
		// column-inheritance flag came back true for every column. Keep the
		// whole text so the expression evaluator sees the NOT.
		if len(exprPart) > 4 && strings.EqualFold(exprPart[:4], "NOT ") {
			items = append(items, selectItem{
				expr:  strings.ToLower(exprPart),
				label: label,
				raw:   exprPart,
			})
			if label == "" {
				items[len(items)-1].label = strings.ToLower(exprPart)
			}
			continue
		}

		// A cast reads the same attribute: N.oid::bigint is still oid.
		bare := exprPart
		if castIdx := strings.Index(bare, "::"); castIdx >= 0 {
			bare = strings.TrimSpace(bare[:castIdx])
		}
		// Strip a table qualifier: d.datname and pg_database.datname both
		// read datname.
		if dotIdx := strings.LastIndex(bare, "."); dotIdx >= 0 {
			bare = strings.TrimSpace(bare[dotIdx+1:])
		}
		bare = strings.Trim(bare, `"`)
		if label == "" {
			// PostgreSQL labels an unaliased expression after the function it
			// calls, and an unaliased subselect ?column?. Falling back to the
			// trailing identifier instead produced columns named "atttypmod)".
			switch {
			case strings.HasPrefix(exprPart, "("):
				label = "?column?"
			case strings.Contains(exprPart, "(") && balancedParens(exprPart):
				fn := strings.TrimSpace(exprPart[:strings.IndexByte(exprPart, '(')])
				if dot := strings.LastIndex(fn, "."); dot >= 0 {
					fn = fn[dot+1:]
				}
				if isPlainIdent(fn) {
					label = strings.ToLower(fn)
				} else {
					label = bare
				}
			default:
				label = bare
			}
		}
		items = append(items, selectItem{expr: strings.ToLower(bare), label: label, raw: exprPart})
	}
	return items
}

// splitSelectList scans the text after SELECT and returns the top-level
// comma-separated entries, stopping at the statement's own FROM. Unlike a
// substring search it survives what real clients send: newlines before
// FROM, commas inside function calls, and quoted identifiers — the scan
// tracks paren depth and both quote kinds, and FROM only terminates the
// list as a word at depth zero.
func splitSelectList(s string) []string {
	var entries []string
	depth := 0
	inSingle, inDouble := false, false
	start := 0
	wordAt := func(i int) bool {
		// s[i:i+4] is FROM as its own word.
		if i+4 > len(s) || !strings.EqualFold(s[i:i+4], "FROM") {
			return false
		}
		before := byte(' ')
		if i > 0 {
			before = s[i-1]
		}
		after := byte(' ')
		if i+4 < len(s) {
			after = s[i+4]
		}
		isWord := func(b byte) bool {
			return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
		}
		return !isWord(before) && !isWord(after)
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && ch == ',':
			entries = append(entries, s[start:i])
			start = i + 1
		case depth == 0 && (ch == 'f' || ch == 'F') && wordAt(i):
			entries = append(entries, s[start:i])
			return entries
		}
	}
	entries = append(entries, s[start:])
	return entries
}

// lastTopLevelAS finds the last " AS " outside quotes and parens, so that
// pg_get_userbyid(x) AS "owner" splits at the alias and CAST(x AS int8)
// does not.
func lastTopLevelAS(s string) int {
	depth := 0
	inSingle, inDouble := false, false
	last := -1
	for i := 0; i+4 <= len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && strings.EqualFold(s[i:i+4], " AS "):
			last = i
		}
	}
	return last
}

// isPlainIdent reports whether s is a bare SQL identifier (or a quoted one),
// which is what an implicit alias must look like.
func isPlainIdent(s string) bool {
	s = strings.Trim(s, `"`)
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			(i > 0 && c >= '0' && c <= '9')
		if !ok {
			return false
		}
	}
	return true
}

// balancedParens reports whether s opens and closes every paren it contains,
// so a fragment of a longer expression is not mistaken for a whole one.
func balancedParens(s string) bool {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}

// outerSelect returns the statement text from its outermost SELECT onward.
//
// A WITH clause puts the SELECT that decides the answer's shape after the CTE
// bodies, and reading the list from position zero found no SELECT at all —
// DataGrip fetches columns through a CTE, so its statement fell back to a
// fixed tuple whose labels it never asked for, and the schemaId it read came
// back NULL: "There's an object from schema [-9223372036854775808], but the
// schema was not in where clause."
//
// The scan is depth- and quote-aware, so a SELECT inside a CTE body or a
// subquery is skipped and only the statement's own SELECT is found.
func outerSelect(sql string) string {
	sql = strings.TrimSpace(sql)
	if len(sql) >= 7 && strings.EqualFold(sql[:7], "SELECT ") {
		return sql
	}
	if len(sql) < 5 || !strings.EqualFold(sql[:5], "WITH ") {
		return sql
	}
	depth, inSingle, inDouble := 0, false, false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '(':
			depth++
		case ch == ')':
			if depth > 0 {
				depth--
			}
		case depth == 0 && (ch == 's' || ch == 'S'):
			if i+7 <= len(sql) && strings.EqualFold(sql[i:i+7], "SELECT ") {
				before := byte(' ')
				if i > 0 {
					before = sql[i-1]
				}
				if before == ' ' || before == '\t' || before == '\n' || before == '\r' || before == ')' {
					return sql[i:]
				}
			}
		}
	}
	return sql
}
