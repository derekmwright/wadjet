package sql

import "strings"

// A nested query block is parsed ONCE, and every layer that reasons about that
// block reasons about the SAME tree.
//
// A derived table and a CTE arrive at the planner as SQL TEXT inside their
// enclosing statement, and two layers read that text: the BINDER
// (physical.validateBlock), which decides the questions a parser cannot
// because they need a schema, and the LOGICAL BUILDER, which plans the block.
// While each parsed the text for itself, a decision the binder RECORDED by
// rewriting the block's terms reached only the top-level statement — the
// binder had mutated a tree the builder threw away.
//
// PostgreSQL's GROUP BY precedence is exactly such a decision: a bare name
// there binds an INPUT COLUMN before a SELECT alias, the parser substitutes
// the alias unconditionally because it has no schema, and
// RevertGroupByAliasesShadowedByInput undoes the substitution at the layer
// that knows the FROM sources (#739). Inside a derived table the undo was
// discarded with the binder's copy of the block, so
//
//	SELECT x.g FROM (SELECT g*0 AS g, COUNT(*) AS n FROM t GROUP BY g) x
//
// grouped by the OUTPUT alias — ONE row where PostgreSQL 17 answers six, on
// every arm and in silence (#851). The same held for a CTE body.
//
// The memo is on the REFERENCE, so it propagates to any nesting depth without
// anything having to carry a path: the builder plans the very SelectInfo the
// binder validated, whose own Tables and CTEs carry their own memos.
//
// Because the cache lives in the struct, a caller that wants the shared tree
// must hold the reference by POINTER — a copy caches into itself and the
// original never sees it. Both readers take one.

// SubSelect returns the parsed SELECT body of a DERIVED TABLE reference,
// memoized on the reference. It returns (nil, nil) when the reference is not a
// derived table, and the parse error when the body does not parse — callers
// wrap that in their own message, which is why the error is memoized too.
func (t *TableRef) SubSelect() (*SelectInfo, error) {
	if t == nil || !strings.HasPrefix(t.Name, "(") {
		return nil, nil
	}
	if t.subDone {
		return t.sub, t.subErr
	}
	t.subDone = true
	t.sub, t.subErr = parseBlockText(strings.TrimSuffix(strings.TrimPrefix(t.Name, "("), ")"))
	return t.sub, t.subErr
}

// BodySelect returns the parsed SELECT body of a CTE definition, memoized on
// the definition, for the same reason SubSelect memoizes a derived table's.
func (c *CTEDef) BodySelect() (*SelectInfo, error) {
	if c == nil {
		return nil, nil
	}
	if c.bodyDone {
		return c.body, c.bodyErr
	}
	c.bodyDone = true
	c.body, c.bodyErr = parseBlockText(c.SQL)
	return c.body, c.bodyErr
}

// parseBlockText parses one block's SQL text into a SelectInfo.
func parseBlockText(sql string) (*SelectInfo, error) {
	parsed, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// BlockOutputColumns lists the column names one query block PUBLISHES, and
// reports whether the list is incomplete because the SELECT list holds a star.
//
// It is the block-namespace rule in ONE place: the binder asks it for the
// output aliases a GROUP BY may name, for a derived table's published columns
// and for a CTE's; correlation analysis asks it for the same reason one level
// down — an unqualified name inside a subquery binds the subquery's own FROM
// first, and a CTE reference or a derived table is a relation with a schema
// exactly as a base table is (ADR-0021 §1k).
//
// A set operation publishes its LEFT arm's names, which is PostgreSQL's rule.
// A star is not expanded here: naming what it stands for needs the sources'
// schemas, which this package does not have, so the second result says "ask
// somebody with a catalog".
func BlockOutputColumns(info *SelectInfo) ([]string, bool) {
	if info == nil {
		return nil, true
	}
	if info.Union != nil {
		return BlockOutputColumns(info.Union.Left)
	}
	var names []string
	for i := range info.Columns {
		c := info.Columns[i]
		if c.Star {
			return nil, true
		}
		if name := SelectItemName(c); name != "" {
			names = append(names, strings.ToLower(name))
		}
	}
	return names, false
}

// SelectItemName is the name one SELECT item publishes: its alias, else the
// column it names, else the expression text as written — and for a window call
// the name the logical builder's projection gives it, so the namespace this
// enumerates is the one the query really produces.
func SelectItemName(c SelectColumn) string {
	if c.IsWindow {
		return WindowOutputName(c)
	}
	if c.Alias != "" {
		return c.Alias
	}
	if c.ColumnRef != "" {
		return c.ColumnRef
	}
	return strings.TrimSpace(c.Expr)
}
