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
