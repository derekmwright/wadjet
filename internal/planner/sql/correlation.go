package sql

import (
	"fmt"
	"sort"
	"strings"
)

// OuterRef represents a correlated column reference to an outer query scope.
type OuterRef struct {
	Table  string // outer table alias (lowercased)
	Column string // column name (lowercased)
}

// TableColumns reports the column names of a table named in a subquery's FROM
// clause, or nil when the table is unknown (a CTE, a table function, a planner
// with no catalog). It is what lets correlation analysis apply the SQL scoping
// rule: an unqualified name inside a subquery is resolved against the
// subquery's own FROM first, and only a name that does NOT resolve there is a
// reference to the outer query.
type TableColumns func(table string) []string

// outerRefScope carries the name-resolution context for one correlation
// analysis. It is threaded through walkForOuterRefs so the recursion does not
// have to pass four parallel maps at every node.
type outerRefScope struct {
	// outerTables holds the enclosing query's table names and aliases.
	outerTables map[string]bool
	// innerTables holds the subquery's own table names and aliases.
	innerTables map[string]bool
	// outerCols maps an unqualified column name to the identifier (alias, or
	// name when unaliased) of the outer table that supplies it.
	outerCols map[string]string
	// innerCols holds the columns the subquery's own FROM supplies. Empty
	// when no TableColumns resolver was available, in which case the weaker
	// innerTables check below is all the shadowing protection there is.
	innerCols map[string]bool
	// resolve is kept so a subquery nested inside this one can build its own
	// innerCols the same way.
	resolve TableColumns
}

// nest returns the scope for a subquery nested inside this one. Both name
// spaces accumulate: a reference the nested level resolves against ITS FROM,
// or against the level that encloses it, is not a reference to the outermost
// query — anything else still is, and has to be substituted from the outer
// row just the same.
func (s *outerRefScope) nest(info *SelectInfo) *outerRefScope {
	inner := make(map[string]bool, len(s.innerTables)+4)
	for t := range s.innerTables {
		inner[t] = true
	}
	for t := range collectInnerTables(info) {
		inner[t] = true
	}
	var cols map[string]bool
	if nestedCols := collectInnerColumns(info, s.resolve); nestedCols != nil || s.innerCols != nil {
		cols = make(map[string]bool, len(s.innerCols)+len(nestedCols))
		for c := range s.innerCols {
			cols[c] = true
		}
		for c := range nestedCols {
			cols[c] = true
		}
	}
	return &outerRefScope{
		outerTables: s.outerTables,
		innerTables: inner,
		outerCols:   s.outerCols,
		innerCols:   cols,
		resolve:     s.resolve,
	}
}

// FindCorrelatedRefs parses a subquery SQL string and returns any column
// references that refer to tables in outerTables but not to tables defined
// within the subquery itself. An empty result means the subquery is uncorrelated.
func FindCorrelatedRefs(subquerySQL string, outerTables map[string]bool) ([]OuterRef, error) {
	return findCorrelatedRefs(subquerySQL, outerTables, nil, nil)
}

// FindCorrelatedRefsWithColumns is like FindCorrelatedRefs but also accepts
// a column-to-table mapping for resolving unqualified column references.
func FindCorrelatedRefsWithColumns(subquerySQL string, outerTables map[string]bool, outerCols map[string]string) ([]OuterRef, error) {
	return findCorrelatedRefs(subquerySQL, outerTables, outerCols, nil)
}

// FindCorrelatedRefsWithScope is FindCorrelatedRefsWithColumns plus the
// subquery's own column namespace, supplied by innerCols. With it, an
// unqualified name that the subquery's FROM supplies is resolved there and is
// not reported as correlated — the actual SQL rule. Without it the analysis
// falls back to comparing table identifiers, which only rejects the outer
// reference when the outer table's identifier happens to be spelled the same
// as one of the subquery's tables (issue #334).
func FindCorrelatedRefsWithScope(subquerySQL string, outerTables map[string]bool, outerCols map[string]string, innerCols TableColumns) ([]OuterRef, error) {
	return findCorrelatedRefs(subquerySQL, outerTables, outerCols, innerCols)
}

func findCorrelatedRefs(subquerySQL string, outerTables map[string]bool, outerCols map[string]string, resolve TableColumns) ([]OuterRef, error) {
	parsed, err := Parse(subquerySQL)
	if err != nil {
		return nil, err
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		return nil, err
	}

	scope := &outerRefScope{
		outerTables: outerTables,
		innerTables: collectInnerTables(info),
		outerCols:   outerCols,
		innerCols:   collectInnerColumns(info, resolve),
		resolve:     resolve,
	}

	var refs []OuterRef
	// Walk WHERE
	if info.WhereExpr != nil {
		walkForOuterRefs(info.WhereExpr, scope, &refs)
	}
	// Walk HAVING
	if info.HavingExpr != nil {
		walkForOuterRefs(info.HavingExpr, scope, &refs)
	}
	// Walk SELECT columns
	for _, col := range info.Columns {
		if col.ASTExpr != nil {
			walkForOuterRefs(col.ASTExpr, scope, &refs)
		}
	}

	return dedup(refs), nil
}

// collectInnerTables returns all table names and aliases from a SelectInfo.
func collectInnerTables(info *SelectInfo) map[string]bool {
	m := make(map[string]bool)
	for _, t := range info.Tables {
		m[strings.ToLower(t.Name)] = true
		if t.Alias != "" {
			m[strings.ToLower(t.Alias)] = true
		}
	}
	for _, j := range info.Joins {
		m[strings.ToLower(j.RightTable)] = true
		if j.RightAlias != "" {
			m[strings.ToLower(j.RightAlias)] = true
		}
	}
	return m
}

// collectInnerColumns returns the column namespace of the subquery's own FROM
// clause: the union of the columns of every table it names. Returns nil when
// no resolver was supplied or none of the tables could be resolved.
func collectInnerColumns(info *SelectInfo, resolve TableColumns) map[string]bool {
	if resolve == nil {
		return nil
	}
	var m map[string]bool
	add := func(table string) {
		if table == "" {
			return
		}
		for _, col := range resolve(table) {
			if m == nil {
				m = make(map[string]bool)
			}
			m[strings.ToLower(col)] = true
		}
	}
	for _, t := range info.Tables {
		add(t.Name)
	}
	for _, j := range info.Joins {
		add(j.RightTable)
	}
	return m
}

func dedup(refs []OuterRef) []OuterRef {
	seen := make(map[string]bool, len(refs))
	var out []OuterRef
	for _, r := range refs {
		key := r.Table + "." + r.Column
		if !seen[key] {
			seen[key] = true
			out = append(out, r)
		}
	}
	return out
}

// walkForOuterRefs recursively walks an AST node and appends any ColRef that
// resolves to the enclosing query rather than to the subquery itself.
func walkForOuterRefs(node Node, s *outerRefScope, refs *[]OuterRef) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *ColRef:
		if n.Table != "" {
			tbl := strings.ToLower(n.Table)
			if s.outerTables[tbl] && !s.innerTables[tbl] {
				*refs = append(*refs, OuterRef{Table: tbl, Column: strings.ToLower(n.Column)})
			}
		} else if s.outerCols != nil {
			col := strings.ToLower(n.Column)
			// SQL scoping: an unqualified name binds to the innermost scope
			// that supplies it. If the subquery's own FROM has this column,
			// the name is the subquery's own and says nothing about the outer
			// query — however the outer table happens to be aliased.
			if s.innerCols[col] {
				return
			}
			if tbl, ok := s.outerCols[col]; ok && !s.innerTables[tbl] {
				*refs = append(*refs, OuterRef{Table: tbl, Column: col})
			}
		}
	// A subquery nested inside this one can correlate on the OUTERMOST
	// query — `... WHERE c2.x > (SELECT AVG(y) FROM t c3 WHERE c3.k < c1.k)`
	// binds c1 two levels up. The walk had no case for a subquery node, so
	// such a reference was never reported as correlated at all, the
	// evaluator never substituted it, and the inner SQL went to the runner
	// naming a table that is not in its FROM.
	case *SubqueryNode:
		walkNestedForOuterRefs(n.SQL, s, refs)
	case *ExistsNode:
		walkNestedForOuterRefs(n.SQL, s, refs)
	case *AnyAllExpr:
		walkForOuterRefs(n.Left, s, refs)
		for _, v := range n.Values {
			walkForOuterRefs(v, s, refs)
		}
	case *TupleNode:
		for _, e := range n.Elements {
			walkForOuterRefs(e, s, refs)
		}
	case *BinaryOp:
		walkForOuterRefs(n.Left, s, refs)
		walkForOuterRefs(n.Right, s, refs)
	case *UnaryOp:
		walkForOuterRefs(n.Inner, s, refs)
	case *CmpExpr:
		walkForOuterRefs(n.Left, s, refs)
		walkForOuterRefs(n.Right, s, refs)
	case *AndNode:
		walkForOuterRefs(n.Left, s, refs)
		walkForOuterRefs(n.Right, s, refs)
	case *OrNode:
		walkForOuterRefs(n.Left, s, refs)
		walkForOuterRefs(n.Right, s, refs)
	case *NotNode:
		walkForOuterRefs(n.Inner, s, refs)
	case *ParenNode:
		walkForOuterRefs(n.Inner, s, refs)
	case *FuncCallNode:
		for _, arg := range n.Args {
			walkForOuterRefs(arg, s, refs)
		}
	case *InExpr:
		walkForOuterRefs(n.Left, s, refs)
		for _, v := range n.Values {
			walkForOuterRefs(v, s, refs)
		}
	case *BetweenExpr:
		walkForOuterRefs(n.Left, s, refs)
		walkForOuterRefs(n.Low, s, refs)
		walkForOuterRefs(n.High, s, refs)
	case *LikeExpr:
		walkForOuterRefs(n.Left, s, refs)
		walkForOuterRefs(n.Pattern, s, refs)
	case *IsExpr:
		walkForOuterRefs(n.Left, s, refs)
	case *CaseNode:
		if n.Subject != nil {
			walkForOuterRefs(n.Subject, s, refs)
		}
		for _, w := range n.Whens {
			walkForOuterRefs(w.Cond, s, refs)
			walkForOuterRefs(w.Result, s, refs)
		}
		if n.Else != nil {
			walkForOuterRefs(n.Else, s, refs)
		}
	case *CastNode:
		walkForOuterRefs(n.Inner, s, refs)
	}
}

// walkNestedForOuterRefs analyses a subquery nested inside the one being
// walked, under a scope where the enclosing level's names count as inner.
// A subquery that does not parse contributes nothing: the compiler parses the
// same text and declines to build a correlated evaluator for it.
func walkNestedForOuterRefs(sql string, s *outerRefScope, refs *[]OuterRef) {
	parsed, err := Parse(sql)
	if err != nil {
		return
	}
	info, err := ExtractSelect(parsed)
	if err != nil || info == nil {
		return
	}
	nested := s.nest(info)
	if info.WhereExpr != nil {
		walkForOuterRefs(info.WhereExpr, nested, refs)
	}
	if info.HavingExpr != nil {
		walkForOuterRefs(info.HavingExpr, nested, refs)
	}
	for _, col := range info.Columns {
		if col.ASTExpr != nil {
			walkForOuterRefs(col.ASTExpr, nested, refs)
		}
	}
}

// rewriteNestedSubquery substitutes outer values inside a nested subquery's
// own WHERE clause and returns its rebuilt SQL text.
//
// The second result is false when nothing changed — an unparseable subquery,
// one with no WHERE, or one whose WHERE holds no reference this substitution
// resolves. The caller then returns the ORIGINAL node untouched, so a
// subquery that needs no substitution never round-trips through RebuildSQL
// and its text is byte-for-byte what the user wrote.
func rewriteNestedSubquery(sql string, rewrite func(Node) Node) (string, bool) {
	parsed, err := Parse(sql)
	if err != nil {
		return "", false
	}
	info, err := ExtractSelect(parsed)
	if err != nil || info == nil || info.WhereExpr == nil {
		return "", false
	}
	rewritten := rewrite(info.WhereExpr)
	if rewritten == nil || rewritten.String() == info.WhereExpr.String() {
		return "", false
	}
	return RebuildSQL(info, rewritten), true
}

// RewriteOuterRefs returns a deep copy of the AST with correlated ColRef
// nodes replaced by literal values from vals. Keys in vals are "table.column"
// (lowercased).
func RewriteOuterRefs(node Node, outerTables map[string]bool, vals map[string]any) Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *ColRef:
		if n.Table != "" && outerTables[strings.ToLower(n.Table)] {
			key := strings.ToLower(n.Table) + "." + strings.ToLower(n.Column)
			if v, ok := vals[key]; ok {
				return anyToLit(v)
			}
		}
		return n
	case *BinaryOp:
		return &BinaryOp{
			Left:  RewriteOuterRefs(n.Left, outerTables, vals),
			Op:    n.Op,
			Right: RewriteOuterRefs(n.Right, outerTables, vals),
		}
	case *UnaryOp:
		return &UnaryOp{Op: n.Op, Inner: RewriteOuterRefs(n.Inner, outerTables, vals)}
	case *CmpExpr:
		return &CmpExpr{
			Left:  RewriteOuterRefs(n.Left, outerTables, vals),
			Op:    n.Op,
			Right: RewriteOuterRefs(n.Right, outerTables, vals),
		}
	case *AndNode:
		return &AndNode{
			Left:  RewriteOuterRefs(n.Left, outerTables, vals),
			Right: RewriteOuterRefs(n.Right, outerTables, vals),
		}
	case *OrNode:
		return &OrNode{
			Left:  RewriteOuterRefs(n.Left, outerTables, vals),
			Right: RewriteOuterRefs(n.Right, outerTables, vals),
		}
	case *NotNode:
		return &NotNode{Inner: RewriteOuterRefs(n.Inner, outerTables, vals)}
	case *ParenNode:
		return &ParenNode{Inner: RewriteOuterRefs(n.Inner, outerTables, vals)}
	case *FuncCallNode:
		newArgs := make([]Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = RewriteOuterRefs(a, outerTables, vals)
		}
		return &FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *InExpr:
		newVals := make([]Node, len(n.Values))
		for i, v := range n.Values {
			newVals[i] = RewriteOuterRefs(v, outerTables, vals)
		}
		return &InExpr{Left: RewriteOuterRefs(n.Left, outerTables, vals), Not: n.Not, Values: newVals}
	case *BetweenExpr:
		return &BetweenExpr{
			Left: RewriteOuterRefs(n.Left, outerTables, vals),
			Not:  n.Not,
			Low:  RewriteOuterRefs(n.Low, outerTables, vals),
			High: RewriteOuterRefs(n.High, outerTables, vals),
		}
	case *LikeExpr:
		return &LikeExpr{
			Left:    RewriteOuterRefs(n.Left, outerTables, vals),
			Not:     n.Not,
			Pattern: RewriteOuterRefs(n.Pattern, outerTables, vals),
		}
	case *IsExpr:
		return &IsExpr{Left: RewriteOuterRefs(n.Left, outerTables, vals), Not: n.Not, Check: n.Check}
	case *CaseNode:
		cn := &CaseNode{}
		if n.Subject != nil {
			cn.Subject = RewriteOuterRefs(n.Subject, outerTables, vals)
		}
		for _, w := range n.Whens {
			cn.Whens = append(cn.Whens, WhenClause{
				Cond:   RewriteOuterRefs(w.Cond, outerTables, vals),
				Result: RewriteOuterRefs(w.Result, outerTables, vals),
			})
		}
		if n.Else != nil {
			cn.Else = RewriteOuterRefs(n.Else, outerTables, vals)
		}
		return cn
	case *CastNode:
		return &CastNode{Inner: RewriteOuterRefs(n.Inner, outerTables, vals), TypeName: n.TypeName}
	// The substitution has to reach references two levels down, or the SQL
	// handed to the runner still names the outermost table (see
	// walkNestedForOuterRefs).
	case *SubqueryNode:
		if sql, ok := rewriteNestedSubquery(n.SQL, func(inner Node) Node {
			return RewriteOuterRefs(inner, outerTables, vals)
		}); ok {
			return &SubqueryNode{SQL: sql}
		}
		return n
	case *ExistsNode:
		if sql, ok := rewriteNestedSubquery(n.SQL, func(inner Node) Node {
			return RewriteOuterRefs(inner, outerTables, vals)
		}); ok {
			return &ExistsNode{Not: n.Not, SQL: sql}
		}
		return n
	default:
		return node
	}
}

// RewriteUnqualifiedOuterRefs replaces unqualified column references that were
// detected as outer refs (via column mapping). unqualOuter maps column names
// (lowercased) to their resolved table. vals contains "table.column" → value.
func RewriteUnqualifiedOuterRefs(node Node, unqualOuter map[string]string, vals map[string]any) Node {
	if node == nil || len(unqualOuter) == 0 {
		return node
	}
	switch n := node.(type) {
	case *ColRef:
		if n.Table == "" {
			col := strings.ToLower(n.Column)
			if tbl, ok := unqualOuter[col]; ok {
				key := tbl + "." + col
				if v, ok := vals[key]; ok {
					return anyToLit(v)
				}
			}
		}
		return n
	case *BinaryOp:
		return &BinaryOp{
			Left:  RewriteUnqualifiedOuterRefs(n.Left, unqualOuter, vals),
			Op:    n.Op,
			Right: RewriteUnqualifiedOuterRefs(n.Right, unqualOuter, vals),
		}
	case *UnaryOp:
		return &UnaryOp{Op: n.Op, Inner: RewriteUnqualifiedOuterRefs(n.Inner, unqualOuter, vals)}
	case *CmpExpr:
		return &CmpExpr{
			Left:  RewriteUnqualifiedOuterRefs(n.Left, unqualOuter, vals),
			Op:    n.Op,
			Right: RewriteUnqualifiedOuterRefs(n.Right, unqualOuter, vals),
		}
	case *AndNode:
		return &AndNode{
			Left:  RewriteUnqualifiedOuterRefs(n.Left, unqualOuter, vals),
			Right: RewriteUnqualifiedOuterRefs(n.Right, unqualOuter, vals),
		}
	case *OrNode:
		return &OrNode{
			Left:  RewriteUnqualifiedOuterRefs(n.Left, unqualOuter, vals),
			Right: RewriteUnqualifiedOuterRefs(n.Right, unqualOuter, vals),
		}
	case *NotNode:
		return &NotNode{Inner: RewriteUnqualifiedOuterRefs(n.Inner, unqualOuter, vals)}
	case *ParenNode:
		return &ParenNode{Inner: RewriteUnqualifiedOuterRefs(n.Inner, unqualOuter, vals)}
	case *FuncCallNode:
		newArgs := make([]Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = RewriteUnqualifiedOuterRefs(a, unqualOuter, vals)
		}
		return &FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *InExpr:
		newVals := make([]Node, len(n.Values))
		for i, v := range n.Values {
			newVals[i] = RewriteUnqualifiedOuterRefs(v, unqualOuter, vals)
		}
		return &InExpr{Left: RewriteUnqualifiedOuterRefs(n.Left, unqualOuter, vals), Not: n.Not, Values: newVals}
	case *BetweenExpr:
		return &BetweenExpr{
			Left: RewriteUnqualifiedOuterRefs(n.Left, unqualOuter, vals),
			Not:  n.Not,
			Low:  RewriteUnqualifiedOuterRefs(n.Low, unqualOuter, vals),
			High: RewriteUnqualifiedOuterRefs(n.High, unqualOuter, vals),
		}
	case *CastNode:
		return &CastNode{Inner: RewriteUnqualifiedOuterRefs(n.Inner, unqualOuter, vals), TypeName: n.TypeName}
	case *SubqueryNode:
		if sql, ok := rewriteNestedSubquery(n.SQL, func(inner Node) Node {
			return RewriteUnqualifiedOuterRefs(inner, unqualOuter, vals)
		}); ok {
			return &SubqueryNode{SQL: sql}
		}
		return n
	case *ExistsNode:
		if sql, ok := rewriteNestedSubquery(n.SQL, func(inner Node) Node {
			return RewriteUnqualifiedOuterRefs(inner, unqualOuter, vals)
		}); ok {
			return &ExistsNode{Not: n.Not, SQL: sql}
		}
		return n
	default:
		return node
	}
}

// anyToLit converts a Go value to an AST Lit node with proper quoting.
func anyToLit(v any) *Lit {
	if v == nil {
		return &Lit{Value: "null", Kind: LitNull}
	}
	switch tv := v.(type) {
	case string:
		return &Lit{Value: tv, Kind: LitString}
	case int64:
		return &Lit{Value: fmt.Sprintf("%d", tv), Kind: LitNumber}
	case int:
		return &Lit{Value: fmt.Sprintf("%d", tv), Kind: LitNumber}
	case int32:
		return &Lit{Value: fmt.Sprintf("%d", tv), Kind: LitNumber}
	case float64:
		return &Lit{Value: fmt.Sprintf("%g", tv), Kind: LitNumber}
	case float32:
		return &Lit{Value: fmt.Sprintf("%g", tv), Kind: LitNumber}
	case bool:
		if tv {
			return &Lit{Value: "true", Kind: LitBool}
		}
		return &Lit{Value: "false", Kind: LitBool}
	default:
		return &Lit{Value: fmt.Sprint(v), Kind: LitString}
	}
}

// RebuildSQL reconstructs a full SELECT SQL string from a SelectInfo,
// using the provided expression as the WHERE clause instead of the original.
// This is used by the correlated subquery evaluator to substitute outer values.
func RebuildSQL(info *SelectInfo, rewrittenWhere Node) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if info.Distinct {
		sb.WriteString("DISTINCT ")
	}

	// Columns
	for i, col := range info.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		if col.Star {
			if col.TableRef != "" {
				sb.WriteString(col.TableRef)
				sb.WriteString(".*")
			} else {
				sb.WriteString("*")
			}
		} else {
			sb.WriteString(col.Expr)
			if col.Alias != "" && col.Alias != col.Expr {
				sb.WriteString(" AS ")
				sb.WriteString(col.Alias)
			}
		}
	}

	// FROM
	if len(info.Tables) > 0 {
		sb.WriteString(" FROM ")
		for i, t := range info.Tables {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(t.Name)
			if t.Alias != "" && t.Alias != t.Name {
				sb.WriteString(" ")
				sb.WriteString(t.Alias)
			}
		}
	}

	// JOINs
	for _, j := range info.Joins {
		sb.WriteString(" ")
		sb.WriteString(strings.ToUpper(j.Type))
		sb.WriteString(" ")
		sb.WriteString(j.RightTable)
		if j.RightAlias != "" && j.RightAlias != j.RightTable {
			sb.WriteString(" ")
			sb.WriteString(j.RightAlias)
		}
		if j.Condition != "" {
			sb.WriteString(" ON ")
			sb.WriteString(j.Condition)
		}
	}

	// WHERE (rewritten)
	if rewrittenWhere != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(rewrittenWhere.String())
	}

	// GROUP BY
	if len(info.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(info.GroupBy, ", "))
	}

	// HAVING
	if info.Having != "" {
		sb.WriteString(" HAVING ")
		sb.WriteString(info.Having)
	}

	// ORDER BY
	for i, ob := range info.OrderBy {
		if i == 0 {
			sb.WriteString(" ORDER BY ")
		} else {
			sb.WriteString(", ")
		}
		sb.WriteString(ob.Column)
		if ob.Desc {
			sb.WriteString(" DESC")
		}
	}

	// LIMIT
	if info.Limit != "" {
		sb.WriteString(" LIMIT ")
		sb.WriteString(info.Limit)
	}
	if info.Offset != "" {
		sb.WriteString(" OFFSET ")
		sb.WriteString(info.Offset)
	}

	return sb.String()
}

// OuterColumnCandidates returns the column names a subquery may read from the
// query that ENCLOSES it: every reference it cannot resolve against its own
// FROM clause. Subqueries nested inside it are walked too, so a correlation
// two levels down is reported at the top.
//
// It exists for column pruning. A column a correlated subquery reads is a
// column the outer query NEEDS, even when it appears nowhere in the outer
// SELECT list or WHERE clause — and the pruning walk had no case for a
// subquery node at all, so it never saw one. The outer batch then carried no
// such column, readOuterValues substituted NULL for it, every comparison
// against that NULL was UNKNOWN, and the query answered 0 rows with no
// indication anything had gone wrong (issue #347).
//
// It is deliberately over-inclusive where it cannot be sure. An unqualified
// name is reported even though the subquery's own FROM may well supply it,
// because deciding that needs a catalog this package does not have. Naming a
// column the outer relation does not have costs nothing — the caller filters
// candidates against the scan's own schema (sanitizeScanNeeds) — while
// missing one it does have is the wrong answer above. A reference qualified
// by one of the subquery's own tables or aliases is the one case it can rule
// out, and does.
//
// A subquery that does not parse yields no candidates: the expression
// compiler parses the same text and declines to build a correlated evaluator
// for it, and the runtime guard in readOuterValues fails loudly if one is
// built anyway.
func OuterColumnCandidates(subquerySQL string) []string {
	seen := make(map[string]bool, 4)
	collectOuterCandidates(subquerySQL, seen)
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func collectOuterCandidates(subquerySQL string, out map[string]bool) {
	parsed, err := Parse(subquerySQL)
	if err != nil {
		return
	}
	info, err := ExtractSelect(parsed)
	if err != nil || info == nil {
		return
	}
	inner := collectInnerTables(info)
	if info.WhereExpr != nil {
		walkOuterCandidates(info.WhereExpr, inner, out)
	}
	if info.HavingExpr != nil {
		walkOuterCandidates(info.HavingExpr, inner, out)
	}
	for _, col := range info.Columns {
		if col.ASTExpr != nil {
			walkOuterCandidates(col.ASTExpr, inner, out)
		}
	}
}

// walkOuterCandidates collects every column reference under node that is not
// qualified by one of the subquery's own tables, recursing through nested
// subqueries. Bare column names only: the caller matches against a scan's
// schema, which stores columns unqualified.
func walkOuterCandidates(node Node, inner map[string]bool, out map[string]bool) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *ColRef:
		if n.Table != "" && inner[strings.ToLower(n.Table)] {
			return // the subquery's own relation supplies it
		}
		out[strings.ToLower(n.Column)] = true
	case *SubqueryNode:
		// A reference the nested level attributes to ITS outer scope may
		// belong to this level rather than to ours; keeping both is the
		// over-inclusive direction and costs only a schema-filtered name.
		collectOuterCandidates(n.SQL, out)
	case *ExistsNode:
		collectOuterCandidates(n.SQL, out)
	case *AnyAllExpr:
		walkOuterCandidates(n.Left, inner, out)
		for _, v := range n.Values {
			walkOuterCandidates(v, inner, out)
		}
	case *TupleNode:
		for _, e := range n.Elements {
			walkOuterCandidates(e, inner, out)
		}
	case *BinaryOp:
		walkOuterCandidates(n.Left, inner, out)
		walkOuterCandidates(n.Right, inner, out)
	case *UnaryOp:
		walkOuterCandidates(n.Inner, inner, out)
	case *CmpExpr:
		walkOuterCandidates(n.Left, inner, out)
		walkOuterCandidates(n.Right, inner, out)
	case *AndNode:
		walkOuterCandidates(n.Left, inner, out)
		walkOuterCandidates(n.Right, inner, out)
	case *OrNode:
		walkOuterCandidates(n.Left, inner, out)
		walkOuterCandidates(n.Right, inner, out)
	case *NotNode:
		walkOuterCandidates(n.Inner, inner, out)
	case *ParenNode:
		walkOuterCandidates(n.Inner, inner, out)
	case *FuncCallNode:
		for _, arg := range n.Args {
			walkOuterCandidates(arg, inner, out)
		}
	case *InExpr:
		walkOuterCandidates(n.Left, inner, out)
		for _, v := range n.Values {
			walkOuterCandidates(v, inner, out)
		}
	case *BetweenExpr:
		walkOuterCandidates(n.Left, inner, out)
		walkOuterCandidates(n.Low, inner, out)
		walkOuterCandidates(n.High, inner, out)
	case *LikeExpr:
		walkOuterCandidates(n.Left, inner, out)
		walkOuterCandidates(n.Pattern, inner, out)
	case *IsExpr:
		walkOuterCandidates(n.Left, inner, out)
	case *CaseNode:
		if n.Subject != nil {
			walkOuterCandidates(n.Subject, inner, out)
		}
		for _, w := range n.Whens {
			walkOuterCandidates(w.Cond, inner, out)
			walkOuterCandidates(w.Result, inner, out)
		}
		if n.Else != nil {
			walkOuterCandidates(n.Else, inner, out)
		}
	case *CastNode:
		walkOuterCandidates(n.Inner, inner, out)
	}
}
