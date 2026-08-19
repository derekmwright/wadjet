package sql

import (
	"fmt"
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
