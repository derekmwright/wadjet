package physical

import (
	"context"
	"errors"
	"sort"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// tableColumnSource resolves a table name to its catalog metadata. It is the
// only catalog capability the column binder needs; *catalog.Catalog satisfies
// it, and tests can substitute a fake.
type tableColumnSource interface {
	GetTable(ctx context.Context, name string) (*catalog.TableMeta, error)
}

// ValidateColumns checks that every column reference in a SELECT statement
// resolves to a column available in its scope, returning an error naming the
// first unresolvable reference. This is plan-time name binding: it runs over the
// SQL AST (where query-block boundaries — FROM sources, derived tables, CTEs,
// subqueries — are still explicit) rather than the inlined logical tree.
//
// The binder is deliberately conservative: it errors only when a reference
// provably resolves to no column in any reachable source. Whenever a scope is
// uncertain — an open-schema source is present (table function, recursive CTE,
// SELECT *, a table absent from the catalog), an expression fails to parse, or a
// qualifier can't be matched — it skips rather than risk rejecting a valid
// query. A false positive breaks a working query; a false negative merely lets a
// typo through to the existing runtime check.
func (p *Planner) ValidateColumns(ctx context.Context, info *plansql.SelectInfo) error {
	if p == nil || p.catalog == nil || info == nil {
		return nil
	}
	return validateColumns(ctx, p.catalog, info)
}

func validateColumns(ctx context.Context, src tableColumnSource, info *plansql.SelectInfo) error {
	b := &binder{src: src, ctes: map[string]cteEntry{}}
	return b.validateBlock(ctx, info, nil)
}

// colScope is the set of columns visible at a point in a query. `cols` holds
// every bare (unqualified) column name; `quals` maps a qualifier (table name or
// alias) to its column set for qualified resolution; `srcCount` counts how many
// of this block's OWN FROM sources provide each bare name (outer scopes never
// contribute — an inner name shadows an outer one silently, per PostgreSQL).
// `open` means an unenumerable source is present, so no reference can be proven
// absent.
type colScope struct {
	open     bool
	cols     map[string]bool
	quals    map[string]map[string]bool
	srcCount map[string]int
}

func newColScope() *colScope {
	return &colScope{cols: map[string]bool{}, quals: map[string]map[string]bool{}, srcCount: map[string]int{}}
}

func (s *colScope) addColumn(col string) {
	s.cols[strings.ToLower(col)] = true
}

// addOutputColumn adds a SELECT output alias. An output name resolves before
// input columns in ORDER BY / GROUP BY / HAVING, so it also clears any input
// ambiguity the same name carries.
func (s *colScope) addOutputColumn(col string) {
	c := strings.ToLower(col)
	s.cols[c] = true
	if s.srcCount[c] > 1 {
		s.srcCount[c] = 1
	}
}

// addQualified registers one column of one FROM source. Each call site invokes
// it once per column per source, so it doubles as the ambiguity census: a bare
// name two sources provide reaches srcCount 2.
func (s *colScope) addQualified(qual, col string) {
	c := strings.ToLower(col)
	s.cols[c] = true
	s.srcCount[c]++
	if qual == "" {
		return
	}
	q := strings.ToLower(qual)
	if s.quals[q] == nil {
		s.quals[q] = map[string]bool{}
	}
	s.quals[q][c] = true
}

// merge folds an OUTER (or sibling) scope into s for resolution only: names
// and qualifiers become visible, but srcCount is untouched — outer columns
// never make an inner reference ambiguous.
func (s *colScope) merge(o *colScope) {
	if o == nil {
		return
	}
	if o.open {
		s.open = true
	}
	for c := range o.cols {
		s.cols[c] = true
	}
	for q, cs := range o.quals {
		if s.quals[q] == nil {
			s.quals[q] = map[string]bool{}
		}
		for c := range cs {
			s.quals[q][c] = true
		}
	}
}

func (s *colScope) clone() *colScope {
	c := newColScope()
	c.merge(s)
	for col, n := range s.srcCount {
		c.srcCount[col] = n
	}
	return c
}

// resolveRef resolves ref against this scope, returning nil when it resolves
// (or when the scope is too uncertain to judge) and a SQLSTATE-carrying error
// for the three refusals PostgreSQL makes at analysis time: an unknown column
// (42703), a qualifier no FROM entry provides (42P01, #380), and a bare name
// two sources both provide (42702, #367). Every uncertain case resolves —
// a false positive breaks a working query; a false negative merely lets a
// typo through to the existing runtime check.
func (s *colScope) resolveRef(ref *plansql.ColRef) error {
	if s == nil || s.open {
		return nil
	}
	col := strings.ToLower(ref.Column)
	if ref.Table != "" {
		q := strings.ToLower(ref.Table)
		if cols, ok := s.quals[q]; ok {
			// Qualifier is a known table/alias: a miss iff the column is absent.
			if cols[col] {
				return nil
			}
			return s.unknownColumn(ref)
		}
		if s.cols[q] {
			// Qualifier is itself a column → dotted ROW/struct field access
			// (e.g. attrs.score). Field existence is checked at runtime.
			return nil
		}
		if strings.Contains(q, ".") {
			// Multi-part qualifier (catalog.schema.table) — not modeled here.
			return nil
		}
		// Every FROM source either registers its qualifier or opens the scope,
		// and outer aliases arrive via merge — so an unmatched qualifier in a
		// closed scope provably names no FROM entry. Answering such a query
		// with rows was #380's silent-empty-result defect.
		return sqlerr.New("42P01", "missing FROM-clause entry for table %q", ref.Table)
	}
	if !s.cols[col] {
		return s.unknownColumn(ref)
	}
	if s.srcCount[col] > 1 {
		// Two of this block's sources both provide the name. Resolving it
		// silently would pick a side the client never learns about.
		return sqlerr.New("42702", "column reference %q is ambiguous", ref.Column)
	}
	return nil
}

// unknownColumn builds the 42703 undefined_column error for ref.
func (s *colScope) unknownColumn(ref *plansql.ColRef) error {
	if avail := s.available(); len(avail) > 0 {
		return sqlerr.New("42703", "unknown column %q (available: %s)", ref.String(), strings.Join(avail, ", "))
	}
	return sqlerr.New("42703", "unknown column %q", ref.String())
}

// available returns up to a handful of sorted column names for an error message.
func (s *colScope) available() []string {
	names := make([]string, 0, len(s.cols))
	for c := range s.cols {
		names = append(names, c)
	}
	sort.Strings(names)
	if len(names) > 8 {
		names = names[:8]
	}
	return names
}

type cteEntry struct {
	cols []string
	open bool
}

type binder struct {
	src  tableColumnSource
	ctes map[string]cteEntry
}

func (b *binder) validateBlock(ctx context.Context, info *plansql.SelectInfo, outer *colScope) error {
	if info == nil {
		return nil
	}

	// Register this block's CTEs first so FROM sources and later CTEs can
	// reference them. CTEs accumulate on the binder (additive scoping is only
	// ever more lenient, never a source of false positives).
	for i := range info.CTEs {
		if err := b.registerCTE(ctx, info.CTEs[i]); err != nil {
			return err
		}
	}

	// UNION / INTERSECT / EXCEPT: each branch is its own block.
	if info.Union != nil {
		if err := b.validateBlock(ctx, info.Union.Left, outer); err != nil {
			return err
		}
		return b.validateBlock(ctx, info.Union.Right, outer)
	}

	// Build the FROM scope from base tables, joins, derived tables and CTE refs.
	from := newColScope()
	for i := range info.Tables {
		if err := b.resolveSource(ctx, info.Tables[i], nil, from); err != nil {
			return err
		}
	}
	for i := range info.Joins {
		ref := joinRightRef(info.Joins[i])
		var lateralOuter *colScope
		if info.Joins[i].Lateral {
			// A LATERAL right side may reference columns from sources to its
			// left (and any outer scope).
			lateralOuter = from.clone()
			lateralOuter.merge(outer)
		}
		if err := b.resolveSource(ctx, ref, lateralOuter, from); err != nil {
			return err
		}
	}

	// Resolution scope for WHERE and SELECT: FROM sources plus any outer scope
	// (correlated subqueries). Output aliases are NOT visible here — a SELECT
	// item cannot reference its own output, and WHERE cannot see aliases.
	resolve := from.clone()
	resolve.merge(outer)

	// GROUP BY / HAVING / ORDER BY / QUALIFY may additionally reference SELECT
	// output aliases.
	outNames, starOut := blockOutputs(info)
	withOut := resolve.clone()
	if starOut {
		withOut.open = true
	}
	for _, n := range outNames {
		withOut.addOutputColumn(n)
	}

	// WHERE
	if err := b.checkExpr(info.WhereExpr, resolve); err != nil {
		return err
	}
	// JOIN conditions reference columns from the joined sources (all already in
	// `resolve`). USING/NATURAL/CROSS joins have no CondExpr → skipped.
	for i := range info.Joins {
		if err := b.checkExpr(info.Joins[i].CondExpr, resolve); err != nil {
			return err
		}
	}
	// SELECT expressions (skip stars and window functions — window outputs and
	// star expansion add no enumerable refs the binder can reason about safely).
	for i := range info.Columns {
		col := info.Columns[i]
		if col.Star || col.IsWindow {
			continue
		}
		if err := b.checkExpr(col.ASTExpr, resolve); err != nil {
			return err
		}
		if err := b.checkExpr(col.AggArgExpr, resolve); err != nil {
			return err
		}
	}
	// A bare column beside an aggregate with no GROUP BY has no defined
	// answer — which n_name should the single aggregate row carry?
	if err := checkUngrouped(info, from); err != nil {
		return err
	}
	// GROUP BY expressions
	for _, gb := range info.GroupByExprs {
		if err := b.checkExpr(gb, withOut); err != nil {
			return err
		}
	}
	// HAVING / QUALIFY
	if err := b.checkExpr(info.HavingExpr, withOut); err != nil {
		return err
	}
	if err := b.checkExpr(info.QualifyExpr, withOut); err != nil {
		return err
	}
	// ORDER BY — items are raw expression strings; parse and check what parses.
	for _, ob := range info.OrderBy {
		expr, err := plansql.ParseExpression(ob.Column)
		if err != nil {
			continue
		}
		if err := b.checkExpr(expr, withOut); err != nil {
			return err
		}
	}

	// Recurse into subqueries embedded in this block's expressions. They run in
	// their own scope but can see this block's columns (correlation), so pass
	// `resolve` as their outer scope.
	for _, sql := range b.blockSubqueries(info) {
		sub := parseSelect(sql)
		if sub == nil {
			continue
		}
		if err := b.validateBlock(ctx, sub, resolve); err != nil {
			return err
		}
	}

	return nil
}

// checkExpr errors on the first column reference the scope refuses.
func (b *binder) checkExpr(expr plansql.Node, scope *colScope) error {
	if expr == nil || scope == nil || scope.open {
		return nil
	}
	var refs []*plansql.ColRef
	walkExpr(expr, &refs, nil)
	for _, r := range refs {
		if pgSystemColumns[strings.ToLower(r.Column)] {
			continue
		}
		if err := scope.resolveRef(r); err != nil {
			return err
		}
	}
	return nil
}

// resolveSource resolves one FROM source into `into`. lateralOuter is the scope a
// LATERAL derived table may reference (nil otherwise). It returns an error only
// when a confirmed miss is found while validating a derived table's internals.
func (b *binder) resolveSource(ctx context.Context, tr plansql.TableRef, lateralOuter *colScope, into *colScope) error {
	qual := tr.Alias
	if qual == "" {
		qual = tr.Name
	}

	// Table function (read_json, read_csv, unnest, ...) → open schema.
	if tr.IsFunction {
		into.open = true
		return nil
	}

	// Derived table: FROM (SELECT ...) alias.
	if strings.HasPrefix(tr.Name, "(") {
		inner := parseSelect(strings.TrimSuffix(strings.TrimPrefix(tr.Name, "("), ")"))
		if inner == nil {
			into.open = true
			return nil
		}
		// Validate the derived block's internals. Non-LATERAL derived tables
		// cannot see the enclosing query; LATERAL ones can.
		if err := b.validateBlock(ctx, inner, lateralOuter); err != nil {
			return err
		}
		names, star := blockOutputs(inner)
		if star {
			into.open = true
			return nil
		}
		for _, n := range names {
			into.addQualified(qual, n)
		}
		return nil
	}

	// CTE reference.
	if e, ok := b.ctes[strings.ToLower(tr.Name)]; ok {
		if e.open {
			into.open = true
			return nil
		}
		for _, n := range e.cols {
			into.addQualified(qual, n)
		}
		return nil
	}

	// Base table. Use the original-case name (matching AnnotateScanColumns).
	// A confirmed miss — the catalog was reachable and says the table does not
	// exist — is 42P01: nothing downstream can answer this query, and before
	// this check nothing refused it either, so a typo'd table read as "no
	// matching rows" (#367). A transport failure keeps the old open-scope
	// stance: the table's existence is unknown, and a false rejection breaks
	// a working query.
	meta, err := b.src.GetTable(ctx, tr.Name)
	if err != nil {
		if errors.Is(err, catalog.ErrTableNotFound) {
			return sqlerr.New("42P01", "relation %q does not exist", tr.Name)
		}
		into.open = true
		return nil
	}
	if meta == nil {
		into.open = true
		return nil
	}
	for _, c := range meta.Schema.Columns {
		into.addQualified(qual, c.Name)
	}
	return nil
}

// registerCTE parses, validates and records a CTE's output columns so later
// references resolve. Any uncertainty registers it as open.
func (b *binder) registerCTE(ctx context.Context, cte plansql.CTEDef) error {
	name := strings.ToLower(cte.Name)
	if _, exists := b.ctes[name]; exists {
		return nil
	}
	// Recursive CTEs reference themselves; register open and skip body
	// validation to avoid a false miss on the self-reference.
	if cte.Recursive {
		b.ctes[name] = cteEntry{open: true}
		return nil
	}
	body := parseSelect(cte.SQL)
	if body == nil {
		b.ctes[name] = cteEntry{open: true}
		return nil
	}
	// Register before validating the body so a body parse/validation issue can't
	// leave the name unregistered.
	if len(cte.Columns) > 0 {
		b.ctes[name] = cteEntry{cols: cte.Columns}
	} else if names, star := blockOutputs(body); star {
		b.ctes[name] = cteEntry{open: true}
	} else {
		b.ctes[name] = cteEntry{cols: names}
	}
	return b.validateBlock(ctx, body, nil)
}

// checkUngrouped rejects a bare column in the SELECT list beside an aggregate
// when the block has no GROUP BY at all — PostgreSQL's 42803 grouping_error
// for the shape `SELECT n_name, COUNT(*) FROM nation` (#367). The check is
// deliberately narrow: with a GROUP BY (or GROUPING SETS) present the legal
// forms are richer (expressions over grouped columns, matching grouping
// expressions), so those stay unchecked. A ref that does not certainly resolve
// to one of this block's OWN sources is skipped too — it may be a correlated
// outer reference (constant per invocation) or a niladic the parser reads as a
// column — as is the whole check when any source is unenumerable.
func checkUngrouped(info *plansql.SelectInfo, from *colScope) error {
	if from == nil || from.open || len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 {
		return nil
	}
	hasAgg := false
	for i := range info.Columns {
		if info.Columns[i].IsAgg && !info.Columns[i].IsWindow {
			hasAgg = true
			break
		}
	}
	if !hasAgg {
		return nil
	}
	for i := range info.Columns {
		col := info.Columns[i]
		if col.IsAgg || col.IsWindow || col.Star {
			continue
		}
		var refs []*plansql.ColRef
		walkExpr(col.ASTExpr, &refs, nil)
		for _, r := range refs {
			c := strings.ToLower(r.Column)
			if pgSystemColumns[c] {
				continue
			}
			if r.Table != "" {
				q := strings.ToLower(r.Table)
				if cols, ok := from.quals[q]; !ok || !cols[c] {
					continue
				}
			} else if !from.cols[c] {
				continue
			}
			return sqlerr.New("42803", "column %q must appear in the GROUP BY clause or be used in an aggregate function", r.String())
		}
	}
	return nil
}

// blockSubqueries collects the raw SQL of every subquery embedded in a block's
// expressions (WHERE, SELECT, HAVING, QUALIFY) for recursive validation.
func (b *binder) blockSubqueries(info *plansql.SelectInfo) []string {
	var subs []string
	walkExpr(info.WhereExpr, nil, &subs)
	walkExpr(info.HavingExpr, nil, &subs)
	walkExpr(info.QualifyExpr, nil, &subs)
	for i := range info.Columns {
		walkExpr(info.Columns[i].ASTExpr, nil, &subs)
		walkExpr(info.Columns[i].AggArgExpr, nil, &subs)
	}
	for i := range info.Joins {
		walkExpr(info.Joins[i].CondExpr, nil, &subs)
	}
	return subs
}

// blockOutputs returns a block's output column names and whether it has a star
// (which makes its output unenumerable → treat as open).
func blockOutputs(info *plansql.SelectInfo) ([]string, bool) {
	if info == nil {
		return nil, true
	}
	if info.Union != nil {
		return blockOutputs(info.Union.Left)
	}
	var names []string
	for i := range info.Columns {
		c := info.Columns[i]
		if c.Star {
			return nil, true
		}
		name := c.Alias
		if name == "" {
			name = c.ColumnRef
		}
		if name == "" {
			name = strings.TrimSpace(c.Expr)
		}
		if c.IsWindow && c.WindowSpec != nil && c.WindowSpec.Alias != "" {
			name = c.WindowSpec.Alias
		}
		if name != "" {
			names = append(names, strings.ToLower(name))
		}
	}
	return names, false
}

// joinRightRef extracts the right-hand table reference of a join.
func joinRightRef(j plansql.JoinInfo) plansql.TableRef {
	if j.RightTableRef != nil {
		return *j.RightTableRef
	}
	return plansql.TableRef{Name: j.RightTable, Alias: j.RightAlias}
}

// parseSelect parses a SQL string into a SelectInfo, returning nil on any error
// (the caller treats nil as "can't reason about this → open/skip").
func parseSelect(sql string) *plansql.SelectInfo {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil
	}
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil
	}
	return info
}

// walkExpr collects column references and/or embedded subquery SQL from an
// expression AST. It does NOT descend into subqueries (they form their own
// scope) — it records their SQL for separate validation. Any node type it does
// not recognize contributes nothing (safe: an un-walked ref is a false negative,
// never a false positive).
func walkExpr(node plansql.Node, refs *[]*plansql.ColRef, subs *[]string) {
	switch n := node.(type) {
	case nil:
		return
	case *plansql.ColRef:
		if refs != nil {
			*refs = append(*refs, n)
		}
	case *plansql.SubqueryNode:
		if subs != nil {
			*subs = append(*subs, n.SQL)
		}
	case *plansql.ExistsNode:
		if subs != nil {
			*subs = append(*subs, n.SQL)
		}
	case *plansql.BinaryOp:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.UnaryOp:
		walkExpr(n.Inner, refs, subs)
	case *plansql.CmpExpr:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.AndNode:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.OrNode:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Right, refs, subs)
	case *plansql.NotNode:
		walkExpr(n.Inner, refs, subs)
	case *plansql.ParenNode:
		walkExpr(n.Inner, refs, subs)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			walkExpr(a, refs, subs)
		}
	case *plansql.CastNode:
		walkExpr(n.Inner, refs, subs)
	case *plansql.InExpr:
		walkExpr(n.Left, refs, subs)
		for _, v := range n.Values {
			walkExpr(v, refs, subs)
		}
	case *plansql.BetweenExpr:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Low, refs, subs)
		walkExpr(n.High, refs, subs)
	case *plansql.LikeExpr:
		walkExpr(n.Left, refs, subs)
		walkExpr(n.Pattern, refs, subs)
	case *plansql.IsExpr:
		walkExpr(n.Left, refs, subs)
	case *plansql.CaseNode:
		walkExpr(n.Subject, refs, subs)
		for _, w := range n.Whens {
			walkExpr(w.Cond, refs, subs)
			walkExpr(w.Result, refs, subs)
		}
		walkExpr(n.Else, refs, subs)
	case *plansql.ArrayLitNode:
		for _, e := range n.Elements {
			walkExpr(e, refs, subs)
		}
	case *plansql.TupleNode:
		for _, e := range n.Elements {
			walkExpr(e, refs, subs)
		}
	case *plansql.AnyAllExpr:
		walkExpr(n.Left, refs, subs)
		for _, v := range n.Values {
			walkExpr(v, refs, subs)
		}
	}
}

// pgSystemColumns are PostgreSQL's per-row system columns. Clients request
// them unconditionally — DataGrip opens a table with `SELECT t.*, CTID FROM
// public.customer t`, and rejecting the reference made double-clicking a table
// fail outright.
//
// They resolve to NULL rather than to a value this server invents. ctid is a
// physical row address, and this engine has none: rows live in immutable
// Parquet files that compaction rewrites, so any identifier synthesized here
// would be stable only until the next rewrite. Since UPDATE and DELETE are
// supported, a fabricated ctid is not merely useless but dangerous — a client
// re-issuing `DELETE ... WHERE ctid = '(0,5)'` could address a row other than
// the one the user saw. NULL keeps the read working and makes the write match
// nothing, which is the honest outcome for a row identity that does not exist.
var pgSystemColumns = map[string]bool{
	"ctid":     true,
	"xmin":     true,
	"xmax":     true,
	"cmin":     true,
	"cmax":     true,
	"tableoid": true,
}
