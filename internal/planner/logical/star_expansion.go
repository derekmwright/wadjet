package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ExpandStarProjections rewrites every `*` / `alias.*` select item into one
// projection per column of the star's source, in schema order.
//
// A star that shares its SELECT list with another item — `SELECT t.*, ctid` is
// how DataGrip opens a table — reaches the planner as a Projection carrying the
// literal expression "*" and no column reference at all. That is why it must be
// expanded HERE, before computeRequiredColumns: the pruner collects the columns
// each Project names, the star named none, so the scan below was narrowed to
// the SIBLING item's columns and every column the star contributed read back
// NULL. The single-process path returned those NULLs (#315); the distributed
// path only escaped because an unknown name trips the worker's all-or-nothing
// parquet projection guard, which falls back to full width.
//
// A star's source columns come from the scan's catalog-annotated schema
// (ScanColumns, populated by physical.AnnotateScanColumns), so this resolves
// only when a single base-table scan sits below the projection — the shape
// clients send. A star over a join — or over a derived table whose own FROM is
// a join — is left alone: its column set is not knowable here, and guessing it
// would silently change which columns a query returns.
func ExpandStarProjections(n *Node) {
	if n == nil {
		return
	}
	for _, child := range n.Children {
		ExpandStarProjections(child)
	}
	if n.Type != NodeProject || len(n.Children) == 0 || !HasStarProjection(n) {
		return
	}

	expanded := make([]Projection, 0, len(n.Projections)+8)
	changed := false
	for _, proj := range n.Projections {
		if !isStarProjection(proj) {
			expanded = append(expanded, proj)
			continue
		}
		// A QUALIFIED star names its own relation, so it expands wherever
		// that relation's scan is — a join below does not make `o.*`
		// unknowable, only `*` (#955's rule, applied to the shape a lateral
		// produces). `SELECT o.*, s.n` was `column "o.*" does not exist in
		// the input schema` on the single-process arms and, on the DAG, a
		// column whose NAME and VALUE were both the string `*`.
		qual := starQualifier(proj)
		cols := StarSourceColumns(n.Children[0], qual)
		if len(cols) == 0 {
			expanded = append(expanded, proj)
			continue
		}
		changed = true
		for _, col := range cols {
			// A QUALIFIED star expands to QUALIFIED references. The bare name
			// binds the FIRST column of that name in the join's output, which
			// for `SELECT o.*, li.amount FROM o JOIN li` is li's `id`: the
			// star's own relation was named and the expansion has to keep
			// naming it. The published name stays the column's own, which is
			// what PostgreSQL publishes.
			ref := &plansql.ColRef{Column: col}
			expr := col
			if qual != "" {
				ref.Table = qual
				expr = qual + "." + col
			}
			expanded = append(expanded, Projection{
				Column:  expr,
				Alias:   col,
				Expr:    expr,
				ASTExpr: ref,
			})
		}
	}
	if !changed {
		return
	}
	n.Projections = expanded
}

// StarSourceColumns is the ONE list a star expands from: what the relation the
// star names PUBLISHES to the plan above it, for THIS identity. qualifier is
// "" for a bare `*` (the star's source is the single scan below it) and the
// relation's name for `alias.*`. nil means "not knowable here", and the caller
// leaves the star unexpanded, which is a refusal one pass later.
//
// PUBLISHES, not "declares in the catalog". Where an ABAC column policy applies
// the plan carries a SECURITY PROJECTION directly above the scan (#859,
// ADR-0033 decision 1): it drops every DENIED column and replaces every MASKED
// one with its mask, and it is what every consumer above the scan reads. A star
// is such a consumer. Reading the scan's catalog-annotated ScanColumns past that
// projection published a denied column's NAME to an identity the policy denies
// it to — `SELECT a.* FROM e7emp a` came back with a `salary` column on the
// embedded, pgwire and HTTP doors — and the column read NULL only because the
// name resolved to nothing above the barrier, which is an accident of the
// resolver and not the policy working.
//
// Every star spelling asks THIS function, so the answer cannot differ between
// `*` beside an item, `a.*` alone, `a.*` beside an item, a derived table's or a
// CTE's star, a star under a positional ORDER BY, or a star nested inside any of
// them: a star is its source in its position, and its source is what the plan
// below it publishes.
func StarSourceColumns(input *Node, qualifier string) []string {
	if input == nil {
		return nil
	}
	if qualifier != "" {
		return relationOutputColumns(input, qualifier)
	}
	scan, barrier := loneScan(input)
	if scan == nil {
		return nil
	}
	return publishedScanColumns(scan, barrier)
}

// publishedScanColumns is what one scan PUBLISHES: the security projection's
// column list when a column policy put one over it, and the scan's own
// catalog-annotated columns when none applies.
//
// A barrier whose own list cannot be enumerated answers nil — the star then
// stays unexpanded and the query is refused. That direction is deliberate: a
// security control never degrades to a grant, so "I could not read the policed
// list" must never fall back to the catalog's.
func publishedScanColumns(scan, barrier *Node) []string {
	if barrier != nil {
		return projectionOutputNames(barrier)
	}
	return scan.ScanColumns
}

// starQualifier is the relation a QUALIFIED star names, or "" for a bare `*`.
func starQualifier(proj Projection) string {
	e := strings.TrimSpace(proj.Expr)
	if e == "" {
		e = strings.TrimSpace(proj.Column)
	}
	if !strings.HasSuffix(e, ".*") {
		return ""
	}
	return strings.TrimSpace(strings.TrimSuffix(e, ".*"))
}

// relationOutputColumns is what the relation called alias PUBLISHES, in order,
// or nil when this pass cannot enumerate it — in which case the star stays
// unexpanded and the query stays LOUD, which is what it was before this
// expansion existed. Never a guess.
//
// The OUTPUT list, never a scan beneath a projection. A derived table, a CTE
// and a VALUES list all sit above their own scans, and expanding from the scan
// published columns the relation does not have:
// `SELECT d.*, x.id FROM (SELECT id, customer FROM lat_ord) d` came back with
// `total` as well, and a `d(a, b)` column-alias list was ignored entirely —
// loud → silently wrong.
//
// A block that names itself (`DerivedAlias`, `CTEName`) is therefore answered
// ONLY from its own projection, and where that projection was elided — the
// planner drops one whose shape matches its input — there is no list here to
// read and the answer is nil. A base-table scan is the one relation whose
// output IS its catalog schema.
//
// A decorrelated LATERAL is enumerated by nobody here: `setSubtreeAlias` puts
// its alias on its SCAN too, its own output is a projection this pass does not
// resolve, and that projection carries the correlation slot the join is about
// to drop — so `s.*` beside another item stays unexpanded and LOUD.
func relationOutputColumns(n *Node, alias string) []string {
	var found []string
	// barrier is the nearest enclosing security projection, carried down the
	// walk the way CheckPolicyPlanOrder carries it: a scan reached through one
	// publishes the barrier's list and not its own (StarSourceColumns).
	var walk func(cur, barrier *Node)
	walk = func(cur, barrier *Node) {
		if cur == nil || found != nil {
			return
		}
		if cur.LateralSubtree {
			return
		}
		if cur.Type == NodeProject && cur.SecurityBarrier {
			barrier = cur
		}
		// A block that NAMES itself answers for that name and hides what is
		// under it, whichever way the answer comes out.
		named := cur.DerivedAlias != "" || cur.CTEName != ""
		if named {
			if !strings.EqualFold(cur.DerivedAlias, alias) && !strings.EqualFold(cur.CTEName, alias) {
				return
			}
			if cur.Type == NodeProject {
				found = projectionOutputNames(cur)
			}
			return
		}
		if cur.Type == NodeScan {
			// The scan's OWN names, not the DERIVED-TABLE aliases stamped on
			// it by setSubtreeAlias: `(SELECT * FROM o JOIN li) d` stamps `d`
			// on BOTH inner scans, and expanding from one of them published
			// that TABLE's columns for a relation whose output is the join's
			// — `id,order_id,product,amount` all NULL where the block
			// publishes seven columns. A base-table scan is the only relation
			// whose output IS its catalog schema.
			if strings.EqualFold(cur.TableName, alias) || strings.EqualFold(cur.TableAlias, alias) {
				found = publishedScanColumns(cur, barrier)
				return
			}
		}
		for _, child := range cur.Children {
			walk(child, barrier)
		}
	}
	walk(n, nil)
	return found
}

// projectionOutputNames is a Project's published column list, or nil when one
// of its items is a star this pass has not expanded — a column set that is not
// knowable is not guessed at.
func projectionOutputNames(n *Node) []string {
	out := make([]string, 0, len(n.Projections))
	for _, pr := range VisibleProjections(n.Projections) {
		name := pr.PublishedName
		if name == "" {
			name = pr.Alias
		}
		if name == "" {
			name = pr.Column
		}
		if name == "" || name == "*" || strings.HasSuffix(name, ".*") {
			return nil
		}
		out = append(out, name)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// HasStarProjection reports whether node is a Project that still carries an
// unexpanded `*` or `alias.*` select item.
func HasStarProjection(n *Node) bool {
	if n == nil || n.Type != NodeProject {
		return false
	}
	for _, proj := range n.Projections {
		if isStarProjection(proj) {
			return true
		}
	}
	return false
}

// isStarProjection reports whether proj is `*` or a qualified `alias.*`.
func isStarProjection(proj Projection) bool {
	e := strings.TrimSpace(proj.Expr)
	if e == "" {
		e = strings.TrimSpace(proj.Column)
	}
	return e == "*" || strings.HasSuffix(e, ".*")
}

// loneScan returns the single scan node under n and the nearest security
// projection enclosing it, or nil when n reads from none or from more than one
// scan. The barrier travels with the scan because what the scan PUBLISHES is
// the barrier's list wherever one stands over it (StarSourceColumns).
func loneScan(n *Node) (scan, barrier *Node) {
	count := 0
	var walk func(cur, bar *Node)
	walk = func(cur, bar *Node) {
		if cur == nil {
			return
		}
		if cur.Type == NodeProject && cur.SecurityBarrier {
			bar = cur
		}
		if cur.Type == NodeScan {
			scan, barrier = cur, bar
			count++
		}
		for _, child := range cur.Children {
			walk(child, bar)
		}
	}
	walk(n, nil)
	if count != 1 {
		return nil, nil
	}
	return scan, barrier
}
