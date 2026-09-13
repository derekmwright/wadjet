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
			// naming it.
			//
			// The reference is spelled with the relation's RESOLUTION name and
			// the item carries its PUBLISHED one — ADR-0026 §2's pair, in the
			// direction a star needs. An UNALIASED item publishes `?column?`,
			// which is a rendering and not a handle: the block emits that
			// column under its own expression text, so a star that referenced
			// `?column?` named nothing and read NULL under a STRING
			// declaration where PostgreSQL answers the value (#1077).
			ref := &plansql.ColRef{Column: col.Resolve}
			expr := col.Resolve
			if qual != "" {
				ref.Table = qual
				expr = qual + "." + col.Resolve
			}
			item := Projection{
				Column:  expr,
				Alias:   col.Resolve,
				Expr:    expr,
				ASTExpr: ref,
			}
			if !strings.EqualFold(col.Publish, col.Resolve) {
				item.PublishedName = col.Publish
			}
			expanded = append(expanded, item)
		}
	}
	if !changed {
		return
	}
	n.Projections = expanded
}

// StarSourceColumns is the ONE list every star spelling asks: this identity's
// PUBLISHED relation output, in the star's position, never a catalog bypass.
// qualifier is empty for bare star over a single scan, or the relation name for alias.*;
// nil means unknown: leave unexpanded for refusal in the next pass.
// An ABAC security projection drops DENIED columns and replaces MASKED values
// (#859, ADR-0033 decision 1); stars must read that projection, including column NAMES.
// This applies beside items, alone, through derived/CTE scopes, under positional
// ORDER BY and nested combinations alike.
// See docs/internals/star-source-policy-publication.md for the design.
func StarSourceColumns(input *Node, qualifier string) []StarColumn {
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

// StarColumn is one column of a star's source relation, under both of the
// names ADR-0026 §2 gives a column: Resolve is what the relation's producer
// EMITS and what a reference above must be spelled with, Publish is what the
// client is told. They differ for exactly one class of select item — an
// unaliased expression, which every engine emits under some spelling of its
// own and PostgreSQL publishes as `?column?`.
type StarColumn struct {
	Resolve string
	Publish string
}

// starColumnsOf pairs each name with itself, for a relation whose producer
// emits what it publishes: a base-table scan, and a security projection over
// one.
func starColumnsOf(names []string) []StarColumn {
	if len(names) == 0 {
		return nil
	}
	out := make([]StarColumn, len(names))
	for i, n := range names {
		out[i] = StarColumn{Resolve: n, Publish: n}
	}
	return out
}

// publishedScanColumns is what one scan PUBLISHES: the security projection's
// column list when a column policy put one over it, and the scan's own
// catalog-annotated columns when none applies.
//
// A barrier whose own list cannot be enumerated answers nil — the star then
// stays unexpanded and the query is refused. That direction is deliberate: a
// security control never degrades to a grant, so "I could not read the policed
// list" must never fall back to the catalog's.
func publishedScanColumns(scan, barrier *Node) []StarColumn {
	if barrier != nil {
		return projectionOutputNames(barrier)
	}
	return starColumnsOf(scan.ScanColumns)
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

// relationOutputColumns returns alias's PUBLISHED outputs in order, or nil:
// leave unknown stars unexpanded and LOUD, never guess from a scan beneath a Project.
// DerivedAlias/CTEName roots answer ONLY from their OWN projection; if elided,
// there is no list here and the answer is nil. Respect positional column aliases.
// Base scans publish their schema subject to the enclosing security projection.
// Do not enumerate decorrelated LATERAL: its alias also marks the scan, but its
// projection carries the correlation slot the join will drop; s.* beside an item
// therefore remains unexpanded and LOUD.
// See docs/internals/qualified-star-relation-output-list.md for the design.
func relationOutputColumns(n *Node, alias string) []StarColumn {
	var found []StarColumn
	// hidden is every slot a JOIN on the path above MINTED for itself and
	// drops from its own output (ADR-0026 §3c). A decorrelated LATERAL's
	// correlation key is an ordinary select item of the block's list, so the
	// block's published relation is that list MINUS the slots the join owes.
	hidden := map[string]bool{}
	// barrier is the nearest enclosing security projection, carried down the
	// walk the way CheckPolicyPlanOrder carries it: a scan reached through one
	// publishes the barrier's list and not its own (StarSourceColumns).
	var walk func(cur, barrier *Node)
	walk = func(cur, barrier *Node) {
		if cur == nil || found != nil {
			return
		}
		if cur.Type == NodeJoin {
			for _, h := range cur.HiddenJoinCols {
				hidden[strings.ToLower(bareColumn(strings.TrimSpace(h)))] = true
			}
		}
		if cur.LateralSubtree {
			// A decorrelated LATERAL is a relation the enclosing query NAMES,
			// and its published list is its projection minus the correlation
			// slot the join above minted and drops (ADR-0026 §3c). The alias
			// is recorded on the SCANS below it (setSubtreeAlias), not on the
			// subtree root, so that is where the name is looked for; the
			// scan's own columns are never the answer here, because what this
			// relation publishes is the body's SELECT list.
			if subtreeScanAnswersTo(cur, alias) {
				if proj := blockOutputProjection(cur); proj != nil {
					found = starColumnsWithout(projectionOutputNames(proj), hidden)
				}
			}
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
			// The block's own SORT, LIMIT or DISTINCT stands between its root
			// and its projection, and none of them changes a column or its
			// position. Stopping at the root answered nil for every block that
			// carries one, and the query was refused where PostgreSQL answers
			// it — `SELECT x.* FROM (SELECT order_id, product FROM lat_item
			// ORDER BY product) x`.
			if proj := blockOutputProjection(cur); proj != nil {
				found = starColumnsWithout(projectionOutputNames(proj), hidden)
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

// subtreeScanAnswersTo reports whether a scan inside this subtree carries
// alias — the association setSubtreeAlias records for a derived table and for
// a LATERAL body alike.
func subtreeScanAnswersTo(n *Node, alias string) bool {
	if n == nil || alias == "" {
		return false
	}
	if n.Type == NodeScan {
		if strings.EqualFold(n.TableAlias, alias) || strings.EqualFold(n.TableName, alias) {
			return true
		}
		for _, d := range n.DerivedAliases {
			if strings.EqualFold(d, alias) {
				return true
			}
		}
		return false
	}
	for _, c := range n.Children {
		if subtreeScanAnswersTo(c, alias) {
			return true
		}
	}
	return false
}

// starColumnsWithout drops the slots a join above MINTED and will remove from
// its own output: they are the planner's own, no query can spell them, and a
// star must not publish one.
func starColumnsWithout(cols []StarColumn, hidden map[string]bool) []StarColumn {
	if len(cols) == 0 || len(hidden) == 0 {
		return cols
	}
	out := make([]StarColumn, 0, len(cols))
	for _, c := range cols {
		if hidden[strings.ToLower(bareColumn(c.Resolve))] {
			continue
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// projectionOutputNames is a Project's column list under BOTH names, or nil
// when one of its items is a star this pass has not expanded — a column set
// that is not knowable is not guessed at.
func projectionOutputNames(n *Node) []StarColumn {
	out := make([]StarColumn, 0, len(n.Projections))
	for _, pr := range VisibleProjections(n.Projections) {
		resolve := pr.Alias
		if resolve == "" {
			resolve = pr.Column
		}
		publish := pr.PublishedName
		if publish == "" {
			publish = resolve
		}
		if publish != "" && resolve == "" {
			// An item the parser NAMED but that carries no alias and no
			// column reference is an unaliased expression, and the column the
			// Project emits for it is its expression text — the rule
			// physical.projectionOutputName states, read from the other end.
			// The two names are what ADR-0026 §2 calls a resolution spelling
			// and a published name, and the one to REFERENCE is the first.
			resolve = cleanExpr(pr.Expr)
		}
		// A projection with no name of any kind is one this pass cannot
		// enumerate, and the answer is nil rather than a guess: a SECURITY
		// barrier is a Project too, and a control that cannot state its list
		// must never degrade to the catalog's (publishedScanColumns).
		if resolve == "" || resolve == "*" || strings.HasSuffix(resolve, ".*") {
			return nil
		}
		if publish == "" || publish == "*" || strings.HasSuffix(publish, ".*") {
			return nil
		}
		out = append(out, StarColumn{Resolve: resolve, Publish: publish})
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
