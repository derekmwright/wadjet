// SPDX-License-Identifier: MIT

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
// A star's source columns come from the relation's published list: the scan's
// catalog annotation or a derived block's visible projection (ADR-0026 §9).
// A star over a join expands the FROM clause's arms in written order, before
// join reordering. Each item carries its resolution and publication names;
// only a shape whose column list cannot be stated is left unexpanded, since
// guessing it would silently change which columns a query returns.
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
		// that relation's list is — a join below does not make `o.*`
		// unknowable (ADR-0026 §9; #955, applied to the shape a lateral
		// produces). `SELECT o.*, s.n` was `column "o.*" does not exist in
		// the input schema` on the single-process arms and, on the DAG, a
		// column whose NAME and VALUE were both the string `*`.
		qual := starQualifier(proj)
		// A BARE star over a JOIN is the FROM clause's arms in WRITTEN order
		// (star_join_order.go, #997/#1012): every arm's own list, each item
		// qualified so it binds its own relation whichever side the plan
		// builds, published under the column's own name. It is asked before
		// the single-source list below because that list answers only for a
		// lone scan, and a star that reaches a join otherwise reads the join
		// operator's stream — which is the PLAN's order and the PLAN's
		// qualification.
		if qual == "" {
			if items := joinStarColumns(n.Children[0]); len(items) > 0 {
				changed = true
				for _, it := range items {
					expanded = append(expanded, starItemProjection(it.qualifier, it.column))
				}
				continue
			}
		}
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

// relationOutputColumns returns alias's PUBLISHED outputs in order, under both
// of the names ADR-0026 §2 gives a column, or nil: leave a star this pass
// cannot state unexpanded and LOUD, never guess from a scan beneath a Project.
//
// A DerivedAlias/CTEName root answers from its OWN projection, reached through
// the operators that change neither a column nor its position — its own Sort,
// Limit and Distinct (blockOutputProjection). Where the projection was elided
// there is no list here and the answer is nil. Positional column aliases are
// respected; base scans publish their schema subject to the enclosing security
// projection.
//
// A DECORRELATED LATERAL is enumerated TOO, and its published list is its
// projection MINUS the slots the join above minted — `Node.HiddenJoinCols`,
// the same identity the join's own drop uses (ADR-0026 §3c). It was excluded
// until arc O2, because the correlation slot the lowering injects is an
// ordinary select item of that projection and this pass could not tell it from
// a user's own.
//
// TWO published columns of ONE resolution name answer nil: the expansion emits
// one reference per column and two spelled alike both bind the first, which is
// a wrong VALUE (projectionOutputNames).
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
			// …and the slots a LIFTED correlated predicate is evaluated
			// against, which the join EMITS (the predicate may run above it)
			// and no star may publish.
			for _, h := range cur.StarLiftedRefCols {
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
			if cur.LateralBoundNotPerRow {
				// A body whose own bound the decorrelation cannot apply per
				// outer row is not a relation this pass can state: the
				// columns are knowable but the ROW COUNT is not the one the
				// query wrote (#1019, Node.LateralBoundNotPerRow). The star
				// stays unexpanded and the query is refused, which is the
				// disposition this spelling had before the reach fix reached
				// it; the named spellings keep theirs, pinned.
				return
			}
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
				return
			}
			// A RECURSIVE CTE REFERENCE IS A NAMED BLOCK WITH NO PROJECTION
			// UNDER IT: it is a tagged Scan the physical planner resolves from
			// its own cache (ADR-0021 §1b, §1o), so blockOutputProjection
			// finds nothing and the star stayed unexpanded — `SELECT r.* FROM
			// r` was refused where PostgreSQL 17.11 answers, and where the
			// BARE star over the same relation already answered. The list it
			// publishes is recorded on the reference itself (§1p,
			// recursiveCTEColumns), and the star is one more consumer of it.
			// The list still comes from publishedScanColumns and nowhere else,
			// which asks the security barrier first
			// (TestOnlyOnePathReadsAScanColumnListForAStar).
			if cur.Type == NodeScan {
				if cols := publishedScanColumns(cur, barrier); len(cols) > 0 {
					found = starColumnsWithout(cols, hidden)
				}
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
	// TWO ITEMS OF ONE RESOLUTION NAME cannot be enumerated by NAME. The
	// expansion emits one reference per published column, and two references
	// spelled alike both bind the FIRST column of that name
	// (`batch.RecordBatch.ColumnIndex`), so `SELECT x.* FROM (SELECT a.id,
	// b.id FROM lat_item a JOIN lat_item b …) x` published the first `id`
	// TWICE where PostgreSQL publishes the pair — a wrong VALUE, and a wrong
	// TYPE where the two items differ in type (`order_id AS k, amount AS k`).
	//
	// A list this pass cannot state is answered nil, which is the direction
	// the loop above already takes for an item with no name at all: the star
	// stays unexpanded and the query is REFUSED. Closing it properly means the
	// block's published list travelling by POSITION rather than by name —
	// `ProjectExprSpec.SourceSlot` one relation out — and until it does, loud
	// beats a plausible wrong value (#1076).
	// A PENDING COLUMN-ALIAS LIST is part of what this block publishes.
	//
	// `ApplyDeferredColumnAliases` renames the leading items of a star Project
	// carrying one, and it runs AFTER this expansion — so a star ABOVE such a
	// block read the names the block had BEFORE the rename and referenced
	// columns that were about to stop existing. `SELECT b.* FROM zzp b(k, v)`
	// published `id, d92` and answered NULL for both, and a bare star over a
	// join whose arm carried a list did the same for that arm (#959, #1158).
	// Overlaying it here is what makes the two passes agree; once the rename
	// has run the field is cleared, so it is applied exactly once.
	//
	// An OVERLONG list states nothing: the width is now known and is smaller
	// than the list, which is PostgreSQL's 42P10 and
	// `RefuseUnappliedColumnAliasLists`' to raise.
	if len(n.DeferredColumnAliases) > 0 {
		if len(n.DeferredColumnAliases) > len(out) {
			return nil
		}
		for i, name := range n.DeferredColumnAliases {
			out[i] = StarColumn{Resolve: name, Publish: name}
		}
	}
	seen := make(map[string]bool, len(out))
	for _, c := range out {
		k := strings.ToLower(c.Resolve)
		if seen[k] {
			return nil
		}
		seen[k] = true
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
