package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The BUILD side of a decorrelated subquery is the subquery's OWN PLAN
// ------------------------------------------------------------------------
//
// decorrelateExists, decorrelateInSubqueries and decorrelateScalarSubqueries
// lower a correlated subquery into a semi / anti / LEFT join whose build side
// is what the subquery reads. Each of them used to assemble that side out of
// `NewScan(info.Tables[0].Name, …)` plus one Scan per explicit JOIN, and that
// is a model of a FROM clause with three holes in it:
//
//   - a DERIVED TABLE has no name a Scan can hold. The parser keeps a
//     FROM-subquery as a table whose NAME is its own SQL text, so the build
//     side became a scan of a table the catalog has never heard of. That scan
//     does not fail — it yields zero batches, so `IN` answered nothing and
//     `NOT IN` answered every row, in silence (#571).
//   - a CTE REFERENCE has the same exposure spelled as a bare identifier
//     (#535, #581).
//   - a COMMA-JOINED inner drops every relation past the first outright.
//
// The answer up to now was to DECLINE all three (innerRelationsAreScannable),
// which is right and slow: the subquery stays a per-row predicate and the
// re-run reads the whole inner relation once per outer row — measured at
// 2N+1 reads for N outer rows against a flat 3 for the spelling that lowers
// (#852, `coordinator.TestCorrelatedRerunReadsTheInnerOncePerOuterRow`).
//
// The answer here is to BUILD it: `buildFromClause` is the builder's own FROM
// assembly, so a derived table, a CTE reference, a comma list and an explicit
// JOIN all plan exactly as they do at the top level. Two things then have to
// follow, and they are the whole of the delicacy ADR-0021 §1 is about:
//
//  1. NAMES. The build side carries the names its ROOT emits, and a derived
//     table's or a CTE's root is a Project whose columns answer to the SCOPE
//     the enclosing query gave it — `d.k`, not `d5_inner.k`. emittedColumns
//     learns that scope here (scopeOwnerOf), so repairDecorrelatedSpelling can
//     resolve a key spelled `d.k` against a subtree that emits `k`.
//  2. A COMMA inner's equalities are JOIN CONDITIONS, not filters. Built as
//     written they are condition-less cross joins with the equalities left in
//     the subquery's WHERE, where innerOnlyPredicate declines them for naming
//     two relations at once. liftWhereEquiPredsIntoJoins is the pass that
//     already fixes that shape one level up, and running it here — on an
//     ANNOTATED subtree, so it can attribute an unqualified column to its
//     relation — is what lets a comma-joined correlated inner lower at all
//     (#616). Whatever it cannot lift is DECLINED rather than left above the
//     join, because a qualified residual there names a column the join emits
//     bare, which is a wrong answer and not an error.

// decorrelatedInnerPlan builds a decorrelated subquery's build side: its FROM
// clause as a plan, with its inner-only WHERE conditions applied above it.
//
// annotate, when non-nil, is Optimize's catalog annotator. It runs on the
// freshly built subtree so the comma lift can attribute an UNQUALIFIED column
// to the relation that owns it — TPC-H Q2's official comma spelling writes
// every key that way.
//
// ok=false means DECLINE the whole rewrite: the subquery stays a predicate,
// which is right and slow, and never a build side this pass cannot name.
func decorrelatedInnerPlan(info *plansql.SelectInfo, innerOnly []plansql.Node,
	ctes []plansql.CTEDef, annotate func(*Node)) (*Node, bool) {
	if info == nil || len(info.Tables) == 0 {
		return nil, false
	}
	if !innerRelationsAreBuildable(info, ctes) {
		return nil, false
	}
	// A derived table or a CTE reference JOINED to another relation declines.
	//
	// The build side then carries TWO renamings: the join's own (probe bare,
	// build qualified where the bare name collides, decided by reorderJoins)
	// and the derived arm's Project, whose published name — `k` for
	// `SELECT c.n AS k` — is a name no scan below it produces. The logical
	// model tracks both, and the single-process arm answers correctly; the
	// stage DAG's carried-column derivation does not, and answers a DIFFERENT
	// number rather than failing:
	//
	//	SELECT COUNT(*) FROM nation a WHERE a.n_nationkey IN (
	//	  SELECT s.k FROM (SELECT c.n_nationkey AS k, c.n_regionkey AS rk
	//	                     FROM nation c) s
	//	  JOIN nation b ON b.n_regionkey = s.rk WHERE s.k < 3)
	//	-- PostgreSQL 17 and the single-process arm: 3.  Stage DAG: 10.
	//
	// Declining leaves it a subquery predicate, which both arms answer. The
	// spelling that puts the derived arm on the PROBE happens to agree today,
	// and that is the reason to decline BOTH rather than the shape that was
	// caught: which arm the estimator puts where is `reorderJoins`' decision
	// from row counts, so a cut drawn there would move under the fixture.
	// This is the same boundary ADR-0021 §1 draws for the key SPELLING, one
	// layer out; closing it is the stage model's carried columns, not this
	// rewrite's (report deferral).
	if len(info.Tables)+len(info.Joins) > 1 && fromHasDerivedOrCTE(info, ctes) {
		return nil, false
	}

	// buildFromClause reads the same info the caller has already classified,
	// and the LATERAL arm REWRITES it (the empty-input default, ADR-0021
	// §1h). A classification made before that rewrite is stale, so a LATERAL
	// inside the subquery's own FROM declines here rather than lowering
	// against a WHERE clause that has since changed.
	for _, j := range info.Joins {
		if j.Lateral {
			return nil, false
		}
	}

	plan, err := buildFromClause(info, ctes)
	if err != nil || plan == nil {
		return nil, false
	}

	// A pure comma FROM is a chain of condition-less cross joins, and its
	// equalities belong on those joins. Only there is a two-relation
	// inner-only condition kept qualified; anywhere else it declines, for
	// innerOnlyPredicate's reason.
	commaChain := len(info.Joins) == 0 && len(info.Tables) > 1
	joinedInner := len(info.Joins) > 0 || len(info.Tables) > 1

	var preds []Predicate
	lifted := map[string]bool{}
	for _, node := range innerOnly {
		if commaChain && predicateNamesTwoRelations(node) {
			raw := node.String()
			preds = append(preds, Predicate{Raw: raw, ASTExpr: node})
			lifted[raw] = true
			continue
		}
		pred, ok := innerOnlyPredicate(node, joinedInner)
		if !ok {
			return nil, false
		}
		preds = append(preds, pred)
	}
	if len(preds) > 0 {
		plan = NewFilter(plan, preds)
	}
	if len(lifted) > 0 {
		// Annotate HERE and only here. The lift is the one reader that needs
		// the scans' own column lists this early — it attributes an
		// unqualified column to its relation from them. Annotating every
		// inner subtree instead would hand reorderJoins statistics it did not
		// have before this change, which moves the join order of plans that
		// have nothing to do with a comma inner (TPC-H Q2's explicit-JOIN
		// spelling was the one that showed it).
		if annotate != nil {
			annotate(plan)
		}
		plan = liftWhereEquiPredsIntoJoins(plan)
		if !everyLiftedPredicateLanded(plan, lifted) {
			// The lift could not attribute one of the equalities to two
			// relations of the chain. Leaving it above the cross join would
			// ask for `b.g` from a join that emits `g`, which resolves by
			// stripping the qualifier to whichever relation the reorderer put
			// on the probe — the silent wrong answer ADR-0021 §1 exists for.
			return nil, false
		}
	}
	return plan, true
}

// predicateNamesTwoRelations reports whether node is a bare column equality
// whose two sides carry DIFFERENT qualifiers, or carry none — the shape
// liftWhereEquiPredsIntoJoins turns into a join condition. Anything else over
// a comma chain takes innerOnlyPredicate's ordinary path.
func predicateNamesTwoRelations(node plansql.Node) bool {
	cmp, ok := node.(*plansql.CmpExpr)
	if !ok || cmp.Op != "=" {
		return false
	}
	lq, lc := colRefParts(cmp.Left)
	rq, rc := colRefParts(cmp.Right)
	if lc == "" || rc == "" {
		return false
	}
	if lq != "" && rq != "" {
		return !strings.EqualFold(lq, rq)
	}
	// At least one side unqualified: only the annotated chain can say which
	// relation owns it, and that is exactly what the lift decides.
	return true
}

// everyLiftedPredicateLanded reports whether the comma lift moved every
// predicate in want onto a join condition — i.e. none of them is still
// sitting in a Filter.
func everyLiftedPredicateLanded(n *Node, want map[string]bool) bool {
	if n == nil {
		return true
	}
	if n.Type == NodeFilter {
		for _, p := range n.Predicates {
			if want[p.Raw] {
				return false
			}
		}
	}
	for _, c := range n.Children {
		if !everyLiftedPredicateLanded(c, want) {
			return false
		}
	}
	return true
}

// innerRelationsAreBuildable reports whether the decorrelations can turn every
// relation the subquery's FROM/JOIN list names into a plan.
//
// It is what is left of innerRelationsAreScannable, and the list is one entry
// long: a RECURSIVE CTE. Everything else — a derived table, an ordinary CTE
// reference, a comma list — is now built by buildFromClause and needs no
// decline (#852).
//
// A recursive CTE reference is a TAGGED SCAN the physical planner resolves by
// fixed-point iteration from its own cache (materializeRecursiveCTE), and the
// caches a semi-join build side is prepared through are not that one. So it
// stays declined here, exactly as before, and the materialized-IN route
// refuses it too rather than reading its cacheless set as empty
// (physical/in_subquery_set.go, #F1).
func innerRelationsAreBuildable(info *plansql.SelectInfo, ctes []plansql.CTEDef) bool {
	recursive := map[string]bool{}
	for _, c := range ctes {
		if c.Recursive {
			recursive[strings.ToLower(c.Name)] = true
		}
	}
	for _, c := range info.CTEs {
		if c.Recursive {
			recursive[strings.ToLower(c.Name)] = true
		}
	}
	if len(recursive) == 0 {
		return true
	}
	return !fromReadsAny(info, recursive, 0)
}

// fromHasDerivedOrCTE reports whether any FROM/JOIN item is a derived table or
// a CTE reference — the two relations whose subtree root RENAMES, and so the
// two that add a second naming layer under a join.
func fromHasDerivedOrCTE(info *plansql.SelectInfo, ctes []plansql.CTEDef) bool {
	cteName := make(map[string]bool, len(ctes)+len(info.CTEs))
	for _, c := range ctes {
		cteName[strings.ToLower(c.Name)] = true
	}
	for _, c := range info.CTEs {
		cteName[strings.ToLower(c.Name)] = true
	}
	renames := func(name string) bool {
		name = strings.TrimSpace(name)
		return strings.HasPrefix(name, "(") || cteName[strings.ToLower(name)]
	}
	for _, t := range info.Tables {
		if renames(t.Name) {
			return true
		}
	}
	for _, j := range info.Joins {
		if renames(j.RightTable) {
			return true
		}
	}
	return false
}

// fromReadsAny reports whether any relation the FROM/JOIN list names — at any
// derived-table nesting — is in names.
func fromReadsAny(info *plansql.SelectInfo, names map[string]bool, depth int) bool {
	if info == nil || depth > 8 {
		return depth > 8 // too deep to prove otherwise: treat as a hit
	}
	reads := func(name string) bool {
		name = strings.TrimSpace(name)
		if strings.HasPrefix(name, "(") {
			parsed, err := plansql.Parse(name[1 : len(name)-1])
			if err != nil {
				return true
			}
			sub, err := plansql.ExtractSelect(parsed)
			if err != nil {
				return true
			}
			return fromReadsAny(sub, names, depth+1)
		}
		return names[strings.ToLower(name)]
	}
	for _, t := range info.Tables {
		if reads(t.Name) {
			return true
		}
	}
	for _, j := range info.Joins {
		if reads(j.RightTable) {
			return true
		}
	}
	return false
}

// scopeOwnerOf returns the name the ENCLOSING query calls a subtree by, or ""
// when the subtree is not a named scope.
//
// A derived table's alias is stamped on the scans below it AND recorded on the
// subtree root (DerivedAlias); a CTE's name is recorded on the root only, and
// deliberately — stamping it would make two relations comma-joined inside the
// body share one identity (ADR-0021 §1d). Every reader has to know both, and
// emittedColumns is one of them: a key spelled `d.k` over a derived subtree
// that emits `k` resolves only if the model knows `d` owns that column.
func scopeOwnerOf(n *Node) string {
	if n == nil {
		return ""
	}
	if n.DerivedAlias != "" {
		return n.DerivedAlias
	}
	if n.CTERefAlias != "" {
		return n.CTERefAlias
	}
	return n.CTEName
}
