// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// resolveShuffleKey follows Project aliases to the source columns ordinary
// DAG Projects leave unchanged. Recurse into output-visible join children:
// both sides for inner/outer, probe only for semi/anti; first resolution wins.
// Use physical.DerivedScopeBareName to drop qualifiers only in their owning scope
// (#467, #480). Follow chained renames, substituting at most once per Project:
// a projection list is simultaneous (b AS a, a AS b must not chase itself).
// The walk only descends, so it terminates.
func resolveShuffleKey(key string, child *logical.Node, published map[*logical.Node]bool) string {
	if child == nil {
		return key
	}
	resolved := key
	for n := child; n != nil; {
		if n.Type == logical.NodeProject {
			bare := physical.DerivedScopeBareName(resolved, n)
			proj := physical.ProjectionForName(n.Projections, resolved, bare)
			// A key already spelled BARE names the same column; the scope
			// stripper answers "" for it because there is no qualifier to
			// strip, not because the name is unknown. Without this the stop
			// below is reachable only from a QUALIFIED spelling, and a
			// projection that republishes the slot under its own name
			// (logical.dropBlockHiddenSlots, #991) makes the key bare one
			// operator higher — after which the walk chased the minted key's
			// SOURCE column and the shuffle refused it (`key "order_id" not
			// in schema`).
			stop := bare
			if stop == "" && proj != nil && !strings.Contains(resolved, ".") {
				stop = resolved
			}
			if proj != nil && stop != "" && (published[n] || projectsAMintedGroupKey(n, stop)) {
				// The aggregate below PUBLISHES this name (a hidden
				// correlation slot, ADR-0026 3a): the stage emits it under
				// exactly this name and nothing below carries it, so the walk
				// stops here. Chasing proj.Column would hand the shuffle the
				// key's SOURCE column, which the aggregate stage does not
				// emit -- measured as `partitioned shuffle: key "g" not in
				// schema` and, where the join still built, zero matched rows.
				return stop
			}
			switch {
			case proj != nil && proj.Column != "" &&
				!strings.EqualFold(projKeySpelling(proj, n), resolved):
				// THE SPELLING THE PRODUCING STREAM CARRIES, which is the
				// bare source name except where the block holds two relations
				// answering to it — see projKeySpelling (#1099).
				resolved = projKeySpelling(proj, n)
			case proj != nil && proj.Column == "" && bare != "" && !strings.EqualFold(bare, resolved):
				// A COMPUTED output (`COUNT(*) + 1 AS k`) has no source
				// column to chase. It exists on the DAG only under its own
				// alias — absorbAggregateOutputProjection materializes it on
				// the producing stage — so the key is the BARE alias, and
				// the walk stops: nothing below this Project carries it.
				// Before, the qualified spelling travelled to the worker and
				// the shuffle refused it (`key "b.k" not in schema`, #681).
				return bare
			}
		}
		if n.Type == logical.NodeJoin && len(n.Children) == 2 {
			if r := resolveShuffleKey(resolved, n.Children[0], published); r != resolved {
				return r
			}
			jt := strings.ToLower(n.JoinType)
			if jt != "semi" && jt != "anti" {
				return resolveShuffleKey(resolved, n.Children[1], published)
			}
			return resolved
		}
		// Continue down to single-child nodes (Filter, Sort, Limit, Project, Aggregate)
		if len(n.Children) == 1 {
			n = n.Children[0]
		} else {
			break
		}
	}
	return resolved
}

// projectsAMintedGroupKey reports whether the aggregate below a Project
// publishes `name` as one of its group keys' MINTED slots -- the reverse of an
// ordinary key, whose published name is a column of the aggregate's input.
//
// A minted key exists on the DAG only under that slot, exactly as a computed
// output exists only under its own alias, so a name walk that reaches one has
// arrived rather than having something further to chase.
func projectsAMintedGroupKey(project *logical.Node, name string) bool {
	if project == nil || len(project.Children) == 0 {
		return false
	}
	// A Project that REPUBLISHES this name under itself is transparent to the
	// question: the block re-projects to its visible list above its own sort
	// (logical.dropBlockHiddenSlots, #991), and the aggregate that minted the
	// slot then sits one Project further down. Stopping at the first Project
	// answered "not minted" for a key that is, and the caller chased it to the
	// key's SOURCE column — `partitioned shuffle: key "order_id" not in
	// schema` over a grouped LATERAL with an `ORDER BY` of its own.
	for child := project.Children[0]; child != nil; {
		if agg := physical.FindAggregateAncestor(child); agg != nil {
			for i := range agg.GroupBy {
				if i < len(agg.GroupByPublish) && agg.GroupByPublish[i] != "" &&
					strings.EqualFold(agg.GroupByPublish[i], name) {
					return true
				}
			}
			return false
		}
		child = passThroughProjectFor(child, name)
	}
	return false
}

// passThroughProjectFor is the input of the first Project at or below n that
// publishes name UNCHANGED — the same name in and out — or nil when the next
// operator down is one this question cannot see through.
func passThroughProjectFor(n *logical.Node, name string) *logical.Node {
	for cur := n; cur != nil && len(cur.Children) == 1; cur = cur.Children[0] {
		if cur.Type != logical.NodeProject {
			if !physical.AggScopePreservingWrapper(cur.Type) {
				return nil
			}
			continue
		}
		for _, pr := range cur.Projections {
			if !strings.EqualFold(pr.Alias, name) {
				continue
			}
			if pr.Column == "" || strings.EqualFold(pr.Column, name) {
				return cur.Children[0]
			}
			return nil
		}
		return nil
	}
	return nil
}

// aggStageGroupKey reports the name the aggregate STAGE will emit for a
// logical GROUP BY key, and whether that differs from the key as written.
//
// A key naming a subquery's rename is dispatched under the source column; a
// key naming a subquery's computed alias is dispatched under the EXPRESSION
// TEXT, which is the spelling the worker's pre-aggregate projection already
// compiles and emits (buildAggInputProjection treats a group-by entry that
// does not parse as a bare column reference as derived). Both walkStages and
// the sort's aggregateOutputName have to agree on it, so both call this.
func aggStageGroupKey(key string, e plansql.Node, child *logical.Node) (string, bool) {
	// Only a term that IS a column reference may be resolved as one. The
	// recorded key text cannot say which: `GROUP BY "g + 1"` names a column
	// and `GROUP BY g + 1` is arithmetic, and both are recorded as `g + 1`
	// because a delimited identifier's quotes are not part of its name
	// (#725). Asked of the TEXT, the arithmetic key found a delimited column
	// spelled the same way and bound to it — PostgreSQL answered five groups
	// of the sum and both DAG arms answered nine groups of the column,
	// silently. PostgreSQL's rule is that unquoted `g + 1` is arithmetic,
	// full stop.
	if e != nil {
		if _, bare := e.(*plansql.ColRef); !bare {
			return key, false
		}
	}
	resolved, expr, _, renamed := physical.ResolveAggInputName(key, child)
	if !renamed {
		return key, false
	}
	if expr != nil {
		// The alias names an EXPRESSION. There are two answers to "what is this
		// value called on the DAG" — the expression, or the name the producing
		// fragment materialized it under — and which one is right is a question
		// about a STAGE, not about the logical plan. It cannot be answered here:
		// walkStages runs BEFORE attachScanSelectProjections and
		// absorbWindowArmProjection, the passes that decide whether any fragment
		// materializes the alias at all.
		//
		// Round 1 of this arc inferred the answer from NODE KINDS and got it
		// wrong three times in three different directions (#777's history is in
		// ADR-0026 §4a). A key whose value no stage can name is REFUSED and
		// routed instead — refuseUnstageableGroupKey condition (3).
		return expr.String(), true
	}
	return resolved, true
}

// aggStageDispatchKey is aggStageGroupKey for the DISPATCHED spelling: the
// text the worker parses and computes the key from.
//
// It answers for a DERIVED key too, which aggStageGroupKey deliberately does
// not. `a_b + 1` is not a name, so physical.ResolveAggInputName declines it — but its
// LEAVES are names, and a rename Project between the aggregate and its scan
// emits no stage of its own, so the key reached the worker spelled over `a_b`,
// which the scan does not emit: the key computed NULL for every row and the
// whole table collapsed into ONE group.
//
// aggregateOutputName deliberately does NOT take this path. It answers what a
// SORT KEY should name, and a sort key is resolved on both engines — the
// single-process aggregate publishes the key under its own canonical text and
// knows nothing of the DAG's source re-spelling.
func aggStageDispatchKey(key string, e plansql.Node, child *logical.Node) (string, bool) {
	if resolved, renamed := aggStageGroupKey(key, e, child); renamed {
		return resolved, true
	}
	return physical.AggStageDerivedKey(key, child)
}

// resolveSortKeyColumn maps ORDER BY aliases to an aggregate's emitted
// GroupByCols/AggSpec.OutputCol names (#313). Ordinary DAG Projects do not
// perform the rename. Scope this to aggregates: scan/join producers may get
// an alias-naming OpProject later via attachScanSelectProjections, which
// declines aggregate plans. Each Project substitutes at most once because
// its projection list is simultaneous.
func resolveSortKeyColumn(key string, child *logical.Node) string {
	// resolved is the preferred candidate, alt a second one to try when the
	// first names no output of the aggregate below.
	resolved, alt := key, ""
	for n := child; n != nil; {
		switch n.Type {
		case logical.NodeProject:
			for _, proj := range n.Projections {
				if !strings.EqualFold(proj.Alias, resolved) &&
					(alt == "" || !strings.EqualFold(proj.Alias, alt)) {
					continue
				}
				// Expression text first, because it keeps the table
				// qualifier: a self-joined table gives both aliases the same
				// bare column name, so `n1.n_name AS supp_nation` carries
				// column "n_name" — which matches neither group key and
				// cannot, since n2 shares it. Only "n1.n_name" identifies
				// which alias, and a GROUP BY that spells its key bare is
				// covered by the Column fallback below (#314/#313).
				resolved, alt = proj.Expr, proj.Column
				if resolved == "" {
					resolved, alt = proj.Column, ""
				}
				if alt == resolved {
					alt = ""
				}
				break
			}
		case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
			// Order-preserving passthroughs: keep descending.
		case logical.NodeAggregate:
			if out, ok := aggregateOutputName(n, resolved); ok {
				return out
			}
			if alt != "" {
				if out, ok := aggregateOutputName(n, alt); ok {
					return out
				}
			}
			return key
		default:
			return key
		}
		if len(n.Children) != 1 {
			return key
		}
		n = n.Children[0]
	}
	return key
}

// aggregateOutputName reports the name an Aggregate node emits for col — its
// own spelling of the matching group key or aggregate output — so a sort key
// resolved through a rename lands on a column the stage really produces.
//
// A group key is reported the way the aggregate's fragment will EMIT it —
// `physical.StageEmittedKeyNames` over the key's PUBLISHED name — which is one answer
// for both engines. It used to be the DISPATCH re-spelling
// (`aggStageGroupKey`), because a stage published its keys under the spelling
// the worker computed them from; now the two names are separate fields and the
// published one is what any consumer above the aggregate reads (ADR-0026 §2b,
// #355 and #794).
func aggregateOutputName(n *logical.Node, col string) (string, bool) {
	var child *logical.Node
	if len(n.Children) == 1 {
		child = n.Children[0]
	}
	published, resolve := physical.StageGroupKeyNames(n, child)
	names := physical.StageEmittedKeyNames(published, resolve, physical.LogicalAggOutNames(n))
	emit := func(i int, g string) (string, bool) {
		if i >= 0 && i < len(names) {
			return names[i], true
		}
		return g, true
	}
	for i, g := range n.GroupBy {
		if strings.EqualFold(g, col) {
			return emit(i, g)
		}
	}
	// The two spellings of one key: `GROUP BY u.k` names the same output as
	// the `k` the SELECT list and the ORDER BY use, and either side may be
	// the qualified one. Both are dropped to their bare form only inside the
	// derived scope that owns the qualifier — see physical.DerivedScopeBareName
	// (#467). A bare spelling that matches TWO group keys is a self-join's
	// `n1.n_name`/`n2.n_name`: naming one of them would order by an
	// arbitrary side, so the key is left for the caller to give up on, the
	// same call lookupEmittedColumn makes on the same ambiguity.
	bare := func(name string) string {
		if b := physical.DerivedScopeBareName(name, child); b != "" {
			return b
		}
		return name
	}
	cb := bare(col)
	match, matchIdx, count := "", -1, 0
	for i, g := range n.GroupBy {
		if strings.EqualFold(bare(g), cb) {
			match, matchIdx, count = g, i, count+1
		}
	}
	if count == 1 {
		return emit(matchIdx, match)
	}
	for _, a := range n.AggExprs {
		if strings.EqualFold(a.OutputCol, col) {
			return a.OutputCol, true
		}
	}
	return "", false
}

// projKeySpelling is the name a KEY into this block's item reaches the
// producing stream under.
//
// A plain rename publishes its SOURCE column, because an ordinary Project
// emits no stage — and the source is spelled bare, because that is how the
// stream carries it. With ONE exception, and it is the one #1099 is: where the
// block's body is itself a JOIN and BOTH of its relations answer to that bare
// name, the join qualifies the build's copy by its own alias, so the stream
// carries `o2.id` and `id` side by side and only the qualified spelling names
// one of them. Chasing the bare name there let the OUTER join key on
// `lat_item`'s id instead of `lat_ord`'s:
//
//	SELECT DISTINCT o.id, o.customer, s.c, s.k FROM lat_ord o JOIN
//	  (SELECT o2.customer AS c, o2.id AS k FROM lat_item i2
//	   JOIN lat_ord o2 ON o2.id = i2.order_id) s ON s.k = o.id
//	-- PostgreSQL 17.11: two rows; the three DAG arms: three, two of them
//	-- pairing rows where s.k <> o.id
//
// The count is the test and not the qualifier's presence, because a qualifier
// the stream does not carry is not free: every binding resolves it
// (`exec.ColumnIndexFallback` strips one on a miss), but
// `exec.HashJoin.FixKeyAssignment` asks whether a key is "in the build schema"
// with an EXACT-name map, reads a spelling the build emits bare as "not in the
// build", and SWAPS a correctly assigned pair — after which a null-aware anti
// join keys on the probe's column and loses NOT IN's NULL
// (`TestTwoPathInvariance/NotInSubqueryDerivedInnerNullInList`).
func projKeySpelling(proj *logical.Projection, block *logical.Node) string {
	src := physical.ProjSourceName(proj)
	dot := strings.LastIndexByte(src, '.')
	if dot <= 0 || dot == len(src)-1 {
		return proj.Column
	}
	if relationsPublishing(block, src[dot+1:]) < 2 &&
		!aggregatePublishesQualified(block, src) {
		return proj.Column
	}
	return src
}

// aggregatePublishesQualified reports whether an AGGREGATE inside this block
// publishes a group key under the QUALIFIED spelling src.
//
// `exec.PublishedGroupKeyNames` strips a key's qualifier UNLESS stripping
// would make two columns of the operator's own output share one name — another
// key, or an AGGREGATE OUTPUT (#1078). That is the second way a stream comes to
// carry a qualified spelling, and it is the same fact as the join's duplicate
// qualification one relation up: the producer publishes the bare name twice, so
// only the qualified one is an address.
//
//	WITH g AS (SELECT x.a AS b, SUM(x.b) AS a FROM decpair x GROUP BY x.a)
//	SELECT g1.a AS a1, g2.b AS b2 FROM g g1 JOIN g g2 ON g1.b = g2.b
//
// The key `g1.b` names the CTE's rename of the GROUP KEY; resolving it to the
// bare `a` bound the aggregate's OUTPUT instead, so the arms joined on the SUM
// — whose values are all non-NULL — and a fifth row arrived that PostgreSQL's
// `NULL = NULL` excludes (arc K1's `968 PINNED` cell, closed here).
func aggregatePublishesQualified(block *logical.Node, src string) bool {
	if block == nil || len(block.Children) != 1 {
		return false
	}
	agg := physical.FindAggregateAncestor(block.Children[0])
	if agg == nil || len(agg.GroupBy) == 0 {
		return false
	}
	want := strings.TrimSpace(src)
	for _, n := range exec.PublishedGroupKeyNames(agg.GroupBy, agg.GroupByPublish,
		physical.LogicalAggOutNames(agg), false) {
		if strings.EqualFold(strings.TrimSpace(n), want) {
			return true
		}
	}
	return false
}

// relationsPublishing counts the relations inside a subtree whose own columns
// include this bare name — the join's duplicate-qualification test, asked of
// the logical plan.
func relationsPublishing(n *logical.Node, bare string) int {
	if n == nil || bare == "" {
		return 0
	}
	lc := strings.ToLower(strings.TrimSpace(bare))
	hits := 0
	for _, cols := range physical.SubtreeNamingOf(n).AliasCols {
		if cols[lc] {
			hits++
		}
	}
	return hits
}
