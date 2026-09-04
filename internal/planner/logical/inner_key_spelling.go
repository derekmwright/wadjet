package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Spelling a decorrelated subquery's own column references
// ------------------------------------------------------------------
//
// decorrelateInSubqueries and decorrelateExists lower an IN / EXISTS to a
// semi/anti join whose BUILD side is the subquery's own plan —
// Scan → [Join …] → [Filter] → [Aggregate], and never a Project. That side
// therefore carries the SOURCE column names of the relations it reads, and
// the rewrites have to name their build-side keys the way it emits them.
//
// With ONE inner relation that is knowable on the spot: the bottom Scan
// emits every column bare, so the key is the source column with any
// qualifier stripped (#516). With a JOIN it is not knowable on the spot at
// all. A join emits its PROBE side's columns bare and qualifies a BUILD
// column only where the bare name collides (exec.joinOutputSchemaWithMapping),
// and which side is which is decided by reorderJoins from estimated row
// counts at Optimize step 73 — long after the rewrites run at steps 35/36.
// Naming the key from write order then answers over whichever relation the
// estimator happened to put on the probe (#526), and correlating on a
// stripped column correlates on whichever relation the estimator put there
// (#527). Both are silent: the physical planner splits the condition
// literally and exec.HashJoin's key repair swaps the pair.
//
// So the rewrites record what they MEAN — the relation qualifier and the
// source column, as the subquery wrote them — and repairDecorrelatedSpelling
// settles the TEXT after reorderJoins has made the join order final, by
// modelling what each build subtree actually emits.

// InnerKeyRef is one reference into a decorrelated subquery's own relations,
// recorded the way the subquery spelled it.
//
// Text is what the rewrite wrote into the plan when it built the node. It is
// what the repair keeps when it cannot resolve the reference — an un-annotated
// Scan (no ScanColumns) is the reachable case — so a plan that never reaches
// the repair reads exactly as it did before this machinery existed.
type InnerKeyRef struct {
	Qualifier string // relation alias, or table name when unaliased; "" = unqualified
	Column    string // source column, unqualified
	Text      string // the spelling the rewrite wrote, and the repair's fallback
}

// spelled returns the reference's current text.
func (r InnerKeyRef) spelled() string {
	if r.Text != "" {
		return r.Text
	}
	if r.Qualifier != "" && r.Column != "" {
		return r.Qualifier + "." + r.Column
	}
	return r.Column
}

// DecorrelatedKey is one conjunct of a decorrelated semi/anti join: a
// probe-side term the rewrite already spelled correctly (it names the OUTER
// query's columns, which no inner reordering can move), an operator, and a
// build-side reference whose spelling only reorderJoins can settle.
type DecorrelatedKey struct {
	Outer string
	Op    string
	Inner InnerKeyRef
}

// conjunct renders the key as the join condition text.
func (k DecorrelatedKey) conjunct() string {
	op := k.Op
	if op == "" {
		op = "="
	}
	return k.Outer + " " + op + " " + k.Inner.spelled()
}

// renderDecorrelatedKeys joins the conjuncts with AND, or returns "" for none.
func renderDecorrelatedKeys(keys []DecorrelatedKey) string {
	if len(keys) == 0 {
		return ""
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k.conjunct()
	}
	return strings.Join(parts, " AND ")
}

// emittedCol is one column a logical subtree's ROOT produces: the NAME
// downstream expressions must use to reach it, and the relation that owns it.
type emittedCol struct {
	name  string
	owner string // scan alias (or table name when unaliased); "" = computed
}

// emittedColumns models the names a subtree's root emits.
//
// The join arm mirrors exec.joinOutputSchemaWithMapping: probe columns first
// and verbatim, then build columns, each qualified by its owning relation
// exactly when its bare name already occurs on the probe side. A build column
// that already carries a qualifier (a nested join inside the build subtree
// named it) is emitted verbatim, and a colliding build column with no alias
// to disambiguate by is dropped — both are what the executor does.
//
// A subtree that is a NAMED SCOPE — a derived table with an alias, or a CTE
// reference — emits its root's columns under that scope's name and no other.
// The enclosing query writes `d.k` for them, and the scan below emits `k`
// owned by whatever base relation it reads, so without the override the
// qualified spelling resolves to nothing and the key keeps the rewrite's
// guess. A CTE records the name on the subtree ROOT rather than on the scans
// (ADR-0021 §1d), which is why this is read here and not from the scan arm.
//
// This is a model of another package's behavior, so it is pinned by value:
// the semi/anti-join answers this spelling decides are asserted against
// PostgreSQL over fixtures that put each arm on the probe in turn.
func emittedColumns(n *Node) []emittedCol {
	cols := emittedColumnsOfNode(n)
	owner := scopeOwnerOf(n)
	if owner == "" || len(cols) == 0 {
		return cols
	}
	out := make([]emittedCol, len(cols))
	for i, c := range cols {
		out[i] = emittedCol{name: stripQualifier(c.name), owner: owner}
	}
	return out
}

func emittedColumnsOfNode(n *Node) []emittedCol {
	if n == nil {
		return nil
	}
	switch n.Type {
	case NodeScan:
		owner := n.TableAlias
		if owner == "" {
			owner = n.TableName
		}
		out := make([]emittedCol, 0, len(n.ScanColumns))
		for _, c := range n.ScanColumns {
			out = append(out, emittedCol{name: c, owner: owner})
		}
		return out

	case NodeJoin:
		if len(n.Children) < 2 {
			return emittedColumns(firstChild(n))
		}
		// A semi/anti join is a filter on its probe: it emits no build column.
		if jt := strings.ToLower(n.JoinType); jt == "semi" || jt == "anti" {
			return emittedColumns(n.Children[0])
		}
		left := emittedColumns(n.Children[0])
		right := emittedColumns(n.Children[1])
		seen := make(map[string]bool, len(left)+len(right))
		for _, e := range left {
			seen[strings.ToLower(e.name)] = true
		}
		out := make([]emittedCol, 0, len(left)+len(right))
		out = append(out, left...)
		for _, e := range right {
			if strings.IndexByte(e.name, '.') >= 0 {
				out = append(out, e)
				seen[strings.ToLower(e.name)] = true
				continue
			}
			if seen[strings.ToLower(e.name)] {
				if e.owner == "" {
					continue // nothing to disambiguate by; the executor drops it
				}
				qualified := e.owner + "." + e.name
				out = append(out, emittedCol{name: qualified, owner: e.owner})
				seen[strings.ToLower(qualified)] = true
				continue
			}
			out = append(out, e)
			seen[strings.ToLower(e.name)] = true
		}
		return out

	case NodeProject:
		child := emittedColumns(firstChild(n))
		if len(n.Projections) == 0 {
			return child
		}
		out := make([]emittedCol, 0, len(n.Projections))
		for _, p := range n.Projections {
			name := p.Alias
			if name == "" {
				name = p.Column
			}
			if name == "" {
				name = cleanExpr(p.Expr)
			}
			if name == "" {
				continue
			}
			owner := ""
			if !p.IsAgg && p.Column != "" {
				owner = ownerOf(child, p.Column)
			}
			out = append(out, emittedCol{name: name, owner: owner})
		}
		return out

	case NodeAggregate:
		child := emittedColumns(firstChild(n))
		out := make([]emittedCol, 0, len(n.GroupBy)+len(n.AggExprs))
		for i, name := range aggregateGroupOutputNames(n.GroupBy) {
			// A group key READS one name and EMITS another: the aggregate
			// strips the qualifier off its output column. The key of a semi
			// join over a grouped inner has to say the emitted one, and the
			// owner comes from what it READ.
			out = append(out, emittedCol{name: name, owner: ownerOf(child, n.GroupBy[i])})
		}
		for _, a := range n.AggExprs {
			if a.OutputCol != "" {
				out = append(out, emittedCol{name: a.OutputCol})
			}
		}
		return out

	case NodeWindow:
		out := append([]emittedCol(nil), emittedColumns(firstChild(n))...)
		for _, w := range n.WindowExprs {
			if w.OutputCol != "" {
				out = append(out, emittedCol{name: w.OutputCol})
			}
		}
		return out

	default:
		// Filter, Sort, Limit, Distinct, Union: the root passes its first
		// child's schema through unchanged.
		return emittedColumns(firstChild(n))
	}
}

// aggregateGroupOutputNames returns the names an Aggregate EMITS for its group
// keys, which are not the names it reads them under.
//
// It mirrors exec.HashAggregate.outputSchema: the qualifier is stripped, and
// kept only where stripping would make two keys collide (`GROUP BY n1.n_name,
// n2.n_name` must stay distinguishable downstream). Modelling the output as
// the key's own TEXT was the #526 defect one node higher — the semi join above
// a grouped inner named its key `c.x` while the aggregate emitted `x`, so the
// executor's key repair swapped the pair and the join matched nothing.
func aggregateGroupOutputNames(groupBy []string) []string {
	out := make([]string, len(groupBy))
	base := make(map[string]int, len(groupBy))
	for i, name := range groupBy {
		out[i] = stripQualifier(name)
		base[strings.ToLower(out[i])]++
	}
	for i, name := range groupBy {
		if base[strings.ToLower(out[i])] > 1 {
			out[i] = name // ambiguous stripped: the aggregate keeps it qualified
		}
	}
	return out
}

func firstChild(n *Node) *Node {
	if n == nil || len(n.Children) == 0 {
		return nil
	}
	return n.Children[0]
}

// ownerOf returns the relation owning the column ref names in cols, or "".
func ownerOf(cols []emittedCol, ref string) string {
	for _, e := range cols {
		if strings.EqualFold(e.name, ref) {
			return e.owner
		}
	}
	bare := stripQualifier(ref)
	qual := ""
	if bare != ref {
		qual = ref[:len(ref)-len(bare)-1]
	}
	for _, e := range cols {
		if strings.EqualFold(e.name, bare) && (qual == "" || strings.EqualFold(e.owner, qual)) {
			return e.owner
		}
	}
	return ""
}

// spellInner returns the name the subtree emitting cols carries for ref.
//
// A qualified reference is emitted under its qualifier exactly when its bare
// name collided on the probe side; otherwise the join emitted it bare and the
// qualified spelling names nothing. ok=false means the reference could not be
// resolved at all — the caller keeps whatever the rewrite wrote.
func spellInner(ref InnerKeyRef, cols []emittedCol) (string, bool) {
	if ref.Column == "" || len(cols) == 0 {
		return "", false
	}
	if ref.Qualifier == "" {
		for _, e := range cols {
			if strings.EqualFold(e.name, ref.Column) {
				return e.name, true
			}
		}
		return "", false
	}
	qualified := ref.Qualifier + "." + ref.Column
	for _, e := range cols {
		if strings.EqualFold(e.name, qualified) {
			return e.name, true
		}
	}
	for _, e := range cols {
		if strings.EqualFold(e.name, ref.Column) && strings.EqualFold(e.owner, ref.Qualifier) {
			return e.name, true
		}
	}
	return "", false
}

// repairDecorrelatedSpelling re-spells every reference the IN and EXISTS
// decorrelations left pending, now that reorderJoins has settled which
// relation's columns each inner join emits bare.
//
// Bottom-up: an Aggregate's group keys are the names it emits, so they are
// settled before the semi join above resolves a key against them.
//
// Runs after reorderJoins and before the passes that read a join condition as
// text (computeRequiredColumns, attachScanPredicates, the physical planner's
// key split).
func repairDecorrelatedSpelling(n *Node) *Node {
	if n == nil {
		return nil
	}
	for i, child := range n.Children {
		n.Children[i] = repairDecorrelatedSpelling(child)
	}

	switch n.Type {
	case NodeAggregate:
		if len(n.InnerGroupRefs) == 0 {
			return n
		}
		cols := emittedColumns(firstChild(n))
		for i := range n.GroupBy {
			if i >= len(n.InnerGroupRefs) {
				break
			}
			if spelled, ok := spellInner(n.InnerGroupRefs[i], cols); ok {
				n.GroupBy[i] = spelled
			}
		}
		n.InnerGroupRefs = nil

	case NodeJoin:
		if len(n.InnerKeys) == 0 && len(n.InnerFilterKeys) == 0 {
			return n
		}
		if len(n.Children) < 2 {
			return n
		}
		deferred := deferSemiAntiDedup(n)
		cols := emittedColumns(n.Children[1])
		for i := range n.InnerKeys {
			if spelled, ok := spellInner(n.InnerKeys[i].Inner, cols); ok {
				n.InnerKeys[i].Inner.Text = spelled
			}
		}
		for i := range n.InnerFilterKeys {
			if spelled, ok := spellInner(n.InnerFilterKeys[i].Inner, cols); ok {
				n.InnerFilterKeys[i].Inner.Text = spelled
			}
		}
		if cond := renderDecorrelatedKeys(n.InnerKeys); cond != "" {
			n.JoinCond = cond
		}
		if filter := renderDecorrelatedKeys(n.InnerFilterKeys); filter != "" {
			n.JoinFilter = filter
		}
		n.InnerKeys = nil
		n.InnerFilterKeys = nil
		if deferred {
			// dedupSemiAntiBuildSide skipped this join because its keys were
			// not yet spelled. They are now, so the NDV bound is available
			// again — later than the reorderer's costing saw it, which is
			// why only the shapes that need it are deferred.
			return dedupSemiAntiBuildSide(n)
		}
	}
	return n
}

// innerOnlyPredicate turns one of a decorrelated subquery's own WHERE
// conditions into a plan Predicate, and reports ok=false when the rewrite
// must DECLINE rather than produce one.
//
// The decorrelations strip table qualifiers here, for the same reason they
// strip them off a key: the inner plan is Scan → [Join …] → [Filter] and
// carries SOURCE column names, which a single bottom Scan emits bare. Over a
// JOINED inner that reasoning fails the same way #526's did, and worse: the
// stripped predicate is pushed to whichever side of the join owns a column of
// that bare name, so `WHERE c.n_nationkey < 3` over `nation c JOIN nation b`
// filtered on b instead of c and the membership set became a different set
// entirely — a silent wrong answer with no key involved.
//
// Three outcomes over a joined inner, decided by how many of its relations the
// condition names:
//
//   - ONE, fully qualified: keep the qualifiers. pushFilterThroughJoin
//     attributes it by exactly the refs collected here and lands it on that
//     relation's own Scan, where the executor resolves the qualified name to
//     the bare column it stores.
//   - MORE THAN ONE: DECLINE the whole rewrite. There is no spelling that
//     works: stripped, pushdown puts `c.x > b.x` on ONE scan as `x > x`
//     (which evaluates against that relation's own column twice — the
//     membership set collapses); qualified, it stays above the join, where
//     the join emits one side's column bare and the qualified spelling names
//     nothing. Declining leaves the IN a subquery predicate, executed as
//     written — which the stage DAG can now do too (#524).
//   - Unattributable — a bare reference, or a subquery inside the condition:
//     stripped, exactly as before. A bare reference over a joined inner is
//     ambiguous SQL unless one relation owns the name, and in that case the
//     strip names it correctly.
//
// A single-relation inner is unchanged: strip, and the bottom Scan emits it.
func innerOnlyPredicate(node plansql.Node, joinedInner bool) (Predicate, bool) {
	if joinedInner {
		// A nil colToTable resolves nothing, so a bare reference reports
		// unresolved and falls through to the strip. That is the point: only
		// a spelling the pushdown can attribute without guessing is kept.
		if refs := predicateTableRefs(Predicate{ASTExpr: node}, nil); len(refs) > 0 {
			if len(refs) > 1 {
				return Predicate{}, false
			}
			return Predicate{Raw: node.String(), ASTExpr: node}, true
		}
	}
	stripped := stripTableQualifiers(node)
	return Predicate{Raw: stripped.String(), ASTExpr: stripped}, true
}

// deferSemiAntiDedup reports whether dedupSemiAntiBuildSide must wait for
// repairDecorrelatedSpelling before it can name this join's build-side keys.
//
// Only a QUALIFIED reference over a JOINED build subtree is in doubt: which
// relation the join emits bare, and which it qualifies, is reorderJoins'
// decision. An unqualified reference names the same column either way, so the
// overwhelmingly common shape — TPC-H Q04/Q20/Q21's bare inner select item —
// still gets its NDV bound before the reorderer costs the subtree, which is
// where dedupSemiAntiBuildSide has to run to be worth anything.
func deferSemiAntiDedup(n *Node) bool {
	if n == nil || len(n.InnerKeys) == 0 || len(n.Children) < 2 {
		return false
	}
	qualified := false
	for _, k := range n.InnerKeys {
		if k.Inner.Qualifier != "" {
			qualified = true
			break
		}
	}
	return qualified && hasJoinBelow(n.Children[1])
}

// hasJoinBelow reports whether a subtree contains a join node — the condition
// under which a decorrelated subquery's own column spelling is decided by
// reorderJoins rather than by a single bottom Scan.
func hasJoinBelow(n *Node) bool {
	if n == nil {
		return false
	}
	if n.Type == NodeJoin {
		return true
	}
	for _, c := range n.Children {
		if hasJoinBelow(c) {
			return true
		}
	}
	return false
}
