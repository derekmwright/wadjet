package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// testAnnotateTPCHScans populates ScanColumns the way the coordinator's
// catalog annotator does, so ownership-based passes resolve in tests.
func testAnnotateTPCHScans(plan *Node) {
	schema := map[string][]string{
		"customer": {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
		"orders":   {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"},
		"lineitem": {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"},
		"part":     {"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"},
		"partsupp": {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"},
		"supplier": {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"},
		"nation":   {"n_nationkey", "n_name", "n_regionkey", "n_comment"},
		"region":   {"r_regionkey", "r_name", "r_comment"},
	}
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeScan && len(n.ScanColumns) == 0 {
			if cols, ok := schema[strings.ToLower(n.TableName)]; ok {
				n.ScanColumns = cols
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
}

func planFor(t *testing.T, sql string) *Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := BuildFromSelectWithCTEs(info, info.CTEs)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return Optimize(plan, testAnnotateTPCHScans)
}

func countPlanJoins(n *Node) (total, condlessInner int) {
	if n == nil {
		return 0, 0
	}
	if n.Type == NodeJoin {
		total++
		if isInnerJoin(n) && n.JoinCond == "" {
			condlessInner++
		}
	}
	for _, c := range n.Children {
		t, ci := countPlanJoins(c)
		total += t
		condlessInner += ci
	}
	return total, condlessInner
}

// TestCommaFromJoins_Issue281 pins the fix for issue #281: comma-separated
// FROM lists parse into bare info.Tables entries (the parser emits JoinInfo
// only for explicit JOIN syntax), and the builder silently dropped every
// table after the first — the plan collapsed to a single scan and queries
// returned wrong results with no error. The builder now folds the extra
// tables in as cross joins and liftWhereEquiPredsIntoJoins moves the WHERE
// equalities onto them, so comma-FROM plans match their explicit-JOIN
// equivalents.
func TestCommaFromJoins_Issue281(t *testing.T) {
	const q18Comma = `select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from orders, lineitem, customer
where o_orderkey in (
    select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300)
  and c_custkey = o_custkey and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100`

	const q18CTE = `with ol as (
  select o_orderkey, o_custkey, o_orderdate, o_totalprice, l_quantity
  from orders, lineitem
  where o_orderkey = l_orderkey
    and o_orderkey in (
      select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300)
)
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, ol
where c_custkey = o_custkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100`

	const plainComma = `select c_name from customer, orders where c_custkey = o_custkey`

	// Explicit-JOIN control: identical semantics to q18Comma; the lift
	// pass must not disturb it.
	const q18Explicit = `select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer
join orders on c_custkey = o_custkey
join lineitem on o_orderkey = l_orderkey
where o_orderkey in (
    select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300)
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100`

	cases := []struct {
		name     string
		sql      string
		minJoins int
	}{
		{"q18_comma_from", q18Comma, 3},   // customer + lineitem inner joins + semi
		{"q18_cte_comma_from", q18CTE, 3}, // issue #281's original repro
		{"plain_two_table_comma", plainComma, 1},
		{"q18_explicit_join_control", q18Explicit, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := planFor(t, tc.sql)
			total, condless := countPlanJoins(opt)
			if total < tc.minJoins {
				t.Fatalf("optimized plan has %d joins, want >= %d (comma-FROM tables dropped — issue #281):\n%s",
					total, tc.minJoins, opt.PrettyPrint(0))
			}
			if condless > 0 {
				t.Fatalf("optimized plan retains %d condition-less inner joins (WHERE equalities not lifted — residual cross product):\n%s",
					condless, opt.PrettyPrint(0))
			}
		})
	}
}

// buildOnlyPlan returns the plan as the BUILDER produced it, before Optimize.
// The FROM-item association #593/#594 turn on is a builder property, and
// asserting it after the optimizer would confuse "built in the right shape"
// with "reordered into a workable one".
func buildOnlyPlan(t *testing.T, sql string) *Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := BuildFromSelectWithCTEs(info, info.CTEs)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	testAnnotateTPCHScans(plan)
	return plan
}

// findJoinWithCond returns the first join in the tree whose condition contains
// sub, or nil.
func findJoinWithCond(n *Node, sub string) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeJoin && strings.Contains(strings.ToLower(n.JoinCond), strings.ToLower(sub)) {
		return n
	}
	for _, c := range n.Children {
		if got := findJoinWithCond(c, sub); got != nil {
			return got
		}
	}
	return nil
}

// assertJoinCondsResolvable checks the invariant #594 broke: every column a
// join's condition names must be exposed by that join's OWN subtree.
//
// The mechanism was not a wrong join order — it was a STRANDED conjunct.
// `FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey, part t2
// WHERE t1.ps_partkey = t2.p_partkey` built as `(supplier × part) ⋈ partsupp`,
// the lift attached the WHERE equality to that top join, and reorderJoins then
// moved `part` OUT to a cross join above while leaving the conjunct naming it
// behind on `partsupp ⋈ supplier` — a join whose subtree no longer contains
// part at all. `t2.p_partkey` resolved to nothing, the hash join probed for a
// name neither side had, and the query answered ZERO ROWS with no error while
// the stranded relation became a real cross product (30 GB and an OOM kill on
// the same shape, #593).
//
// A key that names nothing can only ever match nothing, so this is checkable
// without knowing the right answer — which is what makes it worth asserting
// over a whole corpus.
func assertJoinCondsResolvable(t *testing.T, plan *Node, sql string) {
	t.Helper()
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		for _, c := range n.Children {
			walk(c)
		}
		if n.Type != NodeJoin || n.JoinCond == "" || len(n.Children) != 2 {
			return
		}
		// Semi/anti build sides and decorrelated spellings have their own
		// naming rules (ADR-0021); this asserts the plain inner/outer chain.
		if isSemiOrAnti(n) {
			return
		}
		have := map[string]bool{}
		for _, side := range n.Children {
			for c := range liftExposedColumns(side) {
				have[c] = true
			}
		}
		for _, part := range splitOnAnd(n.JoinCond, strings.ToUpper(n.JoinCond)) {
			cmp, ok := tryParseExpr(part).(*plansql.CmpExpr)
			if !ok {
				continue
			}
			for _, side := range []plansql.Node{cmp.Left, cmp.Right} {
				col := stripQualifier(colRefName(side))
				if col == "" {
					continue // literal, expression, function — not a bare column
				}
				if !have[col] {
					t.Fatalf("join condition %q names %q, which this join's subtree does not expose — "+
						"a key that resolves to nothing matches nothing, so the query answers zero rows "+
						"silently (#594).\nSQL: %s\nplan:\n%s", n.JoinCond, col, sql, plan.PrettyPrint(0))
				}
			}
		}
	}
	walk(plan)
}

// TestCommaJoinMixedWithExplicitJoin pins #593 and #594: a comma-separated
// FROM list mixed with an explicit JOIN ... ON.
//
// The builder folded every comma table in BEFORE the explicit joins, so
// `FROM a JOIN b ON …, c` became `(a × c) ⋈ b` instead of `(a ⋈ b) × c`.
// Both halves of that are wrong: a real cross product sits at the bottom of
// the plan (60,175 × 2,000 rows on the SF0.01 fixture — an OOM kill at 30 GB,
// #593), and the WHERE equality between b and c lands on a join that
// reorderJoins then reshapes out from under it, stranding the conjunct and
// answering zero rows (#594).
func TestCommaJoinMixedWithExplicitJoin(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		// condOn, if set, names a substring the lifted equality must land on
		// together with the relation it references.
		wantCondSubstr string
	}{
		{
			name:           "issue594_join_then_comma",
			sql:            `select count(*) from supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, part t2 where t1.ps_partkey = t2.p_partkey`,
			wantCondSubstr: "p_partkey",
		},
		{
			name:           "issue593_join_then_comma_with_residual",
			sql:            `select l_tax from orders t0 join lineitem t1 on t0.o_orderkey = t1.l_orderkey, part t2 where t1.l_partkey = t2.p_partkey and t0.o_orderstatus <= 'F'`,
			wantCondSubstr: "p_partkey",
		},
		{
			name:           "comma_then_join",
			sql:            `select count(*) from part t2, supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey where t1.ps_partkey = t2.p_partkey`,
			wantCondSubstr: "p_partkey",
		},
		{
			name:           "two_explicit_items",
			sql:            `select count(*) from supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, nation n join region r on n.n_regionkey = r.r_regionkey where t0.s_nationkey = n.n_nationkey`,
			wantCondSubstr: "n_nationkey",
		},
		{
			name:           "derived_table_in_comma_list",
			sql:            `select count(*) from supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, (select p_partkey as pk from part) d where t1.ps_partkey = d.pk`,
			wantCondSubstr: "ps_partkey",
		},
		{
			name:           "self_comma_join_with_aliases",
			sql:            `select count(*) from nation a join region r on a.n_regionkey = r.r_regionkey, nation b where a.n_regionkey = b.n_regionkey`,
			wantCondSubstr: "n_regionkey",
		},
		{
			name: "chain_of_three_comma",
			sql:  `select count(*) from supplier t0, partsupp t1, part t2 where t0.s_suppkey = t1.ps_suppkey and t1.ps_partkey = t2.p_partkey`,
		},
		{
			name: "non_equi_residual_stays_a_filter",
			sql:  `select count(*) from supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, part t2 where t1.ps_partkey = t2.p_partkey and t2.p_size > 20`,
		},
		{
			name: "left_join_item_plus_comma",
			sql:  `select count(*) from supplier t0 left join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, region t2 where t2.r_regionkey = t0.s_nationkey`,
		},
		{
			// A BARE (unqualified) cross-item ON: the join's ON names a column
			// of a comma sibling with no qualifier (`FROM a, b JOIN c ON x =
			// c.y` where x is a's). onRefsEarlierItem cannot see it (no
			// qualifier), so onConfinedToOwnSides is what folds a in — a's
			// column is not confined to the join's own two sides. Answered 0
			// rows before (F1).
			name: "bare_cross_item_on",
			sql:  `select count(*) from customer, orders join nation on c_nationkey = n_nationkey where c_custkey = o_custkey`,
		},
		{
			// The explicit JOIN's ON references an EARLIER comma item rather
			// than its own operand: `FROM a, b JOIN c ON a.k = c.k`. SQL
			// scopes a join's ON over every FROM item to its left, so a must
			// be in the join's left subtree. The builder attaches a join to
			// the single item it follows, which left a naming nothing here and
			// the join answered zero rows — #593/#594's failure reached
			// through an ON instead of a WHERE (fuzzer seed 11).
			name: "on_references_earlier_comma_item",
			sql:  `select count(distinct t1.p_container) from partsupp t0, part t1 join supplier t2 on t0.ps_suppkey = t2.s_suppkey where t0.ps_partkey = t1.p_partkey`,
		},
		{
			// TPC-H Q7's FROM list: the SAME table twice under two aliases,
			// each equi-joined to a different relation. Every bare column
			// name is owned by both, so ownership resolved by name alone put
			// both equalities on whichever alias came first and left the
			// other dangling under a cross join, carrying a condition that
			// names it — zero rows.
			name: "self_aliased_relation_twice_in_comma_list",
			sql: `select n1.n_name, n2.n_name from supplier, lineitem, orders, customer, nation n1, nation n2
				where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey
					and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey`,
		},
		{
			// TPC-H Q5's FROM list: a CYCLE in the join graph
			// (c_nationkey = s_nationkey on top of customer-orders-lineitem-
			// supplier). Two conjuncts over three relations land on one join,
			// which a single edge per join cannot express — the reorderer
			// carried `l_suppkey = s_suppkey` onto a join with no lineitem
			// under it and the revenue came back ~100x too large.
			name: "join_graph_cycle_in_comma_list",
			sql: `select n_name from customer, orders, lineitem, supplier, nation, region
				where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey
					and s_nationkey = n_nationkey and n_regionkey = r_regionkey
					and c_nationkey = s_nationkey`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := planFor(t, tc.sql)
			assertJoinCondsResolvable(t, opt, tc.sql)
			if _, condless := countPlanJoins(opt); condless > 0 {
				t.Fatalf("optimized plan retains %d condition-less inner join(s) — the WHERE equality was "+
					"not lifted and the plan carries a real cross product (#593):\n%s", condless, opt.PrettyPrint(0))
			}
			if tc.wantCondSubstr != "" && findJoinWithCond(opt, tc.wantCondSubstr) == nil {
				t.Fatalf("no join condition mentions %q — the comma-joined relation is not equi-joined:\n%s",
					tc.wantCondSubstr, opt.PrettyPrint(0))
			}
		})
	}
}

// TestCommaJoinKeepsGenuineCrossProducts is the other half: a comma-joined
// relation with NO equality to lift is a real cross product and must stay
// one. The lift must not invent a condition, and the fix for #593 must not
// turn an unconstrained FROM item into a dropped one (#281's failure).
func TestCommaJoinKeepsGenuineCrossProducts(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantScans int
	}{
		{"pure_cross", `select count(*) from nation a, region b`, 2},
		{"cross_alongside_equijoin", `select count(*) from supplier t0, partsupp t1, region t2 where t0.s_suppkey = t1.ps_suppkey`, 3},
		{"cross_between_equijoined_pair", `select count(*) from nation a, region x, supplier b where a.n_nationkey = b.s_nationkey`, 3},
		{"explicit_join_plus_cross", `select count(*) from supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, region t2`, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := planFor(t, tc.sql)
			assertJoinCondsResolvable(t, opt, tc.sql)
			var scans int
			var walk func(*Node)
			walk = func(n *Node) {
				if n == nil {
					return
				}
				if n.Type == NodeScan {
					scans++
				}
				for _, c := range n.Children {
					walk(c)
				}
			}
			walk(opt)
			if scans != tc.wantScans {
				t.Fatalf("plan has %d scans, want %d — a FROM item was dropped or duplicated (#281):\n%s",
					scans, tc.wantScans, opt.PrettyPrint(0))
			}
		})
	}
}

// TestCommaJoinBuilderKeepsFromItemsIntact asserts the builder property
// directly: an explicit JOIN belongs to the FROM item it follows, so the
// comma list's other items cross-join the RESULT of that join rather than
// getting folded underneath it.
func TestCommaJoinBuilderKeepsFromItemsIntact(t *testing.T) {
	// `FROM a JOIN b ON …, c` must build as cross(join(a,b), c).
	plan := buildOnlyPlan(t, `select count(*) from supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey, part t2 where t1.ps_partkey = t2.p_partkey`)
	root := plan
	for root != nil && root.Type != NodeJoin {
		if len(root.Children) == 0 {
			break
		}
		root = root.Children[0]
	}
	if root == nil || root.Type != NodeJoin || !strings.EqualFold(root.JoinType, "cross") {
		t.Fatalf("top join is %v/%q, want a cross join over the two FROM items:\n%s",
			root.Type, root.JoinType, plan.PrettyPrint(0))
	}
	inner := root.Children[0]
	if inner.Type != NodeJoin || inner.JoinCond == "" {
		t.Fatalf("cross join's left child is %v with cond %q, want the explicit JOIN ... ON of the first "+
			"FROM item (the comma table was folded underneath it — #593/#594):\n%s",
			inner.Type, inner.JoinCond, plan.PrettyPrint(0))
	}
	if got := collectSubtreeColumns(root.Children[1]); !got["p_partkey"] {
		t.Fatalf("cross join's right child does not expose part's columns:\n%s", plan.PrettyPrint(0))
	}

	// `FROM a, b JOIN c ON …` must build as cross(a, join(b,c)) — the join
	// attaches to the LAST comma item, not the first.
	plan2 := buildOnlyPlan(t, `select count(*) from part t2, supplier t0 join partsupp t1 on t0.s_suppkey = t1.ps_suppkey where t1.ps_partkey = t2.p_partkey`)
	root2 := plan2
	for root2 != nil && root2.Type != NodeJoin {
		if len(root2.Children) == 0 {
			break
		}
		root2 = root2.Children[0]
	}
	if root2 == nil || root2.Type != NodeJoin || !strings.EqualFold(root2.JoinType, "cross") {
		t.Fatalf("top join is %q, want cross:\n%s", root2.JoinType, plan2.PrettyPrint(0))
	}
	if right := root2.Children[1]; right.Type != NodeJoin || right.JoinCond == "" {
		t.Fatalf("the explicit JOIN did not attach to the LAST comma FROM item:\n%s", plan2.PrettyPrint(0))
	}
	if got := collectSubtreeColumns(root2.Children[0]); !got["p_partkey"] {
		t.Fatalf("cross join's left child is not the leading comma table:\n%s", plan2.PrettyPrint(0))
	}
}
