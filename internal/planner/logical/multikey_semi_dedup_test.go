package logical

import (
	"strings"
	"testing"
)

// #562 — a correlated EXISTS / NOT EXISTS / IN with TWO equality keys answered
// zero rows (a semi join) or every row (an anti join).
//
// dedupSemiAntiBuildSide narrows a semi/anti join's build side to
// Project(keys) → Distinct so the hash table is sized to NDV. It read the keys
// out of the condition TEXT with strings.Split(cond, " and "), and the
// condition a decorrelation writes is rendered with " AND "
// (renderDecorrelatedKeys). The split therefore never fired: a two-key
// condition came through as ONE part, SplitN(part, "=", 2) made its right
// operand the literal text `o_custkey AND o_orderstatus = o_orderstatus`,
// which resolves in no subtree — so the FIRST conjunct's left operand was
// taken as the only build key. The Project then deleted the column the second
// conjunct compares and the join matched nothing.
//
// The invariant these assert is not "the dedup fires" but "the build side
// still emits every key the condition names". That is the property the
// narrowing can violate, and it is checkable from the plan alone.
func TestMultiKeySemiJoinBuildSideEmitsEveryKey(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		wantKeys int
	}{
		{
			name:     "two-key correlated EXISTS on one relation",
			sql:      `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM orders b WHERE b.o_custkey = a.o_custkey AND b.o_orderstatus = a.o_orderstatus AND b.o_orderkey < 500)`,
			wantKeys: 2,
		},
		{
			name:     "the same two conjuncts written the other way round",
			sql:      `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM orders b WHERE b.o_orderstatus = a.o_orderstatus AND b.o_custkey = a.o_custkey AND b.o_orderkey < 500)`,
			wantKeys: 2,
		},
		{
			name:     "three-key correlated EXISTS",
			sql:      `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM orders b WHERE b.o_custkey = a.o_custkey AND b.o_orderstatus = a.o_orderstatus AND b.o_shippriority = a.o_shippriority AND b.o_orderkey < 500)`,
			wantKeys: 3,
		},
		{
			name:     "two-key correlated NOT EXISTS",
			sql:      `SELECT o_orderkey FROM orders a WHERE NOT EXISTS (SELECT 1 FROM orders b WHERE b.o_custkey = a.o_custkey AND b.o_orderstatus = a.o_orderstatus AND b.o_orderkey < 500)`,
			wantKeys: 2,
		},
		{
			name:     "correlated IN: the IN key plus one correlation",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_orderkey IN (SELECT b.o_orderkey FROM orders b WHERE b.o_custkey = a.o_custkey AND b.o_orderkey < 500)`,
			wantKeys: 2,
		},
		{
			// The correlated NOT IN spelling of the same shape used to stand
			// here with wantKeys 3. It is no longer lowered to a join at all
			// — an anti join answers the two-valued question and NOT IN's
			// third value is per correlation GROUP (#538/#578) — so the
			// narrowing this test is about never sees it. Its NOT EXISTS
			// twin, which IS what an anti join means, keeps the coverage.
			name:     "correlated NOT EXISTS: two correlations plus an inner key",
			sql:      `SELECT o_orderkey FROM orders a WHERE NOT EXISTS (SELECT 1 FROM orders b WHERE b.o_orderkey = a.o_orderkey AND b.o_custkey = a.o_custkey AND b.o_orderstatus = a.o_orderstatus)`,
			wantKeys: 3,
		},
		{
			name:     "two keys across DIFFERENT relations, which is where the narrowing does fire",
			sql:      `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM customer c WHERE c.c_custkey = a.o_custkey AND c.c_nationkey = a.o_shippriority AND c.c_acctbal > 0)`,
			wantKeys: 2,
		},
		{
			name:     "two keys over a JOINED inner, where reorderJoins settles the spelling",
			sql:      `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE c.c_custkey = a.o_custkey AND n.n_regionkey = a.o_shippriority)`,
			wantKeys: 2,
		},
		{
			name:     "one key is the control: it always worked and must keep working",
			sql:      `SELECT o_orderkey FROM orders a WHERE EXISTS (SELECT 1 FROM customer c WHERE c.c_custkey = a.o_custkey AND c.c_acctbal > 0)`,
			wantKeys: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildPlan(t, tc.sql)
			annotateScanColumnsForTest(plan)
			opt := Optimize(plan, annotateScanColumnsForTest)
			join := findNodeMatching(opt, func(n *Node) bool {
				return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
			})
			if join == nil {
				t.Fatalf("no semi/anti join in the optimized plan:\n%s", opt.PrettyPrint(0))
			}
			expr := tryParseExpr(join.JoinCond)
			if expr == nil {
				t.Fatalf("join condition %q does not parse", join.JoinCond)
			}
			conjuncts := flattenJoinCondConjuncts(expr)
			if len(conjuncts) != tc.wantKeys {
				t.Fatalf("join condition %q has %d conjuncts, want %d",
					join.JoinCond, len(conjuncts), tc.wantKeys)
			}
			emitted := emittedColumns(join.Children[1])
			for _, conj := range conjuncts {
				for _, side := range joinCondOperands(t, conj) {
					if resolvesIn(emitted, side) {
						continue
					}
					// Every conjunct compares one probe column with one build
					// column, so at least one side must resolve here. None
					// doing so is the defect: the build no longer carries the
					// column this conjunct compares, and the join matches
					// nothing.
					if !anySideResolves(emitted, joinCondOperands(t, conj)) {
						t.Fatalf("conjunct %q of %q names no column the build side emits %v\n%s",
							conj.String(), join.JoinCond, emittedColNames(emitted), opt.PrettyPrint(0))
					}
				}
			}
		})
	}
}

func joinCondOperands(t *testing.T, conj interface{ String() string }) []string {
	t.Helper()
	parts := strings.SplitN(conj.String(), "=", 2)
	if len(parts) != 2 {
		return nil
	}
	return []string{strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])}
}

func resolvesIn(cols []emittedCol, ref string) bool {
	for _, c := range cols {
		if strings.EqualFold(c.name, ref) || strings.EqualFold(stripQualifier(c.name), stripQualifier(ref)) {
			return true
		}
	}
	return false
}

func anySideResolves(cols []emittedCol, refs []string) bool {
	for _, r := range refs {
		if resolvesIn(cols, r) {
			return true
		}
	}
	return false
}

func emittedColNames(cols []emittedCol) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

// extractRightJoinKeys is all-or-nothing by contract: the caller projects the
// build side down to exactly what it returns, so a partial list is a deleted
// column. These pin each way it can decline, and the one it must not.
func TestExtractRightJoinKeysIsAllOrNothing(t *testing.T) {
	build := NewScan("customer", "c")
	build.ScanColumns = []string{"c_custkey", "c_nationkey", "c_acctbal"}

	cases := []struct {
		name string
		cond string
		want []string
	}{
		{
			name: "one key",
			cond: "o_custkey = c_custkey",
			want: []string{"c_custkey"},
		},
		{
			// The #562 defect: rendered with " AND ", which the old lexical
			// split on " and " could not see, so only the first key came back.
			name: "two keys, the operator rendered in upper case",
			cond: "o_custkey = c_custkey AND o_shippriority = c_nationkey",
			want: []string{"c_custkey", "c_nationkey"},
		},
		{
			name: "two keys, lower case, still both",
			cond: "o_custkey = c_custkey and o_shippriority = c_nationkey",
			want: []string{"c_custkey", "c_nationkey"},
		},
		{
			name: "three keys",
			cond: "o_custkey = c_custkey AND o_shippriority = c_nationkey AND o_totalprice = c_acctbal",
			want: []string{"c_custkey", "c_nationkey", "c_acctbal"},
		},
		{
			name: "the build column written on the LEFT of its conjunct",
			cond: "c_custkey = o_custkey AND c_nationkey = o_shippriority",
			want: []string{"c_custkey", "c_nationkey"},
		},
		{
			name: "a non-equality conjunct: no key set to dedup on",
			cond: "o_custkey = c_custkey AND o_totalprice < c_acctbal",
			want: nil,
		},
		{
			name: "the ON-TRUE sentinel names no column",
			cond: "1 = 1",
			want: nil,
		},
		{
			name: "an expression operand is not a column",
			cond: "o_custkey = c_custkey + 1",
			want: nil,
		},
		{
			name: "unparseable text attributes nothing rather than guessing",
			cond: "o_custkey = = c_custkey",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractRightJoinKeys(tc.cond, build)
			if len(got) != len(tc.want) {
				t.Fatalf("extractRightJoinKeys(%q) = %v, want %v", tc.cond, got, tc.want)
			}
			for i := range got {
				if !strings.EqualFold(got[i], tc.want[i]) {
					t.Fatalf("extractRightJoinKeys(%q) = %v, want %v", tc.cond, got, tc.want)
				}
			}
		})
	}
}

// A self-join's `k = k` resolves on both sides, so the condition alone cannot
// say which relation owns it. Declining is the answer that was always right
// for one key; #562 is what happened when two keys made the wrong guess
// reachable.
func TestExtractRightJoinKeysDeclinesAnUnattributableName(t *testing.T) {
	build := NewScan("orders", "b")
	build.ScanColumns = []string{"o_orderkey", "o_custkey", "o_orderstatus"}
	for _, cond := range []string{
		"o_custkey = o_custkey",
		"o_custkey = o_custkey AND o_orderstatus = o_orderstatus",
	} {
		if got := extractRightJoinKeys(cond, build); got != nil {
			t.Errorf("extractRightJoinKeys(%q) = %v, want nil: neither side is attributable", cond, got)
		}
	}
}

// The Project the caller builds aliases each key to its BARE name, so two keys
// that strip to the same name would emit one column twice and the second key
// would read the first's values. Nothing here gets to pick a different
// spelling, so it declines instead.
func TestExtractRightJoinKeysDeclinesCollidingBareNames(t *testing.T) {
	left := NewScan("nation", "n1")
	left.ScanColumns = []string{"n_nationkey", "n_name", "n_regionkey"}
	right := NewScan("nation", "n2")
	right.ScanColumns = []string{"n_nationkey", "n_name", "n_regionkey"}
	build := NewJoin(left, right, "join", "n1.n_regionkey = n2.n_regionkey")
	cond := "r_regionkey = n1.n_nationkey AND r_comment = n2.n_nationkey"
	if got := extractRightJoinKeys(cond, build); got != nil {
		t.Errorf("extractRightJoinKeys(%q) = %v, want nil: both keys strip to n_nationkey", cond, got)
	}
}

// The narrowing's Project aliases every key to its BARE name
// (Projection{Column: k, Alias: stripQualifier(k)}), so a key the CONDITION
// spells qualified would be renamed out from under it: the build emits `q_s`,
// the join still asks for `b1.q_s`, that resolves to index -1 and the join
// matches nothing.
//
// It is reachable whenever the build subtree has two arms sharing a bare name
// — an EXISTS whose inner self-joins — because reorderJoins then qualifies
// one side. Measured against live PostgreSQL over internal/oracle/multikey's
// distinct-name arm, the three shapes it reaches answered 0, 0 and 40 where
// PostgreSQL says 36, 36 and 4.
func TestExtractRightJoinKeysDeclinesAQualifiedKeyItWouldRename(t *testing.T) {
	b1 := NewScan("dn_inner", "b1")
	b1.ScanColumns = []string{"q_id", "q_s", "q_n", "q_g"}
	b2 := NewScan("dn_inner", "b2")
	b2.ScanColumns = []string{"q_id", "q_s", "q_n", "q_g"}
	build := NewJoin(b1, b2, "join", "b1.q_g = b2.q_g")

	// One key bare and one qualified: the qualified one is the whole problem,
	// and a partial list would delete the other's column.
	for _, cond := range []string{
		"p_s = b1.q_s AND p_n = q_n",
		"p_s = q_s AND p_n = b2.q_n",
		"p_s = b1.q_s AND p_n = b2.q_n",
	} {
		if got := extractRightJoinKeys(cond, build); got != nil {
			t.Errorf("extractRightJoinKeys(%q) = %v, want nil: the Project would alias the "+
				"qualified key to its bare name and the condition would name nothing", cond, got)
		}
	}
	// The control: both keys bare over the same build subtree, which the
	// Project can carry verbatim.
	if got := extractRightJoinKeys("p_s = q_s AND p_n = q_n", build); len(got) != 2 {
		t.Errorf("extractRightJoinKeys with two BARE keys = %v, want both — the decline is about "+
			"the spelling the Project would change, not about a joined build side", got)
	}
}

// The kill switch is #287's definition of done for an optimization that can
// change the row set, and this pass can change it in both directions. The
// registry is what the invariance oracle enumerates, so a pass missing from it
// is a pass the oracle cannot disable — #562 would have been reported as a
// divergence under WADJET_SEMIANTI_BUILD_DEDUP=0 the first time a two-key
// correlation entered any corpus.
func TestBuildSideDedupHasAKillSwitch(t *testing.T) {
	if buildDedupToggle.Name != "semianti-build-dedup" || buildDedupToggle.Env != "WADJET_SEMIANTI_BUILD_DEDUP" {
		t.Fatalf("toggle registered as %q/%q", buildDedupToggle.Name, buildDedupToggle.Env)
	}
	sql := `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT c.c_custkey FROM customer c WHERE c.c_nationkey < 5)`
	narrowed := func() bool {
		plan := buildPlan(t, sql)
		annotateScanColumnsForTest(plan)
		join := findNodeMatching(Optimize(plan, annotateScanColumnsForTest), func(n *Node) bool {
			return n.Type == NodeJoin && n.JoinType == "semi"
		})
		if join == nil {
			t.Fatalf("no semi join in the optimized plan")
		}
		return findNodeMatching(join.Children[1], func(n *Node) bool { return n.Type == NodeDistinct }) != nil
	}
	if !narrowed() {
		t.Fatal("the pass did not narrow a shape it should have; the switch test cannot tell on from off")
	}
	prev := buildDedupToggle.Set(false)
	defer buildDedupToggle.Set(prev)
	if narrowed() {
		t.Error("WADJET_SEMIANTI_BUILD_DEDUP=0 did not disable the build-side narrowing")
	}
}
