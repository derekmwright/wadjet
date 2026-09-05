package shapegen

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
)

func TestGeneratorDeterministic(t *testing.T) {
	s := TPCH()
	for seed := int64(1); seed <= 200; seed++ {
		a := New(seed, s).Query().SQL()
		b := New(seed, s).Query().SQL()
		if a != b {
			t.Fatalf("seed %d not deterministic:\n  %s\n  %s", seed, a, b)
		}
		if !strings.HasPrefix(a, "SELECT") && !strings.HasPrefix(a, "WITH") {
			t.Fatalf("seed %d produced malformed SQL: %s", seed, a)
		}
	}
}

// TestGeneratorCoversTargetShapes pins that the generator really reaches every
// risk cluster it claims to. A shape that never fires makes the differential
// silently narrower than the report says it is.
// constArgAgg matches an aggregate whose argument is a bare integer literal
// (MIN(1), SUM(0), COUNT(7)) — the #621 shape.
var constArgAgg = regexp.MustCompile(`(?:MIN|MAX|SUM|COUNT)\([0-9]`)

func TestGeneratorCoversTargetShapes(t *testing.T) {
	s := TPCH()
	counts := map[string]int{}
	bump := func(k string, ok bool) {
		if ok {
			counts[k]++
		}
	}
	for seed := int64(1); seed <= 3000; seed++ {
		q := New(seed, s).Query()
		sql := q.SQL()
		counts["shape:"+q.Shape]++
		bump("qualified-ref", strings.Contains(sql, "t0."))
		bump("bare-ref", strings.Contains(sql, " FROM ") && !strings.Contains(sql, "t0."))
		bump("quoted-ident", strings.Contains(sql, `"`))
		bump("star", q.hasStar())
		bump("self-join", len(q.From) > 1 && q.From[0].Table == q.From[1].Table)
		bump("left-join", strings.Contains(sql, "LEFT JOIN"))
		// Count the FROM entries, not the text: `", "` matches any multi-item
		// SELECT list, so the old predicate was true for nearly every query
		// and would have stayed green with comma-join emission deleted
		// outright. "comma-join-mixed" is the shape #593/#594 turn on — a
		// comma FROM item beside an explicit JOIN ... ON — and it is rarer
		// than a plain comma join, so it needs its own key.
		bump("comma-join", commaJoins(q) > 0)
		bump("comma-join-mixed", commaJoins(q) > 0 && explicitJoins(q) > 0)
		bump("chain-4", len(q.From) >= 4)
		bump("derived", strings.Contains(sql, "FROM ("))
		bump("cte", strings.HasPrefix(sql, "WITH "))
		bump("exists", strings.Contains(sql, "EXISTS"))
		// #562: a correlated EXISTS keyed on MORE THAN ONE column. The FK arm
		// emits exactly one equality, so before genMultiKeyExists this
		// generator could not produce the shape even in principle — and the
		// defect it hides answers zero rows for the EXISTS and every row for
		// the NOT EXISTS, silently.
		bump("exists-multikey", correlatedKeys(sql) >= 2)
		bump("exists-threekey", correlatedKeys(sql) >= 3)
		bump("not-exists-multikey", strings.Contains(sql, "NOT EXISTS") && correlatedKeys(sql) >= 2)
		// A multi-key correlation whose two sides carry DIFFERENT bare names.
		// The self-correlated form cannot make dedupSemiAntiBuildSide fire —
		// `sub.x = t0.x` resolves on both sides, so the pass declines — so
		// without this arm the fuzzer generates the shape and never reaches
		// the code path it lives on.
		bump("exists-multikey-distinct-names", multiKeyDistinctNames(sql))
		bump("in-subquery", strings.Contains(sql, "IN (SELECT"))
		bump("scalar-subquery", strings.Contains(sql, "(SELECT AVG"))
		bump("group-by", len(q.GroupBy) > 0)
		bump("group-by-ordinal", groupByOrdinal(q))
		bump("having", q.Having != "")
		// The HAVING predicate SHAPES, separately: `agg > N` was the only one
		// this generator emitted, so the arms of the differential that found
		// #591 — a NULL check, a negation, a connective, a range, and a bare
		// boolean aggregate — were dormant.
		bump("having-null-check", q.Having != "" && strings.Contains(q.Having, "IS ") &&
			strings.Contains(q.Having, "NULL"))
		bump("having-negated", strings.HasPrefix(q.Having, "NOT ("))
		bump("having-connective", q.Having != "" &&
			(strings.Contains(q.Having, " AND ") || strings.Contains(q.Having, " OR ")))
		bump("having-range", q.Having != "" &&
			(strings.Contains(q.Having, " BETWEEN ") || strings.Contains(q.Having, " IN (")))
		bump("having-bare-agg", strings.HasPrefix(q.Having, "BOOL_OR(") ||
			strings.HasPrefix(q.Having, "BOOL_AND("))
		// #621: an aggregate over a compile-time LITERAL argument — MIN(1),
		// SUM(0), COUNT(7). Every other aggregate arm draws its argument from a
		// column or an expression, so before the constant-argument arm this
		// generator could not emit the shape the soak reduced #621 to.
		bump("const-arg-agg", constArgAgg.MatchString(sql))
		bump("distinct", q.Distinct)
		bump("count-distinct", strings.Contains(sql, "COUNT(DISTINCT"))
		bump("count-case", strings.Contains(sql, "COUNT(CASE"))
		bump("order-by", len(q.Order) > 0)
		bump("order-by-ordinal", orderByOrdinal(q))
		bump("order-by-alias", orderByAlias(q))
		bump("order-by-expr", orderByExpr(q))
		bump("order-by-hidden", orderByHidden(q))
		bump("order-desc", orderDesc(q))
		bump("limit", q.Limit > 0)
		bump("offset", q.Offset > 0)
		// The two boundaries of this clause the generator could not reach
		// (#487). `LIMIT 0` has one right answer under every ordering, and a
		// BARE `OFFSET n` — no LIMIT beside it — is the last-page shape a
		// paginating client sends.
		bump("limit-zero", q.LimitZero)
		bump("limit-zero-sql", strings.HasSuffix(sql, " LIMIT 0"))
		bump("bare-offset", q.Offset > 0 && q.Limit == 0 && !q.LimitZero)
		bump("total-order", q.TotalOrder)
		bump("date-extract", strings.Contains(sql, "EXTRACT("))
		bump("date-cast", strings.Contains(sql, "AS DATE)"))
		bump("is-null", strings.Contains(sql, "IS NULL") || strings.Contains(sql, "IS NOT NULL"))
		bump("like", strings.Contains(sql, " LIKE "))
		bump("between", strings.Contains(sql, " BETWEEN "))
		bump("in-list", strings.Contains(sql, " IN (") && !strings.Contains(sql, "IN (SELECT"))
		bump("case-when", strings.Contains(sql, "CASE WHEN"))
		bump("coalesce", strings.Contains(sql, "COALESCE("))
		bump("cmp-unordered", q.CompareSpec().Mode == oracle.CmpUnordered)
		bump("cmp-ordered", q.CompareSpec().Mode == oracle.CmpOrdered)
		bump("cmp-count", q.CompareSpec().Mode == oracle.CmpCount)
	}
	for _, k := range []string{
		"shape:projection", "shape:alias-shadow", "shape:selfjoin", "shape:joinchain",
		"shape:outerjoin", "shape:groupagg", "shape:distinct", "shape:subquery",
		"shape:dates", "shape:star",
		"qualified-ref", "bare-ref", "quoted-ident", "star", "self-join", "left-join",
		"comma-join", "comma-join-mixed", "chain-4", "derived", "cte", "exists", "in-subquery",
		"scalar-subquery", "group-by", "group-by-ordinal", "having", "distinct",
		"having-null-check", "having-negated", "having-connective", "having-range",
		"having-bare-agg", "const-arg-agg",
		"count-distinct", "count-case", "order-by", "order-by-ordinal", "order-by-alias",
		"order-by-expr", "order-by-hidden", "order-desc", "limit", "offset", "total-order",
		"date-extract", "date-cast", "is-null", "like", "between", "in-list", "case-when",
		"coalesce", "cmp-unordered", "cmp-ordered", "cmp-count",
		"limit-zero", "limit-zero-sql", "bare-offset",
		"exists-multikey", "exists-threekey", "not-exists-multikey",
		"exists-multikey-distinct-names",
	} {
		if counts[k] == 0 {
			t.Errorf("shape %q never generated in 3000 seeds — that arm of the differential is dormant", k)
		}
	}
	if os.Getenv("WADJET_SHAPEGEN_COVERAGE") == "1" {
		for k, v := range counts {
			fmt.Printf("%-24s %d\n", k, v)
		}
	}
}

// TestLimitZeroAndBareOffsetRender is #487's gate: the SQL for the two
// boundary shapes, and the comparison LIMIT 0 earns.
//
// `LIMIT 0` is the one limit whose answer does not depend on the ordering —
// no rows, on every engine — so it is compared row-for-row even when the
// query has no total order, which every other LIMIT drops to a count for. A
// query that returned rows for it would otherwise be judged only by a count
// the generator had already given up on.
func TestLimitZeroAndBareOffsetRender(t *testing.T) {
	q := &Query{
		Items:     []Item{{Expr: "id", Alias: "id"}},
		From:      []From{{Table: "t0", Alias: "t0"}},
		LimitZero: true,
	}
	if got := q.SQL(); !strings.HasSuffix(got, " LIMIT 0") {
		t.Errorf("LimitZero rendered %q, want a trailing LIMIT 0", got)
	}
	if mode := q.CompareSpec().Mode; mode != oracle.CmpOrdered {
		t.Errorf("LIMIT 0 compares as %v; it has ONE right answer under every "+
			"ordering, so it must be compared exactly", mode)
	}
	if lim := q.CompareSpec().Limit; lim != 0 {
		t.Errorf("LIMIT 0 set spec.Limit = %d; Compare treats a positive Limit as a "+
			"row-count ceiling, and 0 is not one", lim)
	}

	q.LimitZero, q.Offset = false, 5
	if got := q.SQL(); !strings.HasSuffix(got, " OFFSET 5") {
		t.Errorf("a bare Offset rendered %q, want a trailing OFFSET 5", got)
	}
	q.LimitZero = true
	if got := q.SQL(); !strings.HasSuffix(got, " LIMIT 0 OFFSET 5") {
		t.Errorf("LimitZero with an Offset rendered %q", got)
	}

	// The shrinker offers removing the LIMIT 0, and every candidate that
	// clears Limit clears it too — otherwise a shrink step meant to drop the
	// clause would leave `LIMIT 0` in the "reduced" query.
	q.Offset = 0
	sawWithout := false
	for _, c := range candidates(TPCH(), q) {
		if !c.LimitZero {
			sawWithout = true
		}
	}
	if !sawWithout {
		t.Error("the shrinker offers no candidate without the LIMIT 0, so a repro that " +
			"does not need it cannot be reduced past it")
	}
}

// TestCompareSpecIsSoundForLimits pins the determinism contract: a LIMIT is
// only compared row-for-row when the generator made the ordering total, and a
// query with no ORDER BY is never compared positionally.
func TestCompareSpecIsSoundForLimits(t *testing.T) {
	s := TPCH()
	for seed := int64(1); seed <= 3000; seed++ {
		q := New(seed, s).Query()
		spec := q.CompareSpec()
		if len(q.Order) == 0 && spec.Mode == oracle.CmpOrdered {
			t.Fatalf("seed %d: no ORDER BY but compared positionally: %s", seed, q.SQL())
		}
		if q.Limit > 0 && spec.Mode == oracle.CmpOrdered && !q.TotalOrder {
			t.Fatalf("seed %d: LIMIT compared row-for-row without a total order: %s", seed, q.SQL())
		}
		if spec.Mode == oracle.CmpUnordered && q.Limit > 0 {
			t.Fatalf("seed %d: LIMIT compared as a multiset: %s", seed, q.SQL())
		}
		for _, k := range spec.OrderKeys {
			if !projectsAlias(q, k.Alias) {
				t.Fatalf("seed %d: order key %q is not projected: %s", seed, k.Alias, q.SQL())
			}
		}
	}
}

func TestShrinkReducesToMinimal(t *testing.T) {
	s := TPCH()
	var q *Query
	for seed := int64(1); ; seed++ {
		q = New(seed, s).Query()
		if len(q.Where) >= 2 && len(q.Items) >= 2 && len(q.Order) > 0 {
			break
		}
	}
	// Failure predicate: "still selects something". Shrink must strip
	// everything not needed to keep that true.
	fails := func(c *Query) bool { return len(c.Items) > 0 }
	min := Shrink(s, q, fails)
	if !fails(min) {
		t.Fatal("shrunk query no longer reproduces")
	}
	if len(min.Items) != 1 || len(min.Where) != 0 || min.Having != "" || min.Distinct ||
		min.Limit != 0 || len(min.Order) != 0 {
		t.Fatalf("not minimal: %s", min.SQL())
	}
}

// TestShrinkNeverStrengthensComparison pins the shrinker's soundness rule: a
// reduction may never claim a stronger comparison than the query it came from.
// Dropping an ORDER BY term can drop the uniqueness tiebreaker, and comparing a
// no-longer-total order positionally reports tie order as a divergence — a
// false positive manufactured by the shrinker itself.
func TestShrinkNeverStrengthensComparison(t *testing.T) {
	s := TPCH()
	checked := 0
	for seed := int64(1); seed <= 500; seed++ {
		q := New(seed, s).Query()
		for _, c := range candidates(s, q) {
			if len(c.Order) < len(q.Order) && c.TotalOrder {
				t.Fatalf("seed %d: reduction dropped an ORDER BY term but kept TotalOrder:\n  from: %s\n  to:   %s",
					seed, q.SQL(), c.SQL())
			}
			if len(c.Order) > 0 && len(q.Order) > 0 && c.CompareSpec().Mode == oracle.CmpOrdered &&
				q.CompareSpec().Mode != oracle.CmpOrdered {
				t.Fatalf("seed %d: reduction compares positionally while the original did not:\n  from: %s\n  to:   %s",
					seed, q.SQL(), c.SQL())
			}
			if !wellFormed(c) {
				t.Fatalf("seed %d: reduction is not a valid query:\n  %s", seed, c.SQL())
			}
			checked++
		}
		if !wellFormed(q) {
			t.Fatalf("seed %d: generator emitted an invalid query:\n  %s", seed, q.SQL())
		}
	}
	if checked == 0 {
		t.Fatal("no reductions examined")
	}
}

func groupByOrdinal(q *Query) bool {
	for _, g := range q.GroupBy {
		if _, ok := atoi(g); ok {
			return true
		}
	}
	return false
}

func orderByOrdinal(q *Query) bool {
	for _, o := range q.Order {
		if _, ok := atoi(o.Expr); ok {
			return true
		}
	}
	return false
}

func orderByAlias(q *Query) bool {
	for _, o := range q.Order {
		if o.Key != "" && o.Expr == o.Key {
			return true
		}
	}
	return false
}

func orderByExpr(q *Query) bool {
	for _, o := range q.Order {
		if o.Key != "" && o.Expr != o.Key {
			if _, ok := atoi(o.Expr); !ok {
				return true
			}
		}
	}
	return false
}

func orderByHidden(q *Query) bool {
	for _, o := range q.Order {
		if o.Key == "" {
			return true
		}
	}
	return false
}

func orderDesc(q *Query) bool {
	for _, o := range q.Order {
		if o.Desc {
			return true
		}
	}
	return false
}

func projectsAlias(q *Query, alias string) bool {
	for _, it := range q.Items {
		if it.Alias == alias {
			return true
		}
	}
	return false
}

// correlatedKeys counts the `sub.x = tN.x` conjuncts inside a generated
// EXISTS — the number of columns its correlation is keyed on.
func correlatedKeys(sql string) int {
	i := strings.Index(sql, "EXISTS (SELECT 1 FROM ")
	if i < 0 {
		return 0
	}
	body := sql[i:]
	if j := strings.Index(body, ")"); j >= 0 {
		body = body[:j]
	}
	n := 0
	for _, conj := range strings.Split(body, " AND ") {
		if strings.Contains(conj, "sub.") && strings.Count(conj, ".") >= 2 && strings.Contains(conj, " = ") {
			n++
		}
	}
	return n
}

// multiKeyDistinctNames reports whether a generated EXISTS correlates on two
// or more columns AND at least one conjunct compares two DIFFERENT bare names
// — the shape whose keys the build-side narrowing can attribute.
func multiKeyDistinctNames(sql string) bool {
	i := strings.Index(sql, "EXISTS (SELECT 1 FROM ")
	if i < 0 {
		return false
	}
	body := sql[i:]
	if j := strings.Index(body, ")"); j >= 0 {
		body = body[:j]
	}
	keys, distinct := 0, false
	for _, conj := range strings.Split(body, " AND ") {
		l, r, ok := strings.Cut(conj, " = ")
		if !ok || !strings.Contains(l, "sub.") {
			continue
		}
		keys++
		lb := l[strings.LastIndexByte(l, '.')+1:]
		rb := r[strings.LastIndexByte(r, '.')+1:]
		if !strings.EqualFold(strings.TrimSpace(lb), strings.TrimSpace(rb)) {
			distinct = true
		}
	}
	return keys >= 2 && distinct
}

// commaJoins counts the FROM entries joined by a comma, and explicitJoins the
// ones joined by a JOIN keyword. The leading entry carries neither.
func commaJoins(q *Query) int {
	n := 0
	for _, f := range q.From {
		if f.Join == "," {
			n++
		}
	}
	return n
}

func explicitJoins(q *Query) int {
	n := 0
	for _, f := range q.From {
		if f.Join != "" && f.Join != "," {
			n++
		}
	}
	return n
}
