package shapegen

import (
	"fmt"
	"os"
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
		bump("comma-join", strings.Contains(sql, ", ") && strings.Contains(sql, " FROM "))
		bump("chain-4", len(q.From) >= 4)
		bump("derived", strings.Contains(sql, "FROM ("))
		bump("cte", strings.HasPrefix(sql, "WITH "))
		bump("exists", strings.Contains(sql, "EXISTS"))
		bump("in-subquery", strings.Contains(sql, "IN (SELECT"))
		bump("scalar-subquery", strings.Contains(sql, "(SELECT AVG"))
		bump("group-by", len(q.GroupBy) > 0)
		bump("group-by-ordinal", groupByOrdinal(q))
		bump("having", q.Having != "")
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
		"comma-join", "chain-4", "derived", "cte", "exists", "in-subquery",
		"scalar-subquery", "group-by", "group-by-ordinal", "having", "distinct",
		"count-distinct", "count-case", "order-by", "order-by-ordinal", "order-by-alias",
		"order-by-expr", "order-by-hidden", "order-desc", "limit", "offset", "total-order",
		"date-extract", "date-cast", "is-null", "like", "between", "in-list", "case-when",
		"coalesce", "cmp-unordered", "cmp-ordered", "cmp-count",
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
