package tpch

import (
	"context"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/wadjet"
)

// scalarInt reads the single integer cell of a one-row, one-column result.
func scalarInt(t *testing.T, res *wadjet.QueryResult) int64 {
	t.Helper()
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	for _, v := range res.Rows[0] {
		switch n := v.(type) {
		case int64:
			return n
		case int32:
			return int64(n)
		case int:
			return int64(n)
		case float64:
			return int64(n)
		default:
			t.Fatalf("count came back as %T (%v), want an integer", v, v)
		}
	}
	t.Fatal("result row had no columns")
	return 0
}

// TestScalarSubqueryOnParallelPipeline is the end-to-end regression for issue
// #334: an ordinary "rows above the average" query killed the whole process.
//
//	SELECT COUNT(*) FROM customer c1
//	WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer)
//
//	fatal error: concurrent map writes
//	created by ...exec.(*Pipeline).runParallel
//
// That is a runtime throw, not a panic — recover() cannot catch it, so in
// server mode the process died and took every other connection with it.
//
// The fixture matters. The crash needs an outer table big enough for the
// planner to fan the pipeline out across worker goroutines: customer at 1500
// rows is, nation at 25 and supplier at 100 are not, which is why the existing
// small-fixture tests never saw it. The table used here is therefore load
// bearing — do not shrink it.
//
// Run under `go test -race` as well: the same path reports a write/write race
// on the planner's scan-alias counter in Planner.buildScan.
func TestScalarSubqueryOnParallelPipeline(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	// Ground truth, computed without a subquery so it cannot share a defect
	// with the shapes under test.
	avg, err := db.Query(ctx, "SELECT AVG(c_acctbal) AS a FROM customer")
	if err != nil {
		t.Fatalf("computing the average: %v", err)
	}
	if len(avg.Rows) != 1 {
		t.Fatalf("average query returned %d rows, want 1", len(avg.Rows))
	}
	var threshold float64
	for _, v := range avg.Rows[0] {
		f, ok := v.(float64)
		if !ok {
			t.Fatalf("average came back as %T, want float64", v)
		}
		threshold = f
	}

	want, err := db.Query(ctx, "SELECT COUNT(*) AS n FROM customer WHERE c_acctbal > "+
		strconv.FormatFloat(threshold, 'f', -1, 64))
	if err != nil {
		t.Fatalf("computing the expected count: %v", err)
	}
	expected := scalarInt(t, want)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			// The reported query: outer table aliased, inner column
			// unqualified. This is the one that crashed.
			name: "aliased outer, unqualified inner",
			sql:  "SELECT COUNT(*) AS n FROM customer c1 WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer)",
		},
		{
			// The issue's own simplification. It escaped the crash only
			// because the unaliased outer table's identifier happened to match
			// the inner table's name.
			name: "unaliased outer, unqualified inner",
			sql:  "SELECT COUNT(*) AS n FROM customer WHERE c_acctbal > (SELECT AVG(c_acctbal) FROM customer)",
		},
		{
			// The workaround named in the issue — proof the binding, not the
			// query, was the problem.
			name: "aliased inner",
			sql:  "SELECT COUNT(*) AS n FROM customer c1 WHERE c1.c_acctbal > (SELECT AVG(sq.c_acctbal) FROM customer sq)",
		},
		{
			// Same shape on a larger table, so a regression that only shows up
			// with more batches in flight is caught too.
			name: "orders",
			sql:  "SELECT COUNT(*) AS n FROM orders o1 WHERE o1.o_totalprice > (SELECT AVG(o_totalprice) FROM orders)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			got := scalarInt(t, res)
			if tc.name == "orders" {
				// No pre-computed ground truth for this one; the point is that
				// it runs at all and returns a plausible non-empty answer.
				if got <= 0 {
					t.Fatalf("got %d rows above the average, want > 0", got)
				}
				return
			}
			if got != expected {
				t.Fatalf("got %d rows above the average, want %d", got, expected)
			}
		})
	}
}

// TestCorrelatedScalarSubqueryStillWorks guards against the scoping fix
// over-correcting: a subquery that genuinely references the outer row must
// still be evaluated per row and still produce the right answer.
func TestCorrelatedScalarSubqueryStillWorks(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	// Per-nation average, correlated on the outer row's nation.
	res, err := db.Query(ctx,
		"SELECT COUNT(*) AS n FROM customer c1 WHERE c1.c_acctbal > "+
			"(SELECT AVG(c2.c_acctbal) FROM customer c2 WHERE c2.c_nationkey = c1.c_nationkey)")
	if err != nil {
		t.Fatalf("correlated query: %v", err)
	}
	got := scalarInt(t, res)

	// Equivalent formulation without a correlated subquery.
	want, err := db.Query(ctx,
		"SELECT COUNT(*) AS n FROM customer c JOIN "+
			"(SELECT c_nationkey AS nk, AVG(c_acctbal) AS avg_bal FROM customer GROUP BY c_nationkey) a "+
			"ON c.c_nationkey = a.nk WHERE c.c_acctbal > a.avg_bal")
	if err != nil {
		t.Fatalf("join formulation: %v", err)
	}
	if expected := scalarInt(t, want); got != expected {
		t.Fatalf("correlated subquery counted %d, join formulation counted %d", got, expected)
	}
	if got <= 0 {
		t.Fatalf("correlated subquery counted %d rows, want > 0 — a dropped outer "+
			"reference makes the inner query empty and every comparison NULL", got)
	}
}
