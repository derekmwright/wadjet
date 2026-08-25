package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// groupingFixture is the table #590 and #591 minimized to, one row set that
// serves both: four groups whose aggregates disagree about which of them they
// select. Group 1 has both boolean values and two non-NULL v; group 2 is
// all-TRUE with every v NULL, so MAX(v)/MIN(v)/SUM(v) are NULL for exactly one
// group; group 3's only flag is NULL, so BOOL_OR and BOOL_AND are UNKNOWN
// there and TLP's `p IS NULL` arm is non-empty; group 4 is all-FALSE.
func groupingFixture(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "flag", Type: parquet.TypeBool, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "g", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := []map[string]any{
		{"k": int64(1), "flag": true, "v": int64(10)},
		{"k": int64(1), "flag": false, "v": int64(20)},
		{"k": int64(2), "flag": true, "v": nil},
		{"k": int64(2), "flag": true, "v": nil},
		{"k": int64(3), "flag": nil, "v": int64(5)},
		{"k": int64(4), "flag": false, "v": int64(7)},
	}
	ing := db.NewIngester("g", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

// An ungrouped, non-aggregated reference under a GROUP BY is 42803. Before
// #590 the engine ANSWERED all of these: the SELECT-list column was replaced
// in place by the grouping key (two columns asked for, one returned, and not
// the one named), and a bare ungrouped column in HAVING excluded every group,
// so all three arms of a TLP partition returned zero rows and summed to zero
// instead of to the unfiltered answer.
func TestGroupedQueryRefusesUngroupedColumn(t *testing.T) {
	ctx := context.Background()
	db := groupingFixture(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"SelectList", `SELECT k, flag FROM g GROUP BY k`},
		{"SelectListBesideAggregate", `SELECT COUNT(*), flag FROM g GROUP BY k`},
		{"SelectListInsideExpression", `SELECT k, v + 1 FROM g GROUP BY k`},
		{"HavingBareColumn", `SELECT k FROM g GROUP BY k HAVING flag`},
		{"HavingNegatedColumn", `SELECT k FROM g GROUP BY k HAVING NOT flag`},
		{"HavingColumnIsNull", `SELECT k FROM g GROUP BY k HAVING flag IS NULL`},
		{"HavingComparison", `SELECT k FROM g GROUP BY k HAVING v > 100`},
		{"OrderByColumn", `SELECT k FROM g GROUP BY k ORDER BY v`},
		{"HavingWithoutGroupBy", `SELECT k FROM g HAVING COUNT(*) > 1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("answered a statement PostgreSQL refuses with 42803: %d rows %v\n  SQL: %s",
					len(res.Rows), res.Rows, tc.sql)
			}
			if got := sqlerr.StateOf(err); got != "42803" {
				t.Errorf("SQLSTATE = %q, want 42803 grouping_error (err: %v)\n  SQL: %s", got, err, tc.sql)
			}
		})
	}
}

// The other half of the same rule, and the more expensive one to get wrong: a
// grouped query that stays inside its grouped expressions has an answer, and
// the refusal must not touch it.
func TestGroupedQueryAcceptsGroupedReferences(t *testing.T) {
	ctx := context.Background()
	db := groupingFixture(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		rows int
	}{
		{"GroupedColumn", `SELECT k, COUNT(*) AS c FROM g GROUP BY k`, 4},
		{"ExpressionOverKey", `SELECT k + 1 AS k1, COUNT(*) AS c FROM g GROUP BY k`, 4},
		{"QualifiedKeyInSelect", `SELECT g.k, COUNT(*) AS c FROM g GROUP BY k`, 4},
		{"QualifiedKeyInGroupBy", `SELECT k, COUNT(*) AS c FROM g GROUP BY g.k`, 4},
		{"GroupByOrdinal", `SELECT k, COUNT(*) AS c FROM g GROUP BY 1`, 4},
		{"GroupByOutputAlias", `SELECT k AS r, COUNT(*) AS c FROM g GROUP BY r`, 4},
		{"GroupByExpressionAlias", `SELECT k + 1 AS r, COUNT(*) AS c FROM g GROUP BY r`, 4},
		{"KeyNotSelected", `SELECT COUNT(*) AS c FROM g GROUP BY k`, 4},
		{"ExtraKeyNotSelected", `SELECT k FROM g GROUP BY k, flag`, 5},
		{"AggregateOverUngroupedColumn", `SELECT k, MAX(v) AS mx FROM g GROUP BY k`, 4},
		{"OrderBySelectedAggregate", `SELECT k, MAX(v) AS mx FROM g GROUP BY k ORDER BY MAX(v)`, 4},
		{"OrderBySelectAlias", `SELECT k, COUNT(*) AS c FROM g GROUP BY k ORDER BY c`, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("refused a legal grouped query: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) != tc.rows {
				t.Errorf("got %d rows, want %d: %v\n  SQL: %s", len(res.Rows), tc.rows, res.Rows, tc.sql)
			}
		})
	}
}

// #591 repro 2: `HAVING <agg> IS [NOT] NULL` used to ignore the aggregate's
// value entirely — every group satisfied IS NULL and none satisfied IS NOT
// NULL — because the HAVING lowering's aggregate walk stopped at every node
// type outside the arithmetic/CASE/CAST core. The aggregate was therefore
// never registered on the Aggregate node, and the filter above it tested a
// column that did not exist.
//
// The predicates below are grouped in TLP triples wherever the shape allows
// it: `p`, `NOT p` and `p IS NULL` must partition the four groups, which is
// the invariant the SQLancer soak asserted 468 times in one run.
func TestHavingOverAggregateReadsTheAggregatesValue(t *testing.T) {
	ctx := context.Background()
	db := groupingFixture(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		want []int64
	}{
		// BOOL_OR: TRUE for {1,2}, FALSE for {4}, UNKNOWN for {3}.
		{"BoolOrBare", `SELECT k FROM g GROUP BY k HAVING BOOL_OR(flag)`, []int64{1, 2}},
		{"BoolOrNegated", `SELECT k FROM g GROUP BY k HAVING NOT BOOL_OR(flag)`, []int64{4}},
		{"BoolOrIsNull", `SELECT k FROM g GROUP BY k HAVING (BOOL_OR(flag)) IS NULL`, []int64{3}},
		// BOOL_AND: TRUE for {2}, FALSE for {1,4}, UNKNOWN for {3}.
		{"BoolAndBare", `SELECT k FROM g GROUP BY k HAVING BOOL_AND(flag)`, []int64{2}},
		{"BoolAndNegated", `SELECT k FROM g GROUP BY k HAVING NOT BOOL_AND(flag)`, []int64{1, 4}},
		{"BoolAndIsNull", `SELECT k FROM g GROUP BY k HAVING (BOOL_AND(flag)) IS NULL`, []int64{3}},
		// MAX/MIN/SUM are NULL for group 2 alone.
		{"MaxIsNull", `SELECT k FROM g GROUP BY k HAVING MAX(v) IS NULL`, []int64{2}},
		{"MaxIsNotNull", `SELECT k FROM g GROUP BY k HAVING MAX(v) IS NOT NULL`, []int64{1, 3, 4}},
		{"MinIsNull", `SELECT k FROM g GROUP BY k HAVING MIN(v) IS NULL`, []int64{2}},
		{"MinIsNotNull", `SELECT k FROM g GROUP BY k HAVING MIN(v) IS NOT NULL`, []int64{1, 3, 4}},
		{"SumIsNull", `SELECT k FROM g GROUP BY k HAVING SUM(v) IS NULL`, []int64{2}},
		{"SumIsNotNull", `SELECT k FROM g GROUP BY k HAVING SUM(v) IS NOT NULL`, []int64{1, 3, 4}},
		// COUNT is never NULL, which is the same lowering asked the opposite
		// question.
		{"CountIsNull", `SELECT k FROM g GROUP BY k HAVING COUNT(v) IS NULL`, nil},
		{"CountIsNotNull", `SELECT k FROM g GROUP BY k HAVING COUNT(v) IS NOT NULL`, []int64{1, 2, 3, 4}},
		// Comparisons, negations and the connective/range forms — AND and OR
		// used to fail outright ("filter column \"count(*)\" does not exist
		// in the input schema"), IN and BETWEEN returned nothing.
		{"CountComparison", `SELECT k FROM g GROUP BY k HAVING COUNT(*) > 1`, []int64{1, 2}},
		{"CountComparisonNegated", `SELECT k FROM g GROUP BY k HAVING NOT (COUNT(*) > 1)`, []int64{3, 4}},
		{"SumComparison", `SELECT k FROM g GROUP BY k HAVING SUM(v) > 15`, []int64{1}},
		{"Conjunction", `SELECT k FROM g GROUP BY k HAVING COUNT(*) > 1 AND MAX(v) > 0`, []int64{1}},
		{"Disjunction", `SELECT k FROM g GROUP BY k HAVING COUNT(*) > 1 OR MAX(v) > 6`, []int64{1, 2, 4}},
		{"AggregateIn", `SELECT k FROM g GROUP BY k HAVING MAX(v) IN (20, 5)`, []int64{1, 3}},
		{"AggregateBetween", `SELECT k FROM g GROUP BY k HAVING MAX(v) BETWEEN 1 AND 9`, []int64{3, 4}},
		// The aggregate the SELECT list already computes.
		{"ReusesSelectedAggregate", `SELECT k FROM g GROUP BY k HAVING COUNT(*) > 1`, []int64{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := groupKeys(t, db, ctx, tc.sql)
			if !equalInt64s(got, tc.want) {
				t.Errorf("got %v, want %v\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}

// TLP's own invariant, asserted directly: for every predicate shape, the
// three arms partition the ungated GROUP BY. This is the assertion the
// SQLancer soak makes, and stating it here means a future regression fails in
// `go test` rather than only under a running soak.
func TestHavingPartitionsTheUngatedGroups(t *testing.T) {
	ctx := context.Background()
	db := groupingFixture(t, ctx)

	all := groupKeys(t, db, ctx, `SELECT k FROM g GROUP BY k`)
	if len(all) != 4 {
		t.Fatalf("the ungated GROUP BY must have 4 groups, got %v", all)
	}

	for _, p := range []string{
		"BOOL_OR(flag)",
		"BOOL_AND(flag)",
		"MAX(v) > 6",
		"COUNT(*) > 1",
		"SUM(v) > 15",
		"MIN(v) IN (5, 7)",
	} {
		t.Run(p, func(t *testing.T) {
			var union []int64
			for _, arm := range []string{
				fmt.Sprintf("HAVING (%s)", p),
				fmt.Sprintf("HAVING NOT (%s)", p),
				fmt.Sprintf("HAVING (%s) IS NULL", p),
			} {
				union = append(union, groupKeys(t, db, ctx, "SELECT k FROM g GROUP BY k "+arm)...)
			}
			sort.Slice(union, func(i, j int) bool { return union[i] < union[j] })
			if !equalInt64s(union, all) {
				t.Errorf("the three arms of %q do not partition the ungated GROUP BY:\n"+
					"  union %v\n  groups %v", p, union, all)
			}
		})
	}
}

// #591 repro 1: the synthetic column the HAVING lowering creates for an
// aggregate the SELECT list does not carry must never reach the client. The
// projection above the aggregate was elided as redundant whenever no item
// needed COMPUTING — a check that never asked whether the aggregate's output
// was the answer's SHAPE — so `__having_0` was published in the result. The
// same elision leaked a grouping key the SELECT list did not ask for.
func TestGroupedResultCarriesOnlyTheSelectedColumns(t *testing.T) {
	ctx := context.Background()
	db := groupingFixture(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		{"HavingBareAggregate", `SELECT k FROM g GROUP BY k HAVING BOOL_OR(flag)`, []string{"k"}},
		{"HavingComparison", `SELECT k FROM g GROUP BY k HAVING MAX(v) > 0`, []string{"k"}},
		{"HavingNullCheck", `SELECT k FROM g GROUP BY k HAVING MAX(v) IS NOT NULL`, []string{"k"}},
		{"HavingTwoAggregates", `SELECT k FROM g GROUP BY k HAVING COUNT(*) > 1 AND MAX(v) > 0`, []string{"k"}},
		{"HavingBesideSelectedAggregate",
			`SELECT k, COUNT(*) AS c FROM g GROUP BY k HAVING MAX(v) > 0`, []string{"k", "c"}},
		{"HavingReusesSelectedAggregate",
			`SELECT k, COUNT(*) AS c FROM g GROUP BY k HAVING COUNT(*) > 1`, []string{"k", "c"}},
		{"GroupedKeyNotSelected", `SELECT k FROM g GROUP BY k, flag`, []string{"k"}},
		{"NoKeySelected", `SELECT COUNT(*) AS c FROM g GROUP BY k`, []string{"c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) == 0 {
				t.Fatalf("this entry needs rows to inspect; got none\n  SQL: %s", tc.sql)
			}
			// Columns is derived from the SELECT list, so it agreed even
			// while the batch carried the extra column. The row map is what
			// the wire path reads.
			for _, row := range res.Rows {
				if len(row) != len(tc.want) {
					t.Fatalf("a result row carries %d columns %v, the SELECT list asked for %d %v\n  SQL: %s",
						len(row), keysOf(row), len(tc.want), tc.want, tc.sql)
				}
				for _, name := range tc.want {
					if _, ok := row[name]; !ok {
						t.Fatalf("row %v is missing the selected column %q\n  SQL: %s", row, name, tc.sql)
					}
				}
			}
			if len(res.Columns) != len(tc.want) {
				t.Errorf("Columns = %v, want %v\n  SQL: %s", res.Columns, tc.want, tc.sql)
			}
		})
	}
}

// An aggregate reached through a NULL check, a negation or a boolean
// connective is still an aggregate — the SELECT list is the same walker gap
// one clause over, where it produced a constant instead of a computed value.
func TestSelectListAggregateUnderANullCheck(t *testing.T) {
	ctx := context.Background()
	db := groupingFixture(t, ctx)

	res, err := db.Query(ctx, `SELECT k, MAX(v) IS NULL AS mx_null, NOT BOOL_OR(flag) AS none_set FROM g GROUP BY k`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	wantNull := map[int64]bool{1: false, 2: true, 3: false, 4: false}
	wantNone := map[int64]any{1: false, 2: false, 3: nil, 4: true}
	if len(res.Rows) != 4 {
		t.Fatalf("got %d rows, want 4: %v", len(res.Rows), res.Rows)
	}
	for _, row := range res.Rows {
		k := row["k"].(int64)
		if got := row["mx_null"]; got != wantNull[k] {
			t.Errorf("k=%d: MAX(v) IS NULL = %v, want %v", k, got, wantNull[k])
		}
		if got := row["none_set"]; got != wantNone[k] {
			t.Errorf("k=%d: NOT BOOL_OR(flag) = %v, want %v", k, got, wantNone[k])
		}
	}
}

func groupKeys(t *testing.T, db *DB, ctx context.Context, sql string) []int64 {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query: %v\n  SQL: %s", err, sql)
	}
	out := make([]int64, 0, len(res.Rows))
	for _, r := range res.Rows {
		switch v := r["k"].(type) {
		case int64:
			out = append(out, v)
		case int32:
			out = append(out, int64(v))
		default:
			t.Fatalf("k came back as %T (%v)", r["k"], r["k"])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func keysOf(row map[string]any) []string {
	out := make([]string, 0, len(row))
	for k := range row {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
