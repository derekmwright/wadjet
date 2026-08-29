package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestDecimalNaNLiteralTwoPath holds the single-process engine and the stage
// DAG to one answer for #534's shapes, and holds both to what live
// postgres:17-alpine answers on the identical nine rows.
//
// The two arms reach the comparison through different code and would have
// diverged on their own. The single-process path vectorizes the filter
// (kernel.compareFilterDecimal, resolving the literal through
// batch.DecimalBoundTextAt against the column's scale) and can also prune a row
// group ahead of it (kernel.StatsDomainValue, which WITHHOLDS for these three
// so the prune declines rather than guesses). The DAG re-parses the filter
// text in a later stage and always compiles to the row-at-a-time evaluator
// (expr.decimalLitCmp), whose accept-set is a separate cached slice. A widening
// that reached one and not the other would answer 188 rows on one path and
// raise 22P02 on the other for the same query — the two-path defect class this
// package exists to close.
//
// The counts are PostgreSQL's, taken live on the same rows loaded as
// numeric(9,2)/numeric(18,4): a is non-NULL on 7 of the 9 rows, b on 7.
func TestDecimalNaNLiteralTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		pred string
		want int64
	}{
		// NaN is above every value a DECIMAL column can hold, so it equals
		// nothing and every non-NULL row is below it.
		{"eq_nan", "a = 'NaN'", 0},
		{"ne_nan", "a <> 'NaN'", 7},
		{"lt_nan", "a < 'NaN'", 7},
		{"le_nan", "a <= 'NaN'", 7},
		{"gt_nan", "a > 'NaN'", 0},
		{"ge_nan", "a >= 'NaN'", 0},
		// Infinity sits at the same end, -Infinity at the other.
		{"eq_infinity", "a = 'Infinity'", 0},
		{"le_infinity", "a <= 'Infinity'", 7},
		{"gt_neg_infinity", "a > '-Infinity'", 7},
		{"lt_neg_infinity", "a < '-Infinity'", 0},
		{"between_infinities", "a BETWEEN '-Infinity' AND 'Infinity'", 7},
		// An IN list keeps its real members and drops the special, which
		// equals nothing; NOT IN over the special alone excludes no row.
		{"in_nan_and_value", "a IN ('NaN', 12.75)", 4},
		{"not_in_nan", "a NOT IN ('NaN')", 7},
		{"not_in_nan_and_value", "a NOT IN ('NaN', 12.75)", 3},
		// The short and lower-case spellings PostgreSQL accepts, on both
		// columns so neither scale is the one that happens to work.
		{"eq_inf_short", "a = 'inf'", 0},
		{"eq_nan_lower", "a = 'nan'", 0},
		{"lt_Inf", "a < 'Inf'", 7},
		{"b_gt_neg_inf_short", "b > '-inf'", 7},
		{"b_lt_nan", "b < 'NaN'", 7},
		{"lt_nan_padded", "a < '  NaN  '", 7},
		{"lt_plus_infinity", "a < '+Infinity'", 7},
		// The BOXED sites, which reach the column as its rendered TEXT.
		{"simple_case_nan", "CASE a WHEN 'NaN' THEN 1 ELSE 0 END = 1", 0},
		{"is_distinct_from_nan", "a IS DISTINCT FROM 'NaN'", 9},
		{"is_not_distinct_from_nan", "a IS NOT DISTINCT FROM 'NaN'", 0},
		{"greatest_nan", "GREATEST(a, 'NaN') = 'NaN'", 9},
		{"least_neg_infinity", "LEAST(a, '-Infinity') = '-Infinity'", 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s", dbpTable, tc.pred)
			var single64, dag64 int64
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				got := ketInt(t, arm.name, rows[0]["n"])
				if arm.dag {
					dag64 = got
				} else {
					single64 = got
				}
				if got != tc.want {
					t.Errorf("%s: %s\n  got %d, want %d (live PostgreSQL 17)", arm.name, sql, got, tc.want)
				}
			}
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d", single64, dag64)
			}
		})
	}
}

// TestNonNumericDecimalLiteralIsRefusedOnBothPaths is the boundary #534's
// widening must not cross, asserted on both arms: the accept-set grew by
// exactly the NaN/±Infinity spellings PostgreSQL's numeric input takes. A
// signed NaN, a partial spelling of an infinity and ordinary garbage all stay
// the 22P02 refusal #463/#517 built, on the single-process path and on the DAG.
func TestNonNumericDecimalLiteralIsRefusedOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, lit := range []string{"abc", "+NaN", "-NaN", "NaN0", "Infin", "infinit", "- inf"} {
		t.Run(lit, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE a = '%s'", dbpTable, lit)
			_, sErr := tmdRunSingle(ctx, single, sql)
			_, dErr := tmdRunDAG(ctx, coord, sql)
			for _, arm := range []struct {
				name string
				err  error
			}{{"single-process", sErr}, {"stage DAG", dErr}} {
				if arm.err == nil {
					t.Errorf("%s ANSWERED %q instead of refusing it\n  SQL: %s", arm.name, lit, sql)
					continue
				}
				if !strings.Contains(arm.err.Error(), "invalid input syntax for type numeric") {
					t.Errorf("%s: error = %v, want PostgreSQL's numeric input-syntax error", arm.name, arm.err)
				}
			}
		})
	}
}
