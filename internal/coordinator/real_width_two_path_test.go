package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The FLOAT32 (`real`) comparison-WIDTH fixture (#631, #633).
//
// PostgreSQL does not compare a `real` column at real width just because the
// column is a real. It resolves the comparison from the LITERAL, and the two
// answers it can give are different predicates over the same rows:
//
//	real <op> <numeric literal>   ->  float8 <op> float8   -- WIDEN the column
//	real IN (lit, lit, ...)       ->  real = ANY(real[])   -- NARROW the list
//	real IN (lit)                 ->  float8 = float8      -- WIDEN (arity 1)
//
// all three read off EXPLAIN VERBOSE on postgres:17-alpine. #549 fixed the
// NARROWING half in the vectorized kernel; #631 is the WIDENING half, which
// the kernel got wrong for all six operators; #633 is that the DISTRIBUTED
// path never evaluated either through that kernel at all.
//
// That last one is the reason this gate stands up a cluster. The stage DAG
// compiles a scan-pushed filter to the ROW evaluator (worker
// compileFilterExprs -> expr.FilterPredicate), a third IN mechanism beside the
// kernel and the #524 subquery-set path, and its `expr.In` compares BOXED
// values — a FLOAT32 column boxes as float64 (ColRef.Eval), so every list
// member was compared at DOUBLE width. `real IN (0.1, 3.1)` therefore matched
// NOTHING on the DAG while the single-process kernel matched both rows, and
// the pg-oracle corpus could not see it: it runs at SF0.01, where the
// coordinator takes the in-process fast path.
//
// Every expectation below is PostgreSQL 17's, taken live over this exact
// fixture — see rwpWant's per-case citation. The two arms are held to
// PostgreSQL, not to each other, so an engine that agrees with the other arm
// and with nothing else still fails.
const rwpTable = "realwidth"

func rwpSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "r_key", Type: parquet.TypeInt64},
		{Name: "r_val", Type: parquet.TypeFloat32, Nullable: true},
		// A second REAL column holding the same values, so a column-to-column
		// comparison and a join key — neither of which has a literal to
		// resolve a width from — can be asserted to be UNMOVED by the change.
		{Name: "r_other", Type: parquet.TypeFloat32, Nullable: true},
		// The same numbers as DOUBLE. PostgreSQL compares `real = double` by
		// widening the real (Filter: r_val = d_val, no cast on either side),
		// so the rows that match are exactly the ones whose real value
		// survives the round trip.
		{Name: "d_val", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// rwpData is the fixture #549 and #631 both turn on, plus the two rows that
// make the INTEGER-literal question non-vacuous.
//
// Rows 0..15 hold real(i)+0.1, which is NOT exactly representable in float32:
// float64(float32(3.1)) != 3.1, so the width of the comparison decides whether
// row 3 answers `= 3.1`, `< 3.1` or `>= 3.1`. Rows 16..17 hold 0.5 and 1.5,
// exactly representable, the values that mask the defect. Row 18 is NULL and
// row 19 is 0.0 — the value every "read the literal as the type's zero"
// defect in this family lands on.
//
// Row 20 holds 16777216 (2^24), the first integer float32 cannot follow: the
// literal 16777217 is a plain INTEGER, exactly representable in double and
// not in real, so `r_val = 16777217` is empty under PostgreSQL's widening and
// would match row 20 under narrowing. It is the one value that proves the
// integer-literal rule without depending on a decimal point.
func rwpData() []map[string]any {
	rows := make([]map[string]any, 0, 21)
	add := func(k int64, v any) {
		rows = append(rows, map[string]any{"r_key": k, "r_val": v, "r_other": v, "d_val": nil})
	}
	for i := 0; i < 16; i++ {
		add(int64(i), float32(i)+0.1)
		rows[i]["d_val"] = float64(i) + 0.1
	}
	add(16, float32(0.5))
	rows[16]["d_val"] = 0.5
	add(17, float32(1.5))
	rows[17]["d_val"] = 1.5
	add(18, nil)
	add(19, float32(0))
	rows[19]["d_val"] = float64(0)
	add(20, float32(16777216))
	rows[20]["d_val"] = float64(16777216)
	return rows
}

// rwpCase is one predicate and the r_key set PostgreSQL answers it with.
type rwpCase struct {
	name  string
	where string
	want  []int64
}

// rwpWant is the corpus. Every `want` was produced by running the identical
// predicate against postgres:17-alpine over a table built from rwpData:
//
//	CREATE TABLE rp (r_key bigint, r_val real, r_other real, d_val double precision);
//	INSERT INTO rp SELECT i, i+0.1, i+0.1, i+0.1 FROM generate_series(0,15) i;
//	INSERT INTO rp VALUES (16,0.5,0.5,0.5),(17,1.5,1.5,1.5),(18,NULL,NULL,NULL),
//	                      (19,0,0,0),(20,16777216,16777216,16777216);
func rwpWant() []rwpCase {
	seq := func(lo, hi int64) []int64 {
		out := make([]int64, 0, hi-lo+1)
		for i := lo; i <= hi; i++ {
			out = append(out, i)
		}
		return out
	}
	join := func(parts ...[]int64) []int64 {
		out := []int64{}
		for _, p := range parts {
			out = append(out, p...)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}

	return []rwpCase{
		// --- #631: the six operators against a non-representable literal ---
		//
		// PostgreSQL WIDENS, so row 3 (float32(3)+0.1, which widens to
		// 3.0999999046325684) is BELOW 3.1, not equal to it. Under the
		// narrowing this replaces, row 3 answered `=`, `<=` and `>=` and was
		// absent from `<` — four of the six operators moved a row.
		{"EqNonRepresentable", "r_val = 3.1", nil},
		{"NeNonRepresentable", "r_val <> 3.1", join(seq(0, 17), []int64{19, 20})},
		{"LtNonRepresentable", "r_val < 3.1", []int64{0, 1, 2, 3, 16, 17, 19}},
		{"LeNonRepresentable", "r_val <= 3.1", []int64{0, 1, 2, 3, 16, 17, 19}},
		{"GtNonRepresentable", "r_val > 3.1", join(seq(4, 15), []int64{20})},
		{"GeNonRepresentable", "r_val >= 3.1", join(seq(4, 15), []int64{20})},
		// The same number spelled with a trailing zero is the same literal:
		// PostgreSQL plans both as '3.1'::double precision.
		{"EqTrailingZero", "r_val = 3.10", nil},
		// Exactly-representable literals answer the same at either width —
		// the case that hid the defect.
		{"EqRepresentable", "r_val = 1.5", []int64{17}},
		{"EqZero", "r_val = 0", []int64{19}},
		{"BetweenNonRepresentable", "r_val BETWEEN 3.1 AND 4.1", []int64{4}},

		// --- #631: an INTEGER literal is widened too ---
		//
		// `r_val = 16777217` plans as '16777217'::double precision, so row 20
		// (2^24) does NOT match; narrowing the literal to real would round it
		// onto row 20's value and match. The companion `= 16777216` proves
		// the row is reachable at all.
		{"EqIntegerLiteralPastMantissa", "r_val = 16777217", nil},
		{"EqIntegerLiteralExact", "r_val = 16777216", []int64{20}},

		// --- #549 kept: a MULTI-element IN still NARROWS ---
		//
		// PostgreSQL casts the whole array literal to real[], so 3.1 becomes
		// the same real row 3 holds and matches. This is the opposite width
		// from `=` on the identical literal, which is why the two must not be
		// lowered to one another.
		{"InMultiNonRepresentable", "r_val IN (3.1, 7.1)", []int64{3, 7}},
		// The arity is SYNTACTIC: a NULL member is stripped for three-valued
		// logic but PostgreSQL still casts a two-element `{3.1,NULL}` to
		// real[], so this narrows and matches.
		{"InMultiWithNull", "r_val IN (3.1, NULL)", []int64{3}},
		{"NotInMulti", "r_val NOT IN (3.1, 7.1)",
			join([]int64{0, 1, 2}, []int64{4, 5, 6}, seq(8, 17), []int64{19, 20})},
		// The narrowing and the widening on ONE literal, in one fixture: the
		// integer 16777217 misses through `=` (widened) and HITS through a
		// multi-element IN (narrowed onto 2^24). Anything that lowers IN to a
		// chain of `=`, on either path, fails exactly here.
		{"InMultiIntegerPastMantissa", "r_val IN (16777217, 99)", []int64{20}},

		// --- #549 kept: a SINGLE-element IN WIDENS (arity 1) ---
		{"InSingleNonRepresentable", "r_val IN (3.1)", nil},
		{"InSingleRepresentable", "r_val IN (1.5)", []int64{17}},

		// --- Column-to-column: no literal, so no width to resolve ---
		//
		// Unchanged by #631 and asserted so: PostgreSQL compares real to real
		// directly (every non-NULL row matches) and real to double by
		// widening the real (only the rows whose value survives the round
		// trip). A change that widened the COLUMN pair, or narrowed the
		// double one, moves one of these.
		{"EqRealColumn", "r_val = r_other", join(seq(0, 17), []int64{19, 20})},
		{"EqDoubleColumn", "r_val = d_val", []int64{16, 17, 19, 20}},

		// A finite literal past real's range is an ordinary double for `=`:
		// no row equals it, and PostgreSQL raises NO error (the 22003 belongs
		// to the multi-element IN, which casts to real[] — #549).
		{"EqOverRange", "r_val = 1e40", nil},
	}
}

// TestRealComparisonWidthTwoPath holds the single-process engine and the
// stage DAG to PostgreSQL's comparison width for a `real` column, predicate by
// predicate (#631 the scalar operators, #633 the distributed IN list).
func TestRealComparisonWidthTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range rwpWant() {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT r_key FROM %s WHERE %s ORDER BY r_key", rwpTable, c.where)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				got := rwpKeys(t, dtpRun(t, ctx, single, coord, sql, arm.dag))
				if !rwpEqual(got, c.want) {
					t.Errorf("%s: %s\n  got  %v\n  want %v (PostgreSQL 17)",
						arm.name, sql, got, c.want)
				}
			}
		})
	}
}

// TestRealInOverRangeLiteralRaisesOnBothPaths is the error half of the arity
// rule, which only shows up once the row path narrows too (#633).
//
// Narrowing a finite literal past real's range yields ±Inf, which would MATCH
// a genuine infinite row. PostgreSQL raises numeric_value_out_of_range for the
// whole predicate when it casts the array to real[], and answers a
// SINGLE-element list — which widens instead — with no rows and no error:
//
//	WHERE r_val IN (1e40, 3.1)  ->  ERROR 22003 ... out of range for type real
//	WHERE r_val IN (1e40)       ->  0 rows, no error
//	WHERE r_val = 1e40          ->  0 rows, no error
//
// Before this, the DAG answered the multi-element form with an empty result:
// the widened comparison simply missed, so an error PostgreSQL raises became a
// value, on the distributed path only.
func TestRealInOverRangeLiteralRaisesOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	raises := fmt.Sprintf("SELECT r_key FROM %s WHERE r_val IN (1e40, 3.1) ORDER BY r_key", rwpTable)
	for _, arm := range []struct {
		name string
		run  func() error
	}{
		{"single", func() error { _, err := tmdRunSingle(ctx, single, raises); return err }},
		{"dag", func() error { _, err := tmdRunDAG(ctx, coord, raises); return err }},
	} {
		err := arm.run()
		if err == nil {
			t.Errorf("%s: %s returned rows; PostgreSQL raises 22003", arm.name, raises)
			continue
		}
		if !strings.Contains(err.Error(), "out of range for type real") {
			t.Errorf("%s: %s raised %v, want the 22003 out-of-range refusal", arm.name, raises, err)
		}
	}

	// The single-element form widens and is not an error at all.
	for _, sql := range []string{
		fmt.Sprintf("SELECT r_key FROM %s WHERE r_val IN (1e40) ORDER BY r_key", rwpTable),
		fmt.Sprintf("SELECT r_key FROM %s WHERE r_val = 1e40 ORDER BY r_key", rwpTable),
	} {
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			if rows := dtpRun(t, ctx, single, coord, sql, arm.dag); len(rows) != 0 {
				t.Errorf("%s: %s returned %d rows; PostgreSQL answers none, with no error",
					arm.name, sql, len(rows))
			}
		}
	}
}

// TestRealKeyedOperationsAreUnmovedByWidth is the other half of the blast
// radius: ORDER BY, GROUP BY, DISTINCT and a hash-join key over a `real`
// column compare COLUMN to COLUMN, so no literal decides their width and
// #631 must leave them exactly where they were. Asserting it is cheaper than
// arguing it — a widening that leaked into the key encoders would split one
// group in two or drop a join pair here.
func TestRealKeyedOperationsAreUnmovedByWidth(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	cases := []struct {
		name string
		sql  string
		want string // the single scalar cell, rendered
	}{
		// 21 rows, 20 distinct non-NULL values plus the NULL: PostgreSQL
		// GROUP BY collects NULL into its own group, so 21.
		{"GroupByReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT r_val FROM %s GROUP BY r_val) s", rwpTable), "21"},
		{"DistinctReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT DISTINCT r_val FROM %s) s", rwpTable), "21"},
		// Every non-NULL row joins exactly itself: 20 pairs.
		{"SelfJoinOnReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.r_val = b.r_val", rwpTable, rwpTable), "20"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, c.sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				if got := fmt.Sprint(rows[0]["n"]); got != c.want {
					t.Errorf("%s: %s = %s, want %s (PostgreSQL 17)", arm.name, c.sql, got, c.want)
				}
			}
		})
	}

	// ORDER BY over the real column, ascending with NULLs last (PostgreSQL's
	// default for ASC). The key order is the float32 one at every position;
	// the literal-width rule never enters it.
	t.Run("OrderByReal", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT r_key FROM %s ORDER BY r_val, r_key", rwpTable)
		want := "19,0,16,1,17,2,3,4,5,6,7,8,9,10,11,12,13,14,15,20,18"
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			keys := rwpKeys(t, dtpRun(t, ctx, single, coord, sql, arm.dag))
			parts := make([]string, len(keys))
			for i, k := range keys {
				parts[i] = fmt.Sprint(k)
			}
			if got := strings.Join(parts, ","); got != want {
				t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17)", arm.name, sql, got, want)
			}
		}
	})
}

// rwpKeys unboxes the r_key column, keeping the row order the query asked for.
func rwpKeys(t *testing.T, rows []map[string]any) []int64 {
	t.Helper()
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		k, ok := r["r_key"].(int64)
		if !ok {
			t.Fatalf("r_key = %#v (%T), want int64", r["r_key"], r["r_key"])
		}
		out = append(out, k)
	}
	return out
}

func rwpEqual(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
