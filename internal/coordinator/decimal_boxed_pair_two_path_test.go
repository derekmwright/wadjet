package coordinator

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The cross-scale DECIMAL pair fixture (#506, #499).
//
// The type-matrix table carries ONE DECIMAL column, so nothing in this package
// could tell an exact comparison from a lexicographic one: a column compared
// against itself renders the same text on both sides whatever rule is applied.
// Two columns at DIFFERENT scales is the shape where the two rules disagree,
// and it is the shape both issues are about — #506 for the boxed comparison
// sites, #499 for the set-operation dedup key.
//
// It rides along in tmdTables() rather than standing up a third cluster, the
// way dtpTable and ketTable already do: the tests below use the same two arms
// and no type-matrix corpus entry names this table.
const dbpTable = "decpair"

func dbpSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		// A genuine STRING column holding numeric-looking text (#504): the
		// other half of the pair no box can distinguish. A DECIMAL renders as
		// text and so does this, and only the DECLARATION says which rule
		// each one takes.
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
}

// dbpDec renders an unscaled int64 as the two halves parquet.Decimal128
// carries, sign-extended the way two's complement requires.
func dbpDec(v int64) parquet.Decimal128 {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	return parquet.Decimal128{Hi: hi, Lo: uint64(v)}
}

// dbpData is the same nine rows wadjet.TestDecimalColumnPairAtBoxedSites uses,
// chosen so lexical and numeric order DISAGREE on the rows that matter: ±1 ulp
// at the wider scale around an exactly-equal pair, a leading-digit trap
// ("2.00" sorts above "10.0000" as text), zero and a negative at two scales,
// and NULL on either side and on both.
func dbpData() []map[string]any {
	src := []struct {
		id         int64
		a, b       int64
		aNil, bNil bool
		s          string
	}{
		{id: 1, a: 1275, b: 127500, s: "1.50"},
		{id: 2, a: 1275, b: 127501, s: "1.5"},
		{id: 3, a: 1275, b: 127499, s: "abc"},
		{id: 4, a: -1, b: -100, s: "10"},
		{id: 5, a: 200, b: 100000, s: "9"},
		{id: 6, a: 0, b: 0, s: "1.500"},
		{id: 7, aNil: true, b: 10000, s: "0"},
		{id: 8, a: 1275, bNil: true, s: "-1"},
		{id: 9, aNil: true, bNil: true, s: "1.5"},
	}
	rows := make([]map[string]any, 0, len(src))
	for _, r := range src {
		m := map[string]any{"id": r.id}
		if !r.aNil {
			m["a"] = dbpDec(r.a)
		}
		if !r.bNil {
			m["b"] = dbpDec(r.b)
		}
		m["s"] = r.s
		rows = append(rows, m)
	}
	return rows
}

// TestDecimalBoxedPairTwoPath holds the single-process engine and the stage
// DAG to the same answer for #506's three boxed comparison sites, and holds
// both to what live postgres:17-alpine answers on the identical rows.
//
// The DAG matters here on its own: it re-parses the filter text in a later
// stage and always compiles to the row-at-a-time evaluator, so a binding that
// reached only the vectorized path would show up as an arm disagreement rather
// than as a wrong answer on both.
func TestDecimalBoxedPairTwoPath(t *testing.T) {
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
		// The direct comparison, bound in NewCmp since #477: the control both
		// boxed forms below must agree with.
		{"direct_eq", "a = b", 3},
		{"direct_ne", "a <> b", 3},
		{"direct_lt", "a < b", 2},
		// #506's three sites over the same pair.
		{"simple_case", "CASE a WHEN b THEN 1 ELSE 0 END = 1", 3},
		{"is_distinct_from", "a IS DISTINCT FROM b", 5},
		{"is_not_distinct_from", "a IS NOT DISTINCT FROM b", 4},
		{"greatest", "GREATEST(a, b) = b", 6},
		{"least", "LEAST(a, b) = a", 6},
		{"greatest_literal", "GREATEST(a, b) = 12.75", 3},
		{"least_zero", "LEAST(a, b) > 0", 6},
		// #504: the STRING column against an UNQUOTED numeric literal. The
		// row path used to read it numerically and the vectorized kernel as
		// text — one predicate, two answers. Both must now answer what
		// PostgreSQL answers for the QUOTED spelling of the same literal,
		// which is what these counts are.
		{"text_eq_numeric_literal", "s = 1.5", 2},
		{"text_ne_numeric_literal", "s <> 1.5", 7},
		{"text_eq_trailing_zero", "s = 1.50", 1},
		{"text_eq_two_trailing_zeros", "s = 1.500", 1},
		{"text_gt_numeric_literal", "s > 1.5", 5},
		{"text_lt_numeric_literal", "s < 1.5", 2},
		{"text_case_numeric_literal", "CASE s WHEN 1.5 THEN 1 ELSE 0 END = 1", 2},
		{"text_is_distinct_numeric_literal", "s IS DISTINCT FROM 1.5", 7},
		{"text_greatest_numeric_literal", "GREATEST(s, 1.5) = s", 7},
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

	// The two arms agree on the COUNTS above and do NOT agree on the VALUES:
	// the stage DAG carries the wider arm's unscaled Int128 across the shuffle
	// and renders it at the FIRST arm's declared scale, so every value from a
	// DECIMAL(18,4) arm comes back 100x too large under a DECIMAL(9,2) first
	// arm (#533). That is a different site from the one #532 fixed — the arm's
	// declared type is decided at plan time and carried in the stage schema,
	// not where the rows are boxed — so it is PINNED here rather than papered
	// over, in the shape the pg-oracle corpus uses: the pin FAILS the moment
	// the two agree, which is the fix's proof.
	t.Run("values_pinned_533", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT a FROM %s UNION ALL SELECT b FROM %s", dbpTable, dbpTable)
		render := func(dag bool) []string {
			rows := dtpRun(t, ctx, single, coord, sql, dag)
			out := make([]string, 0, len(rows))
			for _, r := range rows {
				out = append(out, fmt.Sprintf("%v", r["a"]))
			}
			sort.Strings(out)
			return out
		}
		sv, dv := render(false), render(true)
		if slices.Equal(sv, dv) {
			t.Errorf("the two paths now render the same values, so #533 is FIXED:\n  %v\n"+
				"Delete this pin and assert the values against PostgreSQL instead.", sv)
			return
		}
		t.Logf("known divergence, NOT gated (#533): the stage DAG rescales the wider arm.\n"+
			"  single-process: %v\n  stage DAG:      %v", sv, dv)
	})
}

// TestSetOpDecimalKeyTwoPath holds both arms to PostgreSQL's answer for a set
// operation whose two sides are DECIMAL columns of DIFFERENT scale (#499).
//
// The single-process path boxed every row into a map and keyed it with
// `fmt.Sprintf("%v", ...)`, and a DECIMAL boxes as its RENDERED TEXT — so
// "12.75" and "12.7500" were two keys for one number, and UNION counted a
// value twice where INTERSECT could not find it at all. The DAG lowers a set
// operation to a GroupByAll aggregate, which keys through the columnar
// encoding, so the two paths could disagree about the same query; that is what
// this gate is for, over and above each arm matching PostgreSQL.
func TestSetOpDecimalKeyTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		// a is DECIMAL(9,2) and b is DECIMAL(18,4); they hold the same number
		// at two scales on four of the nine rows, and NULL is a member like
		// any other and matches a NULL on the other side.
		{"union", "SELECT a FROM %s UNION SELECT b FROM %s", 9},
		{"union_all", "SELECT a FROM %s UNION ALL SELECT b FROM %s", 18},
		{"intersect", "SELECT a FROM %s INTERSECT SELECT b FROM %s", 4},
		{"except", "SELECT a FROM %s EXCEPT SELECT b FROM %s", 1},
		{"except_reversed", "SELECT b FROM %s EXCEPT SELECT a FROM %s", 4},
		// One arm against itself: the shape a single scale already answered,
		// pinning that the key did not change for it.
		{"self_union", "SELECT a FROM %s UNION SELECT a FROM %s", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM ("+tc.sql+") u", dbpTable, dbpTable)
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

	// The two arms agree on the COUNTS above and do NOT agree on the VALUES:
	// the stage DAG carries the wider arm's unscaled Int128 across the shuffle
	// and renders it at the FIRST arm's declared scale, so every value from a
	// DECIMAL(18,4) arm comes back 100x too large under a DECIMAL(9,2) first
	// arm (#533). That is a different site from the one #532 fixed — the arm's
	// declared type is decided at plan time and carried in the stage schema,
	// not where the rows are boxed — so it is PINNED here rather than papered
	// over, in the shape the pg-oracle corpus uses: the pin FAILS the moment
	// the two agree, which is the fix's proof.
	t.Run("values_pinned_533", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT a FROM %s UNION ALL SELECT b FROM %s", dbpTable, dbpTable)
		render := func(dag bool) []string {
			rows := dtpRun(t, ctx, single, coord, sql, dag)
			out := make([]string, 0, len(rows))
			for _, r := range rows {
				out = append(out, fmt.Sprintf("%v", r["a"]))
			}
			sort.Strings(out)
			return out
		}
		sv, dv := render(false), render(true)
		if slices.Equal(sv, dv) {
			t.Errorf("the two paths now render the same values, so #533 is FIXED:\n  %v\n"+
				"Delete this pin and assert the values against PostgreSQL instead.", sv)
			return
		}
		t.Logf("known divergence, NOT gated (#533): the stage DAG rescales the wider arm.\n"+
			"  single-process: %v\n  stage DAG:      %v", sv, dv)
	})
}
