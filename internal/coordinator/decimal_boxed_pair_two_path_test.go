package coordinator

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
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

		// B2 (#504 review): a DECIMAL column against an operand whose
		// DECLARATION the boxed comparison cannot read. The binding used to
		// require the other side to be a number it could name, so a scalar
		// subquery, arithmetic, a CAST or a COALESCE left the DECIMAL
		// comparing as its RENDERED TEXT. `a > id` and `a > id + 0` are the
		// same question and are here together for that reason.
		{"decimal_vs_scalar_subquery_gt", "a > (SELECT MIN(id) FROM " + dbpTable + ")", 5},
		{"decimal_vs_scalar_subquery_lt", "a < (SELECT MAX(id) FROM " + dbpTable + ")", 3},
		{"decimal_vs_bare_int_column", "a > id", 4},
		{"decimal_vs_int_arithmetic", "a > id + 0", 4},
		{"decimal_vs_int_cast", "a > CAST(id AS BIGINT)", 4},
		{"decimal_in_coalesce_with_null", "COALESCE(a, NULL) = 12.75", 4},
		{"decimal_in_coalesce_with_null_gt", "COALESCE(a, NULL) > 12.75", 0},
		{"decimal_in_coalesce_two_columns", "COALESCE(a, b) > 2.00", 4},

		// B1 (#504 review): a NUMBER column against a QUOTED numeric literal,
		// which PostgreSQL types from the column. The CASE wrapper forces the
		// row-at-a-time path on both arms; the bare forms take the vectorized
		// filter, whose integer arm still reads the constant as ZERO (#536,
		// pinned in the pg-oracle corpus rather than here).
		{"int_vs_quoted_numeric_gt", "CASE WHEN id > '2' THEN 1 ELSE 0 END = 1", 7},
		{"int_vs_quoted_numeric_le", "CASE WHEN id <= '2' THEN 1 ELSE 0 END = 1", 2},
		{"int_vs_quoted_numeric_eq", "CASE WHEN id = '2' THEN 1 ELSE 0 END = 1", 1},
		{"int_vs_quoted_numeric_in", "CASE WHEN id IN ('2','3') THEN 1 ELSE 0 END = 1", 2},
		{"int_vs_quoted_numeric_between", "CASE WHEN id BETWEEN '2' AND '4' THEN 1 ELSE 0 END = 1", 3},
		{"int_vs_quoted_numeric_greatest", "GREATEST(id, '2') = id", 8},
		// A quoted numeric literal against a DECIMAL column is exact, at the
		// boxed sites as well as the direct one.
		{"decimal_vs_quoted_numeric_eq", "a = '12.75'", 4},
		{"decimal_vs_quoted_numeric_case", "CASE a WHEN '12.75' THEN 1 ELSE 0 END = 1", 4},
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

	// The two arms agree on the COUNTS above and do NOT agree on the VALUES.
	//
	// The stage DAG carries the wider arm's unscaled Int128 across the shuffle
	// and renders it at the FIRST arm's declared scale, so a DECIMAL(18,4) arm
	// comes back 100x too large under a DECIMAL(9,2) first arm, and the
	// distinct forms TRUNCATE instead — which is how a DISTINCT result ends up
	// holding the same rendering twice (#533). That is a plan-time
	// stage-schema site, not the runtime one #532 fixed, so it is PINNED here
	// rather than papered over.
	//
	// The pin has two halves, and both are load-bearing:
	//
	//   - the SINGLE-PROCESS arm's values are GATED against PostgreSQL's, so
	//     the half that IS fixed cannot regress behind the pin. Comparison is
	//     on the canonical value, not the spelling: PostgreSQL's numeric is
	//     variable-scale and renders each value at its own scale, while a
	//     wadjet DECIMAL column has ONE declared scale (ADR-0012 item 6, the
	//     #532 bullet).
	//   - the two arms are asserted to DIFFER, so the pin FAILS the moment
	//     they agree. That failure is #533's fix's proof, and the agent that
	//     fixes it deletes this subtest.
	t.Run("values_pinned_533", func(t *testing.T) {
		canon := func(vals []string) []string {
			out := make([]string, 0, len(vals))
			for _, v := range vals {
				if strings.Contains(v, ".") {
					v = strings.TrimRight(v, "0")
					v = strings.TrimSuffix(v, ".")
					if v == "" || v == "-" {
						v = "0"
					}
				}
				out = append(out, v)
			}
			sort.Strings(out)
			return out
		}
		render := func(sql string, dag bool) []string {
			rows := dtpRun(t, ctx, single, coord, sql, dag)
			out := make([]string, 0, len(rows))
			for _, r := range rows {
				out = append(out, fmt.Sprintf("%v", r["a"]))
			}
			sort.Strings(out)
			return out
		}
		for _, tc := range []struct {
			name string
			sql  string
			// pg is PostgreSQL's answer with each value canonicalized to its
			// minimal spelling; "<nil>" is a NULL, which is a member of a set
			// operation like any other.
			pg []string
		}{
			{"union_all", "SELECT a FROM %s UNION ALL SELECT b FROM %s", []string{
				"-0.01", "-0.01", "0", "0", "1", "10", "12.7499", "12.75", "12.75",
				"12.75", "12.75", "12.75", "12.7501", "2",
				"<nil>", "<nil>", "<nil>", "<nil>"}},
			{"union", "SELECT a FROM %s UNION SELECT b FROM %s", []string{
				"-0.01", "0", "1", "10", "12.7499", "12.75", "12.7501", "2", "<nil>"}},
			{"intersect", "SELECT a FROM %s INTERSECT SELECT b FROM %s", []string{
				"-0.01", "0", "12.75", "<nil>"}},
			{"except", "SELECT a FROM %s EXCEPT SELECT b FROM %s", []string{"2"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				sql := fmt.Sprintf(tc.sql, dbpTable, dbpTable)
				sv, dv := render(sql, false), render(sql, true)
				if got, want := canon(sv), canon(tc.pg); !slices.Equal(got, want) {
					t.Errorf("single-process arm diverges from PostgreSQL\n  got  %v\n  want %v\n  SQL: %s",
						got, want, sql)
				}
				if slices.Equal(sv, dv) {
					t.Errorf("the two paths now render the same values, so #533 is FIXED for %s:\n  %v\n"+
						"Delete this pin and gate the DAG arm against PostgreSQL too.", tc.name, sv)
					return
				}
				t.Logf("known divergence, NOT gated (#533): the stage DAG rescales the wider arm.\n"+
					"  single-process: %v\n  stage DAG:      %v", sv, dv)
			})
		}
	})
}

// TestDecimalLiteralRefusalTwoPath holds both arms to REFUSING the statements
// PostgreSQL refuses at parse/bind time: a constant that names no number,
// against a column declared DECIMAL (#517).
//
// The refusal used to live inside the comparison, so it depended on a row
// reaching it and on which operand won one — and the two arms compile
// expressions differently (the DAG re-parses a filter in a later stage and
// always evaluates row-at-a-time), so the same statement could be refused on
// one path and answered on the other. It is the binder's now, before either
// path exists, which is what makes the two agree by construction rather than
// by both happening to evaluate the same pair.
func TestDecimalLiteralRefusalTwoPath(t *testing.T) {
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
	}{
		{"direct", "a = 'abc'"},
		{"is_distinct_from", "a IS DISTINCT FROM 'abc'"},
		{"simple_case", "CASE a WHEN 'abc' THEN 1 ELSE 0 END = 1"},
		{"in_list", "a IN ('abc', 1.0)"},
		{"between", "a BETWEEN 'abc' AND 'def'"},
		// No row survives the conjunct: nothing evaluates the comparison, so
		// a per-row refusal never fired and the query answered 0.
		{"no_row_survives", "id > 100000 AND a = 'abc'"},
		{"short_circuited", "1 = 0 AND a = 'abc'"},
		// GREATEST and LEAST on the SAME arguments: which pair the runtime
		// compares depends on the values, so these two used to disagree.
		{"greatest", "GREATEST(id, 'abc', a) = 'abc'"},
		{"least", "LEAST(id, 'abc', a) = 'abc'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s", dbpTable, tc.pred)
			for _, arm := range []struct {
				name string
				run  func() error
			}{
				{"single", func() error { _, err := tmdRunSingle(ctx, single, sql); return err }},
				{"dag", func() error { _, err := tmdRunDAG(ctx, coord, sql); return err }},
			} {
				err := arm.run()
				if err == nil {
					t.Errorf("%s arm ANSWERED a statement PostgreSQL refuses: %s", arm.name, sql)
					continue
				}
				if !strings.Contains(err.Error(), "invalid input syntax for type numeric") {
					t.Errorf("%s arm refused %s with the wrong error: %v\n  want PostgreSQL's numeric input-syntax error",
						arm.name, sql, err)
				}
			}
		})
	}

	// And the other direction, so the refusal has not widened: a quoted
	// literal that IS a number is typed from the column and answered, on both
	// arms.
	t.Run("numeric_quoted_literal_still_answers", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE a = '12.75'", dbpTable)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
			}
			if got := ketInt(t, arm.name, rows[0]["n"]); got != 4 {
				t.Errorf("%s: %s\n  got %d, want 4 (live PostgreSQL 17)", arm.name, sql, got)
			}
		}
	})
}

// The CIDR set-operation fixture (#504 review, network cluster). Two spellings
// of ONE address — "10.0.0.1" and "10.0.0.1/32" — plus a second address, so a
// dedup key that compares the stored TEXT splits one value in two while the
// inet order the comparison uses calls them equal.
const cidrTable = "cidrpair"

func cidrSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c", Type: parquet.TypeCIDR},
		{Name: "d", Type: parquet.TypeCIDR},
	}}
}

func cidrData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "c": "10.0.0.1", "d": "10.0.0.1/32"},
		{"id": int64(2), "c": "10.0.0.2/32", "d": "10.0.0.3/32"},
		{"id": int64(3), "c": "10.0.0.1/32", "d": "10.0.0.1"},
	}
}

// TestCidrKeyIsStoredTextNotInet pins what a CIDR value's KEY actually is —
// the stored TEXT — against what PostgreSQL says it is: an inet, where a bare
// address is a /32 host route and `inet '10.0.0.1' = inet '10.0.0.1/32'` is
// TRUE (verified live).
//
// The engine disagrees with ITSELF here, which is the finding: the
// column-to-LITERAL comparison already re-keys through kernel.CidrSortKey
// (#492), so `WHERE c = '10.0.0.1'` finds both spellings, while every KEY and
// the column-to-COLUMN comparison use the raw bytes, so `WHERE c = d` finds
// neither and `GROUP BY c` splits one address in two.
//
// It is pinned rather than fixed here because BOTH ARMS AGREE. Keying the
// local set operation by inet on its own would put `UNION` at 3 while
// `GROUP BY` stayed at 3-against-2 and the DAG stayed at 4 — one engine with
// two answers to "are these the same value", which is #499's own defect class
// one type over. The fix has to move the whole key layer and the shuffle
// router together (#546), the way #459 did for floats.
func TestCidrKeyIsStoredTextNotInet(t *testing.T) {
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
		// pg is what live postgres:17-alpine answers; today is what wadjet
		// answers on BOTH arms. Where they differ the entry is pinned.
		pg, today int64
	}{
		{"union", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s UNION SELECT d FROM %s) u", 3, 4},
		{"intersect", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s INTERSECT SELECT d FROM %s) u", 1, 2},
		{"except", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s EXCEPT SELECT d FROM %s) u", 1, 1},
		{"group_by", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s GROUP BY c) g", 2, 3},
		{"distinct", "SELECT COUNT(*) AS n FROM (SELECT DISTINCT c FROM %s) g", 2, 3},
		{"self_join", "SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.c = b.d", 4, 2},
		{"col_col_equality", "SELECT COUNT(*) AS n FROM %s WHERE c = d", 2, 0},
		// The one site that already follows PostgreSQL, and the reason the
		// rest are visible as a self-disagreement rather than a policy.
		{"col_literal_equality", "SELECT COUNT(*) AS n FROM %s WHERE c = '10.0.0.1'", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := tc.sql
			for strings.Contains(sql, "%s") {
				sql = strings.Replace(sql, "%s", cidrTable, 1)
			}
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
				if got != tc.today {
					t.Errorf("%s arm answered %d; this gate pins today's answer as %d\n  SQL: %s",
						arm.name, got, tc.today, sql)
				}
			}
			// The property that makes a one-site fix WRONG: the two arms must
			// keep answering alike, whatever the answer is.
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d\n  SQL: %s",
					single64, dag64, sql)
			}
			if tc.today == tc.pg {
				return
			}
			t.Logf("known divergence, NOT gated (#546): %d, PostgreSQL %d — a CIDR key is the stored "+
				"TEXT, so two spellings of one address are two values.\n  SQL: %s", tc.today, tc.pg, sql)
		})
	}
}

// TestMixedDecimalIntegerSetOpIsNotReconciled pins the three different things
// a set operation across a DECIMAL arm and an INTEGER arm does today (#547).
//
// PostgreSQL answers it: `numeric ∪ bigint` is `numeric`, so
// `SELECT a UNION SELECT id` is 12 values there. Wadjet:
//
//   - single-process, DECIMAL arm first: ANSWERS, and every integer is read
//     as an UNSCALED value at the DECIMAL's scale, so 1 comes back as 0.01.
//     A silent hundred-fold shrink is the worst of the three.
//   - single-process, INTEGER arm first: fails with an internal message
//     ("cannot store string into INT64 vector").
//   - stage DAG, either order: refused at plan time, because
//     reconcileSetOpArmTypes' widening ladder has no DECIMAL rung.
//
// Pinned rather than fixed in the #504/#506/#499/#517 round: the fix is that
// ladder, not the dedup key or the boxed comparison. The pin fails when any of
// the three changes, which is what says the ladder moved.
func TestMixedDecimalIntegerSetOpIsNotReconciled(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	decFirst := fmt.Sprintf("SELECT a FROM %s UNION SELECT id FROM %s", dbpTable, dbpTable)
	intFirst := fmt.Sprintf("SELECT id FROM %s UNION SELECT a FROM %s", dbpTable, dbpTable)

	t.Run("dag refuses both orders", func(t *testing.T) {
		for _, sql := range []string{decFirst, intFirst} {
			_, err := tmdRunDAG(ctx, coord, sql)
			if err == nil {
				t.Errorf("the stage DAG now ANSWERS %s — #547 moved; re-gate this against PostgreSQL", sql)
				continue
			}
			if !strings.Contains(err.Error(), "arms disagree on the type") {
				t.Errorf("%s refused with an unexpected error: %v", sql, err)
			}
		}
	})

	t.Run("single-process shrinks the integer arm", func(t *testing.T) {
		res, err := tmdRunSingle(ctx, single, decFirst)
		if err != nil {
			t.Fatalf("%s: %v", decFirst, err)
		}
		got := make([]string, 0, len(res.Rows))
		for _, r := range res.Rows {
			got = append(got, fmt.Sprintf("%v", r["a"]))
		}
		sort.Strings(got)
		// PostgreSQL's answer is -0.01, 0.00, 1, 2.00, 3, 4, 5, 6, 7, 8, 9,
		// 12.75 and a NULL. Wadjet's integers arrive divided by 100.
		want := []string{
			"-0.01", "0.00", "0.01", "0.02", "0.03", "0.04", "0.05", "0.06",
			"0.07", "0.08", "0.09", "12.75", "2.00", "<nil>",
		}
		if !slices.Equal(got, want) {
			t.Errorf("the corruption changed shape — #547 moved, re-read it before re-pinning\n"+
				"  got  %v\n  want %v (today's WRONG answer; PostgreSQL says 1..9, not 0.01..0.09)", got, want)
		}
	})

	t.Run("single-process errors in the other arm order", func(t *testing.T) {
		_, err := tmdRunSingle(ctx, single, intFirst)
		if err == nil {
			t.Errorf("%s now answers — #547 moved; re-gate it against PostgreSQL", intFirst)
			return
		}
		if !strings.Contains(err.Error(), "cannot store string into INT64 vector") {
			t.Errorf("%s failed with an unexpected error: %v", intFirst, err)
		}
	})
}
