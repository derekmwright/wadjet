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

	// The two arms agree on the COUNTS above and, since #533's rescale-at-the-
	// arm fix, on the VALUES as well: both are GATED against PostgreSQL's
	// answer here, canonicalized to its minimal spelling since PostgreSQL's
	// numeric is variable-scale and renders each value at its own scale, while
	// a wadjet DECIMAL column has ONE declared scale (ADR-0012 item 6, the
	// #532 bullet). Before the fix the stage DAG carried the wider arm's
	// unscaled Int128 across the shuffle and rendered it at the FIRST arm's
	// declared scale instead of its own, which this gate would have caught by
	// the DAG arm failing to match PostgreSQL rather than by the two arms
	// merely disagreeing with each other.
	t.Run("values_vs_postgres", func(t *testing.T) {
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
				want := canon(tc.pg)
				if got := canon(sv); !slices.Equal(got, want) {
					t.Errorf("single-process arm diverges from PostgreSQL\n  got  %v\n  want %v\n  SQL: %s",
						got, want, sql)
				}
				if got := canon(dv); !slices.Equal(got, want) {
					t.Errorf("stage DAG arm diverges from PostgreSQL\n  got  %v\n  want %v\n  SQL: %s",
						got, want, sql)
				}
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

// The CIDR key fixture (#504 review, network cluster; #546, #565). Two
// spellings of ONE address — "10.0.0.1" and "10.0.0.1/32" — plus a second
// address, so a key or a comparison that reads the stored TEXT splits one
// PostgreSQL inet value in two while inet order calls them equal.
//
// The pair is spelled BOTH WAYS ROUND across the two columns (row 1 is
// c bare / d /32, row 3 is c /32 / d bare) so a column-to-column comparison
// cannot agree by having the same spelling on both sides of one row, and
// `c = d` has two true rows rather than one.
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

// TestCidrKeyIsInetOnBothPaths holds both arms to PostgreSQL's own answer for
// every place a CIDR value is used as a KEY: a set operation's dedup set, a
// GROUP BY, a DISTINCT and a hash join.
//
// PostgreSQL's inet calls a bare address and its own /32 host route ONE value
// (`inet '10.0.0.1' = inet '10.0.0.1/32'` is TRUE, verified live), so the
// fixture's three rows hold one address spelled two ways on each side. Two
// separate fixes have to hold for these to agree:
//
//   - kernel.CidrOrderKey under the ORDER BY / GROUP BY / DISTINCT / hash-join
//     key and the shuffle's own router (#520), which is what makes GROUP BY,
//     DISTINCT and the self join answer 2, 2 and 4 on BOTH arms, and
//   - physical.keyValueText's TypeCIDR arm (#546), the single-process set
//     operation's own dedup key. Without it UNION answered 4 here and 3 on the
//     stage DAG — which lowers a set operation to a GroupByAll aggregate and
//     so already keyed by inet — one engine with two answers to "are these the
//     same value".
//
// This replaces the pin (TestCidrKeyIsStoredTextNotInet) that recorded the
// stored-TEXT answers as today's, and its deletion is #546's and #520's proof
// per ADR-0013 §Pins. It is a POSITIVE gate: every count below is what live
// postgres:17-alpine answers on the identical rows, asserted on both arms, so
// a regression on either one fails rather than being absorbed into a new pin.
func TestCidrKeyIsInetOnBothPaths(t *testing.T) {
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
		// pg is what live postgres:17-alpine answers, and what BOTH arms must
		// answer. No pins: this gate has none left.
		pg int64
	}{
		{"union", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s UNION SELECT d FROM %s) u", 3},
		// The control that says the dedup key is what moved and not the
		// scan: UNION ALL keeps every row whatever the key says.
		{"union_all", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s UNION ALL SELECT d FROM %s) u", 6},
		{"intersect", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s INTERSECT SELECT d FROM %s) u", 1},
		{"except", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s EXCEPT SELECT d FROM %s) u", 1},
		{"group_by", "SELECT COUNT(*) AS n FROM (SELECT c FROM %s GROUP BY c) g", 2},
		{"distinct", "SELECT COUNT(*) AS n FROM (SELECT DISTINCT c FROM %s) g", 2},
		{"count_distinct", "SELECT COUNT(DISTINCT c) AS n FROM %s", 2},
		{"self_join", "SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.c = b.d", 4},
		{"col_literal_equality", "SELECT COUNT(*) AS n FROM %s WHERE c = '10.0.0.1'", 2},
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
				if got != tc.pg {
					t.Errorf("%s arm: %s\n  got %d, want %d (live PostgreSQL 17)", arm.name, sql, got, tc.pg)
				}
			}
			if single64 != dag64 {
				t.Errorf("the two paths disagree: single-process %d, stage DAG %d\n  SQL: %s",
					single64, dag64, sql)
			}
		})
	}

	// The one CIDR key shape that is still WRONG, and it is wrong on ONE arm
	// only: `WHERE c = d`.
	//
	// kernel.colColFilterCidr re-keys a column-to-column CIDR comparison
	// through inet order, but only on the VECTORIZED kernel. expr.compare()'s
	// both-string fast path — the row-at-a-time evaluator, which is what a
	// projection uses and what the stage DAG's re-parsed filter ALWAYS
	// compiles to — still compares the two stored texts byte for byte. So the
	// single-process arm answers PostgreSQL's 2 and the DAG answers 0 for the
	// identical query.
	//
	// Pinned here rather than papered over, and the pin asserts BOTH arms'
	// answers: it fails the moment they agree, which is the fix's proof
	// (ADR-0013 §Pins). Delete this subtest with the fix.
	t.Run("col_col_equality_pinned_565", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c = d", cidrTable)
		got := map[string]int64{}
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
			}
			got[arm.name] = ketInt(t, arm.name, rows[0]["n"])
		}
		if got["single"] == got["dag"] {
			t.Errorf("the two paths now agree on `WHERE c = d` (%d) — #565 is FIXED.\n"+
				"Delete this pin and add col_col_equality to the table above, gated at PostgreSQL's 2.",
				got["single"])
			return
		}
		if got["single"] != 2 || got["dag"] != 0 {
			t.Errorf("#565's shape changed — re-read it before re-pinning\n"+
				"  single-process %d (want 2, PostgreSQL's answer, via kernel.colColFilterCidr)\n"+
				"  stage DAG      %d (want 0, via expr.compare()'s byte comparison)",
				got["single"], got["dag"])
			return
		}
		t.Logf("known divergence, NOT gated (#565): single-process 2 (PostgreSQL's answer), "+
			"stage DAG 0 — the col-col CIDR fix landed on the vectorized kernel only.\n  SQL: %s", sql)
	})
}

// TestMixedDecimalIntegerSetOpIsNotReconciled pins what a set operation across
// a DECIMAL arm and an INTEGER arm does today (#547).
//
// PostgreSQL answers it: `numeric ∪ bigint` is `numeric`, so
// `SELECT a UNION SELECT id` is 12 values there. Wadjet:
//
//   - single-process, DECIMAL arm first: ANSWERS, and every integer is read
//     as an UNSCALED value at the DECIMAL's scale, so 1 comes back as 0.01.
//     A silent hundred-fold shrink, still pinned here.
//   - single-process, INTEGER arm first: fails with an internal message
//     ("cannot store string into INT64 vector"), still pinned here.
//   - stage DAG, either order: fixed as a side effect of #533's coercion —
//     reconcileSetOpArmTypes' widening ladder gained the INT→DECIMAL rung
//     that a set operation's arms reconcile through, so both orders now
//     ANSWER and are gated against PostgreSQL's values here instead of
//     pinned. The single-process arm is untouched by that fix and stays
//     pinned to its two wrong shapes until #547's other half lands.
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

	// PostgreSQL's answer, canonicalized to its minimal spelling: -0.01, 0,
	// 1, 2, 3, 4, 5, 6, 7, 8, 9, 12.75 and a NULL. Wadjet's DECIMAL column
	// has one declared scale, so every value — including what came in
	// through the INTEGER arm — renders at that scale instead of at
	// PostgreSQL's variable one (ADR-0012 item 6): 1 stays 1, i.e. renders as
	// 1.00.
	t.Run("dag answers both orders", func(t *testing.T) {
		want := []string{
			"-0.01", "0.00", "1.00", "12.75", "2.00", "3.00", "4.00", "5.00",
			"6.00", "7.00", "8.00", "9.00", "<nil>",
		}
		for _, sql := range []string{decFirst, intFirst} {
			res, err := tmdRunDAG(ctx, coord, sql)
			if err != nil {
				t.Errorf("%s: %v", sql, err)
				continue
			}
			got := make([]string, 0, len(res.Rows))
			for _, r := range res.Rows {
				for _, v := range r {
					got = append(got, fmt.Sprintf("%v", v))
				}
			}
			sort.Strings(got)
			if !slices.Equal(got, want) {
				t.Errorf("stage DAG diverges from PostgreSQL\n  got  %v\n  want %v\n  SQL: %s", got, want, sql)
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
