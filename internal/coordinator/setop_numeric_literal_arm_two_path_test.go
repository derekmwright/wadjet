package coordinator

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// A numeric LITERAL set-operation arm is `numeric`, exactly, on both paths
// (#665, #683).
//
// PostgreSQL types a numeric constant `numeric` whenever it carries a decimal
// point or an exponent — `1.23456`, `1.`, `1e2`, `1.5e-2` — and an integer
// constant `numeric` only once no integer type holds it. The stage DAG
// resolved that in v0.18.x; the single-process adapter built the literal arm's
// vector from the declared-type layer, which answers float8 for a fractional
// literal everywhere, so the two paths answered one query two ways:
//
//	SELECT a FROM decpair UNION ALL SELECT 1234567890123456.78
//	  DAG     1234567890123456.78           (numeric, exact)
//	  single  1.2345678901234568e+15        (float8, already rounded)
//
// The adapter now restates a literal column as the DECIMAL its SPELLING names
// and replaces its box with the literal's plain decimal TEXT, which is the
// shape every reader below already expects from a DECIMAL and which
// batch.FromRowsChecked parses at the resolved scale with no float in between.
//
// The values are compared NUMERICALLY, so the resolved SCALE is not asserted
// here (a set operation's DECIMAL rung is max(scale), so wadjet renders 12.75
// as 12.75000 beside a numeric(6,5) literal where PostgreSQL's unconstrained
// numeric renders 12.75). What IS asserted is the digits: a float8 arm loses
// them, and `1234567890123456.78` is the cell where that is visible.
type setOpLitCell struct {
	issue, name, sql string
	want             []string
	// wantErr, when set, is a substring of the refusal every arm must give.
	wantErr string
	// pin records a divergence from PostgreSQL that this fix does NOT close,
	// with what wadjet answers instead. A cell that starts agreeing FAILS.
	pin        []string
	pinWhy     string
	wantRoutes a2Routes
}

func setOpLitCells() []setOpLitCell {
	// decpair.a canonicalised, plus one literal.
	dec := func(lit string) []string {
		return []string{"-0.01", "0", "12.75", "12.75", "12.75", "12.75", "2", "NULL", "NULL", lit}
	}
	// decpair.f / decpair.r canonicalised, plus one literal.
	flt := func(lit string) []string {
		return []string{"-3.5", "0.5", "1.5", "100", "20", "3.5", "7.25", "9.5", "NULL", lit}
	}
	return []setOpLitCell{
		{issue: "#665", name: "numeric_arm_and_a_fractional_literal",
			sql:  `SELECT a AS v FROM decpair UNION ALL SELECT 1.23456 FROM decpair WHERE id = 1`,
			want: dec("1.23456")},
		{issue: "#665", name: "numeric_arm_and_a_signed_literal",
			sql:  `SELECT a AS v FROM decpair UNION ALL SELECT -1.5000 FROM decpair WHERE id = 1`,
			want: dec("-1.5")},
		{issue: "#665", name: "numeric_arm_and_an_exponent_literal",
			sql:  `SELECT a AS v FROM decpair UNION ALL SELECT 1e2 FROM decpair WHERE id = 1`,
			want: dec("100")},
		// The one that says float8 was the defect and not merely the OID: a
		// float64 cannot hold this literal, so a float arm renders
		// 1.2345678901234568e+15 for it.
		{issue: "#683", name: "a_literal_wider_than_float64_is_exact",
			sql:  `SELECT a AS v FROM decpair UNION ALL SELECT 1234567890123456.78 FROM decpair WHERE id = 1`,
			want: dec("1234567890123456.78")},
		{issue: "#665", name: "bigint_arm_and_a_fractional_literal",
			sql:  `SELECT id AS v FROM decpair UNION ALL SELECT 1.23456 FROM decpair WHERE id = 1`,
			want: []string{"1", "1.23456", "2", "3", "4", "5", "6", "7", "8", "9"}},
		{issue: "#665", name: "double_precision_arm_and_a_literal",
			sql:  `SELECT f AS v FROM decpair UNION ALL SELECT 1.23456 FROM decpair WHERE id = 1`,
			want: flt("1.23456")},
		{issue: "#665", name: "real_arm_and_a_literal",
			sql:  `SELECT r AS v FROM decpair UNION ALL SELECT 1.23456 FROM decpair WHERE id = 1`,
			want: flt("1.23456")},
		// An INTEGER literal stays on the integer rung — PostgreSQL types it
		// `integer` and the union `bigint`, not numeric.
		{issue: "#665", name: "ctl_bigint_arm_and_an_integer_literal",
			sql:  `SELECT id AS v FROM decpair UNION ALL SELECT 1 FROM decpair WHERE id = 1`,
			want: []string{"1", "1", "2", "3", "4", "5", "6", "7", "8", "9"}},
		// #683's join shape: the union's column is compared against a DECIMAL
		// join key, which a float8 column could not match.
		{issue: "#683", name: "a_join_on_the_union_column",
			sql: `SELECT COUNT(*) AS n FROM (SELECT e2 AS v FROM setopdec ` +
				`UNION ALL SELECT 12.75 FROM setopdec WHERE id = 1) x JOIN setopdec s ON x.v = s.e2`,
			want: []string{"11"}},
		// A literal at a scale that forces the union's DECIMAL wide: both
		// paths carry every digit rather than one of them rounding.
		{issue: "#683", name: "a_literal_that_widens_the_unions_scale",
			sql: `SELECT ew AS v FROM setopdec UNION ALL ` +
				`SELECT 0.12345678901234567890 FROM setopdec WHERE id = 1`,
			want: []string{"-0.01", "0", "0.1234567890123456789", "1", "10", "12.75", "3", "3",
				"NULL", "NULL"}},

		// The TABLE-LESS spelling of the same query. A table-less arm makes the
		// coordinator refuse the DAG plan and answer from its own local
		// pipeline (#806), so both "DAG" arms here ARE that pipeline — the
		// routing counter is what says so, and the cell's claim is about that
		// path rather than about a second engine.
		{issue: "#665", name: "a_table_less_literal_arm_routes_local_and_is_exact",
			sql:        `SELECT a AS v FROM decpair UNION ALL SELECT 1234567890123456.78`,
			want:       dec("1234567890123456.78"),
			wantRoutes: a2Routes{TableLess: 1}},

		// A literal NO DECIMAL this engine declares can hold, beside an EXACT
		// arm. PostgreSQL's numeric is unbounded and answers it; wadjet's
		// carrier is 38 digits, and the honest answer is the overflow error —
		// which is what ADR-0024 item 4 and ADR-0012 item 12 both say, and what
		// this path did NOT do: it fell back to float8 and answered
		// `1.2345678901234568e+38`, a silently rounded number under an exact
		// type, on both paths.
		{issue: "#683", name: "a_literal_wider_than_any_decimal_is_22003",
			sql: `SELECT a AS v FROM decpair UNION ALL ` +
				`SELECT 123456789012345678901234567890123456789.5 FROM decpair WHERE id = 1`,
			wantErr: "numeric field overflow"},
		{issue: "#683", name: "the_same_literal_beside_a_bigint_arm",
			sql: `SELECT id AS v FROM decpair UNION ALL ` +
				`SELECT 123456789012345678901234567890123456789.5 FROM decpair WHERE id = 1`,
			wantErr: "numeric field overflow"},
		// The control: beside a FLOAT arm PostgreSQL resolves double precision,
		// and the float8 the literal folds to IS that type's own answer.
		{issue: "#683", name: "ctl_the_same_literal_beside_a_double_precision_arm",
			sql: `SELECT f AS v FROM decpair UNION ALL ` +
				`SELECT 123456789012345678901234567890123456789.5 FROM decpair WHERE id = 1`,
			// float8's own value for that literal, which is what PostgreSQL
			// answers for a double precision union too.
			want: flt("123456789012345680000000000000000000000")},

		// --- what this fix does NOT reach --------------------------------
		{issue: "#683", name: "a_literal_inside_a_derived_table_arm_is_still_float8",
			sql:  `SELECT a AS v FROM decpair UNION ALL SELECT y FROM (SELECT 1234567890123456.78 AS y FROM decpair WHERE id = 1) d`,
			want: dec("1234567890123456.78"),
			pin:  dec("1234567890123456.8"),
			pinWhy: "the literal is typed by the DERIVED TABLE's own projection, not by the " +
				"set-operation arm, and the declared-type layer answers float8 for a fractional " +
				"literal everywhere outside an arm (ADR-0012 item 12: that rule is being decided " +
				"alongside ADR-0024 item 3's decimal arithmetic). Both paths agree on it, which " +
				"is why it is a pin and not a split",
		},
	}
}

func TestANumericLiteralSetOperationArmIsExactOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range setOpLitCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			pin := append([]string(nil), tc.pin...)
			sort.Strings(pin)
			check := func(arm string, res *oracle.Result, err error) {
				t.Helper()
				if tc.wantErr != "" {
					if err == nil {
						t.Fatalf("%s arm ANSWERED %d rows where the literal has no exact carrier\n"+
							"  SQL: %s", arm, len(res.Rows), tc.sql)
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("%s arm refused with %q, want a refusal containing %q\n  SQL: %s",
							arm, err.Error(), tc.wantErr, tc.sql)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
				}
				got := strings.Join(setOpCanonRows(res), " ")
				if tc.pin != nil {
					switch got {
					case strings.Join(want, " "):
						t.Errorf("%s arm now AGREES with PostgreSQL, so this pin is FIXED: delete "+
							"it from setOpLitCells.\n  pinned reason: %s\n  SQL: %s",
							arm, tc.pinWhy, tc.sql)
					case strings.Join(pin, " "):
					default:
						t.Errorf("%s arm answers %v, which is neither PostgreSQL's %v nor the "+
							"pinned %v\n  SQL: %s", arm, got, want, pin, tc.sql)
					}
					return
				}
				if got != strings.Join(want, " ") {
					t.Errorf("%s arm rows\n  got  %v\n  want %v (PostgreSQL 17)\n  SQL: %s",
						arm, got, want, tc.sql)
				}
			}
			sres, serr := tmdRunSingle(ctx, single, tc.sql)
			check("single", sres, serr)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
				check(arm.name, dres, derr)
			}
		})
	}
}
