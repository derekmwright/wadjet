package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// a2DomainRun renders a result with the Go BOX named exactly and the number
// printed in full.
//
// na2Run cannot be used here and the reason is the defect itself: it prints
// every float as "float:%.6g", so a float32 and a float64 render identically
// and 0.10000000149011612 renders as "0.1". Both halves of this census are
// about which WIDTH a value came back at, so a renderer that folds the two
// widths together is a fixture that cannot tell the two rules apart (protocol
// method 2) — and it passed on a tree with the fix reverted, which is how it
// was found.
func a2DomainRun(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			if r[c] == nil {
				parts = append(parts, c+"=NULL")
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%T:%v", c, r[c], r[c]))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, nil
}

// A function answers in its argument's own DOMAIN — on both engines, and at
// the width PostgreSQL declares.
//
// Two issues, one consumer. `physical.nodeDeclaredType` reads whatever the
// declaration layer says and turns it into the wire's OID; when that
// declaration is a FIXED float64 the kernel has no domain to compute in, and
// the wrong OID becomes a wrong NUMBER:
//
//	#768  ABS(real 0.1) answered 0.10000000149011612 — the float32 widened to
//	      a double, whose extra digits a real never had — under OID 701 where
//	      PostgreSQL declares 700. MOD(-6, 3) answered math.Mod's signed -0.
//	#757  NULLIF took its type from argument 0 alone. PostgreSQL takes it from
//	      the comparison OPERATOR the two arguments select, which is argument
//	      0's own width within one numeric family and float8 across families.
//
// Every expectation is live PostgreSQL 17's, measured over this fixture. The
// Go box is printed for every non-string value (na2Run), because a float64
// holding an exact integer and an int64 holding it print identically under
// %v — and "the right number under the wrong Go type" is what half of these
// are.
type a2DomainCell struct {
	issue, name, sql string
	want             []string
	pgSays           string
}

func a2DomainCells() []a2DomainCell {
	return []a2DomainCell{
		// ---- #768: ABS and MOD keep their argument's domain ---------------
		{issue: "#768", name: "abs_over_real",
			sql: `SELECT id, ABS(n_f32) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float32:0.1", "id=int64:2|v=NULL",
				"id=int64:3|v=float32:1.6777216e+07", "id=int64:4|v=float32:0.5"},
			pgSays: "real: 0.1, NULL, 1.6777216e+07, 0.5 — it answered " +
				"0.10000000149011612 for the first, a double's digits for a value a real never held"},
		{issue: "#768", name: "abs_over_bigint",
			sql: `SELECT id, ABS(n_i64) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=int64:4", "id=int64:2|v=NULL",
				"id=int64:3|v=int64:16777217", "id=int64:4|v=int64:6"},
			pgSays: "bigint: 4, NULL, 16777217, 6"},
		{issue: "#768", name: "abs_over_integer",
			sql: `SELECT id, ABS(n_i32) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=int32:3", "id=int64:2|v=NULL",
				"id=int64:3|v=int32:16777217", "id=int64:4|v=int32:5"},
			pgSays: "integer: 3, NULL, 16777217, 5"},
		{issue: "#768", name: "mod_over_bigint",
			sql: `SELECT id, MOD(n_i64, 3) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=int64:1", "id=int64:2|v=NULL",
				"id=int64:3|v=int64:2", "id=int64:4|v=int64:0"},
			pgSays: "bigint: 1, NULL, 2, 0 — and 0, never math.Mod's -0"},
		// The CONTROLS that make this a per-function table rather than a
		// rewrite: FLOOR, CEIL and SQRT over an integer ARE double precision
		// in PostgreSQL. A blanket "type this family from its argument" pass
		// would have introduced a divergence where none existed.
		{issue: "#768", name: "ctl_floor_over_bigint_is_double",
			sql: `SELECT id, FLOOR(n_i64) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:4", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:1.6777217e+07", "id=int64:4|v=float64:-6"},
			pgSays: "double precision: 4, NULL, 16777217, -6"},
		{issue: "#768", name: "ctl_abs_over_decimal_unchanged",
			sql: `SELECT id, ABS(n_d152) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=string:12.75", "id=int64:2|v=NULL",
				"id=int64:3|v=string:1.00", "id=int64:4|v=string:3.50"},
			pgSays: "numeric — the DECIMAL arm predates this and must not move"},
		{issue: "#768", name: "ctl_abs_over_double_unchanged",
			sql: `SELECT id, ABS(n_f64) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:0.2", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:1.6777217e+07", "id=int64:4|v=float64:0.25"},
			pgSays: "double precision: 0.2, NULL, 16777217, 0.25"},
		// PostgreSQL's float-to-integer cast is rint(), half to EVEN; its
		// numeric-to-integer cast is not. Both are asserted, because a fix
		// that moved them together would be a new divergence.
		{issue: "#768", name: "cast_float_to_integer_is_half_to_even",
			sql:    `SELECT CAST(CAST(-0.5 AS DOUBLE) AS BIGINT) AS a, CAST(CAST(0.5 AS DOUBLE) AS BIGINT) AS b, CAST(CAST(2.5 AS DOUBLE) AS BIGINT) AS c, CAST(CAST(1.5 AS DOUBLE) AS BIGINT) AS d`,
			want:   []string{"a=int64:0|b=int64:0|c=int64:2|d=int64:2"},
			pgSays: "0, 0, 2, 2"},
		{issue: "#768", name: "ctl_cast_numeric_to_integer_is_half_away",
			sql:    `SELECT CAST(-0.5 AS BIGINT) AS a, CAST(2.5 AS BIGINT) AS b`,
			want:   []string{"a=int64:-1|b=int64:3"},
			pgSays: "-1, 3 — the other rounding, on the same server"},

		// ---- #757: NULLIF resolves through the comparison operator --------
		{issue: "#757", name: "nullif_integer_against_real",
			sql: `SELECT id, NULLIF(n_i32, n_f32) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:3", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:1.6777217e+07", "id=int64:4|v=float64:-5"},
			pgSays: "double precision — there is no int4 = float4 operator, so both coerce to float8"},
		{issue: "#757", name: "nullif_within_the_integer_family",
			sql: `SELECT id, NULLIF(n_i32, n_i64) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=int32:3", "id=int64:2|v=NULL",
				"id=int64:3|v=NULL", "id=int64:4|v=int32:-5"},
			pgSays: "integer — int48eq exists and its LEFT input is int4. " +
				"The load-bearing control: select_common_type would answer bigint"},
		{issue: "#757", name: "nullif_decimal_against_real",
			sql: `SELECT id, NULLIF(n_d152, n_f32) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:12.75", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:1", "id=int64:4|v=float64:-3.5"},
			pgSays: "double precision — and GREATEST over the SAME pair is `real` there, " +
				"which is why NULLIF cannot be a fold"},
		{issue: "#757", name: "nullif_integer_against_decimal",
			sql: `SELECT id, NULLIF(n_i32, n_d152) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=string:3.00", "id=int64:2|v=NULL",
				"id=int64:3|v=string:16777217.00", "id=int64:4|v=string:-5.00"},
			pgSays: "numeric, rendered `3` / `16777217` / `-5`. The TYPE agrees and the " +
				"SCALE is ADR-0012 item 12's recorded class: PostgreSQL's numeric carries a " +
				"per-VALUE dscale and takes the integer's 0, a wadjet DECIMAL column has one " +
				"declared scale for the whole column and renders at it. Same number"},
	}
}

func TestAFunctionAnswersInItsArgumentsOwnDomain(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range a2DomainCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			check := func(arm string, got []string, err error) {
				t.Helper()
				sort.Strings(got)
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17: %s\n  SQL: %s", arm, err, tc.pgSays, tc.sql)
					return
				}
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Errorf("%s arm\n  got  %v\n  want %v (live PostgreSQL 17: %s)\n  SQL: %s",
						arm, got, want, tc.pgSays, tc.sql)
				}
			}
			sgot, serr := a2DomainRun(tmdRunSingle(ctx, single, tc.sql))
			check("single", sgot, serr)
			for i := 0; i < 5; i++ {
				got, err := a2DomainRun(tmdRunSingle(ctx, spilled, tc.sql))
				check("spilled", got, err)
				if t.Failed() {
					break
				}
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				got, err := a2DomainRun(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
			}
		})
	}
}
