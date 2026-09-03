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

// A function answers in its argument's own DOMAIN, and an aggregate at its
// column's own WIDTH — on both engines, and at the width PostgreSQL declares.
//
// Four issues, one consumer. `physical.nodeDeclaredType` reads whatever the
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
//	#760  SUM over a REAL summed each batch at float32 and FOLDED at float64,
//	      so one pass over four rows rounded to the real answer by luck while
//	      three workers' partials kept a residue a real accumulator discards:
//	      16777216 on one engine, 16777215.600000001 on the other, every run.
//	      AVG had the mirror defect — the single path widened the float32
//	      BATCH SUM instead of each value, and answered 5592405.33 where the
//	      DAG and the server answer 5592405.2.
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
	// wantRoutes is the routing delta each DAG arm must show. The zero value
	// means the DAG EXECUTED this shape; anything else names the refusal it
	// took, and says that the two DAG arms are the coordinator-local pipeline
	// for this cell (rule 11).
	wantRoutes a2Routes
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
		// `FROM numfold WHERE id = 1` and not a bare SELECT: a table-less
		// SELECT ROUTES (#806) and its DAG arms would be the local pipeline.
		// The operands are still literals, which is what the rounding rule
		// keys on.
		{issue: "#768", name: "cast_float_to_integer_is_half_to_even",
			sql: `SELECT CAST(CAST(-0.5 AS DOUBLE) AS BIGINT) AS a, CAST(CAST(0.5 AS DOUBLE) AS BIGINT) AS b, ` +
				`CAST(CAST(2.5 AS DOUBLE) AS BIGINT) AS c, CAST(CAST(1.5 AS DOUBLE) AS BIGINT) AS d ` +
				`FROM numfold WHERE id = 1`,
			want:   []string{"a=int64:0|b=int64:0|c=int64:2|d=int64:2"},
			pgSays: "0, 0, 2, 2"},
		{issue: "#768", name: "ctl_cast_numeric_to_integer_is_half_away",
			sql:    `SELECT CAST(-0.5 AS BIGINT) AS a, CAST(2.5 AS BIGINT) AS b FROM numfold WHERE id = 1`,
			want:   []string{"a=int64:-1|b=int64:3"},
			pgSays: "-1, 3 — the other rounding, on the same server"},
		// The table-less spelling of the same pair, with its route ASSERTED:
		// #806 refuses a SELECT with no FROM and the coordinator answers, so
		// this cell's DAG arms are that pipeline and its claim is about it.
		{issue: "#768", name: "ctl_cast_numeric_tableless_routes",
			sql:        `SELECT CAST(-0.5 AS BIGINT) AS a, CAST(2.5 AS BIGINT) AS b`,
			want:       []string{"a=int64:-1|b=int64:3"},
			wantRoutes: a2Routes{TableLess: 1},
			pgSays:     "-1, 3 — routed on both DAG arms"},

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

		// ---- #758: GREATEST/LEAST hand over the winner at the fold's width ---
		//
		// `extremumArms.materialize` rewrote only a QUOTED literal's box, so a
		// COLUMN won at its OWN width. `GREATEST(real, integer)` folds to real
		// and 16777216 / 16777217 are the SAME real, so the integer arm won
		// and came back as the integer. The PROJECTION narrowed it into a real
		// vector, which is why the bare call looked right — and `* 2` never
		// reaches a vector before the multiply, so it answered 33554434 where
		// PostgreSQL answers 33554432.
		//
		// COALESCE over the same pair was ALREADY right (choiceNumberBox), and
		// that is the control below: it localizes the defect to pickExtremum
		// rather than to "the composite fold", and it is the routine the fix
		// mirrors.
		{issue: "#758", name: "greatest_real_integer_arithmetic",
			sql: `SELECT id, GREATEST(n_f32, n_i32) * 2 AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:6", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:3.3554432e+07", "id=int64:4|v=float64:-1"},
			pgSays: "6, NULL, 33554432, -1 — it answered 33554434 for the third"},
		{issue: "#758", name: "greatest_real_integer_projection",
			sql: `SELECT id, GREATEST(n_f32, n_i32) AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float32:3", "id=int64:2|v=NULL",
				"id=int64:3|v=float32:1.6777216e+07", "id=int64:4|v=float32:-0.5"},
			pgSays: "real: 3, NULL, 1.6777216e+07, -0.5 — right BEFORE this too, " +
				"because the projection narrowed it; that is what hid the defect"},
		// LEAST DOES detect the defect, with a strict MINIMUM past -2^24 —
		// the first version of this cell said it could not, because the
		// fixture's 16777217 is a maximum. A negative pair needs no fixture
		// change and makes the control a gate: the integer arm wins, and its
		// value differs by 2 (after the *2) unless it is brought to the
		// fold's REAL width. Both measured on PostgreSQL 17.
		{issue: "#758", name: "least_real_integer_wins_at_real_width",
			sql: `SELECT LEAST(CAST(-16777216 AS REAL), CAST(-16777219 AS BIGINT)) * 2 AS a, ` +
				`LEAST(CAST(-16777216 AS REAL), CAST(-16777217 AS INTEGER)) * 2 AS b ` +
				`FROM numfold WHERE id = 1`,
			want:   []string{"a=float64:-3.355444e+07|b=float64:-3.3554432e+07"},
			pgSays: "-33554440, -33554432 — with #758 reverted: -33554438, -33554434"},
		{issue: "#758", name: "ctl_least_real_integer_arithmetic",
			sql: `SELECT id, LEAST(n_f32, n_i32) * 2 AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:0.20000000298023224", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:3.3554432e+07", "id=int64:4|v=float64:-10"},
			pgSays: "0.20000000298023224, NULL, 33554432, -10 — LEAST shares the site"},
		{issue: "#758", name: "ctl_coalesce_real_integer_arithmetic",
			sql: `SELECT id, COALESCE(n_f32, n_i32) * 2 AS v FROM numfold ORDER BY id`,
			want: []string{
				"id=int64:1|v=float64:0.20000000298023224", "id=int64:2|v=NULL",
				"id=int64:3|v=float64:3.3554432e+07", "id=int64:4|v=float64:-1"},
			pgSays: "the reference: right before this change and after it"},
		{issue: "#758", name: "ctl_greatest_grouped_key",
			sql: `SELECT GREATEST(n_f32, n_i32) AS k, COUNT(*) AS n FROM numfold ` +
				`GROUP BY GREATEST(n_f32, n_i32) ORDER BY k`,
			want: []string{
				"k=NULL|n=int64:1", "k=float32:-0.5|n=int64:1",
				"k=float32:3|n=int64:1", "k=float32:1.6777216e+07|n=int64:1"},
			pgSays: "four groups — the composite as a GROUP BY key, right before and after"},

		// ---- #760: REAL aggregates at REAL width --------------------------
		{issue: "#760", name: "sum_over_real",
			sql:    `SELECT SUM(n_f32) AS v FROM numfold`,
			want:   []string{"v=float32:1.6777216e+07"},
			pgSays: "real 1.6777216e+07 — 16777215.600000001 on the DAG before this"},
		{issue: "#760", name: "min_over_real",
			sql:    `SELECT MIN(n_f32) AS v FROM numfold`,
			want:   []string{"v=float32:-0.5"},
			pgSays: "real -0.5"},
		{issue: "#760", name: "max_over_real",
			sql:    `SELECT MAX(n_f32) AS v FROM numfold`,
			want:   []string{"v=float32:1.6777216e+07"},
			pgSays: "real 1.6777216e+07"},
		{issue: "#760", name: "avg_over_real",
			sql:    `SELECT AVG(n_f32) AS v FROM numfold`,
			want:   []string{"v=float64:5.5924052e+06"},
			pgSays: "double precision 5592405.2 — the single path answered 5592405.33"},
		// The same MIN with a WHERE clause, which is what sends it down the
		// SCAN path instead of the metadata-statistics path. Those are two
		// different producers of one answer and they declared different
		// TYPES: `physical.mmTypeFor`'s own doc says it must track
		// exec.minMaxOutputType, and a float32 column is what proved it.
		{issue: "#760", name: "min_over_real_through_the_scan",
			sql:    `SELECT MIN(n_f32) AS v FROM numfold WHERE id < 99`,
			want:   []string{"v=float32:-0.5"},
			pgSays: "real -0.5, the same answer by a different producer"},
		{issue: "#760", name: "grouped_sum_over_real",
			sql: `SELECT id, SUM(n_f32) AS v FROM numfold GROUP BY id ORDER BY id`,
			want: []string{
				"id=int64:1|v=float32:0.1", "id=int64:2|v=NULL",
				"id=int64:3|v=float32:1.6777216e+07", "id=int64:4|v=float32:-0.5"},
			pgSays: "real, one row per group"},
		{issue: "#760", name: "ctl_sum_over_double_unchanged",
			sql:    `SELECT SUM(n_f64) AS v FROM numfold`,
			want:   []string{"v=float64:1.677721695e+07"},
			pgSays: "double precision 1.677721695e+07"},
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
				before := a2ReadRoutes(arm.c)
				got, err := a2DomainRun(tmdRunDAG(ctx, arm.c, tc.sql))
				check(arm.name, got, err)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), tc.wantRoutes, tc.sql)
			}
		})
	}
}
