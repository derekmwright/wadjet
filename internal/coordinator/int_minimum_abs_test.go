package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/worker"
)

// An integer expression whose result has no room in its declared integer type
// FAILS. It never answers a different number.
//
// The shape is `ABS(<column>)` at the type's most negative value, and it is
// the one cf7d3ae0 claimed and did not have. That commit made
// expr.absKeepsDomain raise for a boxed int32 and expr.vecAbsDomain raise for
// an Int32 vector, and gated both — with a boxed int32 LITERAL. A COLUMN
// reaches neither:
//
//   - ColRef.Eval widens an INT32 column to an int64 box on purpose
//     (ADR-0012's recorded "every integer spelling is INT64"), so
//     absKeepsDomain takes its int64 arm and answers 2147483648 — correct for
//     a bigint, and correct arithmetic;
//   - the vec kernel never runs at all for this shape. The registry declares
//     `abs` float64 while the planner declares the projection int4, so
//     FuncCall.EvalVec's vecOutputHolds guard sends it to the per-row path,
//     and vecAbsDomain's int32 arm is unreachable from a projected ABS over a
//     narrow column.
//
// The wrap was in neither evaluator. It was in the STORE — batch.SetValue's
// TypeInt32 arm narrowed the int64 2147483648 into an int32 and produced
// -2147483648, a different number wearing the right type (ADR-0012 item 9).
// The refusal now lives there, which is why this gate asserts the shape
// through a real COLUMN on every arm rather than through a literal on one.
//
// The five arms are the census's own (numeric_arc2_two_path_test.go): the
// single-process pipeline, that pipeline under a memory budget with the
// aggregate drain FORCED, the stage DAG, the DAG with every join broadcast,
// and the DAG with morsel-parallel breakers in the worker. A refusal raised on
// a worker has to survive the task boundary and the coordinator's merge to
// reach the client as an error rather than as a missing row.
func TestIntegerMinimumIsLoudOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	drained := func(sql string) ([]string, error) {
		restore := exec.ForceAggDrainEvery(1)
		defer exec.ForceAggDrainEvery(restore)
		return na2Run(tmdRunSingle(ctx, spilled, sql))
	}
	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget+forced-drain", drained},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
	}

	for _, tc := range []struct {
		name, sql string
		// want is the expected result, or nil when the query must FAIL with
		// wantErr in its message.
		want    []string
		wantErr string
	}{
		// THE VECTOR-PATH CELLS. An int4 column and an int8 column, each at
		// its own floor, each reached through a real scan rather than through
		// a folded literal. PostgreSQL 17.11 raises 22003 for both, with these
		// two messages.
		{name: "abs_of_the_int32_minimum_column",
			sql: `SELECT ABS(i32) AS a FROM intmin WHERE id = 1`, wantErr: "integer out of range"},
		{name: "abs_of_the_int64_minimum_column",
			sql: `SELECT ABS(i64) AS a FROM intmin WHERE id = 1`, wantErr: "bigint out of range"},

		// The same two with NO predicate, so the refusal has to survive a
		// batch that also holds rows which answer. A per-row channel that
		// only fires when the whole batch refuses would pass the cells above
		// and fail these.
		{name: "abs_over_the_whole_int32_column",
			sql: `SELECT ABS(i32) AS a FROM intmin ORDER BY id`, wantErr: "integer out of range"},
		{name: "abs_over_the_whole_int64_column",
			sql: `SELECT ABS(i64) AS a FROM intmin ORDER BY id`, wantErr: "bigint out of range"},

		// Under an AGGREGATE, where the value never becomes an output column
		// of its own. #841's rule — one disposition per expression — says the
		// refusal is the same wherever the expression is written.
		{name: "aggregated_abs_of_the_int32_minimum",
			sql: `SELECT MAX(ABS(i32)) AS m FROM intmin`, wantErr: "integer out of range"},

		// THE BOUNDARY, and the reason this is a value rule and not a sign
		// rule: min+1 has an absolute value, and both columns must answer it.
		// An implementation that refused the negative half outright would pass
		// every cell above and fail these two.
		{name: "abs_just_inside_the_int32_floor_answers",
			sql:  `SELECT ABS(i32) AS a FROM intmin WHERE id = 2`,
			want: []string{"a=int32:2147483647"}},
		{name: "abs_just_inside_the_int64_floor_answers",
			sql:  `SELECT ABS(i64) AS a FROM intmin WHERE id = 2`,
			want: []string{"a=int64:9223372036854775807"}},

		// A NULL is a NULL and not a refusal: the guard reads the VALUE, and
		// row 3 has none.
		{name: "abs_of_a_null_is_null",
			sql:  `SELECT ABS(i32) AS a FROM intmin WHERE id = 3`,
			want: []string{"a=NULL"}},

		// The recorded WIDENING divergence, pinned so the store's new refusal
		// cannot quietly swallow it. PostgreSQL computes `-int4` in int4 and
		// raises 22003 here; wadjet computes every integer expression in int64
		// and answers 2147483648 under a bigint declaration (ADR-0012's
		// "every integer spelling is INT64"). That is a SUPERSET — we answer
		// where PostgreSQL refuses — and it stays. If a future change narrows
		// the declaration to int4 without also folding this pin, the store
		// will refuse and this cell will say so.
		{name: "unary_minus_widens_and_still_answers",
			sql:  `SELECT -i32 AS a FROM intmin WHERE id = 1`,
			want: []string{"a=int64:2147483648"}},

		// THE PREDICATE POSITION, and it is a SUPERSET in the same direction:
		// `WHERE ABS(i32) > …` at the int4 floor ANSWERS where PostgreSQL
		// 17.11 raises `integer out of range` (measured live on both
		// spellings, `> 0` and the one below). A predicate never crosses
		// batch.SetValue — there is no int4-declared output column for the
		// comparison's operand to be stored into — so the store's refusal
		// does not reach here, and the comparison sees the widened int64 the
		// kernel actually computed.
		//
		// Base-identical, and pinned rather than changed: refusing here would
		// make wadjet reject rows it can evaluate exactly, which is the wrong
		// direction, and it would be a RIGHT→LOUD regression against
		// fd679ae9. The int8 twin DOES raise in this position (absKeepsDomain
		// raises wherever it runs), so the two widths disagree here on
		// purpose — an asymmetry this cell records rather than hides.
		//
		// The bound is `> 2147483646` and not `> 0` deliberately: only the
		// widened 2147483648 satisfies it from row 1. A wrapped
		// -2147483648 would not, so this cell fails if the predicate ever
		// starts answering the wrapped number, and fails again if it starts
		// refusing. Row 3 is NULL and drops out.
		{name: "predicate_position_answers_the_widened_value",
			sql:  `SELECT id AS k FROM intmin WHERE ABS(i32) > 2147483646 ORDER BY id`,
			want: []string{"k=int64:1", "k=int64:2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.wantErr != "" {
					if err == nil {
						t.Errorf("%s arm: answered %v, want a 22003 refusal (%q)\n  SQL: %s\n  "+
							"|min| has no value in its own integer type; PostgreSQL 17.11 raises "+
							"and the only other honest reading is a failure — never a wrapped "+
							"number (ADR-0012 item 9, ADR-0024)", arm.name, got, tc.wantErr, tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("%s arm: failed with %v, want a message naming %q\n  SQL: %s",
							arm.name, err, tc.wantErr, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  want %v", arm.name, err, tc.sql, tc.want)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.want, tc.sql)
					continue
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17.11)\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
						break
					}
				}
			}
		})
	}
}
