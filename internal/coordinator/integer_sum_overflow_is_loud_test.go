package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/worker"
)

// An integer SUM answers PostgreSQL's exact number, or it fails; it never
// wraps.
//
// #784 gave a BARE integer column an exact Int128 carrier, so `SUM(b)` over
// the bigsum fixture answers PostgreSQL's 18446744073709551616. A COMPUTED
// integer argument was read as int4 whatever its operands were, which kept
// `SUM(CASE WHEN … THEN 1 ELSE 0 END)` — TPC-H Q12's shape and a BI staple —
// under PostgreSQL's int8 OID, and left a residual: a computed int8 sum past
// 2^63 REFUSED where PostgreSQL answers. That residual is CLOSED since #841.
// physical.aggInputIsWideInteger reads the expression's own width off the AST
// and the column declarations, so `SUM(-b)` and `SUM(CASE … ELSE b END)` take
// the same exact carrier the bare column does — PostgreSQL's own by-WIDTH SUM
// rule, `sum(int8)` numeric and `sum(int4)` bigint, on the shapes whose width
// can be read.
//
// The three cells that USED to assert a refusal now assert PostgreSQL's exact
// number; deleting a pin is what a fix's proof looks like. What has NOT moved
// is the boundary: `SUM(CASE WHEN b > 0 THEN 1 ELSE 0 END)` is bigint on the
// live server (`pg_typeof`, measured) and int64 here, and the walk answers
// "not wide" for it because every operand is an int4 literal. Both halves are
// the gate: an implementation that made every computed sum exact would fail
// the boundary cells, and one that made none exact would fail the three above.
//
// The five arms are the census's own (numeric_arc2_two_path_test.go): the
// single-process pipeline, that pipeline under a memory budget with the
// aggregate drain FORCED (ADR-0027 §6 — the reference arm stays disarmed),
// the stage DAG, the DAG with every join broadcast, and the DAG with
// morsel-parallel breakers in the worker. The carrier's flag has to survive
// the drain, the partial-state merge and the clone; each arm is where one of
// those lives.
func TestIntegerSumOverflowIsLoudOnEveryArm(t *testing.T) {
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
		// want is the expected result, or nil when the query must FAIL.
		want []string
	}{
		// b + 0 and b * 1 fold back to the bare column before the aggregate is
		// built, so they take #784's exact carrier whatever the width walk
		// says; they are the control that the exactness below is about the
		// CARRIER and not about arithmetic appearing in an argument at all.
		{"bare_column_is_exact", `SELECT SUM(b) AS s FROM bigsum`,
			[]string{"s=18446744073709551616"}},
		{"folded_zero_add_is_exact", `SELECT SUM(b + 0) AS s FROM bigsum`,
			[]string{"s=18446744073709551616"}},
		{"folded_one_multiply_is_exact", `SELECT SUM(b * 1) AS s FROM bigsum`,
			[]string{"s=18446744073709551616"}},

		// The two the int64 carrier could not hold. They REFUSED until #841
		// and now answer PostgreSQL 17.11's own numbers, measured live:
		// `sum(-b)` is numeric -18446744073709551616 and the CASE is
		// 18446744073709551616. Both carry the int8 column `b`, so the width
		// walk reads them wide.
		{"negated_column_is_exact", `SELECT SUM(-b) AS s FROM bigsum`,
			[]string{"s=-18446744073709551616"}},
		{"case_arm_over_the_column_is_exact",
			`SELECT SUM(CASE WHEN b IS NULL THEN 0 ELSE b END) AS s FROM bigsum`,
			[]string{"s=18446744073709551616"}},

		// GROUPED, where the running sums live in the flat SoA arrays and not
		// in an accumulator at all. Group 0's negated total is past the int64
		// floor by 9 006 661 665 254 993 and group 1's fits, so this cell says
		// the exact carrier reaches the SoA path and not only the scalar one.
		{"grouped_negation_is_exact",
			`SELECT g AS k, SUM(-b) AS s FROM bigsum GROUP BY g ORDER BY k`,
			[]string{"k=int32:0|s=-9232379236109516801", "k=int32:1|s=-9214364837600034815"}},

		// THE BOUNDARY, and the reason the walk answers "wide" only for an
		// expression it can see an int8 operand in. `pg_typeof` of this on the
		// live server is BIGINT — every operand is an int4 literal — so it
		// must NOT become numeric. Five of the eight rows hold a positive b;
		// the NULL row takes the ELSE arm because `NULL > 0` is not true.
		{"case_of_ones_stays_bigint",
			`SELECT SUM(CASE WHEN b > 0 THEN 1 ELSE 0 END) AS s FROM bigsum`,
			[]string{"s=int64:5"}},

		// The same boundary over a COLUMN rather than literals: g is int32, so
		// PostgreSQL's sum(int4) is bigint and so is this.
		{"negated_small_column_stays_bigint", `SELECT SUM(-g) AS s FROM bigsum`,
			[]string{"s=int64:-4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.want == nil {
					if err == nil {
						t.Errorf("%s arm: answered %v, want a 22003 refusal\n  SQL: %s\n  "+
							"PostgreSQL 17 answers exactly; wadjet's int64 carrier cannot, "+
							"so the only readings are the exact number or a failure — never a "+
							"wrapped one (ADR-0012 item 9)", arm.name, got, tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), "overflowed the 64-bit accumulator") {
						t.Errorf("%s arm: failed with %v, want the integer-carrier overflow\n  SQL: %s",
							arm.name, err, tc.sql)
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
