package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// A number with no int32 is REFUSED on every arm, never wrapped into a
// plausible-looking value.
//
// `<bigint>::DATE` was the one shape in the grammar that reached
// batch.Vector.SetValue's DATE arm, and at de5bc970 it WRAPPED:
// `SELECT 3000000000::DATE` answered -3543531-12-19, a date rendered like any
// other, with no error on any path (CodeQL go/incorrect-integer-conversion
// #34). #32 and #33 are the PORT/PROTOCOL twins, and the record here used to
// say no SQL cast reaches them: it did not, because `::PORT` and `::PROTOCOL`
// — and `::INT32` — matched no label in Cast.Eval's switch and were declared
// STRING, so the number came back as TEXT rather than reaching any vector.
// Those three are in the table below since #901.
//
// PostgreSQL has no int-to-date cast at all: `SELECT 3000000000::date` is
// 42846, `cannot cast type bigint to date`. Wadjet's cast is a deliberate
// SUPERSET (ADR-0012 item 5), and inside a superset the rule is that a value
// it cannot represent is LOUD — 22003, the class PostgreSQL uses for the same
// magnitude reaching an int4 — and never a different number.
//
// Four arms, because a per-row refusal has to SURVIVE the trip: a worker's
// panic boundary turning it into a query error is what makes the DAG arms say
// anything at all, and a refusal that is swallowed there reads as an empty
// result rather than an error.
func TestAnInt32DomainRefusalHoldsOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	// The FOURTH arm is the spilled one (ADR-0027): a refusal raised per row
	// has to survive the drain and the merge as well as the worker's panic
	// boundary, and this census had only the three distribution arms.
	spilled := na2Standalone(t, ctx, 512*1024)

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
		{"dag-shuffled", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, sql) }},
		{"spilled", func(sql string) (*oracle.Result, error) {
			restore := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			defer func() { restoreRuns(); exec.ForceAggDrainEvery(restore) }()
			return tmdRunSingle(ctx, spilled, sql)
		}},
	}

	tbl := typematrix.Table
	for _, tc := range []struct {
		name    string
		sql     string
		refused bool
	}{
		// 3000000000 has no int32. Built from the column so the value reaches
		// the store as a computed per-row box, which is the shape #841's guard
		// exists for.
		{"date_above_the_range", "SELECT (c_i64 + 3000000000)::DATE AS v FROM " + tbl, true},
		{"date_below_the_range", "SELECT (c_i64 - 3000000000)::DATE AS v FROM " + tbl, true},
		// The boundary from the other side: a day count an int32 holds is an
		// ANSWER, so the refusals above are about the VALUE and not about the
		// cast existing. c_i32 rather than c_i64 because c_i64 runs to
		// i*1000003 and leaves the int32 range on its own.
		{"date_inside_the_range", "SELECT (c_i32 + 1)::DATE AS v FROM " + tbl, false},
		// #911: the magnitudes that escaped the store guard entirely, because
		// parseDateValue read the day count through time.Date's unmodulated
		// 86400 multiply and handed SetValue an int64 that FITS an int32 —
		// 2^63-1 days came back as day -1, rendered 1969-12-31. Constants,
		// because the wrap needs a magnitude past 2^57 that no column in the
		// matrix carries.
		{"date_int64_max", "SELECT 9223372036854775807::DATE AS v", true},
		{"date_int64_min", "SELECT (-9223372036854775808)::DATE AS v", true},
		{"date_two_to_the_62", "SELECT 4611686018427387904::DATE AS v", true},
		// #901: the three siblings that reached NO guard at all, because the
		// cast declared STRING and Cast.Eval handed the operand back. Built
		// from the column for the same reason the DATE cells are, so the
		// value is a computed per-row box and not a constant the planner
		// could fold.
		{"int32_above_the_range", "SELECT (c_i64 + 3000000000)::INT32 AS v FROM " + tbl, true},
		{"int32_below_the_range", "SELECT (c_i64 - 3000000000)::INT32 AS v FROM " + tbl, true},
		{"int32_inside_the_range", "SELECT (c_i32 + 1)::INT32 AS v FROM " + tbl, false},
		{"port_above_the_range", "SELECT (c_i64 + 3000000000)::PORT AS v FROM " + tbl, true},
		{"port_inside_the_range", "SELECT (c_i32 + 1)::PORT AS v FROM " + tbl, false},
		{"protocol_above_the_range", "SELECT (c_i64 + 3000000000)::PROTOCOL AS v FROM " + tbl, true},
		{"protocol_inside_the_range", "SELECT (c_i32 + 1)::PROTOCOL AS v FROM " + tbl, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				_, err := arm.run(tc.sql)
				switch {
				case tc.refused && err == nil:
					t.Errorf("%s arm: %s answered instead of refusing; no int32 holds that value",
						arm.name, tc.sql)
				case tc.refused && !strings.Contains(err.Error(), "integer out of range"):
					t.Errorf("%s arm: %s refused with %v, want PostgreSQL's \"integer out of range\"",
						arm.name, tc.sql, err)
				case !tc.refused && err != nil:
					t.Errorf("%s arm: %s refused a value an int32 holds: %v",
						arm.name, tc.sql, err)
				}
			}
		})
	}

	// FLOAT32 is the same enumeration gap one family over and carries REAL's
	// own message rather than int4's, so it is asserted apart from the table
	// (#901). `CAST(x AS FLOAT32)` used to answer the double as TEXT where
	// `CAST(x AS REAL)` raised on the same value.
	t.Run("float32_past_the_carrier", func(t *testing.T) {
		for _, arm := range arms {
			_, err := arm.run("SELECT CAST(c_f64 + 1e300 AS FLOAT32) AS v FROM " + tbl)
			if err == nil {
				t.Errorf("%s arm: a value no float4 holds was answered, not refused", arm.name)
				continue
			}
			// The wording depends on the source: a LITERAL operand names its
			// own text ("value 1e40 is out of range for type real"), a COLUMN
			// one says "value out of range: overflow". Both are REAL's 22003;
			// what this cell is about is that the cast refuses at all.
			if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("%s arm: refused with %v, want REAL's own 22003", arm.name, err)
			}
		}
	})
	t.Run("float32_inside_the_carrier", func(t *testing.T) {
		for _, arm := range arms {
			if _, err := arm.run("SELECT CAST(c_i32 AS FLOAT32) AS v FROM " + tbl); err != nil {
				t.Errorf("%s arm: refused a value a float4 holds: %v", arm.name, err)
			}
		}
	})

	// The VALUE and its BOX on every arm, not only the disposition (round-1
	// review P2).
	//
	// The table above asserts `err != nil` and `err == nil`, and a right→wrong
	// move on the DAG's S3 round trip is invisible to that: the cast's result
	// is MATERIALIZED between stages there, and a declaration the gather reads
	// differently would change the box a client sees while every cell stayed
	// green. na2Run prints the Go box beside the value (`int64:` / `int32:` /
	// `float:`), so a PORT that arrives as a float64 or an INT32 that arrives
	// as a string fails here.
	//
	// The values are the fixture's own arithmetic — `c_i32` is `id*3`, NULL on
	// every 29th row — so nothing here is transcribed from a run. Where
	// PostgreSQL has the cast (`::INT32` is `int4`), its answer is the same
	// number; `PORT`, `PROTOCOL`, `FLOAT32` and an integer `DATE` are wadjet's
	// own spellings and the number is the engine's.
	for _, c := range []struct {
		name, sql string
		want      []string
	}{
		{"int32_value", `SELECT (c_i32 + 1)::INT32 AS v FROM ` + tbl + ` WHERE id = 400`,
			[]string{"v=int64:1201"}},
		{"port_value", `SELECT (c_i32 + 1)::PORT AS v FROM ` + tbl + ` WHERE id = 400`,
			[]string{"v=int32:1201"}},
		{"protocol_value", `SELECT (c_i32 + 1)::PROTOCOL AS v FROM ` + tbl + ` WHERE id = 400`,
			[]string{"v=int32:1201"}},
		{"float32_value", `SELECT CAST(c_i32 AS FLOAT32) AS v FROM ` + tbl + ` WHERE id = 400`,
			[]string{"v=float:1200"}},
		{"date_value", `SELECT (c_i32 % 100)::DATE AS v FROM ` + tbl + ` WHERE id = 400`,
			[]string{"v=1970-01-01"}},
		// A PORT cast as a GROUP BY key: on the shuffled arm this is the
		// partition key, so the declaration crosses a repartition and comes
		// back out of a .wshf file.
		{"port_group_key",
			`SELECT (c_i32 + 1)::PORT AS g, COUNT(*) AS n FROM ` + tbl +
				` WHERE id > 0 AND id < 5 GROUP BY (c_i32 + 1)::PORT ORDER BY 1`,
			[]string{"g=int32:10|n=int64:1", "g=int32:13|n=int64:1",
				"g=int32:4|n=int64:1", "g=int32:7|n=int64:1"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(c.sql)
				rows, rerr := na2Run(got, err)
				if rerr != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, rerr, c.sql)
					continue
				}
				if strings.Join(rows, ";") != strings.Join(c.want, ";") {
					t.Errorf("%s arm: = %v, want %v — the value or its BOX moved\n  SQL: %s",
						arm.name, rows, c.want, c.sql)
				}
			}
		})
	}
}
