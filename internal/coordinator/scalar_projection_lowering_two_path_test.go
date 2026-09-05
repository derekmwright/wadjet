package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// The SELECT-list scalar-subquery lowering (#659), gated on VALUES rather than
// on routes.
//
// The correlation census asserts what each shape ROUTES, which is the property
// the lowering exists to move — and three defects lived entirely inside the
// cells where the route was already 0. Round 1's review found all three, and
// each one is a query whose DAG answer differed from its single-process answer
// with no counter moving and no error raised:
//
//	twelve SELECT-list subqueries in one list  -> a wrong NUMBER, per run
//	two producers over one CTE                 -> a hard task failure
//	a not-provably-one-row subquery            -> a wrong VALUE and a wrong box
//
// So this file compares the ANSWER on all three arms for the shapes the census
// samples one of, and every cell is replicated: two of the three defects were
// nondeterministic (Go map order in one, and which columns it corrupted in the
// other), so a single passing run proves nothing.
func TestSelectListSubqueriesAnswerTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	// MANY producers in one SELECT list. Ten was not reachable from ordinary
	// SQL before this lowering — a filter carries one or two — and the
	// coordinator's placeholder substitution was `strings.ReplaceAll` per
	// entry over a MAP range, so `:scalar_1` matched inside `:scalar_10` and
	// left a stray digit on that producer's literal. Twelve items is what
	// makes a two-digit placeholder certain; the replicates are what make the
	// map order irrelevant.
	manyItems := func(n int) string {
		var b strings.Builder
		b.WriteString("SELECT id")
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, ", (SELECT MAX(c_i64) + %d FROM typemx) AS s%d", i, i)
		}
		b.WriteString(" FROM typemx WHERE id < 2 ORDER BY id")
		return b.String()
	}

	const cte = `WITH c AS (SELECT id, c_i64 AS v FROM typemx) `

	cells := []struct {
		name, sql string
		// wantRoutes is the ScalarProjectionLocalRoutes delta each DAG arm
		// must show. 0 = the DAG computed it; 1 = it declined and the
		// coordinator-local pipeline answered. Both are asserted, because a
		// shape that silently stopped being lowered is a cost regression and
		// one that silently started being lowered is how all three round-1
		// defects reached a client.
		wantRoutes int64
		// readsACTE marks a cell whose subquery reads a CTE. Such a subquery
		// defers whatever WADJET_SCALAR_DEFER says -- eager evaluation over
		// the cteCache float-drifts against the outer query's distributed
		// aggregate, which is Q15's zero-row root cause -- so its route does
		// not move with the switch. Every other cell declines when the switch
		// is off, because with no deferral there is no producer.
		readsACTE bool
	}{
		{"twelve_producers_in_one_select_list", manyItems(12), 0, false},
		{"sixteen_producers_in_one_select_list", manyItems(16), 0, false},
		{"two_producers_in_one_select_list", manyItems(2), 0, false},

		// A CTE body consumed by TWO producers. `ctePlannedTerminal` hands the
		// second producer the first's stage and the emission puts a
		// `cte-alias` phantom in its place — and the pass that resolves those
		// runs inside generateStages, before this lowering exists, so the
		// phantom reached dispatch and the stage failed three times with
		// `empty Operators on task … (StageType="cte-alias")` and no SQLSTATE.
		// Declined now, which is what these two had at base.
		{"two_select_list_producers_over_one_cte",
			cte + `SELECT id, (SELECT MAX(v) FROM c) AS hi, (SELECT MIN(v) FROM c) AS lo ` +
				`FROM typemx WHERE id<2 ORDER BY id`, 1, true},
		{"a_select_list_and_a_where_producer_over_one_cte",
			cte + `SELECT id, (SELECT MAX(v) FROM c) AS mx FROM typemx ` +
				`WHERE c_i64 < (SELECT MAX(v) FROM c) AND id<2 ORDER BY id`, 1, true},
		// The control that says the decline is about SHARING a CTE body and
		// not about CTEs: two producers over two DIFFERENT CTEs lower.
		{"two_producers_over_two_different_ctes",
			`WITH c AS (SELECT id, c_i64 AS v FROM typemx), d AS (SELECT id, c_i32 AS w FROM typemx) ` +
				`SELECT id, (SELECT MAX(v) FROM c) AS hi, (SELECT MAX(w) FROM d) AS lo ` +
				`FROM typemx WHERE id<2 ORDER BY id`, 0, true},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			want, err := na2Run(tmdRunSingle(ctx, single, tc.sql))
			if err != nil {
				t.Fatalf("single arm: %v\n  SQL: %s", err, tc.sql)
			}
			sort.Strings(want)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				// THREE replicates per arm: the substitution defect depended
				// on Go map iteration order, so it corrupted different columns
				// on different runs and one pass proved nothing.
				for rep := 0; rep < 3; rep++ {
					before := arcD5Routes(arm.c)
					got, err := na2Run(tmdRunDAG(ctx, arm.c, tc.sql))
					if err != nil {
						t.Fatalf("%s arm, replicate %d: %v\n  SQL: %s", arm.name, rep, err, tc.sql)
					}
					sort.Strings(got)
					if len(got) != len(want) {
						t.Fatalf("%s arm, replicate %d: %d rows, want %d\n  got  %v\n  want %v\n  SQL: %s",
							arm.name, rep, len(got), len(want), got, want, tc.sql)
					}
					for i := range got {
						if got[i] != want[i] {
							t.Fatalf("%s arm, replicate %d, row %d:\n  got  %s\n  want %s "+
								"(the single-process arm, which is PostgreSQL's)\n  SQL: %s",
								arm.name, rep, i, got[i], want[i], tc.sql)
						}
					}
					want := tc.wantRoutes
					if !tc.readsACTE && !physical.ScalarSubqueriesAreDeferred() {
						want = 1 // no deferral, no producer, no lowering
					}
					if d := arcD5RouteDelta(before, arcD5Routes(arm.c))[1]; d != want {
						t.Errorf("%s arm, replicate %d: ScalarProjectionLocalRoutes moved by %d, want %d\n  SQL: %s",
							arm.name, rep, d, want, tc.sql)
					}
				}
			}
		})
	}
}

// A scalar subquery whose value the DAG cannot carry EXACTLY is never
// rendered, for every type and with the deferral switch both ways.
//
// `resolveSubqueryAST` has two exits and only one of them is a producer: a
// subquery that is not provably one row (`… ORDER BY id LIMIT 1`) is EXECUTED
// on the coordinator at plan time and its value spliced into the item as a
// literal. That literal never met scalarProducerValueIsLiteralSafe, so a
// DECIMAL(18,4) reached a worker as `1` where PostgreSQL and the single path
// answer 12.7500, and a TIMESTAMP, a FLOAT64 and a DURATION came back
// int64-boxed where the single path answers text. `WADJET_SCALAR_DEFER=0` is
// the same door: with the switch off NOTHING defers, so every shape took the
// splice — including the census's own DECIMAL cell, which answered float:7.57
// for 7.570000 with its route counter at zero.
//
// The assertion is the DISPOSITION, per type: the item is not lowered, so no
// value is rendered into text at all. That is the property the fix
// establishes, and it is the one that is deterministic — the
// coordinator-local route these shapes take disagrees with the standalone
// engine intermittently for several of these types, at this arc's base and
// with the lowering disabled entirely, which is a defect of its own and is
// filed rather than gated here.
func TestAScalarSubqueryTheDagCannotCarryExactlyIsNeverRendered(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	// Every column of the type matrix that has a literal spelling problem,
	// plus the three whose literal IS the value (the control side of the
	// allowlist).
	notCarried := []struct{ name, sql string }{
		{"decimal_9_2", `SELECT id, (SELECT a FROM decpair WHERE a IS NOT NULL ORDER BY id LIMIT 1) AS v FROM decpair WHERE id<2 ORDER BY id`},
		{"decimal_18_4", `SELECT id, (SELECT b FROM decpair WHERE b IS NOT NULL ORDER BY id LIMIT 1) AS v FROM decpair WHERE id<2 ORDER BY id`},
		{"decimal_via_an_aggregate", `SELECT id, (SELECT AVG(a) FROM decpair) AS v FROM decpair WHERE id<2 ORDER BY id`},
		{"date", `SELECT id, (SELECT c_date FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"timestamp", `SELECT id, (SELECT c_ts FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"uuid", `SELECT id, (SELECT c_uuid FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"ipv4", `SELECT id, (SELECT c_ipv4 FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"cidr", `SELECT id, (SELECT c_cidr FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"float64", `SELECT id, (SELECT c_f64 FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"duration", `SELECT id, (SELECT c_dur FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"bytes", `SELECT id, (SELECT c_bytes FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
		// An INT64 whose subquery is not provably one row: the VALUE is
		// carryable but the plan-time splice is not a producer, so it
		// declines with the rest. That is the pair that says the rule is
		// "every subquery becomes a producer", not "this type is safe".
		{"int64_not_provably_one_row", `SELECT id, (SELECT c_i64 FROM typemx ORDER BY id LIMIT 1) AS v FROM typemx WHERE id<2 ORDER BY id`},
	}
	carried := []struct{ name, sql string }{
		{"int64_aggregate", `SELECT id, (SELECT MAX(c_i64) FROM typemx) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"int32_aggregate", `SELECT id, (SELECT MAX(c_i32) FROM typemx) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"string_aggregate", `SELECT id, (SELECT MAX(c_str) FROM typemx) AS v FROM typemx WHERE id<2 ORDER BY id`},
		{"bool_aggregate", `SELECT id, (SELECT MAX(c_bool) FROM typemx) AS v FROM typemx WHERE id<2 ORDER BY id`},
	}

	check := func(t *testing.T, sql string, wantRoutes int64) {
		t.Helper()
		if !physical.ScalarSubqueriesAreDeferred() {
			// With the deferral off nothing becomes a producer, so every cell
			// declines and the two lists stop being a pair.
			wantRoutes = 1
		}
		for _, arm := range []struct {
			name string
			c    *Coordinator
		}{{"dag", coord}, {"dag-shuffled", coordB}} {
			before := arcD5Routes(arm.c)
			if _, err := na2Run(tmdRunDAG(ctx, arm.c, sql)); err != nil {
				t.Fatalf("%s arm: %v\n  SQL: %s", arm.name, err, sql)
			}
			if d := arcD5RouteDelta(before, arcD5Routes(arm.c))[1]; d != wantRoutes {
				t.Errorf("%s arm: ScalarProjectionLocalRoutes moved by %d, want %d.\n"+
					"  0 means the DAG LOWERED this item, which renders its value into "+
					"literal text a worker re-parses — and this shape's value does not "+
					"survive that round trip.\n  SQL: %s", arm.name, d, wantRoutes, sql)
			}
		}
	}

	for _, tc := range notCarried {
		t.Run("declines/"+tc.name, func(t *testing.T) { check(t, tc.sql, 1) })
	}
	for _, tc := range carried {
		t.Run("lowers/"+tc.name, func(t *testing.T) { check(t, tc.sql, 0) })
	}
}
