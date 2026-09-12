package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A NULL GROUP KEY IS ITS OWN GROUP ON EVERY ARM, INCLUDING A MORSEL-PARALLEL
// WORKER — #1058, five arms, every answer measured on live postgres:17-alpine.
//
// The filing is "a UNION with a NULL arm over a STRING column drops the NULL
// row on some runs of the DAG arms". The mechanism is neither the UNION nor
// the exchange: `runBreakerConsumeParallel` — the branch a fragment takes when
// the worker runs its breaker morsel-parallel — never INITIALIZED the primary
// sink, while the k == 1 branch beside it runs `exec.Pipeline.Run`, which
// does. A `HashAggregate` built by the worker's fragment builder is
// initialized nowhere else, so on that branch it consumed with
// `strNullGroupIdx` at its ZERO VALUE, which is the valid slot 0.
//
// The single-STRING and single-BYTES fast path keeps its NULL group OUT of the
// key table on purpose (a real one-byte "\x01" key would collide with the
// binary null sentinel) and addresses it through that index alone, so every
// NULL key in the PRIMARY's share of the morsels bound whichever group the
// primary minted first:
//
//   - `SELECT c_str, COUNT(*) … WHERE id IN (41,42) GROUP BY c_str` answered
//     one row, `s-000041, 2`, for PostgreSQL's `s-000041,1` and `NULL,1` — the
//     NULL row counted into a group it is not in;
//   - `SELECT DISTINCT c_str …` over the same pair dropped the NULL row, which
//     is the filing's shape one operator down;
//   - where the NULL arrived before any group existed the task failed LOUD in
//     `scatterCountStar` with a group index past the end of the accumulator
//     arrays, three attempts, and the query errored.
//
// Which of the three a run got depends on how the dispenser divided the
// morsels between the primary and its clones, which is why #1058 recorded it
// as a per-run coin toss. The coin toss was in the MEASUREMENT: the filing's
// probe gave the plain coordinator and the morsel coordinator ONE embedded
// NATS, so a task could be executed by either kind of worker, and it read 6/8
// on `dag` and 2/8 on `dag-morsel`. Each arm below has its OWN infra, and with
// that the defect is 8/8 on the morsel arm and 0/8 on the other four.
//
// INT64, BOOL and ROW are stable in the same shape and always were: every
// other key path carries its NULL key IN the table, which needs no index at
// all. So the census below runs every FLAT type rather than the one the filing
// names, from typematrix.Columns() so a type added to the corpus cannot be
// forgotten here.
func TestC3ANullGroupKeyIsItsOwnGroupOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c3Arms(t, ctx)

	// ---- The REPLICATION table (ADR-0027: one passing run proves nothing for
	// a defect whose trigger is a condition). Eight repetitions per arm per
	// type, and the morsel arm's engagement is asserted from the worker's own
	// counter below — a gate on a branch that never ran says nothing.
	before := worker.MorselParallelBreakerRuns.Load()
	for _, col := range typematrix.Columns() {
		if !col.Flat {
			continue // ARRAY/ROW/MAP/VECTOR are not group keys
		}
		sql := fmt.Sprintf(
			"SELECT NULL AS p FROM %s WHERE id=2 UNION SELECT %s AS p FROM %s WHERE id=5",
			typematrix.Table, col.Name, typematrix.Table)
		t.Run("null_arm_union/"+col.Name, func(t *testing.T) {
			for _, arm := range arms {
				for rep := 0; rep < c3Reps; rep++ {
					rows, err := arm.run(sql)
					if err != nil {
						t.Fatalf("%s arm, rep %d: %s: %v", arm.name, rep, sql, err)
					}
					// PostgreSQL answers two rows for every one of these: the
					// NULL and the row's value. Measured on 17.11 for c_str
					// (`s-000005`), c_i64 (`5000015`) and c_bool (`f`); the
					// VALUE half of every column is what the type-matrix
					// two-path corpus already compares, so what this census
					// adds is that the NULL is a row of its own.
					if len(rows) != 2 || !c3HasNull(rows) {
						t.Fatalf("%s arm, rep %d: %s\n  got  %v\n  want two rows, one of them NULL",
							arm.name, rep, sql, rows)
					}
				}
			}
		})
	}
	if moved := worker.MorselParallelBreakerRuns.Load() - before; moved == 0 {
		t.Fatalf("no fragment ran its breaker morsel-parallel: this census proves nothing " +
			"about the branch #1058 lived in")
	}

	// ---- The VALUE cells, every want measured on live postgres:17-alpine
	// over rows identical to the type-matrix fixture. These are the shapes the
	// mechanism really covers: a bare GROUP BY and a bare DISTINCT over a
	// nullable STRING, with no set operation anywhere.
	c3Run(t, arms, []c3Case{
		{
			// The filing's own shape.
			name: "1058 a UNION with a NULL arm over a STRING column",
			sql: "SELECT NULL AS p FROM typemx WHERE id=2 " +
				"UNION SELECT c_str AS p FROM typemx WHERE id=5",
			want: "NULL | s-000005",
		},
		{
			// The same with the arms the other way round: the NULL arrives
			// after a group exists rather than before it.
			name: "1058 the same with the NULL arm second",
			sql: "SELECT c_str AS p FROM typemx WHERE id=5 " +
				"UNION SELECT NULL AS p FROM typemx WHERE id=2",
			want: "NULL | s-000005",
		},
		{
			// UNION ALL: the control. No dedup, so no group key, so no NULL
			// group to lose — it was stable at the base commit and must stay.
			name: "control: UNION ALL over the same two rows",
			sql: "SELECT NULL AS p FROM typemx WHERE id=2 " +
				"UNION ALL SELECT c_str AS p FROM typemx WHERE id=5",
			want: "NULL | s-000005",
		},
		{
			// NO SET OPERATION: one GROUP BY over one nullable STRING column,
			// one NULL row and one string row. `s-000041,2` was the answer on
			// the morsel arm — the NULL row counted into a group it is not in,
			// which no row count and no NULL check downstream can see.
			name: "1058 GROUP BY a nullable STRING, one NULL and one string",
			sql: "SELECT c_str AS p, COUNT(*) AS n FROM typemx WHERE id IN (41,42) " +
				"GROUP BY c_str ORDER BY 1",
			want: "s-000041,1 | NULL,1",
		},
		{
			// ALL-NULL input: the primary's first key is the NULL one, so
			// there is no group 0 to bind to at all. This is the cell that
			// failed LOUD (`scatterCountStar: gi[0]=0 out of range,
			// len(countArr)=0`) rather than silently.
			name: "1058 GROUP BY a nullable STRING over an all-NULL input",
			sql:  "SELECT c_str AS p, COUNT(*) AS n FROM typemx WHERE id = 42 GROUP BY c_str",
			want: "NULL,1",
		},
		{
			// Four NULLs and one string, so a NULL folded into the string's
			// group shows up in the COUNT rather than only in the row count.
			name: "1058 four NULL rows beside one string",
			sql: "SELECT c_str AS p, COUNT(*) AS n FROM typemx WHERE id < 200 " +
				"AND (c_str IS NULL OR id=7) GROUP BY c_str ORDER BY 1",
			want: "s-000007,1 | NULL,4",
		},
		{
			// DISTINCT rather than GROUP BY: the same keys-only hash
			// aggregate, which is also what a distinct UNION's dedup stage is.
			name: "1058 SELECT DISTINCT over a nullable STRING",
			sql:  "SELECT DISTINCT c_str AS p FROM typemx WHERE id IN (42,43) ORDER BY 1",
			want: "s-000043 | NULL",
		},
		{
			// AT SCALE: all 5000 rows, 116 of them NULL, spread across four
			// files — so the morsels really divide and the NULL rows land on
			// the primary AND on clones. The NULL group's COUNT is the
			// assertion: 116 is what PostgreSQL 17.11 answers.
			name: "1058 at 5000 rows: the NULL group's own count",
			sql: "SELECT c_str AS p, COUNT(*) AS n FROM typemx " +
				"GROUP BY c_str ORDER BY n DESC, p LIMIT 3",
			want: "NULL,116 | s-000000,1 | s-000001,1",
		},
		{
			// The same input through DISTINCT: 4884 distinct strings plus one
			// NULL group.
			name: "1058 at 5000 rows: DISTINCT group count",
			sql:  "SELECT COUNT(*) AS ngroups FROM (SELECT DISTINCT c_str FROM typemx) t",
			want: "4885",
		},
		{
			// BYTES takes the same fast path as STRING and lost its NULL the
			// same way. Rendered as the decimal byte list f1WriteRow writes.
			name: "1058 a UNION with a NULL arm over a BYTES column",
			sql: "SELECT NULL AS p FROM typemx WHERE id=2 " +
				"UNION SELECT c_bytes AS p FROM typemx WHERE id=5",
			want: "NULL | [98 121 116 101 115 45 48 48 48 48 48 53 45 120 120 120 120 120]",
		},
		{
			// CONTROL: INT64 in the filing's shape. Its key path carries the
			// NULL key IN the table, so it never had this defect and must not
			// acquire one.
			name: "control: the same UNION over an INT64 column",
			sql: "SELECT NULL AS p FROM typemx WHERE id=2 " +
				"UNION SELECT c_i64 AS p FROM typemx WHERE id=5",
			want: "5000015 | NULL",
		},
		{
			// CONTROL: a nullable STRING with NO NULL in scope. The fast path
			// runs, the NULL group is never minted, and the answer must not
			// move.
			name: "control: a STRING GROUP BY with no NULL in scope",
			sql: "SELECT c_str AS p, COUNT(*) AS n FROM typemx WHERE id IN (5,6) " +
				"GROUP BY c_str ORDER BY 1",
			want: "s-000005,1 | s-000006,1",
		},
	})
}

// c3Reps is the replication factor for the condition-triggered cells
// (ADR-0027: a single passing run proves nothing). The filing measured eight
// repetitions per arm; this census keeps that number so the two tables compare.
const c3Reps = 8

// c3Arm is one execution arm: a name, a runner that renders one answer as
// sorted row strings, and the coordinator whose local-route counters say
// whether the DAG executed the query or refused it and answered in process.
type c3Arm struct {
	name  string
	run   func(sql string) ([]string, error)
	coord *Coordinator
}

// c3Arms stands up the FIVE arms this arc's censuses run on: the two
// single-process ones and the three distributed shapes. dag-morsel is the one
// #1058 lives on and it gets its own infra — sharing one NATS with the plain
// coordinator lets either kind of worker pick up either coordinator's tasks,
// which is what made the filing's measurement a coin toss instead of a fact.
func c3Arms(t *testing.T, ctx context.Context) []c3Arm {
	t.Helper()
	single := tmdStandalone(t, ctx)
	spilled := e3BudgetedStandalone(t, ctx)
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

	local := func(db *wadjet.DB) func(string) ([]string, error) {
		return func(sql string) ([]string, error) { return c3RenderSingle(ctx, db, sql) }
	}
	dag := func(c *Coordinator) func(string) ([]string, error) {
		return func(sql string) ([]string, error) { return c3RenderDAG(ctx, c, sql) }
	}
	return []c3Arm{
		{"single", local(single), nil},
		{spilledArm, local(spilled), nil},
		{"dag", dag(coord), coord},
		{"dag-shuffled", dag(coordB), coordB},
		{"dag-morsel4", dag(coordM), coordM},
	}
}

// c3Case is one shape: the SQL and PostgreSQL 17's answer as c3Render writes
// it, with per-arm overrides for a divergence this arc PINS rather than fixes.
type c3Case struct {
	name string
	sql  string
	want string
	pin  map[string]string
	why  string
}

// c3Run asserts every case on every arm, and asserts the DISPOSITION beside
// the rows on the distributed ones: rows alone cannot tell a query the DAG
// EXECUTED from one it refused and the coordinator answered locally, and both
// are results this arc moves (COMMON.md).
func c3Run(t *testing.T, arms []c3Arm, cases []c3Case) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := f1Counters(arm.coord)
				rows, err := arm.run(tc.sql)
				got := strings.Join(rows, " | ")
				if err != nil {
					got = "ERR " + err.Error()
				}
				want := tc.want
				if p, ok := tc.pin[arm.name]; ok {
					want = p
				}
				if got != want {
					why := ""
					if tc.why != "" {
						why = "\n  pinned: " + tc.why
					}
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s",
						tc.sql, arm.name, got, want, why)
				}
				if arm.coord != nil {
					if moved := f1CounterDelta(before, f1Counters(arm.coord)); moved != "" {
						t.Errorf("%s\n  arm %s routed local (%s), so its rows say nothing "+
							"about the distributed path", tc.sql, arm.name, moved)
					}
				}
			}
		})
	}
}

// c3RenderSingle and c3RenderDAG render one result as SORTED row strings.
//
// Sorted because most of these shapes have no ORDER BY and a group emission
// order is not a contract (ADR-0013); the cells that DO assert an order name
// their keys in the SQL, and sorting a sequence that is already PostgreSQL's
// leaves it alone — every `want` here was written from the live server's own
// output in that state.
func c3RenderSingle(ctx context.Context, db *wadjet.DB, sql string) ([]string, error) {
	out, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	rows := make([]string, 0, len(out.Rows))
	for i := range out.Rows {
		rows = append(rows, c3Row(out.Cells(i)))
	}
	return c3Sorted(sql, rows), nil
}

func c3RenderDAG(ctx context.Context, c *Coordinator, sql string) ([]string, error) {
	out, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, fmt.Errorf("%s", out.Error)
	}
	s := out.Stream()
	defer s.Close()
	var rows []string
	for {
		b, err := s.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRowValues() {
			rows = append(rows, c3Row(r))
		}
	}
	return c3Sorted(sql, rows), nil
}

// c3Sorted sorts a result unless the query asked for an order of its own.
func c3Sorted(sql string, rows []string) []string {
	if !strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		sort.Strings(rows)
	}
	return rows
}

func c3Row(cells []any) string {
	var b strings.Builder
	for i, c := range cells {
		if i > 0 {
			b.WriteString(",")
		}
		if c == nil {
			b.WriteString("NULL")
			continue
		}
		fmt.Fprintf(&b, "%v", c)
	}
	return b.String()
}

// c3HasNull reports whether one of these single-column rows is the NULL one.
func c3HasNull(rows []string) bool {
	for _, r := range rows {
		if r == "NULL" {
			return true
		}
	}
	return false
}
