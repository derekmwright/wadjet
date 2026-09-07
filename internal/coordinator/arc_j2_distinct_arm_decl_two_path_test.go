package coordinator

import (
	"context"
	"testing"
	"time"
)

// A DISTINCT ARM'S COMPUTED COLUMN CROSSES A STAGE UNDER ITS DECLARED TYPE —
// #949, on FOUR arms against live PostgreSQL 17.11 over rows identical to
// `decpair`.
//
// `SELECT SUM(v*2) FROM (SELECT DISTINCT a*2 AS v FROM decpair) x` failed both
// DAG arms with `cannot store string into FLOAT64 vector (#361 silent-write
// guard)`, terminal after three attempts, and answered PostgreSQL's 58.96
// single-process while declaring FLOAT64 where PostgreSQL declares numeric.
//
// One root: the DISTINCT lowering turns every SELECT item into a GROUP BY key,
// so `a * 2` is what the aggregate GROUPS BY and EMITS — under that text — and
// the Project above it renames the emitted column to `v`. The declared-type
// walk read that projection's expression as ARITHMETIC, looked for `a`, found
// nothing (the aggregate emits no `a` at all) and fell to the float rule. `v`
// was DECIMAL(11,2) on the producer and FLOAT64 to every consumer, so
// `SUM(v*2)` declared FLOAT64 on every arm — and on the DAG the worker's
// pre-aggregate projection then allocated a float vector for a DECIMAL value,
// which is what #361's guard is for.
//
// Above such a producer the expression is a NAME, not structure (ADR-0026
// §2c), and the fix is this arc's rule at the declaration: the consumer takes
// the producer's own declaration instead of re-deriving one.
func TestJ2ADistinctArmComputedColumnKeepsItsType(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			// #949's exact SQL. PostgreSQL: numeric 58.96.
			name: "949 SUM over a DISTINCT derived table's computed column",
			sql:  "SELECT SUM(v*2) AS s FROM (SELECT DISTINCT a*2 AS v FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 58.96",
		},
		{
			// The argument is the alias ITSELF rather than an expression over
			// it, so the aggregate reads the column instead of projecting one.
			name: "949 the same aggregate over the bare alias",
			sql:  "SELECT SUM(v) AS s FROM (SELECT DISTINCT a*2 AS v FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 29.48",
		},
		{
			// MAX copies its input's declaration, so this cell says the (p,s)
			// crossing the stage is the arm's own and not SUM's widening.
			name: "949 MAX keeps the arm's own (p,s)",
			sql:  "SELECT MAX(v) AS s FROM (SELECT DISTINCT a*2 AS v FROM decpair) x",
			want: "cols=[s:DECIMAL(11,2)] rows=1 | 25.50",
		},
		{
			// The computed column as a GROUP BY key one level up, where the
			// value crosses the stage as a KEY rather than as an argument.
			name: "949 the arm's computed column as an outer GROUP BY key",
			sql: "SELECT v, COUNT(*) AS n FROM (SELECT DISTINCT a*2 AS v, b FROM decpair) x " +
				"GROUP BY v ORDER BY v",
			want: "cols=[v:DECIMAL(11,2) n:INT64] rows=5 | -0.02,1 | 0.00,1 | 4.00,1 | " +
				"25.50,4 | NULL,2",
		},
		{
			// A predicate on the alias above the DISTINCT, which is the third
			// consumer of the same declaration.
			name: "949 a WHERE on the arm's computed column",
			sql:  "SELECT SUM(v*2) AS s FROM (SELECT DISTINCT a*2 AS v FROM decpair) x WHERE v > 1",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 59.00",
		},
		{
			// An INTEGER source rather than a DECIMAL one: the declaration
			// follows the arm's type, so a rule that only rescued DECIMAL
			// would leave this one behind.
			name: "949 the same shape over an integer column",
			sql:  "SELECT SUM(v*2) AS s FROM (SELECT DISTINCT id*2 AS v FROM decpair) x",
			want: "cols=[s:DECIMAL(38,0)] rows=1 | 180",
		},
	})
}
