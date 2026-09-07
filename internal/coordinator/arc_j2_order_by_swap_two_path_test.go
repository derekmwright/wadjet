package coordinator

import (
	"context"
	"testing"
	"time"
)

// AN ORDER BY TERM NAMES AN OUTPUT COLUMN — #947, on FOUR arms against live
// PostgreSQL 17.11 over rows identical to `decpair`.
//
// `SELECT DISTINCT a AS b, b AS a FROM t ORDER BY a` publishes two columns
// whose names are SWAPPED relative to their sources. PostgreSQL binds the term
// to the OUTPUT column `a`, whose value is the source `b`. On the DAG the
// SELECT list is a Project, which walkStages emits no stage for (ADR-0025):
// the sort is folded onto the producing aggregate, whose output publishes the
// SOURCE names, and the key `a` bound the source `a` — the right rows in the
// wrong sequence on BOTH DAG arms, where the single-process path and
// PostgreSQL agree. Without the DISTINCT or the GROUP BY there is no aggregate
// to fold onto and all four arms already agreed, which is what says this is
// the fold's binding and not the parser's.
//
// `respellSortKeysOverProducerOutput` asks the producer what it calls the
// value the term names, the way every other consumer in this arc does. The
// boundary is a fact: it fires only where the term BINDS on the producer's
// stream and the output's source binds to a DIFFERENT column there — a term
// that binds nothing belongs to resolveDerivedAliasSortKeys, and a term whose
// source is the same column is already right.
func TestJ2AnOrderByTermNamesAnOutputColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const swapped = "cols=[b:DECIMAL(9,2) a:DECIMAL(18,4)] rows=9 | -0.01,-0.0100 | " +
		"0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,12.7499 | 12.75,12.7500 | " +
		"12.75,12.7501 | 12.75,NULL | NULL,NULL"
	const swappedID = "cols=[b:DECIMAL(9,2) a:DECIMAL(18,4) id:INT64] rows=9 | " +
		"-0.01,-0.0100,4 | 0.00,0.0000,6 | NULL,1.0000,7 | 2.00,10.0000,5 | " +
		"12.75,12.7499,3 | 12.75,12.7500,1 | 12.75,12.7501,2 | 12.75,NULL,8 | NULL,NULL,9"

	f1Run(t, arms, []f1Case{
		{
			// #947's exact SQL.
			name: "947 a rename SWAP under DISTINCT orders by the OUTPUT column",
			sql:  "SELECT DISTINCT a AS b, b AS a FROM decpair ORDER BY a",
			want: swapped,
		},
		{
			// The GROUP BY twin, which reaches the same fold.
			name: "947 the GROUP BY spelling of the swap",
			sql:  "SELECT a AS b, b AS a, id FROM decpair GROUP BY a, b, id ORDER BY a, id",
			want: swappedID,
		},
		{
			name: "947 the swap under DISTINCT with a third column in the key list",
			sql:  "SELECT DISTINCT a AS b, b AS a, id FROM decpair ORDER BY a, id",
			want: swappedID,
		},
		{
			// DESC, so the fix is not an artefact of the ascending comparator.
			// `id` is the tiebreaker: the two rows whose key is NULL are peers
			// and their relative order is unspecified (ADR-0013), so a cell
			// without one would assert a coin toss.
			name: "947 the swap under DISTINCT, DESC",
			sql:  "SELECT DISTINCT a AS b, b AS a, id FROM decpair ORDER BY a DESC, id DESC",
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(18,4) id:INT64] rows=9 | NULL,NULL,9 | " +
				"12.75,NULL,8 | 12.75,12.7501,2 | 12.75,12.7500,1 | 12.75,12.7499,3 | " +
				"2.00,10.0000,5 | NULL,1.0000,7 | 0.00,0.0000,6 | -0.01,-0.0100,4",
		},
		{
			// A three-way ROTATION rather than a swap: no output name is its
			// own source, so a rule that only handled a pairwise exchange
			// would miss it.
			name: "947 a three-way rename rotation under DISTINCT",
			sql:  "SELECT DISTINCT a AS b, id AS a, b AS id FROM decpair ORDER BY a",
			want: "cols=[b:DECIMAL(9,2) a:INT64 id:DECIMAL(18,4)] rows=9 | 12.75,1,12.7500 | " +
				"12.75,2,12.7501 | 12.75,3,12.7499 | -0.01,4,-0.0100 | 2.00,5,10.0000 | " +
				"0.00,6,0.0000 | NULL,7,1.0000 | 12.75,8,NULL | NULL,9,NULL",
		},
		{
			// The named output's source is an EXPRESSION, which the DISTINCT
			// lowering publishes under that expression's own text — so the
			// producer's name for it is not a column name at all. The ROWS
			// are the claim: PostgreSQL declares an unconstrained `numeric`
			// for `numeric(18,4) * 2` and wadjet declares DECIMAL(20,4) on
			// all four arms, which is ADR-0024's question and not this cell's.
			name: "947 the named output's source is an expression",
			sql:  "SELECT DISTINCT a AS b, b*2 AS a FROM decpair ORDER BY a",
			want: "cols=[b:DECIMAL(9,2) a:DECIMAL(20,4)] rows=9 | -0.01,-0.0200 | " +
				"0.00,0.0000 | NULL,2.0000 | 2.00,20.0000 | 12.75,25.4998 | 12.75,25.5000 | " +
				"12.75,25.5002 | 12.75,NULL | NULL,NULL",
		},

		// Controls. Each answers PostgreSQL on all four arms at a3f9b664 too.
		{
			// The same query with no DISTINCT and no GROUP BY: no aggregate to
			// fold the sort onto, and every arm was already right.
			name: "947 control: the same swap with no DISTINCT",
			sql:  "SELECT a AS b, b AS a FROM decpair ORDER BY a",
			want: swapped,
		},
		{
			// Written as an ORDINAL, which #557's position identity already
			// binds — the arm the re-spell must not touch.
			name: "947 control: the swap ordered by ORDINAL",
			sql:  "SELECT DISTINCT a AS b, b AS a FROM decpair ORDER BY 2",
			want: swapped,
		},
		{
			// No rename at all: the term and its source are one column, and
			// the re-spell declines by its own equality test.
			name: "947 control: DISTINCT with no rename",
			sql:  "SELECT DISTINCT a, b FROM decpair ORDER BY b",
			want: "cols=[a:DECIMAL(9,2) b:DECIMAL(18,4)] rows=9 | -0.01,-0.0100 | " +
				"0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,12.7499 | 12.75,12.7500 | " +
				"12.75,12.7501 | 12.75,NULL | NULL,NULL",
		},
		{
			// A rename whose target name NOTHING else publishes: the term
			// binds only the output, so the producer's stream carries no
			// column of that name and the re-spell must decline rather than
			// guess.
			name: "947 control: a rename to a name the source relation does not have",
			sql:  "SELECT DISTINCT a AS k, b AS m FROM decpair ORDER BY m",
			want: "cols=[k:DECIMAL(9,2) m:DECIMAL(18,4)] rows=9 | -0.01,-0.0100 | " +
				"0.00,0.0000 | NULL,1.0000 | 2.00,10.0000 | 12.75,12.7499 | 12.75,12.7500 | " +
				"12.75,12.7501 | 12.75,NULL | NULL,NULL",
		},
	})
}
