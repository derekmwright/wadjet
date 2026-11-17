// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// EVERY AGGREGATE ARGUMENT THAT IS NOT A BARE COLUMN IS MATERIALIZED BELOW
// THE AGGREGATE, WHATEVER ITS POSITION (#1694).
//
// Only the first argument used to be. A computed second or third argument
// (`max_by(x, CASE WHEN p THEN y END)`, `corr(x, y + 0)`, `ohlcv(ts, px,
// vol * 2)`) reached the operator as its TEXT, which no input column carries:
// ungrouped it was refused (`aggregate input "..." is not a column of its
// input`), and grouped it answered NO ROWS, in silence. The grouped form lost
// the refusal because the partitioned aggregate consumed the failing batch
// on a worker that had stopped producing, and that drain dropped the error;
// it now reports it, so a consume error there fails the query whatever its
// cause.
//
// testdata/agg_computed_args.tsv is the table: every multi-argument aggregate
// in the registry (CORR, COVAR_SAMP, COVAR_POP, MIN_BY, MAX_BY, the
// two-argument COUNT(DISTINCT), PERCENTILE_CONT/DISC, QUANTILE_CONT/DISC,
// OHLCV, STRING_AGG) x the argument position holding the expression (each
// one alone, and all) x the expression (arithmetic, CASE, a function call, a
// cast, a constant, a correlated outer reference) x the shape (ungrouped,
// GROUP BY, ROLLUP, HAVING, FILTER, FILTER grouped, a window aggregate; the
// correlated cells ungrouped and grouped only). Columns: key, SQL, want,
// declared type, the oracle, the control, and the answer at base b0e0f116
// (NO-ROWS for an empty answer, ERR <SQLSTATE> for a refusal).
//
// The control is the same call over a subquery that projects each computed
// argument as a column first; every answering cell must answer what its
// control answers, value and declared type, on the same engine. The oracle is
// PostgreSQL 17.11 where it has the function ("pg": it answers the want too),
// the control where it has not. "pg-answers-<rows>" records a cell where
// PostgreSQL answers otherwise, and there are two classes: a computed
// percentile fraction or string_agg separator, which PostgreSQL accepts and
// this engine refuses (0A000: they are properties of the aggregate, read as
// literals), and CORR/COVAR over a constant argument, where this engine's
// kernel answers 0 for PostgreSQL's NULL with the control agreeing (a kernel
// property of bare columns too, not of where the argument is computed).
//
// A correlated cell's declared type is pinned from the table and only its
// VALUE is held to its control: the control is a grouped join, which declares
// an integer expression under this engine's top-level widening (INT64), while
// the scalar subquery declares PostgreSQL's integer, as `(SELECT min(x + 1)
// FROM pt)` already does.
//
// A refusal's want is "ERR <SQLSTATE> <words>". They are the window spelling
// (none of these aggregates is a window function here), DISTINCT in a window,
// and a percentile fraction or string_agg separator that is not a literal.
func TestAggregateComputedArgumentsEveryPosition(t *testing.T) {
	raw, err := os.ReadFile("testdata/agg_computed_args.tsv")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, arm := range []struct {
		name   string
		budget int64
		drain  bool
	}{{"single", 0, false}, {"spilled", 1 << 16, true}} {
		t.Run(arm.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test",
				MemoryBudget: arm.budget, SpillDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, ddl := range []string{
				"CREATE TABLE pt (id INT, g INT, x INT, y INT, t TIMESTAMP, s TEXT)",
				"INSERT INTO pt VALUES (1,1,10,100,TIMESTAMP '2024-01-01 00:00:01','a')," +
					"(2,1,20,NULL,TIMESTAMP '2024-01-01 00:00:02','b')," +
					"(3,2,NULL,300,TIMESTAMP '2024-01-01 00:00:03',NULL)," +
					"(4,2,40,400,NULL,'d')," +
					"(5,1,50,500,TIMESTAMP '2024-01-01 00:00:05','e')",
				"CREATE TABLE po (k INT)",
				"INSERT INTO po VALUES (1),(2)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			drainsBefore := exec.ForcedAggDrains.Load()
			if arm.drain {
				prev := exec.ForceAggDrainEvery(1)
				t.Cleanup(func() { exec.ForceAggDrainEvery(prev) })
			}
			cells, refusals, moved := 0, 0, 0
			for _, line := range strings.Split(string(raw), "\n") {
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				f := strings.Split(line, "\t")
				if len(f) != 7 {
					t.Fatalf("malformed cell %q", line)
				}
				cells++
				key, sql, want, wantType, oracle, control, base := f[0], f[1], f[2], f[3], f[4], f[5], f[6]
				wantRefused := strings.HasPrefix(want, "ERR ")
				if base == "NO-ROWS" || (strings.HasPrefix(base, "ERR ") && !wantRefused) ||
					(base == "ERR -" && strings.HasPrefix(want, "ERR 0A000 ")) {
					moved++
				}
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					refusals++
					state, words, _ := strings.Cut(rest, " ")
					if state == "-" {
						state = ""
					}
					_, err := db.Query(ctx, sql)
					if err == nil {
						t.Errorf("%s\n  %s\n  answered %q, want a refusal %s naming %q", key, sql, gseRows(db, sql), state, words)
						continue
					}
					if got := sqlerr.StateOf(err); got != state || !strings.Contains(err.Error(), words) {
						t.Errorf("%s\n  %s\n  refused %s %q, want %s naming %q", key, sql, got, err.Error(), state, words)
					}
					continue
				}
				got := aggFilterNorm(gseRows(db, sql))
				if got != want {
					t.Errorf("%s\n  %s\n  got  %q\n  want %q (%s; base answered %s)", key, sql, got, want, oracle, base)
				}
				gotType := gseTypes(db, sql)
				if gotType != wantType {
					t.Errorf("%s\n  %s\n  declared %q, want %q", key, sql, gotType, wantType)
				}
				if control == "-" {
					continue
				}
				if c := aggFilterNorm(gseRows(db, control)); c != got {
					t.Errorf("%s\n  answers %q, the same call over the projected subquery %q\n  %s\n  %s", key, got, c, sql, control)
				}
				if strings.Contains(key, "|corr|") {
					continue
				}
				if ct := gseTypes(db, control); ct != gotType {
					t.Errorf("%s\n  declares %q, the projected subquery %q\n  %s\n  %s", key, gotType, ct, sql, control)
				}
			}
			if cells < 1290 || refusals < 400 || moved < 640 {
				t.Fatalf("%d cells (%d refusals, %d moved since base) read: the table is not the one the test pins", cells, refusals, moved)
			}
			if arm.drain && exec.ForcedAggDrains.Load() == drainsBefore {
				t.Fatal("the spilled arm never drained an aggregate: the forcing knob did not engage")
			}
		})
	}
}
