// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
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
// the control where it has not. Every answering cell answers PostgreSQL's
// value where PostgreSQL has the call. "refused-pg-answer-<rows>" records the
// one class where it answers and this engine refuses: a computed percentile
// fraction or string_agg separator (0A000: they are properties of the
// aggregate, read as literals). CORR over an argument whose squared
// deviations sum to zero (a constant, or every pair carrying one x) answered
// 0 here, bare columns too; it answers PostgreSQL's NULL since round 2.
//
// The min_by|key|… and max_by|key|… cells hold MIN_BY/MAX_BY's ORDERING key by
// type (round 2): the kernel compares a key through a float64, so a TEXT or
// UUID key skipped every row and answered NULL (a computed one, refused or no
// rows at base). Those are refused 0A000; a TIMESTAMP, BOOL or DECIMAL key
// answers as its control does — ungrouped, `SELECT x FROM pt WHERE x IS NOT
// NULL AND key IS NOT NULL ORDER BY key, id LIMIT 1` ("order-by-pg":
// PostgreSQL answers that control too), grouped the projected subquery. A
// refused cell's oracle names what the ORDER BY control answers.
//
// A correlated cell's declared type is pinned from the table and only its
// VALUE is held to its control: the control is a grouped join, which declares
// an integer expression under this engine's top-level widening (INT64), while
// the scalar subquery declares PostgreSQL's integer, as `(SELECT min(x + 1)
// FROM pt)` already does.
//
// A refusal's want is "ERR <SQLSTATE> <words>". They are the window spelling
// (none of these aggregates is a window function here), DISTINCT in a window,
// a percentile fraction or string_agg separator that is not a literal, and a
// MIN_BY/MAX_BY ordering key the kernel cannot order.
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
					(base == "ERR -" && strings.HasPrefix(want, "ERR 0A000 ")) ||
					(!strings.HasPrefix(base, "ERR ") && base != want) {
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
			if cells < 1347 || refusals < 443 || moved < 703 {
				t.Fatalf("%d cells (%d refusals, %d moved since base) read: the table is not the one the test pins", cells, refusals, moved)
			}
			if arm.drain && exec.ForcedAggDrains.Load() == drainsBefore {
				t.Fatal("the spilled arm never drained an aggregate: the forcing knob did not engage")
			}
		})
	}
}

// MIN_BY/MAX_BY OVER AN ORDERING KEY OF EVERY TYPE, EMBEDDED, PLAIN AND
// SPILLED (#1694, round 2). The five-arm form is
// coordinator.TestAggOrderKeyAndCorrDegenerateFiveArms.
//
// The kernel compares a key through a float64 (exec.orderKeyFloat64), and
// exactly for BIGINT and DECIMAL (exec.orderKeyExact). A key of any other type
// skipped every row and answered NULL — TEXT, BYTEA, IPV6, CIDR, UUID, ARRAY,
// MAP, VECTOR — or was 42883 (ROW); each is refused 0A000 now, bare or
// computed. An orderable key answers what `SELECT x FROM kt WHERE x IS NOT NULL
// AND key IS NOT NULL ORDER BY key LIMIT 1` answers (DESC for MAX_BY): rows
// 1..5 rank 5, 9, 1, NULL, 5, so MAX_BY is row 2's x and MIN_BY row 3's; NaN
// is the greatest float and -Inf the least, and -0.0 equals 0.0, so a group
// holding both may answer either row. BIGINT 2^53 and 2^53 + 1 round to one
// double, as do two 38-digit DECIMALs: through the float they tied and MAX_BY
// answered the first row read.
func TestMinByOrderKeyEveryType(t *testing.T) {
	ctx := context.Background()
	type cell struct {
		sql   string
		wants []string
	}
	ordered := []string{"ki32", "ki64", "kf32", "kf64", "kts", "kip4", "kmac", "kport", "kprot", "kdur", "kdt", "kdec"}
	refused := []string{"ks", "kby", "kip6", "kcidr", "kuuid", "karr", "krow", "kmap", "kvec"}
	var cells []cell
	add := func(sql string, wants ...string) { cells = append(cells, cell{sql, wants}) }
	for _, fn := range []string{"min_by", "max_by"} {
		u, g, b := "30", "1,10 | 2,30", "30"
		fu, fg := "70", []string{"1,30 | 2,40 | 3,70", "1,30 | 2,60 | 3,70"}
		pu := "30"
		if fn == "max_by" {
			u, g, b, fu, pu = "20", "1,20 | 2,50", "20", "20", "20"
			fg = []string{"1,20 | 2,40 | 3,70", "1,20 | 2,60 | 3,70"}
		}
		for _, c := range ordered {
			for _, k := range []string{c, "CASE WHEN id > 0 THEN " + c + " END"} {
				add("SELECT "+fn+"(x, "+k+") FROM kt", u)
				add("SELECT g, "+fn+"(x, "+k+") FROM kt GROUP BY g", g)
			}
		}
		// BOOL: row 2 true, row 3 false, the rest NULL — no tie.
		for _, k := range []string{"kb", "CASE WHEN id > 0 THEN kb END"} {
			add("SELECT "+fn+"(x, "+k+") FROM kt", b)
			add("SELECT g, "+fn+"(x, "+k+") FROM kt GROUP BY g", "1,20 | 2,30")
		}
		for _, c := range refused {
			for _, k := range []string{c, "CASE WHEN id > 0 THEN " + c + " END"} {
				add("SELECT "+fn+"(x, "+k+") FROM kt", "ERR 0A000")
				add("SELECT g, "+fn+"(x, "+k+") FROM kt GROUP BY g", "ERR 0A000")
			}
		}
		for _, k := range []string{"upper(ks)", "ks || 'q'", "CAST(ki32 AS TEXT)", "'lit'", "ARRAY[ki32]", "CAST('[1,2]' AS VECTOR(2))"} {
			add("SELECT "+fn+"(x, "+k+") FROM kt", "ERR 0A000")
			add("SELECT g, "+fn+"(x, "+k+") FROM kt GROUP BY g", "ERR 0A000")
		}
		// A NULL key orders no row.
		add("SELECT "+fn+"(x, NULL) FROM kt", "NULL")
		for _, k := range []string{"f", "r", "f + 0", "CASE WHEN id > 0 THEN r END"} {
			add("SELECT "+fn+"(x, "+k+") FROM kf", fu)
			add("SELECT g, "+fn+"(x, "+k+") FROM kf GROUP BY g", fg...)
		}
		for _, k := range []string{"k", "d", "k + 0", "CASE WHEN id > 0 THEN d END"} {
			add("SELECT "+fn+"(x, "+k+") FROM kp", pu)
		}
	}
	row := func(id, g, x int, k, b string) string {
		if k == "" {
			return fmt.Sprintf("(%d,%d,%d,%s%s)", id, g, x, b, strings.Repeat(",NULL", 21))
		}
		s := strings.NewReplacer("K", k, "C", string(rune('a'+k[0]-'0'))).Replace(
			"K,K,K.0,K.0,'C','\\x0K',TIMESTAMP '2024-01-0K 00:00:00','10.0.0.K','::K','10.0.K.0/24'," +
				"'00:00:00:00:00:0K',K00,K,K000,'00000000-0000-4000-8000-00000000000K',DATE '2024-01-0K',K.00,ARRAY[K],NULL,NULL,'[K,K]'")
		return fmt.Sprintf("(%d,%d,%d,%s,%s)", id, g, x, b, s)
	}
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
				"CREATE TABLE kt (id INT, g INT, x INT, kb BOOLEAN, ki32 INT, ki64 BIGINT, kf32 REAL, kf64 DOUBLE PRECISION, " +
					"ks TEXT, kby BYTES, kts TIMESTAMP, kip4 IPV4, kip6 IPV6, kcidr CIDR, kmac MAC, kport PORT, kprot PROTOCOL, " +
					"kdur DURATION, kuuid UUID, kdt DATE, kdec DECIMAL(10,2), karr ARRAY(INT), krow ROW(a INT), kmap MAP(STRING, INT), kvec VECTOR(2))",
				"INSERT INTO kt VALUES " + strings.Join([]string{row(1, 1, 10, "5", "NULL"), row(2, 1, 20, "9", "true"),
					row(3, 2, 30, "1", "false"), row(4, 2, 40, "", "NULL"), row(5, 2, 50, "5", "NULL")}, ","),
				"CREATE TABLE kf (id INT, g INT, x INT, f DOUBLE PRECISION, r REAL)",
				"INSERT INTO kf VALUES (1,1,10,CAST('0.0' AS DOUBLE PRECISION),CAST('0.0' AS REAL))," +
					"(2,1,20,CAST('NaN' AS DOUBLE PRECISION),CAST('NaN' AS REAL)),(3,1,30,-1.0,-1.0)," +
					"(4,2,40,CAST('-0.0' AS DOUBLE PRECISION),CAST('-0.0' AS REAL)),(5,2,50,NULL,NULL)," +
					"(6,2,60,CAST('0.0' AS DOUBLE PRECISION),CAST('0.0' AS REAL))," +
					"(7,3,70,CAST('-Infinity' AS DOUBLE PRECISION),CAST('-Infinity' AS REAL))",
				"CREATE TABLE kp (id INT, x INT, k BIGINT, d DECIMAL(38,0))",
				"INSERT INTO kp VALUES (1,10,9007199254740992,99999999999999999999999999999999999990)," +
					"(2,20,9007199254740993,99999999999999999999999999999999999991)," +
					"(3,30,9007199254740991,99999999999999999999999999999999999989)",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			if arm.drain {
				prev := exec.ForceAggDrainEvery(1)
				t.Cleanup(func() { exec.ForceAggDrainEvery(prev) })
			}
			for _, c := range cells {
				got := gseRows(db, c.sql)
				if _, err := db.Query(ctx, c.sql); err != nil {
					got = "ERR " + sqlerr.StateOf(err)
				}
				ok := false
				for _, w := range c.wants {
					ok = ok || got == w
				}
				if !ok {
					t.Errorf("%s\n  got  %s\n  want %s", c.sql, got, strings.Join(c.wants, " or "))
				}
			}
		})
	}
}
