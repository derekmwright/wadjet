package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The numeric/decimal arc-2 census: every shape on FOUR arms, each answer
// anchored to live PostgreSQL 17 rather than to the other arm.
//
// The arms are the four an answer can differ between (ADR-0018 §3, ADR-0027):
//
//	single             the embedded engine, no memory budget
//	single+spill       the same engine at 512 KiB, so every pipeline breaker
//	                   drains — a spill is a CONDITION, not a shape, and it is
//	                   the arm a shape corpus never reaches on purpose
//	dag                the stage DAG over an embedded NATS cluster
//	dag+broadcast      the DAG with BroadcastBytesOverride = 1, which forces
//	                   the shuffle rather than the broadcast join
//
// `want` is what `psql` printed on the oracle container (127.0.0.1:55432,
// --locale=C, PostgreSQL 17) for the SAME rows, recorded before any of these
// fixes was written. A DECIMAL is compared DIGIT FOR DIGIT: the whole point of
// these issues is a value that is right to six digits and wrong after them.
//
// Issues: #727 (a CTE's TEXT column re-typed from its first row's VALUE),
// #728 (an aggregate's output declared FLOAT64 through a rename), #786/#781
// (a derived GROUP BY key typed against a scope that stops at a Project),
// #749 (an exact operator's scale reduced to buy integer digits), #703
// (DISTINCT dropped for every aggregate but COUNT), #704 (an integer column
// compared against a non-integral literal by truncating the literal).
func TestNumericArc2ShapesMatchPostgres(t *testing.T) {
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

	for _, tc := range []struct {
		issue, name, sql string
		want             []string
	}{
		// ------------------------------------------------------------------
		// #727 — a CTE materialization used to read the FIRST ROW of every
		// STRING column and, when that one value parsed as a number, re-type
		// the whole column. decpair.s is TEXT holding "1.50","1.5","abc",
		// "1.500": the column came back double precision, `s = '1.50'`
		// compared NUMBERS, and "abc" became NULL and then a hard
		// `invalid input syntax for type double precision`.
		{"#727", "cte_text_column_stays_text",
			`WITH c AS (SELECT s, id FROM decpair) SELECT COUNT(*) AS n FROM c WHERE s = '1.50'`,
			[]string{"n=int64:1"}},
		{"#727", "derived_table_twin_unchanged",
			`SELECT COUNT(*) AS n FROM (SELECT s, id FROM decpair) c WHERE s = '1.50'`,
			[]string{"n=int64:1"}},
		{"#727", "cte_text_in_a_case_arm",
			`WITH c AS (SELECT s, a*2 AS v FROM decpair) SELECT SUM(CASE WHEN s='abc' THEN v ELSE 0 END) AS sm FROM c`,
			[]string{"sm=25.50"}},
		{"#727", "cte_text_column_reads_back_whole",
			`WITH c AS (SELECT s, id FROM decpair) SELECT s AS v FROM c WHERE id IN (1,3,6)`,
			[]string{"v=1.50", "v=1.500", "v=abc"}},
		// The type used to depend on the DATA: the same CTE body restricted
		// to the row holding "abc" kept the string, and without the
		// restriction did not. Both spellings must now answer as TEXT.
		{"#727", "cte_text_type_does_not_depend_on_the_first_row",
			`WITH c AS (SELECT s, id FROM decpair WHERE id = 3) SELECT s AS v FROM c`,
			[]string{"v=abc"}},

		// ------------------------------------------------------------------
		// #728 — SUM/AVG/MIN/MAX resolved a bare column argument by searching
		// the SCANS below for that NAME, which cannot see a rename. Over
		// `(SELECT dw AS v FROM decwin) x` nothing carries `v`, so the
		// aggregate's OUTPUT declared FLOAT64 and the projection above it
		// computed in float: two spellings of one question, two numbers.
		{"#728", "sum_times_two_over_a_rename",
			`SELECT SUM(v*2) AS s FROM (SELECT dw AS v FROM decwin) x`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "sum_times_two_over_the_base_column",
			`SELECT SUM(dw*2) AS s FROM decwin`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "sum_times_two_through_a_cte",
			`WITH c AS (SELECT dw AS v FROM decwin) SELECT SUM(v*2) AS s FROM c`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "max_times_two_over_a_rename",
			`SELECT MAX(v*2) AS m FROM (SELECT dw AS v FROM decwin) x`,
			[]string{"m=1994666666891066.6258890908"}},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				run  func(string) ([]string, error)
			}{
				{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
				{"single+spill", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, spilled, sql)) }},
				{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
				{"dag+broadcast", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
			} {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17: %v", arm.name, err, tc.sql, tc.want)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.want, tc.sql)
					continue
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
						break
					}
				}
			}
		})
	}
}

// na2Standalone is tmdStandalone with a per-query memory budget, so every
// pipeline breaker in the shape drains to disk. A budget alone is not proof
// that it did (ADR-0027 §5); what this arm proves is that the shapes above
// answer the same under pressure, and the exec-level spill suites carry the
// engagement assertions.
func na2Standalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tmdTables() {
		if tmdStoresAReservedName(tbl) {
			continue // the reserved-name fixtures need the catalog door; no shape here reads them
		}
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// na2Run renders a result to comparable text.
//
// A DECIMAL is rendered VERBATIM — it arrives as its own text and every issue
// here is about a digit past the sixth, so rounding it would hide the defect.
// A FLOAT is rendered to six significant digits, which is ADR-0013's
// nondeterminism class 9: a float sum's last digits move with the order three
// workers hand batches to the aggregate. The Go TYPE is printed for every
// non-string box, because a float64 holding an exact integer and an int64
// holding it print identically under %v — and "the right number under the
// wrong Go type" is exactly what #728 and #784 are.
func na2Run(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			v := r[c]
			switch t := v.(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			case float64:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, t))
			case float32:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, float64(t)))
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, v, v))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	// Every shape above either carries an ORDER BY or returns one row, so the
	// sort only makes an unordered multiset comparison total.
	sort.Strings(out)
	return out, nil
}
