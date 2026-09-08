package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AN AGGREGATE WITH NO WINDOW FORM REFUSES 0A000; IT DOES NOT CRASH (#965).
//
// `exec.ParseWindowFunc` answers `(WinRowNumber, false)` for a name the window
// operator has no arm for, and the single-process planner used to DISCARD that
// second value. The plan then reached exec.Window as ROW_NUMBER with an output
// vector typed for the function nobody recognized, and the write off the end of
// a zero-length slice panicked. Census over the 28 names in
// `plansql.IsAggregate`, spelled `<agg> OVER (PARTITION BY g ORDER BY x)`,
// measured 2026-09-08 on the base commit:
//
//	 5 answered right   SUM COUNT AVG MIN MAX
//	23 panicked         "internal error in pipeline: runtime error: index out
//	                    of range [0] with length 0" — ADR-0019's boundary,
//	                    which fails the query with no SQLSTATE a client can
//	                    act on
//	 0 answered wrong
//
// PostgreSQL 17 ANSWERS all 23 — every aggregate is a window function there.
// So the disposition is a loud refusal of PostgreSQL-valid input, which is
// ADR-0012's rule when the value cannot be right, and 0A000 is the class this
// engine's other "PostgreSQL can, we cannot yet" refusals carry.
//
// The gate is written over `plansql.IsAggregate` rather than over a list, so a
// new aggregate joins it for free and cannot ship with the crash.
func TestAnAggregateWithNoWindowFormRefusesRatherThanCrashing(t *testing.T) {
	ctx := context.Background()
	db := wuOpen(t, ctx)

	// The five that HAVE a window form must keep answering: this cell is what
	// makes the refusal a fix rather than a new hole.
	answers := map[string][]any{
		"SUM(x)":   {float64(10), float64(11), float64(22), float64(24)},
		"COUNT(x)": {int64(1), int64(1), int64(2), int64(2)},
		"AVG(x)":   {float64(10), float64(11), float64(11), float64(12)},
		"MIN(x)":   {float64(10), float64(11), float64(10), float64(11)},
		"MAX(x)":   {float64(10), float64(11), float64(12), float64(13)},
	}
	for call, want := range answers {
		t.Run("answers_"+strings.ToLower(strings.SplitN(call, "(", 2)[0]), func(t *testing.T) {
			got := wuRun(t, ctx, db, call)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s OVER (…) = %v, want %v", call, got, want)
			}
		})
	}

	// Every OTHER known aggregate refuses, with a SQLSTATE and a sentence.
	calls := map[string]string{
		"string_agg": "STRING_AGG(s, ',')", "bool_and": "BOOL_AND(b)", "bool_or": "BOOL_OR(b)",
		"every": "EVERY(b)", "stddev": "STDDEV(x)", "stddev_samp": "STDDEV_SAMP(x)",
		"stddev_pop": "STDDEV_POP(x)", "variance": "VARIANCE(x)", "var_samp": "VAR_SAMP(x)",
		"var_pop": "VAR_POP(x)", "approx_distinct": "APPROX_DISTINCT(x)", "corr": "CORR(x, g)",
		"covar_samp": "COVAR_SAMP(x, g)", "covar_pop": "COVAR_POP(x, g)",
		"percentile_cont": "PERCENTILE_CONT(0.5, x)", "percentile_disc": "PERCENTILE_DISC(0.5, x)",
		"quantile_cont": "QUANTILE_CONT(x, 0.5)", "quantile_disc": "QUANTILE_DISC(x, 0.5)",
		"mode": "MODE(x)", "min_by": "MIN_BY(x, g)", "max_by": "MAX_BY(x, g)",
		"median": "MEDIAN(x)", "grouping": "GROUPING(g)",
	}
	names := make([]string, 0, len(calls))
	for n := range calls {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		t.Run("refuses_"+n, func(t *testing.T) {
			if !plansql.IsAggregate(n) {
				t.Fatalf("%q is no longer a known aggregate — retire this cell deliberately", n)
			}
			if _, ok := exec.ParseWindowFunc(n); ok {
				t.Fatalf("%q now HAS a window form. Move it to the answers table above and "+
					"assert its values against PostgreSQL — a supported function must not "+
					"sit in the refusal list", n)
			}
			q := fmt.Sprintf(`SELECT %s OVER (PARTITION BY g ORDER BY x) AS w FROM a1w`, calls[n])
			_, err := db.Query(ctx, q)
			if err == nil {
				t.Fatalf("ANSWERED; want a 0A000 refusal\n  SQL: %s", q)
			}
			if st := sqlerr.StateOf(err); st != "0A000" {
				t.Errorf("SQLSTATE %q, want 0A000 — a refusal with no class reaches a client "+
					"as the generic XX000\n  %v", st, err)
			}
			if !strings.Contains(err.Error(), "is not supported as a window function") {
				t.Errorf("%v\n  want the shared refusal sentence", err)
			}
			// The refusal names what DOES work, so it is actionable.
			if !strings.Contains(err.Error(), "ROW_NUMBER") {
				t.Errorf("%v\n  want the supported set named in the message", err)
			}
			// And it is a REFUSAL, not the panic boundary.
			if strings.Contains(err.Error(), "internal error") {
				t.Errorf("%v\n  this is the crash the refusal replaces", err)
			}
		})
	}

	// The supported set the message names is derived from ParseWindowFunc, so
	// it cannot drift from what actually works.
	for _, n := range exec.WindowFuncNames() {
		if _, ok := exec.ParseWindowFunc(n); !ok {
			t.Errorf("WindowFuncNames lists %q, which ParseWindowFunc does not accept", n)
		}
	}
}

func wuRun(t *testing.T, ctx context.Context, db *DB, call string) []any {
	t.Helper()
	q := fmt.Sprintf(`SELECT %s OVER (PARTITION BY g ORDER BY x) AS w FROM a1w ORDER BY x`, call)
	res, err := db.Query(ctx, q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	out := make([]any, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, r["w"])
	}
	return out
}

func wuOpen(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "x", Type: parquet.TypeFloat64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeBool, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "a1w", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 4)
	for i := 0; i < 4; i++ {
		rows = append(rows, map[string]any{
			"g": int64(i % 2), "x": float64(10 + i),
			"s": fmt.Sprintf("r%d", i), "b": i%2 == 0,
		})
	}
	ing := db.NewIngester("a1w", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
