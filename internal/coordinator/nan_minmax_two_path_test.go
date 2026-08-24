package coordinator

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The distributed half of #457: MIN/MAX over a float column ignored
// PostgreSQL's NaN total order (NaN sorts greatest, verified live against
// postgres:17-alpine — see benchmarks/tpch/postgres_compare_test.go's
// MinMaxFloatNaN* entries), which was fixed on the in-process accumulator
// path (kernel.CompareFloat64) AND on every distributed counterpart: the
// worker-side SoA scatter/merge (exec/agg_scatter.go, aggregate.go's
// mergeFlatAccumRow), the spill k-way merge (kernel.Accumulator.Merge), and
// the coordinator's own probe-split re-aggregation (compareAnyValues,
// reAggregatePartials' minMerge/maxMerge). This test proves the single-
// process engine and the stage DAG agree with each other on every group
// shape #457's audit named, over the SAME rows — which the in-process gates
// in internal/engine/exec cannot see, since they never cross a WSHF shuffle
// or the coordinator merge.
//
// The fixture stores a "kind" tag rather than the NaN/Infinity values
// themselves: ingest computes row-group min/max statistics and JSON-encodes
// them into the catalog manifest, and encoding/json refuses NaN and ±Inf
// outright ("json: unsupported value"). That is a real, separate gap in the
// ingest/manifest path — #459's "reachability note" flags it as unconfirmed
// rather than assumed, and this is the confirmation: a NaN or ±Inf value
// cannot be INGESTED as stored table data today. It is not #457's subject
// (accumulator comparisons), so it is not fixed here; the query itself
// manufactures the NaN/±Inf via CAST(...), same as the pg-oracle corpus
// entries, which keeps the stored column ordinary finite float64/NULL and
// still exercises the full distributed scan → shuffle → partial-aggregate →
// gather-merge path for the MIN/MAX comparison itself.
const nmmTable = "nanminmax"

func nmmSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "kind", Type: parquet.TypeString},
		{Name: "base", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// nmmValueExpr computes the actual test column from the stored kind/base
// pair. Shared by every query in this file so the SQL text — not just the
// intent — is identical across the fixture and every test case.
const nmmValueExpr = `CASE kind
	WHEN 'nan' THEN CAST('NaN' AS DOUBLE PRECISION)
	WHEN 'pinf' THEN CAST('Infinity' AS DOUBLE PRECISION)
	WHEN 'ninf' THEN CAST('-Infinity' AS DOUBLE PRECISION)
	WHEN 'null' THEN CAST(NULL AS DOUBLE PRECISION)
	ELSE base
	END`

// nmmData is the fixture: group 1-3 place a NaN at a different position
// among finite values (first/last/middle), group 4 is all-NaN, group 5 is a
// NaN plus NULLs only, group 6 mixes NULL/NaN/finite, and group 7 is a
// NaN-free sanity check (including ±Infinity, to prove NaN still beats
// Infinity for MAX). Every one of these was verified live against
// PostgreSQL (see MinMaxFloatNaNGrouped in the pg-oracle corpus).
func nmmData() []map[string]any {
	type row struct {
		k    int64
		kind string
		base any // float64 or nil; meaningful only when kind == "val"
	}
	rows := []row{
		{1, "nan", nil}, {1, "val", 5.0}, {1, "val", 3.0}, {1, "val", -100.0}, // NaN first
		{2, "val", 5.0}, {2, "val", 3.0}, {2, "val", -100.0}, {2, "nan", nil}, // NaN last
		{3, "val", 5.0}, {3, "nan", nil}, {3, "val", 3.0}, {3, "val", -100.0}, // NaN middle
		{4, "nan", nil}, {4, "nan", nil}, {4, "nan", nil}, // all NaN
		{5, "nan", nil}, {5, "null", nil}, {5, "null", nil}, // NaN + NULLs only
		{6, "null", nil}, {6, "nan", nil}, {6, "val", 2.0}, // NULL, NaN, finite
		{7, "val", 1.0}, {7, "ninf", nil}, {7, "pinf", nil}, // sanity, no NaN
	}
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		out[i] = map[string]any{"k": r.k, "kind": r.kind, "base": r.base}
	}
	return out
}

type nmmExpect struct{ lo, hi float64 }

func nmmWant() map[int64]nmmExpect {
	nan := math.NaN()
	return map[int64]nmmExpect{
		1: {-100.0, nan},
		2: {-100.0, nan},
		3: {-100.0, nan},
		4: {nan, nan},
		5: {nan, nan},
		6: {2.0, nan},
		7: {math.Inf(-1), math.Inf(1)},
	}
}

// nmmCell compares a MIN/MAX cell against a wanted value, treating NaN as a
// sentinel meaning "must be NaN" (both engines box a float64 MIN/MAX result
// as a plain float64, never as text — unlike DECIMAL).
func nmmCell(t *testing.T, what string, got any, want float64) {
	t.Helper()
	f, ok := got.(float64)
	if !ok {
		t.Errorf("%s = %#v (%T), want float64", what, got, got)
		return
	}
	if math.IsNaN(want) {
		if !math.IsNaN(f) {
			t.Errorf("%s = %v, want NaN", what, f)
		}
		return
	}
	if f != want {
		t.Errorf("%s = %v, want %v", what, f, want)
	}
}

func TestMinMaxFloatNaNTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	want := nmmWant()

	t.Run("grouped", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT k, MIN(%s) AS lo, MAX(%s) AS hi FROM %s GROUP BY k ORDER BY k`,
			nmmValueExpr, nmmValueExpr, nmmTable)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != len(want) {
				t.Fatalf("%s: %d groups, want %d", arm.name, len(rows), len(want))
			}
			for _, r := range rows {
				k, _ := r["k"].(int64)
				w, ok := want[k]
				if !ok {
					t.Fatalf("%s: unexpected group %v", arm.name, r["k"])
				}
				nmmCell(t, fmt.Sprintf("%s group %d MIN", arm.name, k), r["lo"], w.lo)
				nmmCell(t, fmt.Sprintf("%s group %d MAX", arm.name, k), r["hi"], w.hi)
			}
		}
	})

	t.Run("scalar_all_nan", func(t *testing.T) {
		// A scalar (no GROUP BY) aggregate over just the all-NaN group:
		// exercises the isScalarAgg batch path end to end, distributed.
		sql := fmt.Sprintf(`SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s WHERE k = 4`,
			nmmValueExpr, nmmValueExpr, nmmTable)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
			}
			nmmCell(t, arm.name+" scalar MIN", rows[0]["lo"], math.NaN())
			nmmCell(t, arm.name+" scalar MAX", rows[0]["hi"], math.NaN())
		}
	})

	t.Run("scalar_mixed", func(t *testing.T) {
		// Scalar MIN/MAX across every group at once: forces the coordinator's
		// probe-split / gather merge to combine partials some of which
		// carry a NaN extreme and some of which don't (compareAnyValues,
		// reAggregatePartials' minMerge/maxMerge on the DAG path).
		sql := fmt.Sprintf(`SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM %s`, nmmValueExpr, nmmValueExpr, nmmTable)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
			}
			// Global minimum across all groups is group 7's -Infinity (below
			// group 1/2/3's -100); the global maximum is NaN (every group
			// but 7 carries one, and NaN beats even group 7's +Infinity).
			nmmCell(t, arm.name+" global MIN", rows[0]["lo"], math.Inf(-1))
			nmmCell(t, arm.name+" global MAX", rows[0]["hi"], math.NaN())
		}
	})
}
