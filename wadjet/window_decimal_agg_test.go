package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #586 / #475's gate, end to end from SQL: `SUM(d) OVER (…)` and `AVG(d)
// OVER (…)` over a DECIMAL column answer what `SUM(d) … GROUP BY` and
// `AVG(d) … GROUP BY` answer over the same rows — the same exact DECIMAL
// text, under the same declared type.
//
// The reference is the engine's own GROUPED aggregate, which is the property
// the fix claims: the two spellings are one question, and a BI tool flips
// between them freely. Anchoring on the aggregate rather than on hardcoded
// values is deliberate for the reason #569's window gate gives —
// TestDecimalAggregateExactness already pins the grouped side against
// values computed outside the engine, so this inherits an anchored reference.
//
// Before the fix, the windowed side answered a FLOAT64: `SUM(c_dec) OVER
// (PARTITION BY g)` came back as 4.1266696257e+06 where the grouped form
// answered "4126669.6257". The float box alone fails this test, and so does
// the declared (p,s).

// wdaDecCol is the type-matrix fixture's DECIMAL column: DECIMAL(18,4).
func wdaDecCol(t *testing.T) parquet.Column {
	t.Helper()
	for _, c := range mbTypeCols() {
		if c.Type == parquet.TypeDecimal {
			return c
		}
	}
	t.Fatal("the type-matrix fixture carries no DECIMAL column")
	return parquet.Column{}
}

// wdaMeta finds one output column's declared metadata.
func wdaMeta(t *testing.T, metas []ColumnMeta, name string) ColumnMeta {
	t.Helper()
	for _, m := range metas {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("result has no column %q", name)
	return ColumnMeta{}
}

func TestWindowedDecimalSumAvgMatchTheGroupedAggregate(t *testing.T) {
	ctx := context.Background()
	db := mbOpenBudget(t, 0)
	in := wdaDecCol(t)

	// The grouped reference.
	ref, err := db.Query(ctx, fmt.Sprintf(
		"SELECT g, SUM(%s) AS s, AVG(%s) AS a FROM mbtypes GROUP BY g", in.Name, in.Name))
	if err != nil {
		t.Fatalf("grouped reference: %v", err)
	}
	if len(ref.Rows) == 0 {
		t.Fatal("grouped reference produced no groups")
	}
	wantSum := make(map[int32]any, len(ref.Rows))
	wantAvg := make(map[int32]any, len(ref.Rows))
	for _, r := range ref.Rows {
		g, ok := r["g"].(int32)
		if !ok {
			t.Fatalf("grouped reference: g boxed %T, want int32", r["g"])
		}
		wantSum[g], wantAvg[g] = r["s"], r["a"]
	}
	refSum := wdaMeta(t, ref.ColumnMetas, "s")
	refAvg := wdaMeta(t, ref.ColumnMetas, "a")

	for _, over := range []struct {
		name string
		spec string
	}{
		// The whole partition, written two ways: the implicit frame (no
		// ORDER BY widens to the partition) and an explicit UNBOUNDED ROWS
		// frame under an ORDER BY. Both see exactly the rows the GROUP BY
		// sees, so both owe the grouped answer value for value.
		{"implicit_frame", "PARTITION BY g"},
		{"explicit_unbounded", "PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"},
	} {
		over := over
		t.Run(over.name, func(t *testing.T) {
			res, err := db.Query(ctx, fmt.Sprintf(
				"SELECT id, g, SUM(%s) OVER (%s) AS s, AVG(%s) OVER (%s) AS a "+
					"FROM mbtypes ORDER BY id", in.Name, over.spec, in.Name, over.spec))
			if err != nil {
				t.Fatalf("windowed SUM/AVG over a DECIMAL: %v", err)
			}
			if len(res.Rows) != mbRows {
				t.Fatalf("window returned %d rows, want %d", len(res.Rows), mbRows)
			}

			// The DECLARED type is the grouped aggregate's, exactly:
			// DECIMAL(38,4) for SUM and DECIMAL(38,8) for AVG over a
			// DECIMAL(18,4) input (ADR-0012 item 9).
			gotSum := wdaMeta(t, res.ColumnMetas, "s")
			gotAvg := wdaMeta(t, res.ColumnMetas, "a")
			for _, pair := range []struct {
				what     string
				got, ref ColumnMeta
				scale    int
			}{
				{"SUM", gotSum, refSum, in.Scale},
				{"AVG", gotAvg, refAvg, batch.AvgScale(in.Scale)},
			} {
				if pair.got.TypeID != parquet.TypeDecimal {
					t.Errorf("windowed %s declared %s, want DECIMAL", pair.what, pair.got.TypeName)
				}
				if pair.got.Precision != batch.MaxDecimalPrecision || pair.got.Scale != pair.scale {
					t.Errorf("windowed %s declared DECIMAL(%d,%d), want DECIMAL(%d,%d)",
						pair.what, pair.got.Precision, pair.got.Scale,
						batch.MaxDecimalPrecision, pair.scale)
				}
				if pair.got.TypeID != pair.ref.TypeID ||
					pair.got.Precision != pair.ref.Precision || pair.got.Scale != pair.ref.Scale {
					t.Errorf("windowed %s declared %s(%d,%d) where the grouped one declares %s(%d,%d) "+
						"— the two spellings of one question owe one answer type",
						pair.what, pair.got.TypeName, pair.got.Precision, pair.got.Scale,
						pair.ref.TypeName, pair.ref.Precision, pair.ref.Scale)
				}
				// ADR-0024 item 5: a window function is a function call, so
				// the WIRE modifier is -1 even though the engine's own
				// declaration keeps a real (p,s).
				if !pair.got.WireUnconstrained {
					t.Errorf("windowed %s carries a real wire typmod; PostgreSQL declares every "+
						"window function's numeric result unconstrained (ADR-0024 item 5)", pair.what)
				}
			}

			for _, r := range res.Rows {
				g, ok := r["g"].(int32)
				if !ok {
					t.Fatalf("g boxed %T, want int32", r["g"])
				}
				mbAssertEqual(t, fmt.Sprintf("id %v group %d SUM", r["id"], g), r["s"], wantSum[g])
				mbAssertEqual(t, fmt.Sprintf("id %v group %d AVG", r["id"], g), r["a"], wantAvg[g])
			}
		})
	}
}

// TestWindowedDecimalRunningFrameEndsAtTheGroupedAnswer covers the frames the
// whole-partition shapes cannot: a RUNNING total, whose last row per
// partition sees the same rows the GROUP BY does but reaches them through the
// sliding accumulator; and an explicitly SLIDING frame, where the accumulator
// also has to RETRACT a row exactly.
//
// The sliding arm's reference is the running one: `ROWS BETWEEN UNBOUNDED
// PRECEDING AND CURRENT ROW` and the default frame under a unique ORDER BY
// are the same frame written two ways, and only one of them slides its lower
// end. A float running total loses associativity there, so the two spellings
// could disagree without either being obviously wrong.
func TestWindowedDecimalRunningFrameEndsAtTheGroupedAnswer(t *testing.T) {
	ctx := context.Background()
	db := mbOpenBudget(t, 0)
	in := wdaDecCol(t)

	ref, err := db.Query(ctx, fmt.Sprintf(
		"SELECT g, SUM(%s) AS s FROM mbtypes GROUP BY g", in.Name))
	if err != nil {
		t.Fatalf("grouped reference: %v", err)
	}
	want := make(map[int32]any, len(ref.Rows))
	for _, r := range ref.Rows {
		want[r["g"].(int32)] = r["s"]
	}

	run, err := db.Query(ctx, fmt.Sprintf(
		"SELECT id, g, SUM(%s) OVER (PARTITION BY g ORDER BY id) AS s FROM mbtypes ORDER BY id", in.Name))
	if err != nil {
		t.Fatalf("running window: %v", err)
	}
	explicit, err := db.Query(ctx, fmt.Sprintf(
		"SELECT id, g, SUM(%s) OVER (PARTITION BY g ORDER BY id "+
			"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM mbtypes ORDER BY id", in.Name))
	if err != nil {
		t.Fatalf("explicit running window: %v", err)
	}
	if len(run.Rows) != len(explicit.Rows) {
		t.Fatalf("the two spellings returned %d and %d rows", len(run.Rows), len(explicit.Rows))
	}
	for i := range run.Rows {
		mbAssertEqual(t, fmt.Sprintf("id %v: default vs explicit running frame", run.Rows[i]["id"]),
			explicit.Rows[i]["s"], run.Rows[i]["s"])
	}

	// The LAST row of each partition sees the whole partition.
	last := make(map[int32]any, len(want))
	for _, r := range run.Rows {
		last[r["g"].(int32)] = r["s"]
	}
	for g, got := range last {
		mbAssertEqual(t, fmt.Sprintf("running group %d at its last row", g), got, want[g])
	}
}
