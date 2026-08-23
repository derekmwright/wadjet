package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #438: `WHERE v = 0.25` on a DECIMAL(9,2) column holding 0.25 returned no
// rows, while <, <= and > on the same column and literal were all correct.
//
// The equality kernel was never the defect. A DECIMAL column's row-group
// statistics hold the UNSCALED integer (0.25 at scale 2 is 25) and the literal
// reaches the prune layer as float64(0.25); OpEq is the one arm of
// CanPruneRowGroup that passes (literal, stat) rather than (stat, literal),
// and compareValuesOK's asymmetric coercion made that pair comparable in only
// that order. So 0.25 < 25 pruned every row group, and the ordered operators
// were saved by a REFUSAL rather than by being right — an integral literal
// against the same unscaled bounds pruned them too (#442).
//
// The literal is converted into the column's unscaled domain at plan time now,
// so this sweeps the operators that reach a kernel and the operators that
// reach the prune, at three scales, with pruning on and off.
func TestDecimalPredicatesFindTheirRows(t *testing.T) {
	ctx := context.Background()

	for _, sc := range []struct {
		scale int
		// step is the value of row i: i*step, chosen so every value is
		// exactly representable at the column's scale.
		step float64
	}{
		{2, 0.25}, {4, 0.0625}, {10, 0.0009765625},
	} {
		t.Run(fmt.Sprintf("scale%d", sc.scale), func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			schema := parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: sc.scale, Nullable: true},
			}}
			if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
				t.Fatal(err)
			}
			// 199 rows, MONOTONIC and spanning both signs, in row groups of
			// 40. Monotonic is the point: it gives each row group a narrow
			// [min,max] that the target value sits INSIDE and every other
			// row group's bounds sit clear of — which is the only fixture
			// shape a wrong prune can be seen through. A shuffled or
			// zero-centred column gives every group bounds wide enough to
			// contain the literal by accident, and the defect hides.
			const n = 199
			rows := make([]map[string]any, n)
			for i := range rows {
				v := float64(i-99) * sc.step // -99·step … 0 … +99·step
				rows[i] = map[string]any{"id": int64(i), "v": v}
			}
			ing := db.NewIngester("t", schema, nil, ingest.Config{
				MaxBufferRows: n + 1, RowGroupSize: 40,
			})
			if err := ing.Ingest(ctx, rows); err != nil {
				t.Fatal(err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}

			one := fmtDec(sc.step*31, sc.scale)   // row 130, inside row group 3
			neg := fmtDec(-sc.step*31, sc.scale)  // row 68, inside row group 1
			hi := fmtDec(sc.step*60, sc.scale)    // row 159
			miss := fmtDec(sc.step/3, sc.scale+3) // more decimals than the column holds

			for _, tc := range []struct {
				name string
				sql  string
				want int64
			}{
				{"eq", "SELECT COUNT(*) AS n FROM t WHERE v = " + one, 1},
				{"eq_negative", "SELECT COUNT(*) AS n FROM t WHERE v = " + neg, 1},
				{"eq_zero", "SELECT COUNT(*) AS n FROM t WHERE v = 0", 1},
				{"eq_integer_literal", "SELECT COUNT(*) AS n FROM t WHERE v = 0.0", 1},
				// Trailing zeros do not change the value: 0.250 is 0.25.
				{"eq_trailing_zeros", "SELECT COUNT(*) AS n FROM t WHERE v = " + one + "000", 1},
				// A literal the column's scale cannot hold matches nothing —
				// and must not prune anything either.
				{"eq_unrepresentable", "SELECT COUNT(*) AS n FROM t WHERE v = " + miss, 0},
				{"ne", "SELECT COUNT(*) AS n FROM t WHERE v <> " + one, n - 1},
				{"lt", "SELECT COUNT(*) AS n FROM t WHERE v < " + one, 130},
				{"le", "SELECT COUNT(*) AS n FROM t WHERE v <= " + one, 131},
				{"gt", "SELECT COUNT(*) AS n FROM t WHERE v > " + one, 68},
				{"ge", "SELECT COUNT(*) AS n FROM t WHERE v >= " + one, 69},
				{"in", "SELECT COUNT(*) AS n FROM t WHERE v IN (" + one + ", " + neg + ")", 2},
				{"between", "SELECT COUNT(*) AS n FROM t WHERE v BETWEEN " + one + " AND " + hi, 30},
				{"case", "SELECT COUNT(*) AS n FROM t WHERE CASE WHEN v = " + one + " THEN 1 ELSE 0 END = 1", 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, prune := range []bool{true, false} {
						prevStats := scan.StatsPrune.Set(prune)
						prevDict := scan.DictPrune.Set(prune)
						res, err := tmRun(ctx, db, tc.sql)
						scan.StatsPrune.Set(prevStats)
						scan.DictPrune.Set(prevDict)
						if err != nil {
							t.Fatalf("prune=%v: %s: %v", prune, tc.sql, err)
						}
						got, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
						if !ok {
							t.Fatalf("COUNT(*) came back as %#v", res.Rows[0][res.Columns[0]])
						}
						if got != tc.want {
							t.Errorf("prune=%v: %s\n  got %d rows, want %d", prune, tc.sql, got, tc.want)
						}
					}
				})
			}
		})
	}
}

// fmtDec renders a value as a decimal literal with exactly `places` digits
// after the point, so the SQL text carries no exponent and no float noise.
func fmtDec(v float64, places int) string {
	return fmt.Sprintf("%.*f", places, v)
}
