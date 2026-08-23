package wadjet

import (
	"context"
	"fmt"
	"testing"
)

// #406's gate: the five window VALUE functions over all 22 types.
//
// FIRST_VALUE/LAST_VALUE over an ARRAY, ROW or MAP column returned NULL on
// every row. The window declares its output with batch.NewVector(OutputType)
// — a bare TypeID — so the output vector had no Child, no Children and no
// dimension, and Vector.SetValue's container arms return early on exactly
// those, over a vector whose null mask was pre-set all-null. Silent: no
// error, an empty column.
//
// VECTOR was the fourth container with the same hole, and DECIMAL was the
// quiet one: its box is a FORMATTED string that SetValue re-parses against
// the output vector's scale, so `FIRST_VALUE(c_dec)` over a scale-4 column
// answered 3 where the row holds 3.0003 — a wrong number, not a missing one,
// and identical on every arm of every differential gate.
//
// The reference is the engine's own projection of the same column, with the
// window's definition applied in Go — the pattern TestMinByMaxByEveryType
// uses for #392, which is what a value assertion has to look like when the
// stage DAG cannot answer the query at all (#397) and so cannot be a second
// arm.

// wvRows is the fixture prefix the window runs over. Small enough to keep the
// test cheap, large enough that every group has several rows and that the
// per-column NULL strides (23, 29, ... 113) put a NULL inside some group.
const wvRows = 400

type wvRow struct {
	id int64
	g  any
	v  any
}

func TestWindowValueFunctionsEveryType(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	for _, col := range mbTypeCols() {
		col := col
		t.Run(col.Name, func(t *testing.T) {
			proj, err := db.Query(ctx, fmt.Sprintf(
				"SELECT id, g, %s AS v FROM mbtypes WHERE id < %d ORDER BY id", col.Name, wvRows))
			if err != nil {
				t.Fatalf("projection: %v", err)
			}
			if len(proj.Rows) != wvRows {
				t.Fatalf("projection returned %d rows, want %d", len(proj.Rows), wvRows)
			}
			// Partitions in ORDER BY id sequence, keyed the way the engine
			// keys them (a NULL g is its own partition).
			order := make([]wvRow, 0, wvRows)
			partOf := map[string][]int{} // partition key -> indices into order
			for _, r := range proj.Rows {
				id, ok := r["id"].(int64)
				if !ok {
					t.Fatalf("id came back as %T", r["id"])
				}
				key := fmt.Sprintf("%v", r["g"])
				order = append(order, wvRow{id: id, g: r["g"], v: r["v"]})
				partOf[key] = append(partOf[key], len(order)-1)
			}
			posInPart := make([]int, len(order))  // 0-based rank within the partition
			partKey := make([]string, len(order)) // this row's partition
			for key, idxs := range partOf {
				for pos, i := range idxs {
					posInPart[i] = pos
					partKey[i] = key
				}
			}

			const w = " OVER (PARTITION BY g ORDER BY id)"
			res, err := db.Query(ctx, fmt.Sprintf(
				"SELECT id, "+
					"FIRST_VALUE(%[1]s)%[3]s AS f, "+
					"LAST_VALUE(%[1]s)%[3]s AS l, "+
					"LAG(%[1]s)%[3]s AS lg, "+
					"LEAD(%[1]s)%[3]s AS ld, "+
					"NTH_VALUE(%[1]s, 2)%[3]s AS n "+
					"FROM mbtypes WHERE id < %[2]d ORDER BY id",
				col.Name, wvRows, w))
			if err != nil {
				t.Fatalf("window query: %v", err)
			}
			if len(res.Rows) != wvRows {
				t.Fatalf("window query returned %d rows, want %d", len(res.Rows), wvRows)
			}

			for i, r := range res.Rows {
				if got, want := r["id"], order[i].id; got != want {
					t.Fatalf("row %d: id %v, want %v — the two queries are not aligned", i, got, want)
				}
				idxs := partOf[partKey[i]]
				pos := posInPart[i]

				// FIRST_VALUE: the partition's first row.
				mbAssertEqual(t, fmt.Sprintf("row %d FIRST_VALUE", i), r["f"], order[idxs[0]].v)
				// LAST_VALUE under the default frame (unbounded preceding to
				// current row): this row's own value.
				mbAssertEqual(t, fmt.Sprintf("row %d LAST_VALUE", i), r["l"], order[i].v)
				// LAG/LEAD: the neighbouring row in the partition, NULL at
				// the ends.
				var lag, lead any
				if pos > 0 {
					lag = order[idxs[pos-1]].v
				}
				if pos+1 < len(idxs) {
					lead = order[idxs[pos+1]].v
				}
				mbAssertEqual(t, fmt.Sprintf("row %d LAG", i), r["lg"], lag)
				mbAssertEqual(t, fmt.Sprintf("row %d LEAD", i), r["ld"], lead)
				// NTH_VALUE(x, 2) under the same frame: NULL until the frame
				// reaches the partition's second row, then that row's value.
				var nth any
				if pos >= 1 {
					nth = order[idxs[1]].v
				}
				mbAssertEqual(t, fmt.Sprintf("row %d NTH_VALUE", i), r["n"], nth)
			}
		})
	}
}

// TestWindowValueFunctionsAreNotAllNull states the defect directly: an output
// vector the compute cannot write reads back NULL on every row, which is
// indistinguishable from a legitimately NULL column unless something asserts
// the column has content. Every type column in the fixture is non-NULL on the
// vast majority of its rows.
func TestWindowValueFunctionsAreNotAllNull(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)
	for _, col := range mbTypeCols() {
		t.Run(col.Name, func(t *testing.T) {
			res, err := db.Query(ctx, fmt.Sprintf(
				"SELECT FIRST_VALUE(%s) OVER (PARTITION BY g ORDER BY id) AS f "+
					"FROM mbtypes WHERE id < 100", col.Name))
			if err != nil {
				t.Fatal(err)
			}
			nonNull := 0
			for _, r := range res.Rows {
				if r["f"] != nil {
					nonNull++
				}
			}
			if nonNull == 0 {
				t.Fatalf("FIRST_VALUE over %v (%v) is NULL on all %d rows — the output vector "+
					"has no Child/Children/Dimension/Scale and SetValue dropped every write",
					col.Name, col.Type, len(res.Rows))
			}
		})
	}
}
