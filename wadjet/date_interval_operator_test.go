package wadjet

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Issue #332 at query level: a temporal COLUMN plus or minus an INTERVAL. The
// unit-level regression lives in internal/engine/expr; this one runs the whole
// path — parser, planner (which is where the expression's compiled node shape
// and the projection's declared output type are decided), pipeline, result
// rendering — because the defect showed up differently at each layer: the
// interval was dropped in the compiled expression, and the surviving raw epoch
// number then read back as "9568" or as NULL depending on the output column's
// type. A unit test on BinOp alone would not have seen either.
func TestColumnIntervalArithmetic(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	inst := time.Date(1996, 3, 13, 14, 25, 36, 0, time.UTC)
	day := time.Date(1996, 3, 13, 0, 0, 0, 0, time.UTC)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "d", Type: parquet.TypeDate},
		{Name: "s", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 10})
	if err := ing.Ingest(ctx, []map[string]any{{
		"ts": inst.UnixMilli(),
		"d":  int32(day.Unix() / 86400),
		"s":  "1996-03-13",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, `SELECT
		d  - INTERVAL '90' DAY   AS d_minus90,
		d  + INTERVAL '1' MONTH  AS d_plus1m,
		d  + INTERVAL '1' YEAR   AS d_plus1y,
		INTERVAL '1' DAY + d     AS d_plus1d,
		ts - INTERVAL '90' DAY   AS ts_minus90,
		ts + INTERVAL '2' HOUR   AS ts_plus2h,
		s  - INTERVAL '90' DAY   AS s_minus90,
		DATE '1996-03-13' - INTERVAL '90' DAY AS lit_minus90
		FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows: %v", res.Rows)
	}
	r := res.Rows[0]
	for _, tc := range []struct {
		col  string
		want string
	}{
		// A DATE column stays a calendar date; MONTH and YEAR are calendar
		// arithmetic, not a fixed number of days.
		{"d_minus90", "1995-12-14"},
		{"d_plus1m", "1996-04-13"},
		{"d_plus1y", "1997-03-13"},
		{"d_plus1d", "1996-03-14"},
		// A TIMESTAMP column keeps its time-of-day, rendered by
		// batch.FormatTimestamp — the way the column itself reads.
		{"ts_minus90", "1995-12-14 14:25:36"},
		{"ts_plus2h", "1996-03-13 16:25:36"},
		// Text and the date literal: the forms that route through the string
		// path. The literal is the control from the issue — it was already
		// correct, and must stay bit-identical (TPC-H Q1 depends on it).
		{"s_minus90", "1995-12-14"},
		{"lit_minus90", "1995-12-14"},
	} {
		if got := r[tc.col]; got != tc.want {
			t.Errorf("%s = %v (%T), want %q", tc.col, got, got, tc.want)
		}
	}
}
