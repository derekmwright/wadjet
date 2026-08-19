package wadjet

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Issue #322 at query level: date_add / date_sub / date_diff over a TIMESTAMP
// column. The unit-level regression lives in internal/engine/expr; this one
// runs the whole path — parser, planner, projection output vector, result
// rendering — because #319's twin of this bug passed every unit test while the
// query path answered 1970.
func TestDateArithOverTimestampColumn(t *testing.T) {
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
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 10})
	rows := []map[string]any{{
		"ts": inst.UnixMilli(),
		"d":  int32(day.Unix() / 86400),
	}}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, `SELECT date_add(ts, 1) AS ts_plus,
	                                  date_sub(ts, 1) AS ts_minus,
	                                  date_add(d, 1)  AS d_plus,
	                                  date_diff(ts, d) AS same_day,
	                                  date_diff(d, ts) AS back_a_day
	                           FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows: %v", res.Rows)
	}
	r := res.Rows[0]
	// The timestamp shifts by a whole day and keeps its time-of-day; the
	// date shifts by a whole day and stays a date; the two columns hold the
	// same day, so the difference is under one day forward and one whole
	// day back.
	for _, tc := range []struct {
		col  string
		want any
	}{
		{"ts_plus", "1996-03-14 14:25:36"},
		{"ts_minus", "1996-03-12 14:25:36"},
		{"d_plus", "1996-03-14"},
		{"same_day", 0.0},
		{"back_a_day", -1.0},
	} {
		got := r[tc.col]
		if f, ok := toF(got); ok {
			if w, isNum := tc.want.(float64); isNum && f == w {
				continue
			}
		}
		if got != tc.want {
			t.Errorf("%s = %v (%T), want %v", tc.col, got, got, tc.want)
		}
	}
}
