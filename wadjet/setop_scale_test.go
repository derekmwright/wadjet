package wadjet

import (
	"context"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #532: a set operation's result type is the COMMON TYPE of its arms —
// `numeric(9,2) UNION ALL numeric(18,4)` is `numeric(18,4)` in PostgreSQL. The
// single-process path handed the FIRST arm's schema to `batch.FromRows`, which
// re-reads every row's rendered decimal text at that schema's scale, so the
// second arm's values were TRUNCATED on the way out: 12.7501 came back as
// 12.75 and 12.7499 as 12.74.
//
// It is a lost VALUE, not a rendering choice — the truncated rows then
// deduplicate against each other, so `UNION` counted fewer distinct values
// than either engine holds.
//
// The stage DAG did not have this: it reconciles arm types before
// concatenating (`reconcileSetOpArmTypes`). So this was also a two-path
// divergence, gated in `internal/coordinator`.
func setopScaleOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	for _, tbl := range []struct {
		name  string
		prec  int
		scale int
		vals  []int64 // unscaled at this table's scale
	}{
		// Two values a scale-2 column can hold exactly.
		{name: "narrow", prec: 9, scale: 2, vals: []int64{1275, 300}},
		// One value that IS a scale-2 value (12.7500) and two that are one
		// unit of the last place either side of it, which no scale-2 column
		// can hold: those are the two the narrowing destroyed.
		{name: "wide", prec: 18, scale: 4, vals: []int64{127500, 127501, 127499}},
	} {
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "d", Type: parquet.TypeDecimal, Precision: tbl.prec, Scale: tbl.scale},
		}}
		if err := db.CreateTable(ctx, tbl.name, schema, nil); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, len(tbl.vals))
		for i, v := range tbl.vals {
			rows[i] = map[string]any{"d": parquet.Decimal128{Lo: uint64(v)}}
		}
		ing := db.NewIngester(tbl.name, schema, nil, ingest.Config{MaxBufferRows: 16})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestSetOpKeepsTheWiderArmsDecimalScale(t *testing.T) {
	ctx := context.Background()
	db := setopScaleOpen(t)

	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		// The narrow arm first, so the OUTPUT schema has to widen to the
		// second arm's — the direction that was truncating.
		{
			name: "narrow arm first",
			sql:  "SELECT d FROM narrow UNION ALL SELECT d FROM wide",
			want: []string{"12.7499", "12.7500", "12.7500", "12.7501", "3.0000"},
		},
		// The wide arm first: already correct, and it stays correct — the
		// widening must never NARROW an arm.
		{
			name: "wide arm first",
			sql:  "SELECT d FROM wide UNION ALL SELECT d FROM narrow",
			want: []string{"12.7499", "12.7500", "12.7500", "12.7501", "3.0000"},
		},
		// Same scale on both sides: untouched.
		{
			name: "same scale",
			sql:  "SELECT d FROM narrow UNION ALL SELECT d FROM narrow",
			want: []string{"12.75", "12.75", "3.00", "3.00"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmRun(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			got := make([]string, 0, len(res.Rows))
			for _, r := range res.Rows {
				s, ok := r["d"].(string)
				if !ok {
					t.Fatalf("%s: DECIMAL came back as %#v (%T), not its exact text",
						tc.sql, r["d"], r["d"])
				}
				got = append(got, s)
			}
			sort.Strings(got)
			if len(got) != len(tc.want) {
				t.Fatalf("%s\n  got %d rows %v, want %d %v", tc.sql, len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("%s\n  got  %v\n  want %v (live PostgreSQL 17 widens to the second arm's scale)",
						tc.sql, got, tc.want)
					break
				}
			}
		})
	}
}
