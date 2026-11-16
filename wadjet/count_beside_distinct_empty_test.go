// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A COUNT beside a COUNT(DISTINCT) in an ungrouped aggregate answers 0 over
// zero input rows (#1584). The two-level COUNT(DISTINCT) rewrite re-aggregates
// each level-1 COUNT partial with SUM at level 2; over zero rows level 1 has
// no groups and that ungrouped SUM answered NULL, so count(*) and count(v)
// came back NULL. SUM stays NULL. Every want is PostgreSQL 17.11.
func TestCountBesideDistinctOverZeroRows(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	rows := make([]map[string]any, 10)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1), "v": int64(10 * (i + 1))}
	}
	if err := db.CreateTable(ctx, "p", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("p", schema, nil, ingest.Config{MaxBufferRows: 5, RowGroupSize: 2})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, sql, want string }{
		{"lone", "SELECT count(DISTINCT id) FROM p WHERE id < 0", "[[0]]"},
		{"distinctFirst", "SELECT count(DISTINCT id), count(*) FROM p WHERE id < 0", "[[0 0]]"},
		{"distinctSecond", "SELECT count(*), count(DISTINCT id) FROM p WHERE id < 0", "[[0 0]]"},
		{"withSum", "SELECT count(DISTINCT id), sum(v), count(v) FROM p WHERE id < 0", "[[0 <nil> 0]]"},
		{"withAvgMinMax", "SELECT count(DISTINCT id), avg(v), min(v), max(v), count(v) FROM p WHERE id < 0", "[[0 <nil> <nil> <nil> 0]]"},
		{"groupedControl", "SELECT id, count(DISTINCT v), count(*) FROM p WHERE id < 0 GROUP BY id", "[]"},
		{"oneRowControl", "SELECT count(DISTINCT id), sum(v), count(v), count(*) FROM p WHERE id = 3", "[[1 30 1 1]]"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", c.name, c.sql, err, c.want)
			continue
		}
		if got := stCells(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
}
