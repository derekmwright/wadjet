// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestIngesterHoldsPostgreSQLTemporalRange is the embedded ingester API's row
// of the range table (arc VL round-4 review P2): a typed DATE or TIMESTAMP box
// past PostgreSQL's range — an int32 day count, a time.Time, an int64 of
// epoch milliseconds — is 22008 at Ingest and nothing is stored, the same
// answer every SQL door gives. The writer's box normalisation asks the ONE
// range question (parquet.DateDaysInRange / TimestampMillisInRange, which
// expr's constructors read too); it used to store `d = int32(2147483647)` and
// read it back as 5881580-07-11. The boxes exactly at the ends store.
func TestIngesterHoldsPostgreSQLTemporalRange(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ig", schema, nil); err != nil {
		t.Fatal(err)
	}
	ingestOne := func(row map[string]any) error {
		ing := db.NewIngester("ig", schema, nil, ingest.Config{MaxBufferRows: 100})
		if err := ing.Ingest(ctx, []map[string]any{row}); err != nil {
			return err
		}
		return ing.FlushAll(ctx)
	}
	for _, row := range []map[string]any{
		{"id": int64(1), "d": int32(2147483647)},
		{"id": int64(2), "d": int64(parquet.MaxDateDay + 1)},
		{"id": int64(3), "d": int(parquet.MinDateDay - 1)},
		{"id": int64(4), "d": "5874898-01-01"},
		{"id": int64(5), "ts": time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"id": int64(6), "ts": int64(9223372036854775000)},
		{"id": int64(7), "ts": int64(parquet.EndTimestampMilli)},
		{"id": int64(8), "ts": int64(parquet.MinTimestampMilli - 1)},
		{"id": int64(9), "ts": time.Date(-5000, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if got := sqlerr.StateOf(ingestOne(row)); got != "22008" {
			t.Errorf("ingest %v: SQLSTATE %q, want 22008 (PostgreSQL's range)", row, got)
		}
	}
	for _, row := range []map[string]any{
		{"id": int64(10), "d": int32(parquet.MaxDateDay)},
		{"id": int64(11), "d": int64(parquet.MinDateDay)},
		{"id": int64(12), "ts": int64(parquet.EndTimestampMilli - 1)},
		{"id": int64(13), "ts": time.Date(2026, 3, 3, 10, 20, 30, 0, time.UTC)},
	} {
		if err := ingestOne(row); err != nil {
			t.Errorf("ingest %v: %v, want stored (inside the range)", row, err)
		}
	}
	res, err := db.Query(ctx, "SELECT id, CAST(d AS TEXT) AS d, CAST(ts AS TEXT) AS ts FROM ig ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprint(res.Rows)
	want := "[map[d:5874897-12-31 id:10 ts:<nil>] map[d:-4713-11-24 id:11 ts:<nil>] " +
		"map[d:<nil> id:12 ts:294276-12-31 23:59:59.999] map[d:<nil> id:13 ts:2026-03-03 10:20:30]]"
	if got != want {
		t.Errorf("stored rows\n  got  %s\n  want %s", got, want)
	}
}
