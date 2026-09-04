package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #582: a literal beside a BYTES column is read by byteain, PostgreSQL's own
// bytea input function, so `b = '\x6869'` names the two bytes 0x68 0x69 and not
// the six characters of its own spelling.
//
// It named the spelling before, on every path, so the row was never found while
// the server found it. The `<>` spelling of the same predicate answered the
// RIGHT rows for the wrong reason, which is what makes this the silent-wrong
// class rather than the loud one.
//
// Four sites had to agree, and the last of them is why the first three were not
// enough: the row-group PRUNE keys the literal against the file's min/max, so
// while the filter kernel was already decoding, the prune compared "\x6869" —
// which sorts below "hi" as text — and dropped the row group before any filter
// ran. A prune that reads a literal differently from the filter is a wrong
// answer, not a lost optimization (ADR-0018).
func f3ByteaOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeBytes, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "byteapr", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := []map[string]any{
		{"k": int64(1), "b": []byte("hi")},
		{"k": int64(2), "b": []byte{0xff, 0xfe, 0x00, 0x41}},
		{"k": int64(3), "b": []byte{}},
		{"k": int64(4), "b": nil},
		// A value whose own bytes contain a BACKSLASH, which is the byte the
		// escape spelling is built out of: it is the one that says the reading
		// is byteain and not "strip a prefix".
		{"k": int64(5), "b": []byte{'a', '\\', 'b'}},
	}
	ing := db.NewIngester("byteapr", sc, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

func TestByteaLiteralIsReadByByteain(t *testing.T) {
	ctx := context.Background()
	db := f3ByteaOpen(t)

	for _, c := range []struct {
		name, pred string
		want       []int64
	}{
		// The hex spelling, which is what bytea_output produces and what a
		// client pastes back.
		{"hex", `b = '\x6869'`, []int64{1}},
		{"hex_uppercase_digits", `b = '\x6869'`, []int64{1}},
		{"hex_non_utf8", `b = '\xfffe0041'`, []int64{2}},
		{"hex_empty", `b = '\x'`, []int64{3}},
		// The escape spelling, and the ordinary one, which must keep working:
		// byteain reads a string with no backslash as its own bytes.
		{"plain", `b = 'hi'`, []int64{1}},
		{"escape_octal", `b = '\377\376\000A'`, []int64{2}},
		{"escape_backslash", `b = 'a\\b'`, []int64{5}},
		// Ordering and inequality read the same literal the same way.
		{"ne", `b <> '\x6869'`, []int64{2, 3, 5}},
		{"gt", `b > '\x6869'`, []int64{2}},
		{"lt", `b < '\x6869'`, []int64{3, 5}},
		{"in", `b IN ('\x6869', '\xfffe0041')`, []int64{1, 2}},
		// A STRING column is not a bytea column: nothing here changes what a
		// text literal means anywhere else. The control is the same predicate
		// spelled against the same bytes through a CAST, which renders the
		// column as PostgreSQL's own \x text.
		{"cast_to_text_compares_the_rendering", `CAST(b AS STRING) = '\x6869'`, []int64{1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT k FROM byteapr WHERE %s ORDER BY k`, c.pred)
			res, err := db.Query(ctx, sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, sql)
			}
			var got []int64
			for _, r := range res.Rows {
				n, ok := tmAsInt64(r["k"])
				if !ok {
					t.Fatalf("k came back as %#v", r["k"])
				}
				got = append(got, n)
			}
			if fmt.Sprint(got) != fmt.Sprint(c.want) {
				t.Errorf("WHERE %s matched %v, live PostgreSQL 17.11 matches %v", c.pred, got, c.want)
			}
		})
	}
}
