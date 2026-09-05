package wadjet

import (
	"context"
	"fmt"
	"strings"
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
		// The two bytes of an encoded 'é' — VALID UTF-8, so one rune and two
		// bytes. It is the only shape that separates a byte count from a
		// character count, and #583's first pass had it in the file's header
		// comment and in no LENGTH cell (round 2, B4).
		{"k": int64(6), "b": []byte{0xc3, 0xa9}},
		{"k": int64(7), "b": []byte("héllo")},
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

// byteain's REFUSALS, which the four sites that read a bytea literal used to
// answer around: each fell back to the literal's raw SPELLING when the decode
// failed and none of them raised, so `b = '\x6'` and `b <> '\xzz'` ANSWERED
// where PostgreSQL refuses (round 2, P7). The accept-side edges are here too —
// uppercase `\X` is a refusal on the server and whitespace inside the hex
// digits is NOT.
func TestByteaLiteralRefusalsFollowByteain(t *testing.T) {
	ctx := context.Background()
	db := f3ByteaOpen(t)

	for _, c := range []struct{ name, lit, state string }{
		{"odd_hex_digits", `\x6`, "22P02"},
		{"bad_hex_digit", `\xzz`, "22P02"},
		{"lone_backslash", `a\b`, "22P02"},
		{"uppercase_x_is_not_the_hex_form", `\X6869`, "22P02"},
		{"trailing_backslash", `abc\`, "22P02"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, sql := range []string{
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM byteapr WHERE b = '%s'`, c.lit),
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM byteapr WHERE b <> '%s'`, c.lit),
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM byteapr WHERE b IN ('%s')`, c.lit),
				fmt.Sprintf(`SELECT COUNT(*) AS n FROM byteapr WHERE k < 0 AND b = '%s'`, c.lit),
			} {
				_, err := db.Query(ctx, sql)
				if err == nil {
					t.Errorf("answered; PostgreSQL 17.11 refuses with %s\n  SQL: %s", c.state, sql)
					continue
				}
				if !strings.Contains(err.Error(), "bytea") {
					t.Errorf("%v — want a bytea input refusal\n  SQL: %s", err, sql)
				}
			}
		})
	}

	// hex_decode SKIPS whitespace, so this is the same two bytes as
	// `'\x6869'` on the server — and refusing it would be refusing what
	// PostgreSQL accepts.
	res, err := db.Query(ctx, `SELECT k FROM byteapr WHERE b = '\x68 69'`)
	if err != nil {
		t.Fatalf("whitespace inside the hex digits was refused: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Errorf(`b = '\x68 69' matched %d rows, want the row holding 0x68 0x69`, len(res.Rows))
	}
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
		{"ne", `b <> '\x6869'`, []int64{2, 3, 5, 6, 7}},
		{"gt", `b > '\x6869'`, []int64{2, 6, 7}},
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
