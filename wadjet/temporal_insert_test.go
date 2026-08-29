package wadjet

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// `INSERT INTO t VALUES (1, '2020-01-01')` into a DATE column stored the EPOCH
// while ingest.Ingester with the same text stored the date (#673). The SQL
// literal path boxes a DATE as a time.Time and the programmatic one boxes it as
// a string; only the string had a writer-side converter, so the time.Time fell
// through toInt32's `default: return 0`. The same hole swallowed a string
// TIMESTAMP and a string or time.Duration DURATION — every one of them a box
// ingest.checkType DECLARES acceptable.
//
// The gate reads the stored value back TWO ways, because one reader agreeing
// with itself proves nothing: the native columnar scan (what a SELECT runs) and
// the row reader (what compaction and ANALYZE run). A defect that reaches only
// one of them is the #428 class.
func TestSQLInsertStoresEveryTemporalLiteral(t *testing.T) {
	// days since 1970-01-01 / epoch millis / nanoseconds — the units
	// schema.go declares and the row reader hands back.
	const jan1_2020Days = int64(18262)
	const jan1_2020Ten = int64(1577872800000)

	for _, tc := range []struct {
		name    string
		column  string // dt | ts | du
		literal string
		want    int64
	}{
		{"date ISO", "dt", "'2020-01-01'", jan1_2020Days},
		{"date narrow fields", "dt", "'2020-1-1'", jan1_2020Days},
		{"date slash", "dt", "'2020/01/01'", jan1_2020Days},
		{"date dot", "dt", "'2020.1.1'", jan1_2020Days},
		{"date compact", "dt", "'20200101'", jan1_2020Days},
		{"date with time of day", "dt", "'2020-01-01 10:00:00'", jan1_2020Days},
		{"date RFC3339", "dt", "'2020-01-01T10:00:00Z'", jan1_2020Days},
		{"date before the epoch", "dt", "'1969-12-31'", -1},
		{"date far future", "dt", "'9999-12-31'", 2932896},

		{"timestamp space separated", "ts", "'2020-01-01 10:00:00'", jan1_2020Ten},
		{"timestamp T separated", "ts", "'2020-01-01T10:00:00'", jan1_2020Ten},
		{"timestamp RFC3339", "ts", "'2020-01-01T10:00:00Z'", jan1_2020Ten},
		{"timestamp fractional", "ts", "'2020-01-01T10:00:00.250Z'", jan1_2020Ten + 250},
		{"timestamp date only", "ts", "'2020-01-01'", 1577836800000},

		{"duration nanoseconds", "du", "5000", 5000},
		{"duration quoted", "du", "'5000'", 5000},
		{"duration negative", "du", "-5000", -5000},
		{"duration zero", "du", "0", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := objstore.NewMemStore()
			db, err := Open(ctx, Config{Store: store, Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query(ctx,
				"CREATE TABLE tt (id INT64, dt DATE, ts TIMESTAMP, du DURATION)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx,
				"INSERT INTO tt (id, "+tc.column+") VALUES (1, "+tc.literal+")"); err != nil {
				t.Fatalf("INSERT %s = %s: %v", tc.column, tc.literal, err)
			}

			// The row reader, on the file itself.
			for _, row := range temporalRowReaderRows(t, db, "tt") {
				got, ok := row[tc.column].(int64)
				if !ok {
					t.Fatalf("row reader boxed %s as %T (%v), want int64", tc.column, row[tc.column], row[tc.column])
				}
				if got != tc.want {
					t.Errorf("row reader: %s = %d, want %d — the literal %s was stored as a different instant",
						tc.column, got, tc.want, tc.literal)
				}
			}

			// The native columnar scan, through a predicate so the stored
			// value has to be right for the row to come back at all (a
			// rendering that lies would still pass a bare SELECT).
			q, err := db.Query(ctx, "SELECT id FROM tt WHERE "+tc.column+" = "+temporalPredicateLiteral(tc.column, tc.want))
			if err != nil {
				t.Fatalf("SELECT: %v", err)
			}
			if len(q.Rows) != 1 {
				t.Errorf("native scan: %d rows for %s = %s, want 1 — the stored value is not %d",
					len(q.Rows), tc.column, temporalPredicateLiteral(tc.column, tc.want), tc.want)
			}
		})
	}
}

// temporalPredicateLiteral spells the stored value the way a WHERE clause over
// that column reads it: a DATE compares against its text, the two int64-carried
// types against their raw count.
func temporalPredicateLiteral(column string, stored int64) string {
	if column == "dt" {
		return "'" + time.Unix(stored*86400, 0).UTC().Format("2006-01-02") + "'"
	}
	return itoa(stored)
}

func itoa(v int64) string {
	if v < 0 {
		return "-" + itoa(-v)
	}
	if v < 10 {
		return string(rune('0' + v))
	}
	return itoa(v/10) + string(rune('0'+v%10))
}

// The two doors must agree on the VALUE for every box each accepts. The SQL
// door boxes a DATE as time.Time and the programmatic door as a string,
// time.Time, int32 or int64 — ingest.checkType's own accept-set — and before
// #673 only the string survived the writer.
func TestTemporalBoxesAgreeAcrossTheIngestDoors(t *testing.T) {
	const jan1_2020Days = int64(18262)
	const jan1_2020Ten = int64(1577872800000)
	jan1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name   string
		column string
		box    any
		want   int64
	}{
		{"DATE from a string", "dt", "2020-01-01", jan1_2020Days},
		{"DATE from a time.Time", "dt", jan1, jan1_2020Days},
		{"DATE from a time.Time in another zone keeps its calendar date", "dt",
			time.Date(2020, 1, 1, 20, 0, 0, 0, time.FixedZone("x", -5*3600)), jan1_2020Days},
		{"DATE from int32 days", "dt", int32(jan1_2020Days), jan1_2020Days},
		{"DATE from int64 days", "dt", jan1_2020Days, jan1_2020Days},

		{"TIMESTAMP from a time.Time", "ts", jan1.Add(10 * time.Hour), jan1_2020Ten},
		{"TIMESTAMP from a string", "ts", "2020-01-01 10:00:00", jan1_2020Ten},
		{"TIMESTAMP from int64 millis", "ts", jan1_2020Ten, jan1_2020Ten},

		{"DURATION from int64 nanos", "du", int64(5000), 5000},
		{"DURATION from a time.Duration", "du", 5000 * time.Nanosecond, 5000},
		{"DURATION from a string", "du", "5000", 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := objstore.NewMemStore()
			db, err := Open(ctx, Config{Store: store, Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			schema := parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "dt", Type: parquet.TypeDate, Nullable: true},
				{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
				{Name: "du", Type: parquet.TypeDuration, Nullable: true},
			}}
			if err := db.CreateTable(ctx, "tb", schema, nil); err != nil {
				t.Fatal(err)
			}
			ing := db.NewIngester("tb", schema, nil, ingest.DefaultConfig())
			if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), tc.column: tc.box}}); err != nil {
				t.Fatalf("Ingest %T: %v", tc.box, err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatalf("FlushAll: %v", err)
			}
			for _, row := range temporalRowReaderRows(t, db, "tb") {
				got, ok := row[tc.column].(int64)
				if !ok {
					t.Fatalf("%s boxed as %T (%v), want int64", tc.column, row[tc.column], row[tc.column])
				}
				if got != tc.want {
					t.Errorf("%s stored %d from a %T box, want %d — this door disagrees with the SQL one",
						tc.column, got, tc.box, tc.want)
				}
			}
		})
	}
}

// A literal no temporal type can hold is an ERROR with PostgreSQL's SQLSTATE,
// never a stored zero. 22007 is an unreadable literal, 22008 a well-formed but
// nonexistent calendar date.
func TestInvalidTemporalLiteralIsRefusedWithItsSQLSTATE(t *testing.T) {
	for _, tc := range []struct {
		name    string
		column  string
		literal string
		state   string
	}{
		{"date naming no date", "dt", "'abc'", "22007"},
		{"date that does not exist", "dt", "'2020-02-30'", "22008"},
		{"date month out of range", "dt", "'2020-13-01'", "22008"},
		{"date empty", "dt", "''", "22007"},
		{"timestamp naming no timestamp", "ts", "'abc'", "22007"},
		{"duration naming no number", "du", "'abc'", "22007"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query(ctx,
				"CREATE TABLE tf (id INT64, dt DATE, ts TIMESTAMP, du DURATION)"); err != nil {
				t.Fatal(err)
			}
			_, err = db.Execute(ctx, "INSERT INTO tf (id, "+tc.column+") VALUES (1, "+tc.literal+")")
			if err == nil {
				q, qerr := db.Query(ctx, "SELECT "+tc.column+" FROM tf")
				t.Fatalf("INSERT %s = %s succeeded; want %s. Stored: %v (%v)",
					tc.column, tc.literal, tc.state, q, qerr)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE %q for %s = %s, want %q (err: %v)", got, tc.column, tc.literal, tc.state, err)
			}
		})
	}
}

// temporalRowReaderRows reads every file of a table through parquet.Reader —
// the ROW path, the one compaction and ANALYZE use — under the table's declared
// schema.
func temporalRowReaderRows(t *testing.T, db *DB, table string) []map[string]any {
	t.Helper()
	ctx := context.Background()
	meta, err := db.Catalog().GetTable(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := db.Catalog().GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			rc, _, err := db.Store().Get(ctx, db.Catalog().Bucket(), f.Path)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, rc); err != nil {
				rc.Close()
				t.Fatal(err)
			}
			rc.Close()
			r, err := parquet.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.ReadRowsAs(meta.Schema.Columns, nil)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, rows...)
		}
	}
	if len(out) == 0 {
		t.Fatal("no rows read back through the row reader")
	}
	return out
}
