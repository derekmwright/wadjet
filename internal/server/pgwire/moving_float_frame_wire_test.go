// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A MOVING float frame is RECOMPUTED, and the WIRE carries the number that
// comes out of it (arc NV review N1).
//
// PostgreSQL has no inverse transition for either float sum — neither
// `sum(float8)` nor `sum(float4)` has an `msfunc` in `pg_aggregate` — so a
// frame whose lower end advanced is recomputed there. Subtracting the
// departing row instead loses whatever the addition absorbed, and over
// `1e16, 1, 1` a `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` sum answered `1`
// where 17.11 answers `2`.
//
// It is asserted on the wire as well as in the five-arm census because the
// census reads the engine's own box: what a psql or a JDBC ResultSet receives
// is the TEXT below under the OID beside it, and a value oracle cannot see a
// right value under a wrong OID. Both wire FORMATS are exercised, because the
// binary one is a different encoder over the same float.
//
// Every `want` is `psql`'s own output on PostgreSQL 17.11 over these five
// rows; the OIDs are wadjet's, and 701 for the REAL column is the declaration
// residual ADR-0012's #813 entry records (the server declares 700 there) —
// recorded rather than asserted as agreement.
func TestAMovingFloatWindowFrameCarriesTheRecomputedValue(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "nvret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f8", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "f4", Type: parquet.TypeFloat32, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "nvret", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("nvret", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	// 1e16 absorbs a 1 completely at float8's width and 1e7 absorbs one at
	// float4's: the cancellation pair is the only data that can tell a
	// recomputed frame from a retracted one.
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "f8": 1e16, "f4": float32(1e7)},
		{"id": int64(2), "f8": 1.0, "f4": float32(1.0)},
		{"id": int64(3), "f8": 1.0, "f4": float32(1.0)},
		{"id": int64(4), "f8": 1e16, "f4": float32(1e7)},
		{"id": int64(5), "f8": 2.0, "f4": float32(2.0)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())

	const (
		oidFloat4 = 700
		oidFloat8 = 701
	)
	for _, c := range []struct {
		name, sql string
		oid       uint32
		want      string
	}{
		{"f8/sum/rows_1_preceding",
			"SELECT SUM(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM nvret ORDER BY id",
			oidFloat8, "1e+16;1e+16;2;1e+16;1.0000000000000002e+16"},
		{"f8/avg/rows_1_preceding",
			"SELECT AVG(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM nvret ORDER BY id",
			oidFloat8, "1e+16;5e+15;1;5e+15;5.000000000000001e+15"},
		{"f8/sum/rows_2_preceding",
			"SELECT SUM(f8) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v FROM nvret ORDER BY id",
			oidFloat8, "1e+16;1e+16;1e+16;1.0000000000000002e+16;1.0000000000000002e+16"},
		{"f8/sum/current_and_following",
			"SELECT SUM(f8) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS v FROM nvret ORDER BY id",
			oidFloat8, "1e+16;2;1e+16;1.0000000000000002e+16;2"},
		// The REAL half, which the arc's first cut already recomputed. Its
		// OID was 701 where the server declares 700 and is 700 now (#1118,
		// arc ND); the DIGITS are the server's and did not move. AVG stays
		// 701, which is what `avg(real)` declares there.
		{"f4/sum/rows_1_preceding",
			"SELECT SUM(f4) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM nvret ORDER BY id",
			oidFloat4, "1e+07;1.0000001e+07;2;1.0000001e+07;1.0000002e+07"},
		{"f4/avg/rows_1_preceding",
			"SELECT AVG(f4) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM nvret ORDER BY id",
			oidFloat8, "1e+07;5.0000005e+06;1;5.0000005e+06;5.000001e+06"},
		// The CONTROL: a frame whose lower end never moves is the same
		// running total it always was, so the change is bounded to the frames
		// that move.
		{"f8/sum/unbounded_preceding",
			"SELECT SUM(f8) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM nvret ORDER BY id",
			oidFloat8, "1e+16;1e+16;1e+16;2e+16;2e+16"},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(c.name, func(t *testing.T) {
				res := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{format}).Read()
				if res.Err != nil {
					t.Fatalf("format %d: %v\n  SQL: %s", format, res.Err, c.sql)
				}
				if got := res.FieldDescriptions[0].DataTypeOID; got != c.oid {
					t.Errorf("format %d declared OID %d, want %d\n  SQL: %s",
						format, got, c.oid, c.sql)
				}
				got := make([]string, 0, len(res.Rows))
				for _, row := range res.Rows {
					got = append(got, movingFrameValue(t, res.FieldDescriptions[0].DataTypeOID, format, row[0]))
				}
				// Compared as VALUES, not as bytes. Wadjet's text protocol
				// renders an ordinary magnitude in plain decimal where
				// PostgreSQL's float8out switches to e-notation at one
				// significant digit — `10000000` against `1e+07` — which is
				// formatPgFloat's own deliberate rule and predates this arc
				// (an epoch like 1787049120 must not go out as
				// 1.78704912e+09). Parsing both sides back to a float64 is
				// what asks this cell's own question: does the wire carry the
				// number the frame computed.
				if j := strings.Join(got, ";"); j != movingFrameCanonical(t, c.want) {
					t.Errorf("format %d sent %s, want %s (psql on PostgreSQL 17.11, "+
						"compared as values)\n  SQL: %s",
						format, j, movingFrameCanonical(t, c.want), c.sql)
				}
			})
		}
	}
}

// movingFrameValue reads one wire value back as the float it carries: the TEXT
// format is parsed, the BINARY one is decoded through pgx's own codec, and
// both are rendered in ONE canonical spelling so the comparison is about the
// number rather than about either engine's formatter.
func movingFrameValue(t *testing.T, oid uint32, format int16, raw []byte) string {
	t.Helper()
	if raw == nil {
		return "NULL"
	}
	if format == 0 {
		f, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			t.Fatalf("text value %q: %v", raw, err)
		}
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	m := pgtype.NewMap()
	typ, ok := m.TypeForOID(oid)
	if !ok {
		t.Fatalf("unknown OID %d", oid)
	}
	v, err := typ.Codec.DecodeDatabaseSQLValue(m, oid, format, raw)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("binary value is %T, want float64", v)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// movingFrameCanonical puts psql's own output into that same spelling.
func movingFrameCanonical(t *testing.T, pg string) string {
	t.Helper()
	parts := strings.Split(pg, ";")
	for i, p := range parts {
		f, err := strconv.ParseFloat(p, 64)
		if err != nil {
			t.Fatalf("psql value %q: %v", p, err)
		}
		parts[i] = strconv.FormatFloat(f, 'g', -1, 64)
	}
	return strings.Join(parts, ";")
}
