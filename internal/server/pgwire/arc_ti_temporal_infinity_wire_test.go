// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// ARC TI — PostgreSQL's infinite TIMESTAMP and DATE values on the wire, in
// both formats and both directions. PostgreSQL's binary encoding of them is
// the carrier's extremes — DT_NOEND / DT_NOBEGIN, the int64 extremes in
// microseconds, for timestamp and timestamptz; DATEVAL_NOEND /
// DATEVAL_NOBEGIN, the int32 extremes in days, for date — and this engine's
// carrier holds them as its own extremes, so a binary parameter binds as the
// value and a binary result cell is the value's encoding. Every want is
// PostgreSQL 17.11's answer (re-measured when WADJET_PG_DSN names a server).
//
// At 8e681724 a binary timestamp parameter carrying an extreme was refused
// 22023 at Bind (temporal r3), a binary date extreme bound as a date in the
// year 5881610 and was refused 22008, and the text 'infinity' was 22007.

type tiWireCell struct {
	name    string
	sql     string
	params  [][]byte
	oids    []uint32
	pfmt    []int16 // parameter formats
	rfmt    int16   // result format
	wantSQL string  // PostgreSQL 17.11's answer
}

func tiBE32(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }

func tiWireRun(ctx context.Context, conn *pgconn.PgConn, c tiWireCell) string {
	res := conn.ExecParams(ctx, c.sql, c.params, c.oids, c.pfmt, []int16{c.rfmt}).Read()
	if res.Err != nil {
		return pwDoorErrPG(res.Err)
	}
	var rows []string
	for _, r := range res.Rows {
		var f []string
		for _, v := range r {
			switch {
			case v == nil:
				f = append(f, "NULL")
			case c.rfmt == 1:
				f = append(f, hex.EncodeToString(v))
			default:
				f = append(f, string(v))
			}
		}
		rows = append(rows, strings.Join(f, "|"))
	}
	return "rows=[" + strings.Join(rows, " ; ") + "]"
}

func pwDoorErrPG(err error) string {
	if pe, ok := err.(*pgconn.PgError); ok {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

var tiWireFixture = []string{
	"CREATE TABLE tiw (id BIGINT, ts TIMESTAMP, d DATE)",
	"INSERT INTO tiw VALUES (1, 'infinity', 'infinity'), (2, '-infinity', '-infinity'), (3, '2024-01-15 10:30:00', '2024-01-15'), (4, NULL, NULL)",
}

func tiWireCells() []tiWireCell {
	bin := []int16{1}
	txt := []int16{0}
	return []tiWireCell{
		{name: "out/text", sql: "SELECT id, ts, d FROM tiw ORDER BY id", rfmt: 0,
			wantSQL: "rows=[1|infinity|infinity ; 2|-infinity|-infinity ; 3|2024-01-15 10:30:00|2024-01-15 ; 4|NULL|NULL]"},
		{name: "out/binary", sql: "SELECT ts, d FROM tiw ORDER BY id", rfmt: 1,
			wantSQL: "rows=[7fffffffffffffff|7fffffff ; 8000000000000000|80000000 ; 0002b1f843beba00|0000224c ; NULL|NULL]"},
		{name: "out/binary_cast", sql: "SELECT CAST(d AS TIMESTAMP), CAST(ts AS DATE) FROM tiw WHERE id <= 2 ORDER BY id", rfmt: 1,
			wantSQL: "rows=[7fffffffffffffff|7fffffff ; 8000000000000000|80000000]"},
		{name: "in/binary_ts_max", sql: "SELECT id FROM tiw WHERE ts = $1 ORDER BY id", params: [][]byte{be64b(math.MaxInt64)}, oids: []uint32{oidTimestamp}, pfmt: bin,
			wantSQL: "rows=[1]"},
		{name: "in/binary_ts_min", sql: "SELECT id FROM tiw WHERE ts = $1 ORDER BY id", params: [][]byte{be64b(math.MinInt64)}, oids: []uint32{oidTimestamp}, pfmt: bin,
			wantSQL: "rows=[2]"},
		{name: "in/binary_tstz_max", sql: "SELECT id FROM tiw WHERE ts < $1 ORDER BY id", params: [][]byte{be64b(math.MaxInt64)}, oids: []uint32{oidTimestampTZ}, pfmt: bin,
			wantSQL: "rows=[2 ; 3]"},
		{name: "in/binary_date_max", sql: "SELECT id FROM tiw WHERE d = $1 ORDER BY id", params: [][]byte{tiBE32(math.MaxInt32)}, oids: []uint32{oidDate}, pfmt: bin,
			wantSQL: "rows=[1]"},
		{name: "in/binary_date_min", sql: "SELECT id FROM tiw WHERE d > $1 ORDER BY id", params: [][]byte{tiBE32(math.MinInt32)}, oids: []uint32{oidDate}, pfmt: bin,
			wantSQL: "rows=[1 ; 3]"},
		{name: "in/binary_date_beside", sql: "SELECT id FROM tiw WHERE d = $1 ORDER BY id", params: [][]byte{tiBE32(math.MaxInt32 - 1)}, oids: []uint32{oidDate}, pfmt: bin,
			wantSQL: "ERR 22008"},
		{name: "in/text_ts", sql: "SELECT id FROM tiw WHERE ts = $1 ORDER BY id", params: [][]byte{[]byte("infinity")}, oids: []uint32{oidTimestamp}, pfmt: txt,
			wantSQL: "rows=[1]"},
		{name: "in/text_tstz", sql: "SELECT id FROM tiw WHERE ts = $1 ORDER BY id", params: [][]byte{[]byte("-infinity")}, oids: []uint32{oidTimestampTZ}, pfmt: txt,
			wantSQL: "rows=[2]"},
		{name: "in/text_date", sql: "SELECT id FROM tiw WHERE d < $1 ORDER BY id", params: [][]byte{[]byte(" +Infinity ")}, oids: []uint32{oidDate}, pfmt: txt,
			wantSQL: "rows=[2 ; 3]"},
		{name: "in/text_unknown", sql: "SELECT id FROM tiw WHERE d = $1 ORDER BY id", params: [][]byte{[]byte("-infinity")}, oids: []uint32{0}, pfmt: txt,
			wantSQL: "rows=[2]"},
		{name: "roundtrip/binary_ts", sql: "SELECT CAST($1 AS TIMESTAMP)", params: [][]byte{be64b(math.MinInt64)}, oids: []uint32{oidTimestamp}, pfmt: bin, rfmt: 1,
			wantSQL: "rows=[8000000000000000]"},
		{name: "roundtrip/binary_date", sql: "SELECT CAST($1 AS DATE)", params: [][]byte{tiBE32(math.MaxInt32)}, oids: []uint32{oidDate}, pfmt: bin, rfmt: 1,
			wantSQL: "rows=[7fffffff]"},
		{name: "roundtrip/text_ts", sql: "SELECT CAST($1 AS TIMESTAMP)", params: [][]byte{be64b(math.MaxInt64)}, oids: []uint32{oidTimestamp}, pfmt: bin, rfmt: 0,
			wantSQL: "rows=[infinity]"},
		{name: "insert/binary", sql: "INSERT INTO tiw VALUES (5, $1, $2)", params: [][]byte{be64b(math.MinInt64), tiBE32(math.MaxInt32)}, oids: []uint32{oidTimestamp, oidDate}, pfmt: []int16{1, 1},
			wantSQL: "rows=[]"},
		{name: "insert/read_back", sql: "SELECT ts, d FROM tiw WHERE id = 5", rfmt: 0,
			wantSQL: "rows=[-infinity|infinity]"},
	}
}

func TestArcTIInfinityOverTheWireBothFormats(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "tiwire"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	for _, s := range tiWireFixture {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	sides := []struct {
		name string
		conn *pgconn.PgConn
	}{{"engine", connectPgconn(t, srv.Addr())}}
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		pg, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { pg.Close(context.Background()) })
		for _, s := range append([]string{"SET statement_timeout = '30s'", "SET TimeZone = 'UTC'",
			"DROP SCHEMA IF EXISTS tiwire CASCADE", "CREATE SCHEMA tiwire", "SET search_path = tiwire"}, tiWireFixture...) {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		sides = append(sides, struct {
			name string
			conn *pgconn.PgConn
		}{"PostgreSQL 17.11", pg})
	}
	for _, side := range sides {
		for _, c := range tiWireCells() {
			if got := tiWireRun(ctx, side.conn, c); got != c.wantSQL {
				t.Errorf("%s %s: %s\n  got  %s\n  want %s", side.name, c.name, c.sql, got, c.wantSQL)
			}
		}
		// COPY FROM STDIN, text format: each field through the column's
		// input function.
		if _, err := side.conn.CopyFrom(ctx, strings.NewReader("6\t-Infinity\t+infinity\n7\t infinity\t-infinity\n"),
			"COPY tiw (id, ts, d) FROM STDIN"); err != nil {
			t.Errorf("%s COPY: %v", side.name, err)
			continue
		}
		c := tiWireCell{name: "copy/read_back", sql: "SELECT id, ts, d FROM tiw WHERE id >= 6 ORDER BY id",
			wantSQL: "rows=[6|-infinity|infinity ; 7|infinity|-infinity]"}
		if got := tiWireRun(ctx, side.conn, c); got != c.wantSQL {
			t.Errorf("%s %s: %s\n  got  %s\n  want %s", side.name, c.name, c.sql, got, c.wantSQL)
		}
	}
}
