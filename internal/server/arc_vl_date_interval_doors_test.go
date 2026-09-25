// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/format"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// `date ± interval` IS A TIMESTAMP ON EVERY DOOR (arc VL round 6).
//
// PostgreSQL types `date + interval`, `date - interval` and `interval + date`
// timestamp (OID 1114), whole days or not: `DATE '1998-12-01' - INTERVAL '90'
// DAY` is `1998-09-02 00:00:00`. Base declared the expression TEXT and
// produced the rendered date (`1998-09-02`, OID 25); arc VL round 3 made the
// operator declare TIMESTAMP and produce the TIMESTAMP box (epoch
// milliseconds). This gate holds the two together on every door and both
// arms, so the declaration and the value cannot drift apart again:
//
//   - pgwire single and pgwire DAG, text and binary: OID 1114 and PostgreSQL
//     17.11's own text; the binary cell is the int64 microseconds since
//     2000-01-01 of that instant;
//   - the embedded API (`wadjet.DB.Query`): the column is declared TIMESTAMP
//     and the value is that instant's epoch milliseconds — how this door
//     carries EVERY timestamp (`SELECT TIMESTAMP '…'` included), so a
//     client renders it from the declaration (`format.WriteTyped`);
//   - the CLI renderer over the embedded result: PostgreSQL's text;
//   - HTTP and gRPC: the same epoch milliseconds a TIMESTAMP literal of that
//     instant answers on the same door.
//
// The operand axis is a DATE literal, a DATE column, a CAST to DATE and
// CURRENT_DATE; the interval axis is days, months, years, the clock (hours,
// minutes, seconds), negative intervals, and a date shifted twice (months or
// years, then the clock). The cells FAIL at base 6cbe2041 (every door
// answers the date text / TEXT declaration), and pass at 02bc6e0a, whose
// `./test/` failures were pins of base's TEXT answer
// (`TestDateCastIntervalShiftUnchanged`, `TestCurrentDateMinusInterval`).
func TestDateShiftedByAnIntervalIsATimestampOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single, dag, coord, db := owaDoorsOver(t, ctx, vliWriteDates)
	srv := New(Config{Addr: ":0", Catalog: db.Catalog()}, nil)
	httpSrv := httptest.NewServer(srv.Mux())
	t.Cleanup(httpSrv.Close)
	hs := httpSrv.URL
	g := NewGRPCServer(GRPCConfig{DB: db}, slog.Default())
	conns := map[string]*pgconn.PgConn{}
	for name, addr := range map[string]string{"pgwire/single": single, "pgwire/dag": dag} {
		c, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", addr))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		conns[name] = c
	}
	local0 := owaLocalRoutes(coord)

	type cell struct{ class, operand, expr, want string }
	cells := make([]cell, 0, len(vliDateIntervalCells)+8)
	for _, c := range vliDateIntervalCells {
		cells = append(cells, cell{c[0], c[1], c[2], c[3]})
	}
	// CURRENT_DATE: the want is today's UTC midnight shifted, which is what
	// PostgreSQL answers under TimeZone=UTC (the zone every clock function
	// here reads, #870). Months and years are left to the fixed dates above:
	// a shift that lands past a month's end is filing candidate 22 on every
	// operand, and CURRENT_DATE would make that a calendar-day flake.
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for _, c := range []struct {
		expr string
		d    time.Duration
	}{
		{"CURRENT_DATE - INTERVAL '30 days'", -30 * 24 * time.Hour},
		{"CURRENT_DATE + INTERVAL '7 days'", 7 * 24 * time.Hour},
		{"CURRENT_DATE - INTERVAL '90' DAY", -90 * 24 * time.Hour},
		{"CURRENT_DATE + INTERVAL '2' HOUR", 2 * time.Hour},
		{"CURRENT_DATE - INTERVAL '-5 hours'", 5 * time.Hour},
		{"INTERVAL '90 minutes' + CURRENT_DATE", 90 * time.Minute},
		{"CURRENT_DATE + INTERVAL '-3' DAY", -3 * 24 * time.Hour},
	} {
		cells = append(cells, cell{"current_date", "current_date", c.expr,
			today.Add(c.d).Format("2006-01-02 15:04:05")})
	}

	statements, failures := 0, 0
	for _, c := range cells {
		sql := "SELECT " + c.expr + " AS r FROM vld WHERE id = 1"
		want, err := time.Parse("2006-01-02 15:04:05", c.want)
		if err != nil {
			t.Fatalf("%s: want %q: %v", c.expr, c.want, err)
		}
		wantMS := want.UnixMilli()
		var got []string
		fail := func(door, g string) { got = append(got, door+": "+g) }

		// The embedded API: declared TIMESTAMP, the instant's epoch ms.
		res, err := db.Query(ctx, sql)
		switch {
		case err != nil:
			fail("embedded", err.Error())
		case len(res.Rows) != 1 || len(res.ColumnMetas) != 1:
			fail("embedded", fmt.Sprintf("%d rows, %d metas", len(res.Rows), len(res.ColumnMetas)))
		default:
			if tn := res.ColumnMetas[0].TypeName; tn != "TIMESTAMP" {
				fail("embedded", "declared "+tn)
			}
			if v, ok := res.Rows[0]["r"].(int64); !ok || v != wantMS {
				fail("embedded", fmt.Sprintf("%v (%T), want %d", res.Rows[0]["r"], res.Rows[0]["r"], wantMS))
			}
			// The CLI door renders the embedded result from its declaration.
			var buf bytes.Buffer
			types := make([]parquet.TypeID, len(res.OutputSchema))
			for i, oc := range res.OutputSchema {
				types[i] = oc.Type
			}
			if err := format.WriteTyped(&buf, format.CSV, res.Columns, types,
				[][]any{{res.Rows[0]["r"]}}); err != nil {
				fail("cli", err.Error())
			} else if line := strings.TrimSpace(strings.TrimPrefix(buf.String(), "r\n")); line != c.want {
				fail("cli", fmt.Sprintf("%q", line))
			}
		}
		statements += 2

		// HTTP: the number a TIMESTAMP answers on this door.
		if _, rows, _ := hdQuery(t, hs, sql); len(rows) != 1 || rows[0]["r"] != float64(wantMS) {
			fail("http", fmt.Sprintf("%v", rows))
		}
		statements++

		// gRPC: likewise.
		if resp, err := g.Query(ctx, &wadjetv1.QueryRequest{Sql: sql}); err != nil {
			fail("grpc", err.Error())
		} else if len(resp.Rows) != 1 || resp.Rows[0].Fields["r"].GetNumberValue() != float64(wantMS) {
			fail("grpc", fmt.Sprintf("%v", resp.Rows))
		}
		statements++

		// pgwire, both arms, text and binary.
		for _, door := range []string{"pgwire/single", "pgwire/dag"} {
			for _, fmtCode := range []int16{0, 1} {
				r := conns[door].ExecParams(ctx, sql, nil, nil, nil, []int16{fmtCode}).Read()
				statements++
				name := fmt.Sprintf("%s/%s", door, map[int16]string{0: "text", 1: "binary"}[fmtCode])
				if r.Err != nil {
					fail(name, r.Err.Error())
					continue
				}
				if len(r.FieldDescriptions) != 1 || r.FieldDescriptions[0].DataTypeOID != 1114 {
					fail(name, fmt.Sprintf("fields %+v, want OID 1114", r.FieldDescriptions))
					continue
				}
				if len(r.Rows) != 1 {
					fail(name, fmt.Sprintf("%d rows", len(r.Rows)))
					continue
				}
				v := r.Rows[0][0]
				if fmtCode == 0 && string(v) != c.want {
					fail(name, fmt.Sprintf("%q", v))
				}
				if fmtCode == 1 {
					pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
					if len(v) != 8 || int64(binary.BigEndian.Uint64(v)) != want.Sub(pgEpoch).Microseconds() {
						fail(name, fmt.Sprintf("% x", v))
					}
				}
			}
		}
		if len(got) > 0 {
			failures++
			t.Errorf("[%s/%s] %s — PostgreSQL 17.11 %s (timestamp):\n    %s",
				c.class, c.operand, c.expr, c.want, strings.Join(got, "\n    "))
		}
	}

	// The contract the embedded/HTTP/gRPC cells rest on: a TIMESTAMP literal
	// of the same instant answers the same box there. If this control moves,
	// the cells above are measuring a different contract.
	ctl, err := db.Query(ctx, "SELECT TIMESTAMP '1998-09-02 00:00:00' AS r")
	if err != nil || ctl.Rows[0]["r"] != time.Date(1998, 9, 2, 0, 0, 0, 0, time.UTC).UnixMilli() {
		t.Errorf("control: SELECT TIMESTAMP '1998-09-02 00:00:00' on the embedded door = %v, %v", ctl, err)
	}

	// ENGAGEMENT: nothing the DAG door answered fell back in process.
	if got := owaLocalRoutes(coord); got != local0 {
		t.Errorf("a date ± interval statement fell back to an in-process route: %s (before: %s)", got, local0)
	}
	t.Logf("%d cells, %d statements over 8 door × arm × format columns, %d failing cells", len(cells), statements, failures)
}

func vliWriteDates(t *testing.T, ctx context.Context, db *wadjet.DB) {
	t.Helper()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "sd", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "vld", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("vld", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "d": "1998-12-01", "sd": "1998-12-01"},
		{"id": int64(2), "d": "1996-01-10", "sd": "1996-01-10"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
}

// vliDateIntervalCells: class, operand, expression, PostgreSQL 17.11's answer
// over `vld` row 1 (d = sd = 1998-12-01), every one typed `timestamp without
// time zone` by pg_typeof. Measured by the arc's gen_cells.py.
var vliDateIntervalCells = [][4]string{
	{"days", "literal", "DATE '1998-12-01' + INTERVAL '90' DAY", "1999-03-01 00:00:00"},
	{"days", "literal", "DATE '1998-12-01' - INTERVAL '90' DAY", "1998-09-02 00:00:00"},
	{"days", "literal", "INTERVAL '90' DAY + DATE '1998-12-01'", "1999-03-01 00:00:00"},
	{"days", "column", "d + INTERVAL '90' DAY", "1999-03-01 00:00:00"},
	{"days", "column", "d - INTERVAL '90' DAY", "1998-09-02 00:00:00"},
	{"days", "column", "INTERVAL '90' DAY + d", "1999-03-01 00:00:00"},
	{"days", "cast", "CAST(sd AS DATE) + INTERVAL '90' DAY", "1999-03-01 00:00:00"},
	{"days", "cast", "CAST(sd AS DATE) - INTERVAL '90' DAY", "1998-09-02 00:00:00"},
	{"days", "cast", "INTERVAL '90' DAY + CAST(sd AS DATE)", "1999-03-01 00:00:00"},
	{"days", "literal", "DATE '1998-12-01' + INTERVAL '30 days'", "1998-12-31 00:00:00"},
	{"days", "literal", "DATE '1998-12-01' - INTERVAL '30 days'", "1998-11-01 00:00:00"},
	{"days", "literal", "INTERVAL '30 days' + DATE '1998-12-01'", "1998-12-31 00:00:00"},
	{"days", "column", "d + INTERVAL '30 days'", "1998-12-31 00:00:00"},
	{"days", "column", "d - INTERVAL '30 days'", "1998-11-01 00:00:00"},
	{"days", "column", "INTERVAL '30 days' + d", "1998-12-31 00:00:00"},
	{"days", "cast", "CAST(sd AS DATE) + INTERVAL '30 days'", "1998-12-31 00:00:00"},
	{"days", "cast", "CAST(sd AS DATE) - INTERVAL '30 days'", "1998-11-01 00:00:00"},
	{"days", "cast", "INTERVAL '30 days' + CAST(sd AS DATE)", "1998-12-31 00:00:00"},
	{"months", "literal", "DATE '1998-12-01' + INTERVAL '1' MONTH", "1999-01-01 00:00:00"},
	{"months", "literal", "DATE '1998-12-01' - INTERVAL '1' MONTH", "1998-11-01 00:00:00"},
	{"months", "literal", "INTERVAL '1' MONTH + DATE '1998-12-01'", "1999-01-01 00:00:00"},
	{"months", "column", "d + INTERVAL '1' MONTH", "1999-01-01 00:00:00"},
	{"months", "column", "d - INTERVAL '1' MONTH", "1998-11-01 00:00:00"},
	{"months", "column", "INTERVAL '1' MONTH + d", "1999-01-01 00:00:00"},
	{"months", "cast", "CAST(sd AS DATE) + INTERVAL '1' MONTH", "1999-01-01 00:00:00"},
	{"months", "cast", "CAST(sd AS DATE) - INTERVAL '1' MONTH", "1998-11-01 00:00:00"},
	{"months", "cast", "INTERVAL '1' MONTH + CAST(sd AS DATE)", "1999-01-01 00:00:00"},
	{"months", "literal", "DATE '1998-12-01' + INTERVAL '14 months'", "2000-02-01 00:00:00"},
	{"months", "literal", "DATE '1998-12-01' - INTERVAL '14 months'", "1997-10-01 00:00:00"},
	{"months", "literal", "INTERVAL '14 months' + DATE '1998-12-01'", "2000-02-01 00:00:00"},
	{"months", "column", "d + INTERVAL '14 months'", "2000-02-01 00:00:00"},
	{"months", "column", "d - INTERVAL '14 months'", "1997-10-01 00:00:00"},
	{"months", "column", "INTERVAL '14 months' + d", "2000-02-01 00:00:00"},
	{"months", "cast", "CAST(sd AS DATE) + INTERVAL '14 months'", "2000-02-01 00:00:00"},
	{"months", "cast", "CAST(sd AS DATE) - INTERVAL '14 months'", "1997-10-01 00:00:00"},
	{"months", "cast", "INTERVAL '14 months' + CAST(sd AS DATE)", "2000-02-01 00:00:00"},
	{"years", "literal", "DATE '1998-12-01' + INTERVAL '1' YEAR", "1999-12-01 00:00:00"},
	{"years", "literal", "DATE '1998-12-01' - INTERVAL '1' YEAR", "1997-12-01 00:00:00"},
	{"years", "literal", "INTERVAL '1' YEAR + DATE '1998-12-01'", "1999-12-01 00:00:00"},
	{"years", "column", "d + INTERVAL '1' YEAR", "1999-12-01 00:00:00"},
	{"years", "column", "d - INTERVAL '1' YEAR", "1997-12-01 00:00:00"},
	{"years", "column", "INTERVAL '1' YEAR + d", "1999-12-01 00:00:00"},
	{"years", "cast", "CAST(sd AS DATE) + INTERVAL '1' YEAR", "1999-12-01 00:00:00"},
	{"years", "cast", "CAST(sd AS DATE) - INTERVAL '1' YEAR", "1997-12-01 00:00:00"},
	{"years", "cast", "INTERVAL '1' YEAR + CAST(sd AS DATE)", "1999-12-01 00:00:00"},
	{"years", "literal", "DATE '1998-12-01' + INTERVAL '2 years'", "2000-12-01 00:00:00"},
	{"years", "literal", "DATE '1998-12-01' - INTERVAL '2 years'", "1996-12-01 00:00:00"},
	{"years", "literal", "INTERVAL '2 years' + DATE '1998-12-01'", "2000-12-01 00:00:00"},
	{"years", "column", "d + INTERVAL '2 years'", "2000-12-01 00:00:00"},
	{"years", "column", "d - INTERVAL '2 years'", "1996-12-01 00:00:00"},
	{"years", "column", "INTERVAL '2 years' + d", "2000-12-01 00:00:00"},
	{"years", "cast", "CAST(sd AS DATE) + INTERVAL '2 years'", "2000-12-01 00:00:00"},
	{"years", "cast", "CAST(sd AS DATE) - INTERVAL '2 years'", "1996-12-01 00:00:00"},
	{"years", "cast", "INTERVAL '2 years' + CAST(sd AS DATE)", "2000-12-01 00:00:00"},
	{"hours", "literal", "DATE '1998-12-01' + INTERVAL '2' HOUR", "1998-12-01 02:00:00"},
	{"hours", "literal", "DATE '1998-12-01' - INTERVAL '2' HOUR", "1998-11-30 22:00:00"},
	{"hours", "literal", "INTERVAL '2' HOUR + DATE '1998-12-01'", "1998-12-01 02:00:00"},
	{"hours", "column", "d + INTERVAL '2' HOUR", "1998-12-01 02:00:00"},
	{"hours", "column", "d - INTERVAL '2' HOUR", "1998-11-30 22:00:00"},
	{"hours", "column", "INTERVAL '2' HOUR + d", "1998-12-01 02:00:00"},
	{"hours", "cast", "CAST(sd AS DATE) + INTERVAL '2' HOUR", "1998-12-01 02:00:00"},
	{"hours", "cast", "CAST(sd AS DATE) - INTERVAL '2' HOUR", "1998-11-30 22:00:00"},
	{"hours", "cast", "INTERVAL '2' HOUR + CAST(sd AS DATE)", "1998-12-01 02:00:00"},
	{"hours", "literal", "DATE '1998-12-01' + INTERVAL '36 hours'", "1998-12-02 12:00:00"},
	{"hours", "literal", "DATE '1998-12-01' - INTERVAL '36 hours'", "1998-11-29 12:00:00"},
	{"hours", "literal", "INTERVAL '36 hours' + DATE '1998-12-01'", "1998-12-02 12:00:00"},
	{"hours", "column", "d + INTERVAL '36 hours'", "1998-12-02 12:00:00"},
	{"hours", "column", "d - INTERVAL '36 hours'", "1998-11-29 12:00:00"},
	{"hours", "column", "INTERVAL '36 hours' + d", "1998-12-02 12:00:00"},
	{"hours", "cast", "CAST(sd AS DATE) + INTERVAL '36 hours'", "1998-12-02 12:00:00"},
	{"hours", "cast", "CAST(sd AS DATE) - INTERVAL '36 hours'", "1998-11-29 12:00:00"},
	{"hours", "cast", "INTERVAL '36 hours' + CAST(sd AS DATE)", "1998-12-02 12:00:00"},
	{"hours", "literal", "DATE '1998-12-01' + INTERVAL '90 minutes'", "1998-12-01 01:30:00"},
	{"hours", "literal", "DATE '1998-12-01' - INTERVAL '90 minutes'", "1998-11-30 22:30:00"},
	{"hours", "literal", "INTERVAL '90 minutes' + DATE '1998-12-01'", "1998-12-01 01:30:00"},
	{"hours", "column", "d + INTERVAL '90 minutes'", "1998-12-01 01:30:00"},
	{"hours", "column", "d - INTERVAL '90 minutes'", "1998-11-30 22:30:00"},
	{"hours", "column", "INTERVAL '90 minutes' + d", "1998-12-01 01:30:00"},
	{"hours", "cast", "CAST(sd AS DATE) + INTERVAL '90 minutes'", "1998-12-01 01:30:00"},
	{"hours", "cast", "CAST(sd AS DATE) - INTERVAL '90 minutes'", "1998-11-30 22:30:00"},
	{"hours", "cast", "INTERVAL '90 minutes' + CAST(sd AS DATE)", "1998-12-01 01:30:00"},
	{"hours", "literal", "DATE '1998-12-01' + INTERVAL '45' SECOND", "1998-12-01 00:00:45"},
	{"hours", "literal", "DATE '1998-12-01' - INTERVAL '45' SECOND", "1998-11-30 23:59:15"},
	{"hours", "literal", "INTERVAL '45' SECOND + DATE '1998-12-01'", "1998-12-01 00:00:45"},
	{"hours", "column", "d + INTERVAL '45' SECOND", "1998-12-01 00:00:45"},
	{"hours", "column", "d - INTERVAL '45' SECOND", "1998-11-30 23:59:15"},
	{"hours", "column", "INTERVAL '45' SECOND + d", "1998-12-01 00:00:45"},
	{"hours", "cast", "CAST(sd AS DATE) + INTERVAL '45' SECOND", "1998-12-01 00:00:45"},
	{"hours", "cast", "CAST(sd AS DATE) - INTERVAL '45' SECOND", "1998-11-30 23:59:15"},
	{"hours", "cast", "INTERVAL '45' SECOND + CAST(sd AS DATE)", "1998-12-01 00:00:45"},
	{"negative", "literal", "DATE '1998-12-01' + INTERVAL '-3' DAY", "1998-11-28 00:00:00"},
	{"negative", "literal", "DATE '1998-12-01' - INTERVAL '-3' DAY", "1998-12-04 00:00:00"},
	{"negative", "literal", "INTERVAL '-3' DAY + DATE '1998-12-01'", "1998-11-28 00:00:00"},
	{"negative", "column", "d + INTERVAL '-3' DAY", "1998-11-28 00:00:00"},
	{"negative", "column", "d - INTERVAL '-3' DAY", "1998-12-04 00:00:00"},
	{"negative", "column", "INTERVAL '-3' DAY + d", "1998-11-28 00:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) + INTERVAL '-3' DAY", "1998-11-28 00:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) - INTERVAL '-3' DAY", "1998-12-04 00:00:00"},
	{"negative", "cast", "INTERVAL '-3' DAY + CAST(sd AS DATE)", "1998-11-28 00:00:00"},
	{"negative", "literal", "DATE '1998-12-01' + INTERVAL '-1 month'", "1998-11-01 00:00:00"},
	{"negative", "literal", "DATE '1998-12-01' - INTERVAL '-1 month'", "1999-01-01 00:00:00"},
	{"negative", "literal", "INTERVAL '-1 month' + DATE '1998-12-01'", "1998-11-01 00:00:00"},
	{"negative", "column", "d + INTERVAL '-1 month'", "1998-11-01 00:00:00"},
	{"negative", "column", "d - INTERVAL '-1 month'", "1999-01-01 00:00:00"},
	{"negative", "column", "INTERVAL '-1 month' + d", "1998-11-01 00:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) + INTERVAL '-1 month'", "1998-11-01 00:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) - INTERVAL '-1 month'", "1999-01-01 00:00:00"},
	{"negative", "cast", "INTERVAL '-1 month' + CAST(sd AS DATE)", "1998-11-01 00:00:00"},
	{"negative", "literal", "DATE '1998-12-01' + INTERVAL '-5 hours'", "1998-11-30 19:00:00"},
	{"negative", "literal", "DATE '1998-12-01' - INTERVAL '-5 hours'", "1998-12-01 05:00:00"},
	{"negative", "literal", "INTERVAL '-5 hours' + DATE '1998-12-01'", "1998-11-30 19:00:00"},
	{"negative", "column", "d + INTERVAL '-5 hours'", "1998-11-30 19:00:00"},
	{"negative", "column", "d - INTERVAL '-5 hours'", "1998-12-01 05:00:00"},
	{"negative", "column", "INTERVAL '-5 hours' + d", "1998-11-30 19:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) + INTERVAL '-5 hours'", "1998-11-30 19:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) - INTERVAL '-5 hours'", "1998-12-01 05:00:00"},
	{"negative", "cast", "INTERVAL '-5 hours' + CAST(sd AS DATE)", "1998-11-30 19:00:00"},
	{"negative", "literal", "DATE '1998-12-01' + INTERVAL '-2' YEAR", "1996-12-01 00:00:00"},
	{"negative", "literal", "DATE '1998-12-01' - INTERVAL '-2' YEAR", "2000-12-01 00:00:00"},
	{"negative", "literal", "INTERVAL '-2' YEAR + DATE '1998-12-01'", "1996-12-01 00:00:00"},
	{"negative", "column", "d + INTERVAL '-2' YEAR", "1996-12-01 00:00:00"},
	{"negative", "column", "d - INTERVAL '-2' YEAR", "2000-12-01 00:00:00"},
	{"negative", "column", "INTERVAL '-2' YEAR + d", "1996-12-01 00:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) + INTERVAL '-2' YEAR", "1996-12-01 00:00:00"},
	{"negative", "cast", "CAST(sd AS DATE) - INTERVAL '-2' YEAR", "2000-12-01 00:00:00"},
	{"negative", "cast", "INTERVAL '-2' YEAR + CAST(sd AS DATE)", "1996-12-01 00:00:00"},
	{"mixed", "literal", "DATE '1998-12-01' + INTERVAL '1' MONTH + INTERVAL '2' HOUR", "1999-01-01 02:00:00"},
	{"mixed", "literal", "DATE '1998-12-01' - INTERVAL '1' YEAR - INTERVAL '30' MINUTE", "1997-11-30 23:30:00"},
	{"mixed", "literal", "(DATE '1998-12-01' + INTERVAL '3' DAY) - INTERVAL '-6' HOUR", "1998-12-04 06:00:00"},
	{"mixed", "column", "d + INTERVAL '1' MONTH + INTERVAL '2' HOUR", "1999-01-01 02:00:00"},
	{"mixed", "column", "d - INTERVAL '1' YEAR - INTERVAL '30' MINUTE", "1997-11-30 23:30:00"},
	{"mixed", "column", "(d + INTERVAL '3' DAY) - INTERVAL '-6' HOUR", "1998-12-04 06:00:00"},
	{"mixed", "cast", "CAST(sd AS DATE) + INTERVAL '1' MONTH + INTERVAL '2' HOUR", "1999-01-01 02:00:00"},
	{"mixed", "cast", "CAST(sd AS DATE) - INTERVAL '1' YEAR - INTERVAL '30' MINUTE", "1997-11-30 23:30:00"},
	{"mixed", "cast", "(CAST(sd AS DATE) + INTERVAL '3' DAY) - INTERVAL '-6' HOUR", "1998-12-04 06:00:00"},
}
