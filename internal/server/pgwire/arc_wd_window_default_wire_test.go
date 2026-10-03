// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A LAG / LEAD DEFAULT DECLARES THE COMMON TYPE ON THE WIRE (#1435). The
// result column's OID is PostgreSQL's common type of the value and the
// default — numeric for a bigint value and a 2.5 default, timestamp for a
// DATE value and a TIMESTAMP default — and the rows the default fills carry
// it. At v0.25.3 the column went out as int8 with 2 in it (date: the default
// could not be written at all). Every want is PostgreSQL 17.11 (`\gdesc` and
// the rows; the parameter rows through `PREPARE p(numeric | int8) AS …;
// EXECUTE` and an untyped `PREPARE q AS …` bound '7' / '2.5').
func TestArcWDWindowDefaultDeclaresTheCommonTypeOnTheWire(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "wdwire"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	for _, s := range []string{"CREATE TABLE wdw (id BIGINT, x BIGINT, dt DATE)",
		"INSERT INTO wdw VALUES (1, 10, DATE '2024-01-01'), (2, 20, DATE '2024-01-02'), (3, 30, DATE '2024-01-03')"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	const (
		oidUnknown   = 0
		oidInt8      = 20
		oidNumeric   = 1700
		oidTimestamp = 1114
	)
	// numeric is one scale per column here and per value in PostgreSQL
	// (catalog numeric-decimal r18): the digits are compared with trailing
	// fractional zeros stripped.
	strip := regexp.MustCompile(`^(-?\d+)\.(\d*?)0*$`)
	norm := func(oid uint32, v string) string {
		if oid != oidNumeric {
			return v
		}
		m := strip.FindStringSubmatch(v)
		if m == nil {
			return v
		}
		if m[2] == "" {
			return m[1]
		}
		return m[1] + "." + m[2]
	}
	for _, c := range []struct {
		name, expr string
		params     [][]byte
		types      []uint32
		oid        uint32
		want       string
	}{
		{"simple/bigint_dec", "LAG(x, 1, 2.5) OVER (ORDER BY id)", nil, nil, oidNumeric, "1,2.5;2,10;3,20"},
		{"simple/lead_dec", "LEAD(x, 10, 2.5) OVER (ORDER BY id)", nil, nil, oidNumeric, "1,2.5;2,2.5;3,2.5"},
		{"simple/date_ts", "LAG(dt, 1, TIMESTAMP '2020-01-01 00:00:00') OVER (ORDER BY id)", nil, nil, oidTimestamp,
			"1,2020-01-01 00:00:00;2,2024-01-01 00:00:00;3,2024-01-02 00:00:00"},
		// A BOUND PARAMETER default (arc PW's seam; a control, measured): a
		// numeric parameter widens as the literal does, and an untyped one
		// bound '7' takes the value's type.
		{"param/numeric", "LAG(x, 1, $1) OVER (ORDER BY id)", [][]byte{[]byte("2.5")}, []uint32{oidNumeric}, oidNumeric, "1,2.5;2,10;3,20"},
		{"param/int8", "LAG(x, 1, $1) OVER (ORDER BY id)", [][]byte{[]byte("7")}, []uint32{oidInt8}, oidInt8, "1,7;2,10;3,20"},
		{"param/untyped", "LAG(x, 1, $1) OVER (ORDER BY id)", [][]byte{[]byte("7")}, []uint32{oidUnknown}, oidInt8, "1,7;2,10;3,20"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := "SELECT id, " + c.expr + " FROM wdw ORDER BY id"
			var formats []int16
			for range c.params {
				formats = append(formats, 0)
			}
			res := conn.ExecParams(ctx, sql, c.params, c.types, formats, nil).Read()
			if res.Err != nil {
				t.Fatalf("raised %v, want %q (PostgreSQL 17.11)", res.Err, c.want)
			}
			if len(res.FieldDescriptions) != 2 {
				t.Fatalf("%d columns", len(res.FieldDescriptions))
			}
			if got := res.FieldDescriptions[1].DataTypeOID; got != c.oid {
				t.Errorf("declared OID %d, want %d (PostgreSQL 17.11)", got, c.oid)
			}
			var rows []string
			for _, r := range res.Rows {
				v := "NULL"
				if r[1] != nil {
					v = norm(c.oid, string(r[1]))
				}
				rows = append(rows, string(r[0])+","+v)
			}
			if got := strings.Join(rows, ";"); got != c.want {
				t.Errorf("answered %q, want %q (PostgreSQL 17.11)", got, c.want)
			}
		})
	}
	// The untyped parameter bound '2.5' is coerced to the value's bigint, as
	// PostgreSQL does: 22P02.
	res := conn.ExecParams(ctx, "SELECT id, LAG(x, 1, $1) OVER (ORDER BY id) FROM wdw ORDER BY id",
		[][]byte{[]byte("2.5")}, []uint32{oidUnknown}, []int16{0}, nil).Read()
	if res.Err == nil || !strings.Contains(res.Err.Error(), "22P02") {
		t.Errorf("param/untyped/2.5: %v, want 22P02 invalid input syntax for type bigint (PostgreSQL 17.11)", res.Err)
	}
}
