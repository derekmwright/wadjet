// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A PARAMETER rounds by its declared type (#381): a float8 or float4 $1
// half to even, a numeric one half away from zero, at ROUND, the integer
// cast and the cast of an array of it to BIGINT[] — in the text and the
// binary format alike, read back as text over the wire. Every
// want is PostgreSQL 17.11's through `PREPARE a(float8|float4|numeric) AS
// SELECT round($1), CAST($1 AS INTEGER), CAST(ARRAY[$1] AS BIGINT[])`
// (re_author notes): 2.5 → 2|2|{2} and -0.5 → -0|0|{0} for float8 and
// float4, 3|3|{3} and -1|-1|{-1} for numeric.
func TestArcREFloatParameterRoundsByItsType(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "reparam"})
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
	const (
		oidFloat4  = 700
		oidFloat8  = 701
		oidNumeric = 1700
	)
	const sel = "SELECT round($1) AS r, CAST($1 AS INTEGER) AS i, CAST(ARRAY[$1] AS BIGINT[]) AS a"
	for _, c := range []struct {
		oid    uint32
		format int16
		v      string
		want   string
	}{
		{oidFloat8, 0, "2.5", "2|2|{2}"}, {oidFloat8, 1, "2.5", "2|2|{2}"},
		{oidFloat8, 0, "-0.5", "-0|0|{0}"}, {oidFloat8, 1, "-0.5", "-0|0|{0}"},
		{oidFloat8, 0, "3.5", "4|4|{4}"},
		{oidFloat4, 0, "2.5", "2|2|{2}"}, {oidFloat4, 1, "2.5", "2|2|{2}"},
		{oidFloat4, 0, "-0.5", "-0|0|{0}"}, {oidFloat4, 1, "-0.5", "-0|0|{0}"},
		{oidNumeric, 0, "2.5", "3|3|{3}"}, {oidNumeric, 0, "-0.5", "-1|-1|{-1}"},
	} {
		var val []byte
		switch {
		case c.format == 0:
			val = []byte(c.v)
		case c.oid == oidFloat8:
			f, _ := strconv.ParseFloat(c.v, 64)
			val = binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
		default:
			f, _ := strconv.ParseFloat(c.v, 32)
			val = binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(f)))
		}
		name := strconv.Itoa(int(c.oid)) + "/fmt" + strconv.Itoa(int(c.format)) + "/" + c.v
		t.Run(name, func(t *testing.T) {
			read := func(sql string, params [][]byte, oids []uint32, formats []int16) string {
				t.Helper()
				res := conn.ExecParams(ctx, sql, params, oids, formats, nil).Read()
				if res.Err != nil {
					t.Fatalf("%s: %v", sql, res.Err)
				}
				if len(res.Rows) != 1 {
					t.Fatalf("%s: %d rows", sql, len(res.Rows))
				}
				parts := make([]string, len(res.Rows[0]))
				for i, f := range res.Rows[0] {
					parts[i] = string(f)
				}
				return strings.Join(parts, "|")
			}
			if got := read(sel, [][]byte{val}, []uint32{c.oid}, []int16{c.format}); got != c.want {
				t.Errorf("select: got %s, want %s (PostgreSQL 17.11)", got, c.want)
			}
		})
	}
}
