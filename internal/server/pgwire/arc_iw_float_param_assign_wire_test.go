// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A float8 / float4 PARAMETER assigned to a BIGINT column is range-checked as
// PostgreSQL does (#1484). Bind renders it CAST('<text>' AS DOUBLE PRECISION
// | REAL) (floatParamLiteral), which reaches the assignment cast; at 93e4804e
// that cast's bound let the double 2^63 through and stored
// -9223372036854775808. Every want is PostgreSQL 17.11's through
// `PREPARE x(float8|float4) AS INSERT INTO pw VALUES ($1)` / `UPDATE pw SET
// a = $1; EXECUTE x(…)` (iw_author/pg_pgwire.txt): 22003 for 2^63 and NaN,
// -2^63 stored exactly, 2.5 stored 2. A refused statement leaves the table as
// it found it: no row inserted, the row's 7 unchanged.
func TestArcIWFloatParameterAssignedToBigint(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "iwparam"})
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
	exec := func(s string) {
		t.Helper()
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	const (
		oidFloat4 = 700
		oidFloat8 = 701
	)
	n := 0
	for _, oid := range []uint32{oidFloat8, oidFloat4} {
		for _, format := range []int16{0, 1} {
			for _, c := range []struct{ v, want string }{
				{"9223372036854775808", "22003"},
				{"NaN", "22003"},
				{"-9223372036854775808", "-9223372036854775808"},
				{"2.5", "2"},
			} {
				var val []byte
				switch {
				case format == 0:
					val = []byte(c.v)
				case oid == oidFloat8:
					f, _ := strconv.ParseFloat(c.v, 64)
					val = binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
				default:
					f, _ := strconv.ParseFloat(c.v, 32)
					val = binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(f)))
				}
				for _, door := range []string{"insert", "update"} {
					n++
					tb := "pw" + strconv.Itoa(n)
					name := door + "/" + strconv.Itoa(int(oid)) + "/fmt" + strconv.Itoa(int(format)) + "/" + c.v
					t.Run(name, func(t *testing.T) {
						exec("CREATE TABLE " + tb + " (a BIGINT)")
						sql := "INSERT INTO " + tb + " VALUES ($1)"
						after := "(0 rows)"
						if door == "update" {
							exec("INSERT INTO " + tb + " VALUES (7)")
							sql = "UPDATE " + tb + " SET a = $1"
							after = "7"
						}
						res := conn.ExecParams(ctx, sql, [][]byte{val}, []uint32{oid}, []int16{format}, nil).Read()
						got := "ok"
						if res.Err != nil {
							got = "(uncoded) " + res.Err.Error()
							var pe *pgconn.PgError
							if errors.As(res.Err, &pe) {
								got = pe.Code
							}
						}
						rr := conn.ExecParams(ctx, "SELECT a FROM "+tb, nil, nil, nil, nil).Read()
						if rr.Err != nil {
							t.Fatalf("read back: %v", rr.Err)
						}
						var rows []string
						for _, r := range rr.Rows {
							rows = append(rows, string(r[0]))
						}
						stored := strings.Join(rows, ";")
						if stored == "" {
							stored = "(0 rows)"
						}
						if c.want == "22003" {
							if got != "22003" || stored != after {
								t.Fatalf("raised %s and the table holds %s; want 22003 and %s", got, stored, after)
							}
							return
						}
						if got != "ok" || stored != c.want {
							t.Fatalf("raised %s and stored %s; want %s", got, stored, c.want)
						}
					})
				}
			}
		}
	}
}
