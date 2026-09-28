// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A non-finite (or a whole, very large) double on the LEFT of `+` answers
// its sum as PostgreSQL 17.11 does, typed by the SQL or bound as a binary
// float8/float4 parameter. The date arithmetic's reversed shape (`n + date`)
// read the left operand as a whole day count BEFORE it had seen a date on
// the right, and a day count no DATE can be shifted by is 22008 — so
// `CAST('Infinity' AS DOUBLE PRECISION) + 1` raised `22008 date out of range`
// where `- 1`, `* 2` and `1 + …` answered; a binary Infinity parameter binds
// as that cast, so `SELECT $1 + 1` and `INSERT … VALUES ($1 + 1)` raised it
// too (v0.25.1 answered those, binding the value as text).
//
// Every want is PostgreSQL 17.11 (psql and PREPARE x(float8|float4) through
// wadjet-pg-ir6).
func TestANonFiniteDoubleAddsAsANumber(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "infadd"})
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
	if _, err := conn.Exec(ctx, "CREATE TABLE d (v DOUBLE)").ReadAll(); err != nil {
		t.Fatal(err)
	}
	state := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		return "(uncoded)"
	}

	for _, c := range []struct{ name, sql, want string }{
		{"f8/inf+1", "SELECT CAST('Infinity' AS DOUBLE PRECISION) + 1", "Infinity"},
		{"f8/inf+1.5", "SELECT CAST('Infinity' AS DOUBLE PRECISION) + 1.5", "Infinity"},
		{"f8/-inf+1", "SELECT CAST('-Infinity' AS DOUBLE PRECISION) + 1", "-Infinity"},
		{"f8/inf-spelling+1", "SELECT CAST('inf' AS DOUBLE PRECISION) + 1", "Infinity"},
		{"f4/inf+1", "SELECT CAST('Infinity' AS REAL) + 1", "Infinity"},
		{"f4/-inf+1", "SELECT CAST('-Infinity' AS REAL) + 1", "-Infinity"},
		{"f8/1e300+1", "SELECT CAST('1e300' AS DOUBLE PRECISION) + 1", "1e+300"},
		// answered before the repair; kept as the neighbours of the shape
		{"f8/inf-1", "SELECT CAST('Infinity' AS DOUBLE PRECISION) - 1", "Infinity"},
		{"f8/inf*2", "SELECT CAST('Infinity' AS DOUBLE PRECISION) * 2", "Infinity"},
		{"f8/nan+1", "SELECT CAST('NaN' AS DOUBLE PRECISION) + 1", "NaN"},
		{"f8/1+inf", "SELECT 1 + CAST('Infinity' AS DOUBLE PRECISION)", "Infinity"},
		// the date arithmetic the reversed shape exists for
		{"date/n+date", "SELECT CAST(2 + DATE '2020-01-01' AS TEXT)", "2020-01-03"},
	} {
		t.Run("sql/"+c.name, func(t *testing.T) {
			results, err := conn.Exec(ctx, c.sql).ReadAll()
			if err != nil {
				t.Fatalf("raised %s (%v), want %s", state(err), err, c.want)
			}
			if got := string(results[0].Rows[0][0]); got != c.want {
				t.Fatalf("answered %q, want %s", got, c.want)
			}
		})
	}

	const oidFloat4, oidFloat8 = 700, 701
	f8 := func(v float64) []byte { return binary.BigEndian.AppendUint64(nil, math.Float64bits(v)) }
	f4 := func(v float32) []byte { return binary.BigEndian.AppendUint32(nil, math.Float32bits(v)) }
	for _, c := range []struct {
		name, sql string
		oid       uint32
		val       []byte
		want      string
	}{
		{"f8/inf+1", "SELECT $1 + 1", oidFloat8, f8(math.Inf(1)), "Infinity"},
		{"f8/-inf+1", "SELECT $1 + 1", oidFloat8, f8(math.Inf(-1)), "-Infinity"},
		{"f4/inf+1", "SELECT $1 + 1", oidFloat4, f4(float32(math.Inf(1))), "Infinity"},
		{"f8/inf+1.5", "SELECT $1 + 1.5", oidFloat8, f8(math.Inf(1)), "Infinity"},
		{"f8/-inf+1.5", "SELECT $1 + 1.5", oidFloat8, f8(math.Inf(-1)), "-Infinity"},
		{"f4/inf+1.5", "SELECT $1 + 1.5", oidFloat4, f4(float32(math.Inf(1))), "Infinity"},
		{"f8/nan+1", "SELECT $1 + 1", oidFloat8, f8(math.NaN()), "NaN"},
	} {
		t.Run("binary/"+c.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, [][]byte{c.val}, []uint32{c.oid}, []int16{1}, nil).Read()
			if res.Err != nil {
				t.Fatalf("raised %s (%v), want %s", state(res.Err), res.Err, c.want)
			}
			if got := string(res.Rows[0][0]); got != c.want {
				t.Fatalf("answered %q, want %s", got, c.want)
			}
		})
	}

	t.Run("binary/insert-values", func(t *testing.T) {
		res := conn.ExecParams(ctx, "INSERT INTO d VALUES ($1 + 1)", [][]byte{f8(math.Inf(1))}, []uint32{oidFloat8}, []int16{1}, nil).Read()
		if res.Err != nil {
			t.Fatalf("raised %s (%v), want the row written", state(res.Err), res.Err)
		}
		results, err := conn.Exec(ctx, "SELECT v FROM d").ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for _, r := range results[0].Rows {
			rows = append(rows, string(r[0]))
		}
		if got := strings.Join(rows, ";"); got != "Infinity" {
			t.Fatalf("read back %q, want Infinity", got)
		}
	})
}
