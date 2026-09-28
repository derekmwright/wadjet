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

// A float4/float8 parameter in a LIMIT, OFFSET or FETCH FIRST count or a
// TABLESAMPLE percentage answers as PostgreSQL 17.11 does. Bind renders a
// float parameter as CAST('<text>' AS DOUBLE PRECISION | REAL) so that it
// keeps its type; those four positions read a number token, and without the
// parser reading that cast as its number they were 42601 `syntax error at or
// near "CAST"` — the spelling a client that binds a count as a double sends
// (JDBC setDouble, a Python float, a JavaScript number typed float8).
//
// Every want is PostgreSQL 17.11 over `p(id) = (1), (2)` through
// `PREPARE x(float8|float4|int8|numeric) AS …; EXECUTE x(…)`. The integer and
// numeric rows are the controls (they bind as a bare number and never
// changed). The two refusal rows are pinned where they differ from
// PostgreSQL: a count of -1 must stay a refusal (the plan reads a limit of
// -1 as "no limit", so reading the cast's text as a negative count would
// answer the whole table where PostgreSQL raises 2201W), and a fractional
// count is refused as the bare `LIMIT 1.5` is (PostgreSQL rounds it to 2 and
// answers both rows). A pin that moves fails: re-measure it.
func TestAFloatParameterCountsLikeItsNumber(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "cntparam"})
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
	for _, s := range []string{"CREATE TABLE p (id INTEGER)", "INSERT INTO p VALUES (1), (2)"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	const (
		oidInt8    = 20
		oidFloat4  = 700
		oidFloat8  = 701
		oidNumeric = 1700
		limitQ     = "SELECT id FROM p ORDER BY id LIMIT $1"
		offsetQ    = "SELECT id FROM p ORDER BY id OFFSET $1"
		fetchQ     = "SELECT id FROM p ORDER BY id FETCH FIRST $1 ROWS ONLY"
		sampleQ    = "SELECT COUNT(*) FROM p TABLESAMPLE BERNOULLI ($1)"
	)
	f8 := func(v float64) []byte { return binary.BigEndian.AppendUint64(nil, math.Float64bits(v)) }
	f4 := func(v float32) []byte { return binary.BigEndian.AppendUint32(nil, math.Float32bits(v)) }
	for _, c := range []struct {
		name, sql string
		oid       uint32
		format    int16
		val       []byte
		want      string // rows joined by ";", or a SQLSTATE the cell must raise
	}{
		{"limit/float8", limitQ, oidFloat8, 0, []byte("1"), "1"},
		{"limit/float4", limitQ, oidFloat4, 0, []byte("1"), "1"},
		{"offset/float8", offsetQ, oidFloat8, 0, []byte("1"), "2"},
		{"offset/float4", offsetQ, oidFloat4, 0, []byte("1"), "2"},
		{"fetch-first/float8", fetchQ, oidFloat8, 0, []byte("1"), "1"},
		{"fetch-first/float4", fetchQ, oidFloat4, 0, []byte("1"), "1"},
		{"tablesample/float8", sampleQ, oidFloat8, 0, []byte("100"), "2"},
		{"tablesample/float4", sampleQ, oidFloat4, 0, []byte("100"), "2"},
		{"limit/float8/binary", limitQ, oidFloat8, 1, f8(1), "1"},
		{"limit/float4/binary", limitQ, oidFloat4, 1, f4(1), "1"},
		{"offset/float8/binary", offsetQ, oidFloat8, 1, f8(1), "2"},
		{"tablesample/float8/binary", sampleQ, oidFloat8, 1, f8(100), "2"},
		// controls: bound bare before and after the float change
		{"limit/int8", limitQ, oidInt8, 0, []byte("1"), "1"},
		{"limit/numeric", limitQ, oidNumeric, 0, []byte("1"), "1"},
		// pinned refusals (PostgreSQL: 2201W, and `1;2`)
		{"limit/float8/negative", limitQ, oidFloat8, 0, []byte("-1"), "42601"},
		{"limit/float8/fraction", limitQ, oidFloat8, 0, []byte("1.5"), "42000"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, [][]byte{c.val}, []uint32{c.oid}, []int16{c.format}, nil).Read()
			var rows []string
			for _, r := range res.Rows {
				rows = append(rows, string(r[0]))
			}
			got := strings.Join(rows, ";")
			if res.Err != nil {
				state := "(uncoded)"
				var pe *pgconn.PgError
				if errors.As(res.Err, &pe) {
					state = pe.Code
				}
				if state != c.want {
					t.Fatalf("raised %s (%v), want %s", state, res.Err, c.want)
				}
				return
			}
			if got != c.want {
				t.Fatalf("answered %q, want %s", got, c.want)
			}
		})
	}
}
