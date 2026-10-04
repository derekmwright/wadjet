// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The wire arm of arc FO (#1488, #1489): the float total order's two
// published values — max over a column holding NaN, and the -0 group key —
// in the TEXT and the BINARY result formats, against PostgreSQL 17.11's own
// bytes (TestArcFOWireMeasure, FO_PG_DSN).
var foWireWant = []string{
	"f8 minMax text [-Infinity|NaN]",
	"f8 minMax binary [fff0000000000000|7ff8000000000000]",
	"f8 groupBy text [-Infinity|1 -0|1 1.5|1 Infinity|1 NaN|2 NULL|1]",
	"f8 groupBy binary [fff0000000000000|0000000000000001 8000000000000000|0000000000000001 3ff8000000000000|0000000000000001 7ff0000000000000|0000000000000001 7ff8000000000000|0000000000000002 NULL|0000000000000001]",
	"f4 minMax text [-Infinity|NaN]",
	"f4 minMax binary [ff800000|7fc00000]",
	"f4 groupBy text [-Infinity|1 -0|1 1.5|1 Infinity|1 NaN|2 NULL|1]",
	"f4 groupBy binary [ff800000|0000000000000001 80000000|0000000000000001 3fc00000|0000000000000001 7f800000|0000000000000001 7fc00000|0000000000000002 NULL|0000000000000001]",
}

func foWireRun(t *testing.T, conn *pgconn.PgConn) []string {
	t.Helper()
	ctx := context.Background()
	must := func(sql string, formats []int16) *pgconn.Result {
		r := conn.ExecParams(ctx, sql, nil, nil, nil, formats).Read()
		if r.Err != nil {
			t.Fatalf("%s\n  -> %v", sql, r.Err)
		}
		return r
	}
	render := func(r *pgconn.Result, binary bool) string {
		rows := make([]string, 0, len(r.Rows))
		for _, row := range r.Rows {
			f := make([]string, len(row))
			for i, v := range row {
				switch {
				case v == nil:
					f[i] = "NULL"
				case binary:
					f[i] = hex.EncodeToString(v)
				default:
					f[i] = string(v)
				}
			}
			rows = append(rows, strings.Join(f, "|"))
		}
		return "[" + strings.Join(rows, " ") + "]"
	}
	var out []string
	for _, ty := range []struct{ key, spell string }{{"f8", "DOUBLE PRECISION"}, {"f4", "REAL"}} {
		tbl := "fow_" + ty.key
		conn.ExecParams(ctx, "DROP TABLE IF EXISTS "+tbl, nil, nil, nil, nil).Read()
		must("CREATE TABLE "+tbl+" (id BIGINT, c "+ty.spell+")", nil)
		must("INSERT INTO "+tbl+" VALUES (1, CAST('Infinity' AS "+ty.spell+")), (2, 1.5), (3, CAST('-0' AS "+ty.spell+")), "+
			"(4, CAST('NaN' AS "+ty.spell+")), (5, CAST('-Infinity' AS "+ty.spell+")), (6, CAST('NaN' AS "+ty.spell+")), (7, NULL)", nil)
		for _, q := range []struct{ name, sql string }{
			{"minMax", "SELECT min(c) AS lo, max(c) AS hi FROM " + tbl},
			{"groupBy", "SELECT c, count(*) AS n FROM " + tbl + " GROUP BY c ORDER BY c"},
		} {
			out = append(out, fmt.Sprintf("%s %s text %s", ty.key, q.name, render(must(q.sql, nil), false)))
			out = append(out, fmt.Sprintf("%s %s binary %s", ty.key, q.name, render(must(q.sql, []int16{1}), true)))
		}
		conn.ExecParams(ctx, "DROP TABLE IF EXISTS "+tbl, nil, nil, nil, nil).Read()
	}
	return out
}

func TestArcFOFloatTotalOrderOnTheWire(t *testing.T) {
	db, err := wadjet.Open(context.Background(), wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv := startTestServer(t, db)
	got := foWireRun(t, sec5Pgconn(t, srv.Addr()))
	want := make([]string, len(foWireWant))
	pinned := 0
	for i, w := range foWireWant {
		want[i] = w
		if pin := foWirePinned(w); pin != "" {
			want[i] = pin
			pinned++
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the wire answers differ from PostgreSQL 17.11's (with the pinned NaN payload)\n got:\n  %s\n want:\n  %s",
			strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	if pinned != 2 {
		t.Fatalf("%d pinned lines, want 2", pinned)
	}
}

// foWirePinned is the pinned answer of a PostgreSQL line this server writes
// differently for a reason outside the order seam, identically at base: a
// float8 NaN's BINARY form carries Go's math.NaN() payload,
// 7ff8000000000001, where PostgreSQL's float8send writes its NAN,
// 7ff8000000000000. Both are a quiet NaN and every client decodes NaN; the
// text form agrees. The pin FAILS when the bytes start agreeing; delete it
// then. "" = no pin.
func foWirePinned(line string) string {
	if strings.HasPrefix(line, "f8 ") && strings.Contains(line, "binary") && strings.Contains(line, "7ff8000000000000") {
		return strings.ReplaceAll(line, "7ff8000000000000", "7ff8000000000001")
	}
	return ""
}

// TestArcFOWireMeasure prints PostgreSQL's lines for foWireWant (FO_PG_DSN).
func TestArcFOWireMeasure(t *testing.T) {
	dsn := os.Getenv("FO_PG_DSN")
	if dsn == "" {
		t.Skip("FO_PG_DSN unset")
	}
	conn, err := pgconn.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	conn.ExecParams(context.Background(), "SET statement_timeout = '30s'", nil, nil, nil, nil).Read()
	for _, line := range foWireRun(t, conn) {
		t.Logf("%q,", line)
	}
}
