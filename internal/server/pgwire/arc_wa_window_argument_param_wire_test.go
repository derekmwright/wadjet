// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A BOUND PARAMETER AS A WINDOW FUNCTION'S ARGUMENT answers as PostgreSQL
// 17.11 does (#1394 #1399). A parameter reaches the engine as the literal it
// renders to, so a parameter VALUE argument is #1394's literal (answered NULL
// at v0.25.2) and a parameter OFFSET is #1399's (0 answered the previous row).
// Describe runs the statement with NULL standing in for each parameter, so the
// NULL offset is also what the extended protocol's own Describe reads. Every
// want is PostgreSQL 17.11 through `PREPARE p(int4 | numeric) AS …; EXECUTE`
// (and an untyped parameter, `PREPARE p AS …`, bound '0').
func TestAWindowArgumentParameterAnswers(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "waparam"})
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
	for _, s := range []string{"CREATE TABLE wap (id BIGINT, x BIGINT)",
		"INSERT INTO wap VALUES (1, 10), (2, 20), (3, 30)"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	const (
		oidUnknown = 0
		oidInt4    = 23
		oidNumeric = 1700
	)
	i4 := func(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }
	for _, c := range []struct {
		name, expr string
		oid        uint32
		format     int16
		val        []byte
		want       string
	}{
		{"offset/int4/0", "LAG(x, $1) OVER (ORDER BY id)", oidInt4, 0, []byte("0"), "1,10;2,20;3,30"},
		{"offset/int4binary/0", "LEAD(x, $1) OVER (ORDER BY id)", oidInt4, 1, i4(0), "1,10;2,20;3,30"},
		{"offset/untyped/0", "LAG(x, $1) OVER (ORDER BY id)", oidUnknown, 0, []byte("0"), "1,10;2,20;3,30"},
		{"offset/int4/neg1", "LAG(x, $1) OVER (ORDER BY id)", oidInt4, 0, []byte("-1"), "1,20;2,30;3,NULL"},
		{"offset/int4/null", "LAG(x, $1) OVER (ORDER BY id)", oidInt4, 0, nil, "1,NULL;2,NULL;3,NULL"},
		{"offset/int4/2", "LAG(x, $1) OVER (ORDER BY id)", oidInt4, 0, []byte("2"), "1,NULL;2,NULL;3,10"},
		// The int4 minimum is one signed int4 (round 2, P3).
		{"offset/int4/int4min", "LAG(x, $1) OVER (ORDER BY id)", oidInt4, 0, []byte("-2147483648"), "1,NULL;2,NULL;3,NULL"},
		{"offset/int4binary/int4min", "LEAD(x, $1) OVER (ORDER BY id)", oidInt4, 1, i4(-2147483648), "1,NULL;2,NULL;3,NULL"},
		{"value/numeric/sum", "SUM($1) OVER ()", oidNumeric, 0, []byte("2.5"), "1,7.5;2,7.5;3,7.5"},
		{"value/int4/lag", "LAG($1) OVER (ORDER BY id)", oidInt4, 0, []byte("5"), "1,NULL;2,5;3,5"},
		{"value/int4/first_value", "FIRST_VALUE($1) OVER (ORDER BY id)", oidInt4, 0, []byte("5"), "1,5;2,5;3,5"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, "SELECT id, "+c.expr+" FROM wap ORDER BY id",
				[][]byte{c.val}, []uint32{c.oid}, []int16{c.format}, nil).Read()
			if res.Err != nil {
				t.Fatalf("raised %v, want %q (PostgreSQL 17.11)", res.Err, c.want)
			}
			var rows []string
			for _, r := range res.Rows {
				v := "NULL"
				if r[1] != nil {
					v = string(r[1])
				}
				rows = append(rows, string(r[0])+","+v)
			}
			if got := strings.Join(rows, ";"); got != c.want {
				t.Fatalf("answered %q, want %q (PostgreSQL 17.11)", got, c.want)
			}
		})
	}
}
