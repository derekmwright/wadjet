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

// A BOUND PARAMETER AS THE PROBE OF AN UNCORRELATED SUBQUERY ABOVE A JOIN
// answers as PostgreSQL 17.11 does (#1382 #1418). The body scans the table
// the outer query joins with itself, reading a column no outer scan reads;
// at v0.25.2 the body's scan replayed the self-join's duplicate-scan cache
// (the outer's columns only), so a matching `$1 IN (…)` answered no rows,
// `$1 NOT IN (…)` answered every row, and the EXISTS spelling failed with
// `filter column "q.v" does not exist`. Every want is PostgreSQL 17.11 over
// cp_t through `PREPARE x(int8) AS …; EXECUTE x(12 | 99)` (and an untyped
// parameter, `PREPARE x AS …`, bound '12' / '99').
func TestAParameterProbeOverAJoinAnswers(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "cpparam"})
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
	for _, s := range []string{"CREATE TABLE cp_t (id BIGINT, g BIGINT, v BIGINT)",
		"INSERT INTO cp_t VALUES (1, 1, 12), (2, 1, 13), (3, 2, 14), (4, 2, 15)"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	const (
		oidUnknown = 0
		oidInt8    = 20
		join       = "SELECT a.id, b.id FROM cp_t a JOIN cp_t b ON a.id = b.id WHERE "
		all        = "1,1;2,2;3,3;4,4"
	)
	i8 := func(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }
	for _, c := range []struct {
		name, pred string
		oid        uint32
		format     int16
		val        []byte
		want       string
	}{
		{"in/int8/match", "$1 IN (SELECT q.v FROM cp_t q)", oidInt8, 0, []byte("12"), all},
		{"in/int8/miss", "$1 IN (SELECT q.v FROM cp_t q)", oidInt8, 0, []byte("99"), ""},
		{"in/int8binary/match", "$1 IN (SELECT q.v FROM cp_t q)", oidInt8, 1, i8(12), all},
		{"in/untyped/match", "$1 IN (SELECT q.v FROM cp_t q)", oidUnknown, 0, []byte("12"), all},
		{"in/untyped/miss", "$1 IN (SELECT q.v FROM cp_t q)", oidUnknown, 0, []byte("99"), ""},
		{"notIn/int8/match", "$1 NOT IN (SELECT q.v FROM cp_t q)", oidInt8, 0, []byte("12"), ""},
		{"notIn/int8/miss", "$1 NOT IN (SELECT q.v FROM cp_t q)", oidInt8, 0, []byte("99"), all},
		{"exists/int8/match", "EXISTS (SELECT 1 FROM cp_t q WHERE q.v = $1)", oidInt8, 0, []byte("12"), all},
		{"exists/int8/miss", "EXISTS (SELECT 1 FROM cp_t q WHERE q.v = $1)", oidInt8, 0, []byte("99"), ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, join+c.pred+" ORDER BY 1", [][]byte{c.val}, []uint32{c.oid}, []int16{c.format}, nil).Read()
			if res.Err != nil {
				t.Fatalf("raised %v, want %q (PostgreSQL 17.11)", res.Err, c.want)
			}
			var rows []string
			for _, r := range res.Rows {
				rows = append(rows, string(r[0])+","+string(r[1]))
			}
			if got := strings.Join(rows, ";"); got != c.want {
				t.Fatalf("answered %q, want %q (PostgreSQL 17.11)", got, c.want)
			}
		})
	}
}
