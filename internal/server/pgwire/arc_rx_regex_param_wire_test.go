// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A pattern that arrives as a BOUND PARAMETER is read as an ARE like a
// literal one (#1499): the Bind door (text and unknown parameter OIDs) and
// the embedded door (wadjet.DB.Query with the literal spelling) answer what
// PostgreSQL 17.11 answers through `PREPARE x(text, …) AS …; EXECUTE x(…)`
// (rx_author/pg_param.txt). regexp_extract is measured through its
// PostgreSQL equivalent regexp_substr.
func TestArcRXPatternParameterReadsAsAnARE(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "rxparam"})
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

	const oidText, oidInt4 = 25, 23
	cells := []struct {
		name, sql string
		params    []string
		oids      []uint32 // the typed door's OIDs (the untyped door sends 0)
		want      string   // the value's text, NULL, or E:<SQLSTATE>
	}{
		{"like/backspace", "SELECT regexp_like($1, $2)", []string{"abc", `\b`}, nil, "f"},
		{"like/backspaceHit", "SELECT regexp_like($1, $2)", []string{"a\bc", `a\bc`}, nil, "t"},
		{"like/flags", "SELECT regexp_like($1, $2, $3)", []string{"A", "a", "i"}, nil, "t"},
		{"like/flagG", "SELECT regexp_like($1, $2, $3)", []string{"a", "a", "g"}, nil, "E:22023"},
		{"like/z", "SELECT regexp_like($1, $2)", []string{"ab", `b\z`}, nil, "E:2201B"},
		{"count/backspace", "SELECT regexp_count($1, $2)", []string{"abc", `\b`}, nil, "0"},
		{"count/startFlags", "SELECT regexp_count($1, $2, $3, $4)", []string{"éaéa", "A", "3", "i"},
			[]uint32{oidText, oidText, oidInt4, oidText}, "1"},
		{"extract/wordB", "SELECT regexp_extract($1, $2)", []string{"the cat sat", `\bcat\b`}, nil, "NULL"},
		{"op/backspace", "SELECT $1 ~ $2", []string{"abc", `\b`}, nil, "f"},
		{"op/wordY", "SELECT $1 ~ $2", []string{"the cat sat", `\ycat\y`}, nil, "t"},
		{"substring/longest", "SELECT substring($1 FROM $2)", []string{"abc", "a|ab"}, nil, "ab"},
		{"similar/newline", "SELECT $1 SIMILAR TO $2", []string{"a\nc", "a_c"}, nil, "t"},
	}
	render := func(err error, rows [][][]byte) string {
		if err != nil {
			var pe *pgconn.PgError
			if errors.As(err, &pe) {
				return "E:" + pe.Code
			}
			return "E:" + sqlerr.StateOf(err)
		}
		if len(rows) != 1 || len(rows[0]) != 1 {
			return fmt.Sprintf("rows=%d", len(rows))
		}
		if rows[0][0] == nil {
			return "NULL"
		}
		return string(rows[0][0])
	}
	for _, c := range cells {
		vals := make([][]byte, len(c.params))
		for i, p := range c.params {
			vals[i] = []byte(p)
		}
		typed := c.oids
		if typed == nil {
			typed = make([]uint32, len(c.params))
			for i := range typed {
				typed[i] = oidText
			}
		}
		doors := map[string][]uint32{"untyped": make([]uint32, len(c.params)), "text": typed}
		for door, oids := range doors {
			if door == "untyped" && c.oids != nil {
				continue // an untyped integer start is not this cell's question
			}
			res := conn.ExecParams(ctx, c.sql, vals, oids, nil, nil).Read()
			if got := render(res.Err, res.Rows); got != c.want {
				t.Errorf("%s via Bind (%s OIDs): %s with %q = %s, PostgreSQL 17.11 %s", c.name, door, c.sql, c.params, got, c.want)
			}
		}
		// The embedded door: the same statement with the parameters spelled
		// as literals.
		sql := c.sql
		for i := len(c.params); i >= 1; i-- {
			sql = strings.ReplaceAll(sql, fmt.Sprintf("$%d", i), rxLiteral(c.params[i-1]))
		}
		got := func() string {
			r, err := db.Query(ctx, sql)
			if err != nil {
				return "E:" + sqlerr.StateOf(err)
			}
			if len(r.Rows) != 1 {
				return fmt.Sprintf("rows=%d", len(r.Rows))
			}
			v := r.Cells(0)[0]
			switch x := v.(type) {
			case nil:
				return "NULL"
			case bool:
				if x {
					return "t"
				}
				return "f"
			}
			return fmt.Sprint(v)
		}()
		if got != c.want {
			t.Errorf("%s embedded: %s = %s, PostgreSQL 17.11 %s", c.name, sql, got, c.want)
		}
	}
}

// rxLiteral spells s as a SQL string literal: E” when it holds a control
// character, a standard-conforming literal otherwise.
func rxLiteral(s string) string {
	if strings.ContainsAny(s, "\n\b") {
		return "E'" + strings.NewReplacer(`\`, `\\`, "'", `\'`, "\n", `\n`, "\b", `\b`).Replace(s) + "'"
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
