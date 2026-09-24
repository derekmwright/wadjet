// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/oracle/cwfixture"
)

// Arc CW round 2, B2: two output columns of ONE name are two columns, on the
// pgwire door over the single-process engine and over the coordinator (the
// DAG), in text and in binary.
//
// The door resolved a column's declaration by NAME before position — the
// renderer's nested declaration (nestedColumnFor) and, on the routed (DAG)
// path, the RowDescription's metas (routedColumnMetas) — so the LAST column of
// a name lent its declaration to every column of that name: `max(ats),
// max(ad)`, the default naming of an ordinary query, rendered the timestamp[]
// under the date[] element as epoch milliseconds, a binary client decoded a
// wrong timestamp with no error, and the DAG declared 1182 for both. A name is
// not an identity (ADR-0026); the executed schema is positional.
//
// The expected OIDs and text are PostgreSQL 17.11's over cwfixture.PGDDL. The
// binary face is checked the way a typed client reads it: pgx decodes every
// value by its declared OID from the text bytes and from the binary bytes, and
// the two must be the same value.
//
// At the round-1 tip 8b50409c: every timestamp column that shares a name with
// a later column rendered epoch milliseconds on pgwire/single, and the DAG
// declared the last column's OID for all of them. At main 6cbe2041 the
// container cells were text (OID 25, Go rendering) and the scalar DAG cell
// already declared 1082,1082.
type cw2PosCell struct {
	name, sql, oids, text string
}

var cw2PosCells = []cw2PosCell{
	{"default-names-max", `SELECT max(ats), max(ad) FROM cw`, "1115,1182", `{"2024-06-15 12:30:45.5",NULL}|{2024-01-02}`},
	{"alias-twice", `SELECT ats AS a, ad AS a FROM cw WHERE id = 1`, "1115,1182", `{"2024-06-15 12:30:45.5",NULL}|{2024-01-02}`},
	{"alias-twice-reversed", `SELECT ad AS a, ats AS a FROM cw WHERE id = 3`, "1182,1115", `{1970-01-01,NULL}|{"1999-12-31 23:59:59"}`},
	{"constructor-beside-stored", `SELECT ARRAY[id] AS a, at AS a FROM cw WHERE id = 3`, "1016,1009", `{3}|{z}`},
	{"alias-three-times", `SELECT ai AS a, ats AS a, ad AS a FROM cw WHERE id = 1`, "1007,1115,1182",
		`{1,2,3}|{"2024-06-15 12:30:45.5",NULL}|{2024-01-02}`},
	{"scalar-elements", `SELECT ats[1] AS a, ad[1] AS a FROM cw WHERE id = 1`, "1114,1082", `2024-06-15 12:30:45.5|2024-01-02`},
	{"window-twice", `SELECT max(ats) OVER () AS w, max(ad) OVER () AS w FROM cw WHERE id = 1`, "1115,1182",
		`{"2024-06-15 12:30:45.5",NULL}|{2024-01-02}`},
	{"default-names-min-three", `SELECT min(ad), min(ats), max(ai) FROM cw`, "1182,1115,1007", `{1970-01-01,NULL}|{}|{NULL,5}`},
}

func TestArcCW2SameNamedColumnsAreReadByPositionOnPgwire(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUpWith(t, ctx, pmProvider(t),
		pmExtraTable{cwfixture.Table, cwfixture.Schema(), cwfixture.Rows()})

	connect := func(key, addr string) (*pgx.Conn, error) {
		return pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:%s@%s/wadjet?sslmode=disable", key, addr))
	}
	// simple is psql's protocol: the RowDescription's OIDs and the text rows.
	simple := func(conn *pgx.Conn, sql string) (string, string, error) {
		mrr := conn.PgConn().Exec(ctx, sql)
		var oids, rows []string
		for mrr.NextResult() {
			rr := mrr.ResultReader()
			for _, fd := range rr.FieldDescriptions() {
				oids = append(oids, fmt.Sprint(fd.DataTypeOID))
			}
			for rr.NextRow() {
				var vals []string
				for _, v := range rr.Values() {
					if v == nil {
						vals = append(vals, "NULL")
						continue
					}
					vals = append(vals, string(v))
				}
				rows = append(rows, strings.Join(vals, "|"))
			}
			if _, err := rr.Close(); err != nil {
				return "", "", err
			}
		}
		if err := mrr.Close(); err != nil {
			return "", "", err
		}
		return strings.Join(oids, ","), strings.Join(rows, " || "), nil
	}
	// decoded is a typed client's read: every column in format (0 text, 1
	// binary), each value decoded by its declared OID with pgx's codecs.
	decoded := func(conn *pgx.Conn, sql string, format int16) ([][]any, error) {
		rr := conn.PgConn().ExecParams(ctx, sql, nil, nil, nil, []int16{format})
		fds := rr.FieldDescriptions()
		m := pgtype.NewMap()
		var out [][]any
		var scanErr error
		for rr.NextRow() {
			var row []any
			for i, raw := range rr.Values() {
				var v any
				if raw != nil && scanErr == nil {
					if err := m.Scan(fds[i].DataTypeOID, format, raw, &v); err != nil {
						scanErr = fmt.Errorf("column %d (OID %d, format %d): %w", i, fds[i].DataTypeOID, format, err)
					}
				}
				row = append(row, v)
			}
			out = append(out, row)
		}
		// Drain and close even after a decode failure, so the connection is
		// free for the next cell.
		if _, err := rr.Close(); err != nil {
			return nil, err
		}
		if scanErr != nil {
			return nil, scanErr
		}
		return out, nil
	}

	doors := []struct{ name, addr string }{{"pgwire/single", rig.pgSingle}, {"pgwire/dag", rig.pgDAG}}
	answered := 0
	for _, d := range doors {
		conn, err := connect("admin-key", d.addr)
		if err != nil {
			t.Fatalf("%s: %v", d.name, err)
		}
		for _, c := range cw2PosCells {
			oids, text, err := simple(conn, c.sql)
			if err != nil {
				t.Errorf("%s / %s: %s refused: %v", c.name, d.name, c.sql, err)
				continue
			}
			if oids != c.oids || text != c.text {
				t.Errorf("%s / %s: %s\n  got  OIDs %s  %s\n  want OIDs %s  %s", c.name, d.name, c.sql, oids, text, c.oids, c.text)
			}
			tv, terr := decoded(conn, c.sql, 0)
			bv, berr := decoded(conn, c.sql, 1)
			if terr != nil || berr != nil {
				t.Errorf("%s / %s: %s\n  decode: text %v, binary %v", c.name, d.name, c.sql, terr, berr)
				continue
			}
			if !reflect.DeepEqual(tv, bv) {
				t.Errorf("%s / %s: %s\n  binary decodes to %#v\n  text   decodes to %#v", c.name, d.name, c.sql, bv, tv)
				continue
			}
			answered++
		}
		conn.Close(ctx)

		// The policed pair: a masked column beside a container of it, one
		// name for both. A binary client read every row at main; at the
		// round-1 tip pgwire/single sent the array under the text column's
		// declaration and pgx failed with `array header too short`. The mask
		// holds either way (no true value on any row).
		aconn, err := connect("analyst-key", d.addr)
		if err != nil {
			t.Fatalf("%s analyst: %v", d.name, err)
		}
		const masked = `SELECT ARRAY[ssn] AS a, ssn AS a FROM e7emp ORDER BY id`
		bv, err := decoded(aconn, masked, 1)
		aconn.Close(ctx)
		if err != nil {
			t.Errorf("masked-pair / %s: %s\n  binary read refused: %v", d.name, masked, err)
			continue
		}
		if len(bv) == 0 {
			t.Errorf("masked-pair / %s: no rows", d.name)
			continue
		}
		for _, r := range bv {
			all := fmt.Sprint(r)
			for _, bad := range pmTrueValues() {
				if strings.Contains(all, bad) {
					t.Errorf("masked-pair / %s: a true value reached the client: %v", d.name, r)
				}
			}
			if fmt.Sprint(r[0]) != "[***]" || fmt.Sprint(r[1]) != "***" {
				t.Errorf("masked-pair / %s: row %v, want [[***] ***]", d.name, r)
			}
		}
		answered++
	}
	if want := len(doors) * (len(cw2PosCells) + 1); answered != want {
		t.Errorf("%d of %d (cell, door) pairs answered", answered, want)
	}
}
