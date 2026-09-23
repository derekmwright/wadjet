// SPDX-License-Identifier: MIT

package pgwire

import (
	"encoding/binary"
	"strings"
	"testing"
)

// Arc PC's Describe cells (#999, #998): what a PORTAL Describe and a
// STATEMENT Describe promise, measured against PostgreSQL 17.11.
//
// PostgreSQL decides a statement's result shape during parse analysis, before
// a row is read: a portal Describe declares the same columns as the
// statement's, whether the portal will return rows or not (#999), and a
// statement whose ANALYSIS fails raises that failure at Describe — NoData is
// the answer for a statement that returns no rows at all (DDL, DML without
// RETURNING), never for one that cannot be planned (#998).

// pcDescribeRound sends Parse(sql) [, Describe(S)], Bind(params as text)
// [, Describe(P)], Execute, Sync and returns the backend messages, with each
// RowDescription's field names.
func pcDescribeRound(t *testing.T, c *pgClient, sql string, params []string, stmt, portal bool) []pcMsg {
	t.Helper()
	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, sql...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	c.writeMsg('P', parseBuf)
	if stmt {
		c.writeMsg('D', []byte{'S', 0})
	}
	var bindBuf []byte
	bindBuf = append(bindBuf, 0, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, uint16(len(params)))
	for _, p := range params {
		bindBuf = binary.BigEndian.AppendUint32(bindBuf, uint32(len(p)))
		bindBuf = append(bindBuf, p...)
	}
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	c.writeMsg('B', bindBuf)
	if portal {
		c.writeMsg('D', []byte{'P', 0})
	}
	var execBuf []byte
	execBuf = append(execBuf, 0)
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	c.writeMsg('E', execBuf)
	c.writeMsg('S', nil)

	var out []pcMsg
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			t.Fatalf("reading the response to %q: %v", sql, err)
		}
		m := pcMsg{typ: typ}
		switch typ {
		case 'T':
			m.fields = c.parseRowDesc(data)
		case 'E':
			m.text = pcErrorCode(data) + " " + c.parseError(data)
		}
		out = append(out, m)
		if typ == 'Z' {
			return out
		}
	}
}

type pcMsg struct {
	typ    byte
	fields []string
	text   string
}

func pcTrace(msgs []pcMsg) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteByte(m.typ)
		switch {
		case m.typ == 'T':
			b.WriteString("[" + strings.Join(m.fields, ",") + "]")
		case m.typ == 'E':
			b.WriteString("(" + m.text + ")")
		}
		b.WriteByte(' ')
	}
	return b.String()
}

// TestArcPCPortalDescribeDeclaresTheStatementsColumns is #999: a portal that
// will return NO rows describes the same columns as its statement, on
// PostgreSQL 17.11 (measured: `SELECT id, name FROM t WHERE id = $1` bound to
// a value that matches nothing describes id and name at the portal).
func TestArcPCPortalDescribeDeclaresTheStatementsColumns(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	for _, tc := range []struct {
		name, sql string
		params    []string
		want      []string
	}{
		{"bound_no_match", "SELECT id, name FROM users WHERE id = $1", []string{"999"},
			[]string{"id", "name"}},
		{"bound_match", "SELECT id, name FROM users WHERE id = $1", []string{"1"},
			[]string{"id", "name"}},
		{"star_bound_no_match", "SELECT * FROM users WHERE id = $1", []string{"999"},
			[]string{"id", "name", "score"}},
		{"unbound_false", "SELECT id, score FROM users WHERE false", nil,
			[]string{"id", "score"}},
		{"aggregate_group_no_match", "SELECT name, count(*) FROM users WHERE id = $1 GROUP BY name",
			[]string{"999"}, []string{"name", "count"}},
		{"catalog_no_match",
			"SELECT table_name, column_name FROM information_schema.columns WHERE table_name = $1",
			[]string{"nosuch"}, []string{"table_name", "column_name"}},
		{"join_no_match", "SELECT a.id, b.name FROM users a JOIN users b ON a.id = b.id WHERE a.id = $1",
			[]string{"999"}, []string{"id", "name"}},
		{"star_join_no_match", "SELECT * FROM users a JOIN users b ON a.id = b.id WHERE a.id = $1",
			[]string{"999"}, []string{"id", "name", "score", "id", "name", "score"}},
		{"star_join_unbound_false", "SELECT * FROM users a JOIN users b ON a.id = b.id WHERE false",
			nil, []string{"id", "name", "score", "id", "name", "score"}},
		{"qualified_star_join_no_match", "SELECT b.* FROM users a JOIN users b ON a.id = b.id WHERE a.id = $1",
			[]string{"999"}, []string{"id", "name", "score"}},
		{"derived_star_no_match", "SELECT * FROM (SELECT id, name FROM users) d WHERE d.id = $1",
			[]string{"999"}, []string{"id", "name"}},
		{"cte_star_no_match", "WITH c AS (SELECT id FROM users) SELECT * FROM c WHERE id = $1",
			[]string{"999"}, []string{"id"}},
		{"union_no_match", "SELECT id FROM users WHERE id = $1 UNION ALL SELECT id FROM users WHERE id = $1",
			[]string{"999"}, []string{"id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPGClient(t, srv.Addr())
			c.startup("wadjet", "wadjet")
			defer c.terminate()
			msgs := pcDescribeRound(t, c, tc.sql, tc.params, false, true)
			var desc []string
			found := false
			for _, m := range msgs {
				if m.typ == 'E' {
					t.Fatalf("errored: %s", pcTrace(msgs))
				}
				if m.typ == 'T' {
					desc, found = m.fields, true
				}
			}
			if !found {
				t.Fatalf("no RowDescription at the portal Describe: %s", pcTrace(msgs))
			}
			if strings.Join(desc, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("the portal described %v, PostgreSQL 17.11 describes %v: %s",
					desc, tc.want, pcTrace(msgs))
			}
		})
	}
}

// TestArcPCStatementDescribeRaisesThePlanningError is #998: a statement whose
// analysis fails raises that failure at Describe, with its own SQLSTATE —
// PostgreSQL 17.11 raises every one of these before Execute (measured) — and
// never answers NoData, which says "this statement returns no rows".
func TestArcPCStatementDescribeRaisesThePlanningError(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	for _, tc := range []struct {
		name, sql, code string
	}{
		{"qualified_star_unknown_relation", "SELECT x.* FROM users u", "42P01"},
		{"unknown_column", "SELECT nosuch FROM users", "42703"},
		{"unknown_relation", "SELECT * FROM no_such_table_pc", "42P01"},
		{"unknown_function", "SELECT no_such_fn_pc(id) FROM users", "42883"},
		{"parameterized_unknown_column", "SELECT nosuch FROM users WHERE id = $1", "42703"},
		{"catalog_unknown_column", "SELECT nosuch FROM pg_catalog.pg_class", "42703"},
	} {
		for _, door := range []struct {
			name           string
			stmt, portal   bool
			describedFirst bool
		}{
			{"statement", true, false, true},
			{"portal", false, true, true},
		} {
			t.Run(tc.name+"/"+door.name, func(t *testing.T) {
				c := newPGClient(t, srv.Addr())
				c.startup("wadjet", "wadjet")
				defer c.terminate()
				var params []string
				if strings.Contains(tc.sql, "$1") {
					params = []string{"1"}
				}
				msgs := pcDescribeRound(t, c, tc.sql, params, door.stmt, door.portal)
				var errText string
				for _, m := range msgs {
					switch m.typ {
					case 'n':
						t.Fatalf("Describe answered NoData for a statement PostgreSQL refuses: %s",
							pcTrace(msgs))
					case 'T', 'D':
						t.Fatalf("a refused statement was described or answered: %s", pcTrace(msgs))
					case 'E':
						if errText == "" {
							errText = m.text
						}
					}
				}
				if !strings.Contains(errText, tc.code) {
					t.Fatalf("the error does not carry %s: %s", tc.code, pcTrace(msgs))
				}
			})
		}
	}
}

// zrPortalDescribe is zrDescribe through a PORTAL: Parse, Bind, Describe(P),
// Sync — the path pgconn.ExecParams and pgJDBC's unnamed-portal round take.
func zrPortalDescribe(t *testing.T, srv *Server, sql string) (fields []string, trace string) {
	t.Helper()
	c := newPGClient(t, srv.Addr())
	c.startup("wadjet", "wadjet")
	defer c.terminate()
	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, sql...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	c.writeMsg('P', parseBuf)
	var bindBuf []byte
	bindBuf = append(bindBuf, 0, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	c.writeMsg('B', bindBuf)
	c.writeMsg('D', []byte{'P', 0})
	c.writeMsg('S', nil)
	var parts []string
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			t.Fatalf("reading the portal describe of %q: %v", sql, err)
		}
		parts = append(parts, string(typ))
		if typ == 'T' {
			fields = zrParseRowDescFull(data)
		}
		if typ == 'E' {
			t.Fatalf("portal describe of %q failed: %s", sql, c.parseError(data))
		}
		if typ == 'Z' {
			return fields, strings.Join(parts, " ")
		}
	}
}

// TestArcPCZeroRowPortalDescribesLikeItsNonEmptyTwin runs the zero-row shape
// table (zrShapes) through a portal Describe: every empty statement's portal
// declares what its non-empty twin's statement Describe declares — names,
// OIDs and typmods (#999, on the shapes #846 and #978 fixed for the statement).
func TestArcPCZeroRowPortalDescribesLikeItsNonEmptyTwin(t *testing.T) {
	srv := zrSetup(t)
	for _, tc := range zrShapes() {
		t.Run(tc.name, func(t *testing.T) {
			want, _ := zrDescribe(t, srv, tc.full)
			got, trace := zrPortalDescribe(t, srv, tc.empty)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("the portal of %q declares %v; its non-empty twin declares %v (%s)",
					tc.empty, got, want, trace)
			}
		})
	}
}

// pcErrorCode is an ErrorResponse's SQLSTATE field.
func pcErrorCode(data []byte) string {
	for len(data) > 0 && data[0] != 0 {
		field := data[0]
		val := readCString(data[1:])
		if field == 'C' {
			return val
		}
		data = data[1+len(val)+1:]
	}
	return ""
}
