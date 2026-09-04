package pgwire

// #838's declaration half, on the WIRE — the only place it is visible.
//
// `CAST('abcdef' AS VARCHAR(4))` truncates to `abcd` (the VALUE half, closed
// first because ADR-0012 item 5 sets that order) and declared OID 25 (text)
// with atttypmod -1, where PostgreSQL 17.11's \gdesc says `character
// varying(4)` — OID 1043, atttypmod 8. A value oracle sees nothing wrong with
// a right string under a wrong type, which is why this test is here and not
// beside the value cells.
//
// PostgreSQL 17.11, measured live:
//
//	SELECT CAST('abcdef' AS VARCHAR(4));  \gdesc  ->  character varying(4)
//	                                      atttypmod                     8
//	SELECT CAST('abcdef' AS VARCHAR);     \gdesc  ->  character varying
//	                                      atttypmod                    -1

import "testing"

func TestVarcharCastDeclaresItsLengthOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	for _, c := range []struct {
		name, sql string
		oid       uint32
		typmod    int32
		value     string
	}{
		// n + VARHDRSZ, PostgreSQL's own encoding of a length modifier.
		{"varchar_n", `SELECT CAST('abcdef' AS VARCHAR(4)) AS v FROM users WHERE id = 1`,
			1043, 8, "abcd"},
		{"varchar_one", `SELECT CAST('abcdef' AS VARCHAR(1)) AS v FROM users WHERE id = 1`,
			1043, 5, "a"},
		{"character_varying_n",
			`SELECT CAST('abcdef' AS CHARACTER VARYING(4)) AS v FROM users WHERE id = 1`,
			1043, 8, "abcd"},
		// CHAR(n) declares `character varying(n)` and NOT `character(n)`
		// (1042), deliberately: PostgreSQL's bpchar pads a short value to n and
		// then strips the blanks again for length(), for `||` and for every
		// comparison, and this engine has one TypeString and none of that.
		// Declaring 1042 would name a type whose three defining behaviours it
		// does not implement; `character varying(n)` states exactly what the
		// value IS. The residual is in ADR-0012 item 5.
		{"char_n", `SELECT CAST('abcdef' AS CHAR(4)) AS v FROM users WHERE id = 1`,
			1043, 8, "abcd"},
		// A value already inside n keeps the declaration — the modifier is a
		// bound, not a measurement of this row.
		{"varchar_n_within_length", `SELECT CAST('ab' AS VARCHAR(4)) AS v FROM users WHERE id = 1`,
			1043, 8, "ab"},
		// A non-string operand: PostgreSQL truncates the RENDERING and declares
		// the same modifier.
		{"varchar_n_over_a_number", `SELECT CAST(12345 AS VARCHAR(3)) AS v FROM users WHERE id = 1`,
			1043, 7, "123"},
		// The UNPARAMETERIZED spellings, which are the controls: they must stay
		// text with no modifier, or the pair says nothing about the length.
		{"bare_varchar", `SELECT CAST('abcdef' AS VARCHAR) AS v FROM users WHERE id = 1`,
			25, -1, "abcdef"},
		{"text", `SELECT CAST('abcdef' AS TEXT) AS v FROM users WHERE id = 1`,
			25, -1, "abcdef"},
		// Bare CHAR is `character(1)` on the server — `CAST('abcdef' AS CHAR)`
		// is `a` there — and this engine reads it as the unparameterized
		// string. That is part of the bpchar residual ADR-0012 item 5 records,
		// and it is recorded rather than fixed here because the DDL door reads
		// the same name the same way: fixing one would give one type name two
		// dispositions across two doors, which is the property #838's own gate
		// exists to hold.
		{"residual_bare_char", `SELECT CAST('abcdef' AS CHAR) AS v FROM users WHERE id = 1`,
			25, -1, "abcdef"},
		// A bare STRING column: the catalog does not store a VARCHAR(n)
		// column's n, so the engine is unconstrained here and says so.
		{"bare_column", `SELECT name AS v FROM users WHERE id = 1`, 25, -1, "alice"},
		// A non-string column must not acquire a modifier from the map, which
		// answers by NAME: the type gate is what stops that.
		{"integer_column", `SELECT visits AS v FROM users WHERE id = 1`, 20, -1, "100"},
		// A NULL VALUE under a parameterized destination still carries the
		// declaration — the modifier describes the COLUMN, not the row.
		// PostgreSQL's \gdesc for `CAST(NULL AS VARCHAR(3))` is
		// `character varying(3)`, measured.
		{"null_value_keeps_the_declaration",
			`SELECT CAST(NULL AS VARCHAR(3)) AS v FROM users WHERE id = 1`, 1043, 7, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(t.Context(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			if len(res.FieldDescriptions) != 1 {
				t.Fatalf("%d fields, want 1", len(res.FieldDescriptions))
			}
			f := res.FieldDescriptions[0]
			if f.DataTypeOID != c.oid {
				t.Errorf("declared OID %d, want %d (#838)", f.DataTypeOID, c.oid)
			}
			if f.TypeModifier != c.typmod {
				t.Errorf("declared atttypmod %d, want %d — PostgreSQL sends n+4 for a "+
					"length modifier and -1 for none (#838)", f.TypeModifier, c.typmod)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1", len(res.Rows))
			}
			if res.Rows[0][0] == nil {
				if c.value != "" {
					t.Errorf("sent NULL, want %q", c.value)
				}
				return
			}
			if string(res.Rows[0][0]) != c.value {
				t.Errorf("sent %q, want %q", res.Rows[0][0], c.value)
			}
		})
	}
	// The RESIDUAL: a length does NOT survive a derived-table boundary.
	//
	// PostgreSQL's \gdesc for `SELECT * FROM (SELECT CAST('abcdef' AS
	// VARCHAR(4)) AS v) x` is `character varying(4)` — the modifier rides with
	// the column through the sub-select. Here the OUTPUT projection is a bare
	// reference to `v`, and a bare reference carries no length for the same
	// reason a stored column does not: the only place a length lives is a
	// CAST's own type name, which the boundary has already consumed.
	//
	// It is the same fact as the `bare_column` cell above rather than a second
	// one, and closing it is the same change: a length the plan can carry
	// through a projection, which today's colDecls has no field for.
	//
	// TODO(#838): delete this pin when a derived table propagates the modifier.
	t.Run("residual_derived_table_loses_the_length", func(t *testing.T) {
		conn := connectPgconn(t, srv.Addr())
		const sql = `SELECT v FROM (SELECT CAST('abcdef' AS VARCHAR(4)) AS v FROM users WHERE id = 1) x`
		res := conn.ExecParams(t.Context(), sql, nil, nil, nil, []int16{0}).Read()
		if res.Err != nil {
			t.Fatalf("%v", res.Err)
		}
		f := res.FieldDescriptions[0]
		if f.DataTypeOID != 25 || f.TypeModifier != -1 {
			t.Errorf("declared OID %d atttypmod %d; this pin records 25/-1 and PostgreSQL "+
				"17.11 describes it as character varying(4). If the modifier now survives "+
				"the boundary, delete this pin and the bare-column note in ADR-0012 item 5",
				f.DataTypeOID, f.TypeModifier)
		}
		if len(res.Rows) != 1 || string(res.Rows[0][0]) != "abcd" {
			t.Errorf("sent %q, want abcd — the VALUE crosses the boundary either way", res.Rows)
		}
	})
}
