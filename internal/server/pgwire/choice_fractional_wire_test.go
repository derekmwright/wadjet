package pgwire

// #849's boundary on the WIRE: a choice construct with a FRACTIONAL literal
// arm is `numeric` (OID 1700), never `int8` (20) over a truncated value
// (round-1 review, B3).
//
// PostgreSQL 17.11, measured live:
//
//	SELECT LEAST(visits, 1.5) + 1 …   -->  numeric, 2.5
//	SELECT LEAST(visits, 1.5) * 3 …   -->  numeric, 4.5
//
// This engine declared OID 20 and sent "2" and "4": the integer rung was
// reached through the choice's declared type, the evaluator produced 1.5, and
// the store TRUNCATED it into the int64 vector the declaration had asked for.
// The wire arm is the one that shows it as a type error as well as a value
// error — and the value error alone would have been enough.

import (
	"context"
	"testing"
)

func TestChoiceWithAFractionalLiteralIsNumericOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	for _, c := range []struct {
		name, sql string
		oid       uint32
		value     string
	}{
		// `visits` is bigint = 100 at id 1.
		{"least", `SELECT LEAST(visits, 1.5) AS v FROM users WHERE id = 1`, 1700, "1.5"},
		{"least_plus_one", `SELECT LEAST(visits, 1.5) + 1 AS v FROM users WHERE id = 1`, 1700, "2.5"},
		{"least_times_three", `SELECT LEAST(visits, 1.5) * 3 AS v FROM users WHERE id = 1`, 1700, "4.5"},
		{"case_else", `SELECT (CASE WHEN id > 900 THEN visits ELSE 1.5 END) AS v ` +
			`FROM users WHERE id = 1`, 1700, "1.5"},
		{"case_else_plus_one", `SELECT (CASE WHEN id > 900 THEN visits ELSE 1.5 END) + 1 AS v ` +
			`FROM users WHERE id = 1`, 1700, "2.5"},
		{"coalesce", `SELECT COALESCE(NULLIF(visits, 100), 2.5) AS v FROM users WHERE id = 1`,
			1700, "2.5"},
		{"greatest", `SELECT GREATEST(0 - visits, 1.5) + 1 AS v FROM users WHERE id = 1`,
			1700, "2.5"},
		// The BOUNDARY: a WHOLE-number literal keeps the integer domain and
		// its int8 declaration, which is what #849 is for. A pass that widened
		// every choice would fail here.
		{"ctl_whole_literal", `SELECT LEAST(visits, 2) + 1 AS v FROM users WHERE id = 1`, 20, "3"},
		{"ctl_whole_literal_case",
			`SELECT (CASE WHEN id > 0 THEN visits ELSE 1 END) * 2 AS v FROM users WHERE id = 1`,
			20, "200"},
		// A FLOAT column beside the literal is double precision on the ladder,
		// not the decimal rung.
		{"ctl_float_column", `SELECT COALESCE(score, 1.5) AS v FROM users WHERE id = 1`,
			701, "95.5"},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			f := res.FieldDescriptions[0]
			if f.DataTypeOID != c.oid {
				t.Errorf("declared OID %d, want %d — PostgreSQL 17.11 types a choice with a "+
					"fractional literal arm `numeric`, and int8 over a value of 1.5 is a "+
					"TRUNCATED answer, not a mislabelled one (round-1 B3)\n  SQL: %s",
					f.DataTypeOID, c.oid, c.sql)
			}
			if len(res.Rows) != 1 || string(res.Rows[0][0]) != c.value {
				t.Errorf("sent %q, want %q (live PostgreSQL 17.11)\n  SQL: %s",
					res.Rows, c.value, c.sql)
			}
		})
	}
}
