package pgwire

// #544 stated as the identity it is: the text a TIMESTAMP is COERCED to and
// the text the wire sends for the same TIMESTAMP are the same bytes.
//
// Comparing them here rather than each against a literal is what makes this
// unable to pass for the wrong reason. `SELECT ts` is sent under OID 1114 by
// sendDataRow's timestamp arm; `SELECT ts::text` is sent under OID 25 by the
// expression layer. Those are two different code paths reading one value, and
// #544 is exactly the case where they disagreed — the wire rendered the
// instant and every expression rendered the epoch. They call one formatter
// now, and this compares the bytes rather than trusting that they do.
//
// The operand here is a computed TIMESTAMP (a CAST), which is the shape
// boxedTextOperand's *Cast arm answers. A TIMESTAMP COLUMN reaching the same
// sites is gated by wadjet.TestTimestampHasOneRenderingAtEverySite, because
// this package's fixture has no timestamp column; the two together cover both
// operand shapes. Reverting the fraction trim fails three cells here.

import (
	"context"
	"testing"
)

func TestTimestampTextAndTheWireSendTheSameBytes(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	for _, lit := range []string{
		"1996-03-13 14:25:36",    // a whole second
		"1996-03-13 14:25:36.5",  // a fraction PostgreSQL trims to one digit
		"1996-03-13 14:25:36.12", // …and to two
		"1996-03-13 14:25:36.123",
		"1996-03-13 14:25:36.001", // a leading zero inside the fraction
		"0999-03-13 14:25:36",     // a year below 1000, which keeps four digits
		"1970-01-01 00:00:00",     // the epoch
	} {
		t.Run(lit, func(t *testing.T) {
			ts := `CAST('` + lit + `' AS TIMESTAMP)`
			res := conn.ExecParams(context.Background(),
				`SELECT `+ts+` AS a, CAST(`+ts+` AS TEXT) AS b, `+ts+` || '' AS c `+
					`FROM users WHERE id = 1`, nil, nil, nil, []int16{0, 0, 0}).Read()
			if res.Err != nil {
				t.Fatalf("%v", res.Err)
			}
			if len(res.Rows) != 1 || len(res.Rows[0]) != 3 {
				t.Fatalf("got %v", res.Rows)
			}
			wire := string(res.Rows[0][0])
			for i, name := range []string{"CAST AS TEXT", "|| ''"} {
				if got := string(res.Rows[0][i+1]); got != wire {
					t.Errorf("%s sent %q where the wire sent %q for the same value — "+
						"one column, one connection, two renderings (#544)", name, got, wire)
				}
			}
			if wire != lit {
				t.Errorf("the wire sent %q for the literal %q; PostgreSQL 17.11 renders the "+
					"instant with its fraction trimmed", wire, lit)
			}
			// The declarations differ and must: the first is a timestamp, the
			// other two are text. A pass that made all three text would satisfy
			// the byte comparison above and be wrong.
			if oid := res.FieldDescriptions[0].DataTypeOID; oid != 1114 {
				t.Errorf("column a declared OID %d, want 1114 (timestamp)", oid)
			}
			for i := 1; i < 3; i++ {
				if oid := res.FieldDescriptions[i].DataTypeOID; oid != 25 {
					t.Errorf("column %d declared OID %d, want 25 (text)", i, oid)
				}
			}
		})
	}
}
