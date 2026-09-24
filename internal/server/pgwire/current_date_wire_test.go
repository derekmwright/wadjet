// SPDX-License-Identifier: MIT

package pgwire

// Regression test for #1254: CURRENT_DATE declared TEXT (OID 25) on the
// wire instead of DATE (OID 1082), so `INSERT ... SELECT CURRENT_DATE` into
// a DATE column was refused as a type mismatch even though the rendered
// VALUE was already a date-looking string. The fix is the registry entry
// (internal/engine/expr/expr.go), one mechanism every door reads from; this
// table pins the whole CURRENT_* / now() family on the wire door so a
// future registration keeps the same rule ("one mechanism, all spellings").

import (
	"testing"
	"time"
)

func TestCurrentDateFamilyDeclaresItsRealTypeOID(t *testing.T) {
	_, srv := setupRealDB(t)
	for _, tc := range []struct {
		name string
		sql  string
		oid  uint32
		size int16
	}{
		{"current_date", `SELECT CURRENT_DATE AS v`, 1082, 4},
		{"now", `SELECT now() AS v`, 1114, 8},
		{"current_timestamp", `SELECT CURRENT_TIMESTAMP AS v`, 1114, 8},
		{"localtimestamp", `SELECT LOCALTIMESTAMP AS v`, 1114, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oid, size, _ := wireField(t, srv.Addr(), tc.sql)
			if oid != tc.oid {
				t.Errorf("%s declared OID %d, want %d", tc.sql, oid, tc.oid)
			}
			if size != tc.size {
				t.Errorf("%s declared size %d, want %d", tc.sql, size, tc.size)
			}
		})
	}
}

// TestCurrentDateArithmeticDeclaresItsRealTypeOID is round-2 review B3:
// current_date's own declaration (the table above) did not reach a BinaryOp
// built OVER it. `CURRENT_DATE + 1` answered the raw day-count number under
// OID 25 (text) rather than the rendered date under OID 1082 (date) — the
// registry declares the FUNCTION's return; nodeTemporalKind
// (declared_output.go) is what has to recognize the function CALL as a
// temporal operand for the arithmetic's own declared output to follow it,
// and it only recognized a CAST or a column reference before this fix.
func TestCurrentDateArithmeticDeclaresItsRealTypeOID(t *testing.T) {
	_, srv := setupRealDB(t)
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	for _, tc := range []struct {
		name string
		sql  string
		oid  uint32
		size int16
		val  string
	}{
		{"current_date_plus_1", `SELECT CURRENT_DATE + 1 AS v`, 1082, 4, tomorrow},
		{"1_plus_current_date", `SELECT 1 + CURRENT_DATE AS v`, 1082, 4, tomorrow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oid, size, val := wireField(t, srv.Addr(), tc.sql)
			if oid != tc.oid {
				t.Errorf("%s declared OID %d, want %d", tc.sql, oid, tc.oid)
			}
			if size != tc.size {
				t.Errorf("%s declared size %d, want %d", tc.sql, size, tc.size)
			}
			if val != tc.val {
				t.Errorf("%s = %q, want %q", tc.sql, val, tc.val)
			}
		})
	}
	// date - date is an integer count of days (PostgreSQL: integer), not
	// text — the OID this arm used to declare for CURRENT_DATE - DATE '...'.
	oid, _, val := wireField(t, srv.Addr(), `SELECT CURRENT_DATE - CURRENT_DATE AS v`)
	if oid == 25 {
		t.Errorf("CURRENT_DATE - CURRENT_DATE declared OID 25 (text), want a numeric OID")
	}
	if val != "0" {
		t.Errorf("CURRENT_DATE - CURRENT_DATE = %q, want \"0\"", val)
	}
}
