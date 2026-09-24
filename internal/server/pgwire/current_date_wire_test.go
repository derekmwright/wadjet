// SPDX-License-Identifier: MIT

package pgwire

// Regression test for #1254: CURRENT_DATE declared TEXT (OID 25) on the
// wire instead of DATE (OID 1082), so `INSERT ... SELECT CURRENT_DATE` into
// a DATE column was refused as a type mismatch even though the rendered
// VALUE was already a date-looking string. The fix is the registry entry
// (internal/engine/expr/expr.go), one mechanism every door reads from; this
// table pins the whole CURRENT_* / now() family on the wire door so a
// future registration keeps the same rule ("one mechanism, all spellings").

import "testing"

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
