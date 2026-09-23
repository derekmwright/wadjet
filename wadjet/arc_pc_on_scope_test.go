// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestALaterJoinsColumnIsNotInAnEarlierON: an ON sees the relations joined so
// far inside its own FROM item — and so their COLUMNS, not only their names.
// A bare name that only a LATER join's relation publishes is 42703 there on
// PostgreSQL 17.11 (measured); the binder removed that relation's qualifier
// from the ON's scope but kept its columns, so the statement ANSWERED (zero
// rows), and a qualifier naming that relation's column read as a ROW
// container (arc PC, found through the base-type alias rule).
func TestALaterJoinsColumnIsNotInAnEarlierON(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for name, col := range map[string]string{"pco": "id", "pcev": "n", "pcnn": "a"} {
		sch := parquet.Schema{Columns: []parquet.Column{{Name: col, Type: parquet.TypeInt64, Nullable: true}}}
		if err := db.CreateTable(ctx, name, sch, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ sql, state string }{
		{"SELECT 1 FROM pco JOIN pcev ON a = pco.id JOIN pcnn ON true", "42703"},
		{"SELECT 1 FROM pco JOIN pcev ON pcnn.a = pco.id JOIN pcnn ON true", "42P01"},
		{"SELECT 1 FROM pco JOIN generate_series(1, 3) g ON h.x = pco.id JOIN generate_series(1, 2) h ON true",
			"42P01"},
		// The control: the relation joined in the SAME item before the ON
		// is visible, bare and qualified.
		{"SELECT 1 FROM pco JOIN pcev ON n = pco.id JOIN pcnn ON a = n", ""},
	} {
		_, err := db.Query(ctx, tc.sql)
		if tc.state == "" {
			if err != nil {
				t.Errorf("%s: %v", tc.sql, err)
			}
			continue
		}
		if got := sqlerr.StateOf(err); got != tc.state {
			t.Errorf("%s: SQLSTATE %q (%v), PostgreSQL 17.11 raises %s", tc.sql, got, err, tc.state)
		}
	}
}
