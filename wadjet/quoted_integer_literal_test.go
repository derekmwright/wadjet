// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestQuotedIntegerLiteralRefusalIsInt4insSentence pins round-4 review N4: a
// quoted literal (a bound pgwire parameter arrives as one) that is not an
// integer, assigned to an integer column, is refused in int4in / int8in's own
// words on the unquoted text — `invalid input syntax for type integer:
// "2.5"`, 22003 `value "…" is out of range for type integer` — where it said
// `invalid input syntax for type numeric: "'2.5'"`. PostgreSQL 17.11.
func TestQuotedIntegerLiteralRefusalIsInt4insSentence(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "qi", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ sql, state, msg string }{
		{"INSERT INTO qi (id, i) VALUES (1, '2.5')", "22P02", `invalid input syntax for type integer: "2.5"`},
		{"INSERT INTO qi (id, n) VALUES (1, '2.5')", "22P02", `invalid input syntax for type bigint: "2.5"`},
		{"INSERT INTO qi (id, i) VALUES (1, 'abc')", "22P02", `invalid input syntax for type integer: "abc"`},
		{"INSERT INTO qi (id, i) VALUES (1, '99999999999')", "22003", `value "99999999999" is out of range for type integer`},
		{"UPDATE qi SET i = '2.5'", "22P02", `invalid input syntax for type integer: "2.5"`},
	} {
		_, err := db.Execute(ctx, tc.sql)
		if sqlerr.StateOf(err) != tc.state || err == nil || err.Error() != tc.msg {
			t.Errorf("%s: %q (%v), PostgreSQL 17.11 %s %s", tc.sql, sqlerr.StateOf(err), err, tc.state, tc.msg)
		}
	}
}
