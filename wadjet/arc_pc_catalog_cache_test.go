// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestArcPCCatalogScanSeesEveryDDL holds the system catalog's per-table
// descriptor cache (sysrows, keyed by the table definition's KV revision) to
// the rule a cache must keep: a catalog scan after DDL describes the table as
// it is now — a dropped table is gone and a re-created one has its new columns.
func TestArcPCCatalogScanSeesEveryDDL(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	cols := func() string {
		res, err := db.Query(ctx, `SELECT attname FROM pg_attribute WHERE attrelid = 'pcc'::regclass AND attnum > 0 ORDER BY attnum`)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err)
		}
		out := ""
		for _, r := range res.Rows {
			out += fmt.Sprint(r["attname"]) + " "
		}
		return out
	}
	steps := []struct{ ddl, want string }{
		{`CREATE TABLE pcc (a BIGINT, b TEXT)`, "a b "},
		{`DROP TABLE pcc`, "ERR 42P01"},
		{`CREATE TABLE pcc (z FLOAT64, y BIGINT)`, "z y "},
	}
	for _, s := range steps {
		if _, err := db.Query(ctx, s.ddl); err != nil {
			t.Fatalf("%s: %v", s.ddl, err)
		}
		if got := cols(); got != s.want {
			t.Errorf("after %s: pg_attribute lists %q, want %q", s.ddl, got, s.want)
		}
	}
}
