package server

import (
	"context"
	"fmt"
	"testing"
)

// #939. CREATE TABLE, DROP TABLE and ANALYZE fell through `wadjet.DB.Query`
// with no authorization at all, and pgwire's non-SELECT path is exactly that
// call. Measured at 672bb5e1 with `reader-key` (role `reader`, allow `[read]`):
//
//	embedded  CREATE TABLE d939a (a BIGINT)   Table "d939a" created — and in the catalog
//	embedded  ANALYZE TABLE users             Table "users" analyzed (1 files)
//	pgwire    DROP TABLE d939a                Table "d939a" dropped — and GONE from the catalog
//	embedded  CREATE TABLE … (NO identity,    Table created
//	          auth ENABLED)
//
// while the HTTP door refused the identical statements 403, which is the rule
// the product intends. The gate is the door census: the same statement, the
// same identity, the same refusal class everywhere, and the catalog state
// asserted after each refusal — an error with a side effect is not a refusal.
func TestDDLAuthorizationHoldsOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)

	// --- refused: an authenticated identity holding only `read` ---
	for _, d := range rig.doors {
		d := d
		t.Run("refused/"+d.name, func(t *testing.T) {
			create := fmt.Sprintf("CREATE TABLE d939_%s (a BIGINT)", d.name)
			_, class, err := d.run(t, sec4Reader, create)
			sec4Refused(t, d, "CREATE TABLE", class, err, "42501")
			if rig.tableExists(t, ctx, "d939_"+d.name) {
				t.Errorf("%s: CREATE TABLE was refused but the table exists", d.name)
			}

			_, class, err = d.run(t, sec4Reader, "DROP TABLE users")
			sec4Refused(t, d, "DROP TABLE", class, err, "42501")
			if !rig.tableExists(t, ctx, "users") {
				t.Fatalf("%s: DROP TABLE was refused but `users` is gone", d.name)
			}

			// IF EXISTS on a table that does not exist: the permission check
			// comes FIRST, so this is a refusal and not a no-op that reports
			// catalog contents to a caller who may not read them.
			_, class, err = d.run(t, sec4Reader, "DROP TABLE IF EXISTS d939_nosuch")
			sec4Refused(t, d, "DROP TABLE IF EXISTS", class, err, "42501")

			_, class, err = d.run(t, sec4Reader, "ANALYZE TABLE users")
			sec4Refused(t, d, "ANALYZE TABLE", class, err, "42501")
		})
	}

	// --- refused: no identity at all, with auth ENABLED (fail closed) ---
	// pgwire and HTTP cannot produce this case — their middleware refuses an
	// unauthenticated connection first (28000 / 401), which is the
	// authentication class, not the authorization one. The embedded door can,
	// and it is the one an in-process caller reaches.
	emb := rig.door("embedded")
	_, class, err := emb.run(t, "", "CREATE TABLE d939_nilid (a BIGINT)")
	sec4RefusedWith(t, "embedded", "CREATE TABLE/nil identity", class, err, "42501")
	if rig.tableExists(t, ctx, "d939_nilid") {
		t.Error("embedded: CREATE TABLE with no identity under auth enabled created the table")
	}

	// --- allowed: the same statements under an identity holding `write` ---
	for _, d := range rig.doors {
		d := d
		t.Run("allowed/"+d.name, func(t *testing.T) {
			name := "d939ok_" + d.name
			if _, _, err := d.run(t, sec4Writer, fmt.Sprintf("CREATE TABLE %s (a BIGINT)", name)); err != nil {
				t.Fatalf("%s: writer CREATE TABLE: %v", d.name, err)
			}
			if !rig.tableExists(t, ctx, name) {
				t.Fatalf("%s: writer CREATE TABLE reported success but the table is absent", d.name)
			}
			if _, _, err := d.run(t, sec4Writer, "ANALYZE TABLE users"); err != nil {
				t.Fatalf("%s: writer ANALYZE: %v", d.name, err)
			}
			if _, _, err := d.run(t, sec4Writer, fmt.Sprintf("DROP TABLE %s", name)); err != nil {
				t.Fatalf("%s: writer DROP TABLE: %v", d.name, err)
			}
			if rig.tableExists(t, ctx, name) {
				t.Fatalf("%s: writer DROP TABLE reported success but the table remains", d.name)
			}
		})
	}
}

// The boundary claim: with NO provider attached — the embedded / CLI use, and
// every existing no-auth test — nothing is enforced and DDL behaves exactly as
// it did. A gate whose only cells are refusals cannot see a fix that broke the
// unauthenticated path.
func TestDDLIsUnchangedWithoutAnAuthProvider(t *testing.T) {
	ctx := context.Background()
	db := sec4DB(t, ctx)
	if _, err := db.Query(ctx, "CREATE TABLE d939_noauth (a BIGINT)"); err != nil {
		t.Fatalf("no-provider CREATE TABLE: %v", err)
	}
	if _, err := db.Catalog().GetTable(ctx, "d939_noauth"); err != nil {
		t.Fatalf("no-provider CREATE TABLE did not create the table: %v", err)
	}
	if _, err := db.Query(ctx, "ANALYZE TABLE users"); err != nil {
		t.Fatalf("no-provider ANALYZE: %v", err)
	}
	if _, err := db.Query(ctx, "DROP TABLE d939_noauth"); err != nil {
		t.Fatalf("no-provider DROP TABLE: %v", err)
	}
	if _, err := db.Catalog().GetTable(ctx, "d939_noauth"); err == nil {
		t.Fatal("no-provider DROP TABLE did not drop the table")
	}
}
