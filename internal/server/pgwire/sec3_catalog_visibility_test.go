package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The pg_catalog emulation is a metadata door, and it follows the effective
// table decision like every other one (ADR-0034).
//
// It did not. `SELECT relname FROM pg_catalog.pg_class WHERE relname='secret'`
// answered `secret` to an identity whose policy denies it, and the
// pg_attribute join handed over that table's column names and types. This is
// the route `psql \d`, DataGrip's tree and every BI tool's schema discovery
// take, so it is the door the product's "metadata follows the table decision"
// position matters most on — and the one where it was false.
//
// The filter is at the SOURCE of the rows (`visibleCatalogTables`), not per
// view: pg_class, pg_tables, pg_attribute and both information_schema views
// render from one list, and `\d` joins two of them.

// sec3CatalogDB has a table the analyst may read and one its policy denies.
func sec3CatalogDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, name := range []string{"public_t", "secret"} {
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: name + "_col", Type: parquet.TypeString, Nullable: true},
		}}
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}
	return db
}

// sec3CatalogProvider denies `analyst` the relation `secret` while its legacy
// role still lists every table — the shape a roles-to-ABAC migration leaves.
func sec3CatalogProvider() *auth.Provider {
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "sec3-catalog", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "analyst-public", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "public_t"}},
				Actions:   []auth.Action{auth.ActionRead},
			},
			{
				ID: "analyst-secret-denied", EffectStr: "deny", Priority: 100,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "secret"}},
				Actions:   []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
			{
				ID: "admin-all", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions: []auth.Action{auth.ActionRead, auth.ActionWrite, auth.ActionAdmin,
					auth.ActionCreate, auth.ActionDrop, auth.ActionDescribe},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "analyst-key", Name: "analyst-user", Role: "analyst"},
			{Key: "admin-key", Name: "admin-user", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// sec3CatalogQuery runs one statement over a real authenticated connection and
// returns every field of every row, flattened.
func sec3CatalogQuery(t *testing.T, addr, user, key, sql string) string {
	t.Helper()
	client := newPGClient(t, addr)
	defer client.terminate()
	if errMsg := client.startupWithPassword(user, "testdb", key); errMsg != "" {
		t.Fatalf("authenticating %s: %s", user, errMsg)
	}
	cols, rows, tag := client.simpleQuery(sql)
	var b strings.Builder
	fmt.Fprintf(&b, "cols=%v tag=%s rows=", cols, tag)
	for _, r := range rows {
		fmt.Fprintf(&b, "%v", r)
	}
	return b.String()
}

func TestPGCatalogHidesADeniedRelation(t *testing.T) {
	db := sec3CatalogDB(t)
	provider := sec3CatalogProvider()
	db.SetAuthProvider(provider)
	srv := startTestServerWithAuth(t, db, provider)

	for _, tc := range []struct{ name, sql string }{
		{"pg_class by name",
			"SELECT relname FROM pg_catalog.pg_class WHERE relname = 'secret'"},
		{"pg_class listing",
			"SELECT relname, relkind FROM pg_catalog.pg_class"},
		{"pg_tables listing",
			"SELECT tablename FROM pg_tables"},
		{"pg_attribute joined to pg_class",
			"SELECT a.attname FROM pg_catalog.pg_attribute a " +
				"JOIN pg_catalog.pg_class c ON a.attrelid = c.oid WHERE c.relname = 'secret'"},
		{"information_schema.tables",
			"SELECT table_name FROM information_schema.tables"},
		{"information_schema.columns",
			"SELECT column_name FROM information_schema.columns WHERE table_name = 'secret'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sec3CatalogQuery(t, srv.Addr(), "analyst-user", "analyst-key", tc.sql)
			if strings.Contains(got, "secret") {
				t.Errorf("the catalog published a DENIED relation to an identity that may not "+
					"read it:\n  %s\n  -> %s", tc.sql, got)
			}
			// The permitted relation is still discoverable, so the filter is
			// not simply emptying the view.
			if strings.Contains(tc.sql, "= 'secret'") {
				return
			}
			if !strings.Contains(got, "public_t") {
				t.Errorf("the permitted relation is missing from the catalog:\n  %s\n  -> %s",
					tc.sql, got)
			}
		})
	}

	// An identity the policy permits still sees the relation, on the same
	// statements — the filter is the DECISION, not a blanket hide.
	for _, sql := range []string{
		"SELECT relname FROM pg_catalog.pg_class WHERE relname = 'secret'",
		"SELECT tablename FROM pg_tables",
		"SELECT table_name FROM information_schema.tables",
	} {
		got := sec3CatalogQuery(t, srv.Addr(), "admin-user", "admin-key", sql)
		if !strings.Contains(got, "secret") {
			t.Errorf("an identity that MAY read the relation cannot discover it:\n  %s\n  -> %s",
				sql, got)
		}
	}
}

// With no provider the catalog answers exactly what it always did.
func TestPGCatalogIsUnchangedWithoutAuth(t *testing.T) {
	db := sec3CatalogDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	defer client.terminate()
	client.startup("anyone", "testdb")

	_, rows, _ := client.simpleQuery("SELECT tablename FROM pg_tables")
	var names []string
	for _, r := range rows {
		names = append(names, r...)
	}
	joined := strings.Join(names, ",")
	if !strings.Contains(joined, "secret") || !strings.Contains(joined, "public_t") {
		t.Fatalf("pg_tables without auth listed %v; want both tables", names)
	}
}

// `\d secret` is the statement psql sends, over the real protocol through pgx:
// a pg_class lookup by name, then the pg_attribute join. A denied relation is
// "did not find any relation" — nothing at all, not a partial answer.
func TestPsqlDescribeFindsNoDeniedRelation(t *testing.T) {
	db := sec3CatalogDB(t)
	provider := sec3CatalogProvider()
	db.SetAuthProvider(provider)
	srv := startTestServerWithAuth(t, db, provider)

	// The two lookups psql's \d makes, in order. The relation lookup is
	// spelled with `=` rather than psql's own
	// `relname OPERATOR(pg_catalog.~) '^(secret)$'`: this server's catalog
	// emulation does not model the regex operator and answers ZERO rows to
	// that spelling for every identity, so asserting on it would prove
	// nothing about the filter. The equality spelling is what the emulation
	// answers, and what a client that resolved the name already sends.
	const relLookup = `SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c ` +
		`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
		`WHERE c.relname = 'secret' ORDER BY 2, 3`
	const attrLookup = `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) ` +
		`FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid ` +
		`WHERE c.relname = 'secret' AND a.attnum > 0 ORDER BY a.attnum`

	for _, tc := range []struct {
		name, user, key string
		wantFound       bool
	}{
		{"the denied identity finds no relation", "analyst-user", "analyst-key", false},
		{"an identity that may read it does", "admin-user", "admin-key", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := sec3Pgconn(t, srv.Addr(), tc.user, tc.key)
			for _, sql := range []string{relLookup, attrLookup} {
				res := conn.ExecParams(context.Background(), sql, nil, nil, nil, nil).Read()
				if res.Err != nil {
					t.Fatalf("%s: %v", sql, res.Err)
				}
				found := false
				for _, row := range res.Rows {
					for _, v := range row {
						if strings.Contains(string(v), "secret") {
							found = true
						}
					}
				}
				if found != tc.wantFound {
					t.Errorf("relation %q in the answer to\n  %s\ngot %v, want %v (rows %d)",
						"secret", sql, found, tc.wantFound, len(res.Rows))
				}
			}
		})
	}
}

// sec3Pgconn is connectPgconn with a password, so the connection carries an
// identity.
func sec3Pgconn(t *testing.T, addr, user, key string) *pgconn.PgConn {
	t.Helper()
	conn, err := pgconn.Connect(context.Background(),
		fmt.Sprintf("postgres://%s:%s@%s/wadjet?sslmode=disable", user, key, addr))
	if err != nil {
		t.Fatalf("pgconn.Connect as %s: %v", user, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}
