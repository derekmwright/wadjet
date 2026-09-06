package server

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #941. `DB.Query` dispatched DESCRIBE and SHOW TABLES ahead of the
// plan-enforcement path, and neither `db.describe` nor `db.showTables`
// consulted the identity, the Authorizer or the PolicyEvaluator. Measured at
// 672bb5e1 with `reader` (tables [users], allow [read]):
//
//	embedded  DESCRIBE secret   id INT64 / sensitive_column STRING
//	pgwire    DESCRIBE secret   the same rows
//	embedded  SHOW TABLES       users, secret, …
//	pgwire    SHOW TABLES       the same list
//	*any*     SELECT * FROM secret   access denied to table "secret"
//
// The data door and the metadata door disagreed about the same relation for
// the same identity. They now ask one question — `auth.TableAccess` /
// `auth.VisibleTables`.
func TestMetadataFollowsTheTableDecisionOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)

	// The HTTP door already implemented this rule; it is in the census so the
	// three doors are asserted to AGREE, which is the property that failed.
	for _, d := range rig.doors {
		d := d
		t.Run(d.name, func(t *testing.T) {
			// DESCRIBE of a relation this identity may not read.
			_, class, err := d.run(t, sec4Reader, "DESCRIBE secret")
			sec4Refused(t, d, "DESCRIBE secret", class, err, "42501")

			// SHOW COLUMNS FROM is the same statement under another spelling,
			// and it is the one a PostgreSQL client sends.
			_, class, err = d.run(t, sec4Reader, "SHOW COLUMNS FROM secret")
			sec4Refused(t, d, "SHOW COLUMNS FROM secret", class, err, "42501")

			// The relation it MAY read is still described.
			rows, _, err := d.run(t, sec4Reader, "DESCRIBE users")
			if err != nil {
				t.Fatalf("%s: reader DESCRIBE users: %v", d.name, err)
			}
			if !anyContains(rows, "name") {
				t.Errorf("%s: DESCRIBE users did not list its columns: %v", d.name, rows)
			}

			// SHOW TABLES lists only what this identity may read.
			rows, _, err = d.run(t, sec4Reader, "SHOW TABLES")
			if err != nil {
				t.Fatalf("%s: reader SHOW TABLES: %v", d.name, err)
			}
			if anyContains(rows, "secret") {
				t.Errorf("%s: SHOW TABLES published a relation the identity may not read: %v",
					d.name, rows)
			}
			if !anyContains(rows, "users") {
				t.Errorf("%s: SHOW TABLES dropped a relation the identity MAY read: %v",
					d.name, rows)
			}

			// An identity that may read everything still sees everything.
			rows, _, err = d.run(t, sec4Ops, "SHOW TABLES")
			if err != nil {
				t.Fatalf("%s: ops SHOW TABLES: %v", d.name, err)
			}
			if !anyContains(rows, "secret") || !anyContains(rows, "users") {
				t.Errorf("%s: an admin identity must see every table: %v", d.name, rows)
			}
			if _, _, err := d.run(t, sec4Ops, "DESCRIBE secret"); err != nil {
				t.Errorf("%s: ops DESCRIBE secret: %v", d.name, err)
			}
		})
	}

	// No identity at all, auth ENABLED, on the door that can express it.
	emb := rig.door("embedded")
	_, class, err := emb.run(t, "", "DESCRIBE users")
	sec4RefusedWith(t, "embedded", "DESCRIBE/nil identity", class, err, "42501")
	rows, _, err := emb.run(t, "", "SHOW TABLES")
	if err != nil {
		t.Fatalf("embedded SHOW TABLES with no identity: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("SHOW TABLES with no identity under auth enabled listed %v", rows)
	}
}

// An EXPLICIT ABAC deny governs the metadata doors, not only the role's table
// list. This is the half the legacy `FilterTables` cannot express: the role
// here is scoped to `["*"]`, so every RBAC check passes and only the policy
// says no.
func TestAnExplicitABACDenyHidesTheTableFromMetadata(t *testing.T) {
	ctx := context.Background()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "deny-secret", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "broad-allow", EffectStr: "allow", Priority: 100,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionDescribe},
			},
			{
				ID: "deny-secret", EffectStr: "deny", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "secret"}},
				Actions:   []auth.Action{auth.ActionRead, auth.ActionDescribe},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	rig := sec4RigWithProvider(t, ctx, provider)
	for _, d := range rig.doors {
		d := d
		t.Run(d.name, func(t *testing.T) {
			_, class, err := d.run(t, "analyst-key", "DESCRIBE secret")
			sec4Refused(t, d, "DESCRIBE under an explicit deny", class, err, "42501")

			rows, _, err := d.run(t, "analyst-key", "SHOW TABLES")
			if err != nil {
				t.Fatalf("%s: SHOW TABLES: %v", d.name, err)
			}
			if anyContains(rows, "secret") {
				t.Errorf("%s: an explicit ABAC deny must remove the table from SHOW TABLES: %v",
					d.name, rows)
			}
			if !anyContains(rows, "users") {
				t.Errorf("%s: the broad allow should keep `users`: %v", d.name, rows)
			}
		})
	}
}

// A CamelCase catalog table is reachable under its folded spelling, and the
// authorization decision is asked on the CATALOG's spelling — which is the one
// a policy is bound to (#731, #882). Without the resolution a policy naming
// `Ledger` would not police `DESCRIBE ledger`.
func TestMetadataAuthorizationResolvesTheCatalogSpelling(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "Ledger", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("Ledger", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "amount": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "p", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "deny-ledger", EffectStr: "deny", Priority: 10,
			Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
			// The policy names the CATALOG spelling.
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "Ledger"}},
			Actions:   []auth.Action{auth.ActionRead, auth.ActionDescribe},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "reader-key", Name: "reader", Role: "reader"}},
		Roles:   []auth.RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)
	if err := db.SetAuthProvider(provider); err != nil {
		t.Fatal(err)
	}
	id, _ := provider.Authenticator().AuthenticateToken("reader-key")
	readerCtx := auth.ContextWithIdentity(ctx, id)

	// The statement spells it folded, the way an unquoted reference arrives.
	if _, err := db.Query(readerCtx, "DESCRIBE ledger"); err == nil {
		t.Error("a policy bound to `Ledger` must police `DESCRIBE ledger`")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("unexpected refusal: %v", err)
	}
	res, qerr := db.Query(readerCtx, "SHOW TABLES")
	if qerr != nil {
		t.Fatalf("SHOW TABLES: %v", qerr)
	}
	for _, row := range res.Rows {
		if row["table_name"] == "Ledger" {
			t.Error("SHOW TABLES published a relation an explicit deny covers")
		}
	}
}

// The boundary claim: with NO provider the metadata statements are unchanged.
func TestMetadataIsUnchangedWithoutAnAuthProvider(t *testing.T) {
	ctx := context.Background()
	db := sec4DB(t, ctx)
	res, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("no-provider SHOW TABLES: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("no-provider SHOW TABLES returned %d rows, want 2: %v", len(res.Rows), res.Rows)
	}
	if _, err := db.Query(ctx, "DESCRIBE secret"); err != nil {
		t.Fatalf("no-provider DESCRIBE: %v", err)
	}
}
