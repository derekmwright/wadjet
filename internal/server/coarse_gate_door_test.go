package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/wadjet"
)

// The coarse gate ON THE DATA PATHS, under an explicit `abac_policies:` block.
//
// `auth.TableAccess` applies the role's `allow` list first, but the plan and
// DML paths went straight to the evaluator when one was installed, so the gate
// existed only on the metadata decision that no data door asks. A role written
// `allow: [read]` whose policy permitted the write DELETED rows on the
// embedded door and on pgwire while `TableAccess(write)` refused — the
// metadata/data disagreement this arc exists to remove, with a real side
// effect on the wrong side of it.

// cgProvider: roles `reader` (read only) and `writer` (read+write), both
// scoped to the policed table, with an explicit ABAC set that allows BOTH of
// them to read and write it. The policy is deliberately more permissive than
// the roles: what must decide is the narrower of the two.
func cgProvider(t *testing.T) *auth.Provider {
	t.Helper()
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader", Role: "reader"},
			{Key: "writer-key", Name: "writer", Role: "writer"},
			{Key: "writeonly-key", Name: "writeonly", Role: "writeonly"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{pmTable}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{pmTable}, Allow: []string{"read", "write"}},
			{Name: "writeonly", Tables: []string{pmTable}, Allow: []string{"write"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "permissive", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "everyone-both", EffectStr: "allow", Priority: 10,
			Actions: []auth.Action{auth.ActionRead, auth.ActionWrite},
		}},
	}}))
	return p
}

func cgRig(t *testing.T, ctx context.Context) (*wadjet.DB, *auth.Provider, string) {
	t.Helper()
	p := cgProvider(t)
	db := pmEmbeddedDB(t, ctx, 0)
	if err := db.SetAuthProvider(p); err != nil {
		t.Fatal(err)
	}
	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: p}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	return db, p, pg.Addr()
}

func cgIdentityCtx(t *testing.T, ctx context.Context, p *auth.Provider, key string) context.Context {
	t.Helper()
	id, err := p.Authenticator().AuthenticateToken(key)
	if err != nil {
		t.Fatalf("authenticate %q: %v", key, err)
	}
	return auth.ContextWithIdentity(ctx, id)
}

func cgCount(t *testing.T, ctx context.Context, db *wadjet.DB, p *auth.Provider) int {
	t.Helper()
	res, err := db.Query(cgIdentityCtx(t, ctx, p, "writer-key"), "SELECT COUNT(*) AS n FROM "+pmTable)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	var n int
	fmt.Sscan(fmt.Sprint(res.Rows[0]["n"]), &n)
	return n
}

// TestTheCoarseGateReachesTheDataDoorsUnderABAC — the blocker's own cell.
func TestTheCoarseGateReachesTheDataDoorsUnderABAC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, p, addr := cgRig(t, ctx)
	before := cgCount(t, ctx, db, p)

	// The metadata decision refuses the write for `reader`.
	if err := auth.TableAccess(cgIdentityCtx(t, ctx, p, "reader-key"), p,
		pmTable, auth.ActionWrite); err == nil {
		t.Fatal("test setup: TableAccess should refuse the read-only role's write")
	}

	// embedded: the same answer, and no side effect.
	if _, err := db.Execute(cgIdentityCtx(t, ctx, p, "reader-key"),
		"DELETE FROM "+pmTable+" WHERE id = 1"); err == nil {
		t.Error("embedded: a role with allow: [read] DELETEd rows under a permissive policy")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("embedded: refusal is not an authorization refusal: %v", err)
	}

	// pgwire: the same.
	conn, cerr := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:reader-key@%s/wadjet?sslmode=disable", addr))
	if cerr != nil {
		t.Fatal(cerr)
	}
	defer conn.Close(ctx)
	if _, qerr := conn.Exec(ctx, "DELETE FROM "+pmTable+" WHERE id = 2"); qerr == nil {
		t.Error("pgwire: a role with allow: [read] DELETEd rows under a permissive policy")
	}

	if after := cgCount(t, ctx, db, p); after != before {
		t.Fatalf("rows went from %d to %d: a refused DELETE destroyed data", before, after)
	}

	// The control: the role that HOLDS write still writes, so the gate is a
	// gate and not a wall.
	if _, err := db.Execute(cgIdentityCtx(t, ctx, p, "writer-key"),
		"DELETE FROM "+pmTable+" WHERE id = 1"); err != nil {
		t.Fatalf("the authorized DELETE was refused: %v", err)
	}
	if after := cgCount(t, ctx, db, p); after != before-1 {
		t.Fatalf("the authorized DELETE removed %d rows, want 1", before-after)
	}
}

// The read side of the same hole: an identity whose role the configuration
// does not define holds no permission, and a policy that matches everyone must
// not rescue it.
func TestTheCoarseGateReachesThePlanPathUnderABAC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, p, _ := cgRig(t, ctx)

	stranger := auth.ContextWithIdentity(ctx,
		&auth.Identity{Name: "ghost", Role: "undefined-role", Method: "apikey"})
	if err := auth.TableAccess(stranger, p, pmTable, auth.ActionRead); err == nil {
		t.Fatal("test setup: TableAccess should refuse an identity with no permissions")
	}
	if _, err := db.Query(stranger, "SELECT id FROM "+pmTable); err == nil {
		t.Fatal("the plan path served an identity the shared decision refuses")
	}

	// And a role that IS defined still reads.
	if _, err := db.Query(cgIdentityCtx(t, ctx, p, "reader-key"),
		"SELECT id FROM "+pmTable); err != nil {
		t.Fatalf("the authorized read was refused: %v", err)
	}
}

// TestAWriteOnlyRoleWritesWhatPostgreSQLLetsItWrite (B5).
//
// The DML door required write AND read for every statement, so a role holding
// `allow: [write]` was refused an INSERT and an unqualified DELETE that
// PostgreSQL allows — and `TableAccess(ActionWrite)` allowed them, so the
// metadata decision and the data door disagreed about the same identity.
// PostgreSQL attaches SELECT to the PREDICATE, not to the write.
func TestAWriteOnlyRoleWritesWhatPostgreSQLLetsItWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, p, _ := cgRig(t, ctx)
	wo := cgIdentityCtx(t, ctx, p, "writeonly-key")

	// The metadata decision allows the write.
	if err := auth.TableAccess(wo, p, pmTable, auth.ActionWrite); err != nil {
		t.Fatalf("TableAccess refused a write-only role's write: %v", err)
	}
	// So does the door, for the statements that read nothing.
	if _, err := db.Execute(wo, fmt.Sprintf(
		"INSERT INTO %s (id, dept, ssn, acct, salary, amt) VALUES (9001, 'x', 's', 1, 1, 1)",
		pmTable)); err != nil {
		t.Errorf("INSERT refused for a write-only role, which PostgreSQL allows: %v", err)
	}
	if _, err := db.Execute(wo, "DELETE FROM "+pmTable+" WHERE 1=0"); err != nil {
		t.Errorf("unqualified-shaped DELETE refused for a write-only role: %v", err)
	}

	// A statement that READS the relation still needs `read`: a predicate is
	// how a stored value is observed.
	if _, err := db.Execute(wo, "DELETE FROM "+pmTable+" WHERE id = 9001"); err == nil {
		t.Error("a predicated DELETE ran for a role that may not read the relation")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("refusal is not an authorization refusal: %v", err)
	}

	// And the two decisions agree about this identity, which is the point.
	if err := auth.TableAccess(wo, p, pmTable, auth.ActionRead); err == nil {
		t.Error("TableAccess allowed a read for a role with allow: [write]")
	}
}

// TestADeniedSelectDisclosesNoRuleID (SEC3's P7).
//
// The plan path's refusal carried `: denied by rule "…"`, which tells the
// refused caller the NAME of the control that stopped them and differs from
// the text every other door uses. One text everywhere — the helper's — and the
// rule id goes to the audit log.
func TestADeniedSelectDisclosesNoRuleID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "gate", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{ID: "broad-allow", EffectStr: "allow", Priority: 100,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []auth.Action{auth.ActionRead}},
			{ID: "secret-internal-rule-name", EffectStr: "deny", Priority: 10,
				Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:     []auth.Action{auth.ActionRead},
				Description: "an operator note the caller has no business reading",
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq",
					Value: pmTable}}},
		},
	}}))

	db := pmEmbeddedDB(t, ctx, 0)
	if err := db.SetAuthProvider(p); err != nil {
		t.Fatal(err)
	}
	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: p}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	conn, cerr := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:analyst-key@%s/wadjet?sslmode=disable", pg.Addr()))
	if cerr != nil {
		t.Fatal(cerr)
	}
	defer conn.Close(ctx)

	// Two refusal SITES, and both must say the same thing. The first is the
	// loop over the relations the plan names. The second is the resolver's
	// late `lookup`, which every pass that meets a scan the resolved set never
	// saw asks — a relation reached through an `IN` or `EXISTS` subquery
	// arrives there, not through the loop. Only the loop was gated, so
	// restoring the rule id in `lookup` alone passed the whole arc.
	for _, sql := range []string{
		"SELECT id FROM " + pmTable,
		"SELECT id FROM " + pmOther + " WHERE id IN (SELECT id FROM " + pmTable + ")",
		"SELECT id FROM " + pmOther + " WHERE EXISTS (SELECT 1 FROM " + pmTable +
			" WHERE " + pmTable + ".id = " + pmOther + ".id)",
	} {
		_, qerr := conn.Exec(ctx, sql)
		if qerr == nil {
			t.Errorf("the denied relation was served: %s", sql)
			continue
		}
		var pgErr *pgconn.PgError
		if !errors.As(qerr, &pgErr) {
			t.Errorf("%s: refusal is not a PostgreSQL error: %v", sql, qerr)
			continue
		}
		want := fmt.Sprintf("permission denied for table %q", pmTable)
		if pgErr.Message != want {
			t.Errorf("%s: message = %q, want exactly %q", sql, pgErr.Message, want)
		}
		for _, leak := range []string{"secret-internal-rule-name", "denied by rule",
			"an operator note"} {
			if strings.Contains(pgErr.Message, leak) {
				t.Errorf("%s: refusal discloses %q: %s", sql, leak, pgErr.Message)
			}
		}
	}
}
