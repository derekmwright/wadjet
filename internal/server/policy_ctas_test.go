package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A CTAS reads through the POLICED list on every door (#1024, ADR-0034).
//
// This is the one shape where a masking defect is not a leaked ANSWER but a
// leaked TABLE: the statement's rows are written to storage under a new name
// that carries no policy of its own, so a denied column that reaches the
// writer is disclosed permanently, to everyone, and no later policy can take
// it back. It is the reason the SELECT half of a query-sourced write goes
// through the ordinary planner — `db.Query` — rather than any shortcut: the
// security projection is applied at the scan (ADR-0033) and the declared
// output the new table is built from is what that plan publishes, not what the
// catalog holds (#994's rule).
//
// The census runs the same statement on all NINE doors and records the
// disposition per door: the doors that RUN writes must mask and deny, and the
// native-DAG doors must REFUSE — the coordinator runs no writes at all, which
// is this arc's recorded boundary — and having refused must leave no table.
func TestACreateTableAsSelectReadsThroughThePolicedList(t *testing.T) {
	ctx := context.Background()
	provider := ctasPolicyProvider(t)
	rig := pmRigUpWith(t, ctx, provider)

	ran, refused := 0, 0
	for i, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			dst := fmt.Sprintf("ctas_pol_%d", i)
			_, err := door.run(t, "writer-key",
				fmt.Sprintf("CREATE TABLE %s AS SELECT id, ssn, acct FROM %s", dst, pmTable))
			if err != nil {
				// A door that does not run writes refuses BY NAME, and the
				// refusal is the whole answer: nothing was created.
				refused++
				if got := sqlerr.StateOf(err); got != "0A000" {
					t.Errorf("refused %s: %v; a door that runs no writes refuses 0A000", got, err)
				}
				if !strings.Contains(err.Error(), "CREATE TABLE AS SELECT") {
					t.Errorf("the refusal does not name the statement: %v", err)
				}
				if _, rerr := door.run(t, "writer-key", "SELECT id FROM "+dst); rerr == nil {
					t.Errorf("the refused statement created %q anyway", dst)
				}
				return
			}
			ran++

			// The written table is read back through the SAME door, by the
			// ADMIN, whose identity no obligation covers: if the mask had been
			// applied only at the read, the admin would see the stored value.
			// What the admin sees IS what was stored.
			got, err := door.run(t, "admin-key", "SELECT id, ssn, acct FROM "+dst)
			if err != nil {
				t.Fatalf("reading the created table back: %v", err)
			}
			if len(got.rows) != pmRows {
				t.Fatalf("the created table holds %d rows, want %d", len(got.rows), pmRows)
			}
			for _, row := range got.rows {
				if v := row["ssn"]; v != pmMaskSSN {
					t.Errorf("ssn stored as %q, want the mask %q — the writer saw the "+
						"stored value", v, pmMaskSSN)
				}
				if v := row["acct"]; v != pmMaskAcct {
					t.Errorf("acct stored as %q, want the mask %q", v, pmMaskAcct)
				}
			}

			// And the DENIED column does not exist in the new table at all:
			// it never reached the declared output, so there is nothing to
			// select.
			deny := fmt.Sprintf("ctas_deny_%d", i)
			if _, err := door.run(t, "writer-key",
				fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", deny, pmTable)); err != nil {
				t.Fatalf("CTAS over a star of the policed table: %v", err)
			}
			star, err := door.run(t, "admin-key", "SELECT * FROM "+deny)
			if err != nil {
				t.Fatalf("reading the star copy back: %v", err)
			}
			for _, c := range star.cols {
				if c == "salary" {
					t.Errorf("the created table has a %q column; a denied column must never land", c)
				}
			}
			for _, row := range star.rows {
				for c, v := range row {
					if strings.HasPrefix(v, "true-") {
						t.Errorf("column %q holds the STORED value %q; the mask did not reach the writer", c, v)
					}
					if strings.HasPrefix(v, "7000") {
						t.Errorf("column %q holds a denied salary value %q", c, v)
					}
				}
			}
		})
	}
	if ran == 0 {
		t.Fatalf("no door ran the statement; the census proves nothing")
	}
	if refused == 0 {
		t.Fatalf("no door refused it; the native-DAG doors run no writes and this census "+
			"asserts that boundary (ran=%d)", ran)
	}
	t.Logf("nine-door census: %d doors ran the write, %d refused it", ran, refused)
}

// An identity that may not write is refused BEFORE the query runs, and an
// identity with no read on the source is refused by the query.
func TestAQuerySourcedWriteAuthorizesBeforeItActs(t *testing.T) {
	ctx := context.Background()
	provider := ctasPolicyProvider(t)
	rig := pmRigUpWith(t, ctx, provider)

	for _, door := range rig.doors {
		if !strings.HasPrefix(door.name, "embedded/s") && !strings.HasPrefix(door.name, "pgwire") &&
			!strings.HasPrefix(door.name, "http") {
			continue // the native-DAG doors refuse every write; covered above
		}
		t.Run(door.name, func(t *testing.T) {
			// `analyst-key` holds read and NOT write.
			_, err := door.run(t, "analyst-key", "CREATE TABLE nope AS SELECT id FROM "+pmTable)
			if err == nil {
				t.Fatal("a read-only identity created a table")
			}
			// The CLASS where the door carries one. This rig's HTTP runner
			// rebuilds the refusal from the response's `error` string alone
			// and drops `sqlstate`, so the assertion there is on the MESSAGE
			// — the HTTP door's own class mapping is gated by the two-door
			// SQLSTATE census, not by this harness.
			if got := sqlerr.StateOf(err); got != "42501" && got != "" {
				t.Errorf("SQLSTATE %q, want 42501: %v", got, err)
			}
			if !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("the refusal is not the permission gate's: %v", err)
			}
			if _, rerr := door.run(t, "admin-key", "SELECT id FROM nope"); rerr == nil {
				t.Error("the refused statement created the table anyway")
			}
		})
	}
}

// ctasPolicyProvider is pmProvider's sibling with one addition: a `writer`
// role that may READ the policed table under the same obligations AND WRITE.
//
// pmProvider's analyst holds `read` alone, which is right for a masking census
// and useless for a write census: every statement would be refused at the
// permission gate before the mask could be tested.
func ctasPolicyProvider(t *testing.T) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "ctas-policy", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "writer-masks", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "writer"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "deny_column", Target: "salary"},
					{Type: "mask_column", Target: "ssn", Value: "'" + pmMaskSSN + "'"},
					{Type: "mask_column", Target: "acct", Value: pmMaskAcct},
				},
			},
			{
				// Everything the writer creates is its own to read and write.
				ID: "writer-open", EffectStr: "allow", Priority: 5,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "writer"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
			{
				ID: "analyst-read", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []auth.Action{auth.ActionRead},
			},
			{
				ID: "admin-raw", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "writer-key", Name: "writer", Role: "writer"},
			{Key: "analyst-key", Name: "analyst", Role: "analyst"},
			{Key: "admin-key", Name: "admin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}
