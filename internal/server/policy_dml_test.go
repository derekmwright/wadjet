package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/wadjet"
)

// #859 round 1, blocker 3. INSERT / UPDATE / DELETE reached the engine through
// wadjet.DB.ExecuteParsed with NO ABAC of any kind, on the embedded door and
// through pgwire. Measured at f1bf19b7 under an identity whose role allows
// only `read` and whose policy masks `ssn` and denies `salary`:
//
//	DELETE FROM e7emp WHERE ssn = 'true-ssn-01'   DELETE 1 — a probe oracle for
//	                                              the masked value, and
//	                                              destructive, from a READ-only
//	                                              role
//	UPDATE e7emp SET dept = ssn WHERE id = 7      dept becomes the STORED ssn,
//	                                              permanently, in a column the
//	                                              identity may read
//	UPDATE e7emp SET dept='zz' WHERE salary=700009  matches exactly the row with
//	                                              that salary — a predicate on a
//	                                              DENIED column answered from the
//	                                              stored value
//
// The rules are the SELECT door's rules, said for a statement that writes: a
// write needs an ActionWrite grant, a denied column does not exist inside the
// statement, and a masked column reads as its mask.

// dmlProvider builds a provider with three roles over e7emp: `reader` (read
// only, policed), `writer` (read+write, policed) and `admin` (everything, no
// obligations).
func dmlProvider(t *testing.T) *auth.Provider {
	t.Helper()
	obligations := []auth.Obligation{
		{Type: "deny_column", Target: "salary"},
		{Type: "mask_column", Target: "ssn", Value: "'" + pmMaskSSN + "'"},
		{Type: "mask_column", Target: "acct", Value: pmMaskAcct},
	}
	rule := func(id, role string, actions []auth.Action) auth.PolicyRule {
		return auth.PolicyRule{
			ID: id, EffectStr: "allow", Priority: 10,
			Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: role}},
			Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
			Actions:     actions,
			Obligations: obligations,
		}
	}
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "e7-dml", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			rule("reader", "reader", []auth.Action{auth.ActionRead}),
			rule("writer", "writer", []auth.Action{auth.ActionRead, auth.ActionWrite}),
			{
				ID: "admin", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions: []auth.Action{auth.ActionRead, auth.ActionWrite, auth.ActionAdmin,
					auth.ActionCreate, auth.ActionDrop, auth.ActionDescribe},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader", Role: "reader"},
			{Key: "writer-key", Name: "writer", Role: "writer"},
			{Key: "admin-key", Name: "admin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

type dmlDoor struct {
	name string
	// exec runs a statement and returns its command tag or an error.
	exec func(t *testing.T, key, sql string) (string, error)
	// read returns one column of one row, by id, under the ADMIN identity —
	// so the assertion sees what is STORED, not what the policy shows.
	read func(t *testing.T, id int, col string) string
}

func dmlRig(t *testing.T, ctx context.Context) (*wadjet.DB, *auth.Provider, []dmlDoor) {
	t.Helper()
	provider := dmlProvider(t)
	db := pmEmbeddedDB(t, ctx, 0)
	db.SetAuthProvider(provider)

	idCtx := func(key string) context.Context {
		id, err := provider.Authenticator().AuthenticateToken(key)
		if err != nil {
			t.Fatalf("authenticate %q: %v", key, err)
		}
		return auth.ContextWithIdentity(ctx, id)
	}

	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)

	srv := New(Config{Addr: ":0", Catalog: db.Catalog(), Provider: provider}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	stored := func(t *testing.T, id int, col string) string {
		t.Helper()
		res, err := db.Query(idCtx("admin-key"), fmt.Sprintf(
			"SELECT %s AS v FROM %s WHERE id = %d", col, pmTable, id))
		if err != nil {
			t.Fatalf("stored read: %v", err)
		}
		if len(res.Rows) == 0 {
			return "<gone>"
		}
		return fmt.Sprint(res.Rows[0]["v"])
	}

	doors := []dmlDoor{
		{
			name: "embedded",
			exec: func(t *testing.T, key, sql string) (string, error) {
				res, err := db.Execute(idCtx(key), sql)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s %d", res.Command, res.RowsAffected), nil
			},
			read: stored,
		},
		{
			name: "pgwire",
			exec: func(t *testing.T, key, sql string) (string, error) {
				conn, err := pgx.Connect(ctx, fmt.Sprintf(
					"postgres://wadjet:%s@%s/wadjet?sslmode=disable", key, pg.Addr()))
				if err != nil {
					return "", err
				}
				defer conn.Close(ctx)
				tag, err := conn.Exec(ctx, sql)
				if err != nil {
					return "", err
				}
				return tag.String(), nil
			},
			read: stored,
		},
		{
			// The THIRD door. It reaches the same wadjet.DB.ExecuteParsed
			// through Server.handleDML, so it should carry the same
			// enforcement — and a census with two doors cannot say so.
			name: "http",
			exec: func(t *testing.T, key, sql string) (string, error) {
				body, err := json.Marshal(map[string]string{"sql": sql})
				if err != nil {
					return "", err
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost,
					hs.URL+"/v1/queries", bytes.NewReader(body))
				if err != nil {
					return "", err
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+key)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return "", err
				}
				defer resp.Body.Close()
				raw, err := io.ReadAll(resp.Body)
				if err != nil {
					return "", err
				}
				var out struct {
					Rows  []map[string]any `json:"rows"`
					Error string           `json:"error"`
				}
				if uerr := json.Unmarshal(raw, &out); uerr != nil {
					return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
				}
				if out.Error != "" {
					return "", fmt.Errorf("%s", out.Error)
				}
				if resp.StatusCode != http.StatusOK {
					return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
				}
				if len(out.Rows) == 0 {
					return "", fmt.Errorf("HTTP door returned no command tag")
				}
				return fmt.Sprint(out.Rows[0]["result"]), nil
			},
			read: stored,
		},
	}
	return db, provider, doors
}

// TestDMLUnderAColumnPolicyRefusesAWriteItMayNotMake — rule 1.
func TestDMLUnderAColumnPolicyRefusesAWriteItMayNotMake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	_, _, doors := dmlRig(t, ctx)

	for _, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			for _, sql := range []string{
				`DELETE FROM e7emp WHERE ssn = 'true-ssn-01'`,
				`DELETE FROM e7emp WHERE id = 1`,
				`UPDATE e7emp SET dept = 'zz' WHERE id = 1`,
				`INSERT INTO e7emp (id, dept) VALUES (99, 'zz')`,
			} {
				tag, err := door.exec(t, "reader-key", sql)
				if err == nil {
					t.Errorf("a READ-only identity ran %q: %s", sql, tag)
					continue
				}
				if !strings.Contains(err.Error(), "permission denied for table") {
					t.Errorf("%q: %v\n  want 42501 permission denied", sql, err)
				}
			}
			// The row is still there.
			if got := door.read(t, 1, "id"); got != "1" {
				t.Fatalf("row 1 is %s after the refused statements", got)
			}
		})
	}
}

// TestDMLUnderAColumnPolicySeesWhatASelectSees — rule 2, for an identity that
// MAY write. Each case runs on its own row so the doors do not interfere.
func TestDMLUnderAColumnPolicySeesWhatASelectSees(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	_, _, doors := dmlRig(t, ctx)

	for i, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			// Each door gets its own row so a DELETE in one does not change
			// what the other measures.
			row := 5 + i

			// A predicate on a MASKED column compares the MASK, so the stored
			// value matches nothing — the probe oracle is gone.
			tag, err := door.exec(t, "writer-key",
				fmt.Sprintf(`UPDATE e7emp SET dept = 'hit' WHERE ssn = 'true-ssn-%02d'`, row))
			if err != nil {
				t.Fatalf("update on the stored ssn: %v", err)
			}
			if !strings.HasSuffix(strings.TrimSpace(tag), "0") {
				t.Errorf("UPDATE ... WHERE ssn = <stored value> affected rows (%s); "+
					"the predicate must compare the MASK", tag)
			}
			if got := door.read(t, row, "dept"); got == "hit" {
				t.Errorf("row %d was updated by a predicate on the stored ssn", row)
			}

			// The same predicate against the MASK matches.
			if _, err := door.exec(t, "writer-key",
				fmt.Sprintf(`UPDATE e7emp SET dept = 'hit' WHERE ssn = '%s' AND id = %d`,
					pmMaskSSN, row)); err != nil {
				t.Fatalf("update on the mask: %v", err)
			}
			if got := door.read(t, row, "dept"); got != "hit" {
				t.Errorf("row %d dept = %q after a predicate on the mask; want hit", row, got)
			}

			// A masked column READ into a column the identity may read writes
			// the MASK, never the stored value.
			if _, err := door.exec(t, "writer-key",
				fmt.Sprintf(`UPDATE e7emp SET dept = ssn WHERE id = %d`, row)); err != nil {
				t.Fatalf("SET dept = ssn: %v", err)
			}
			if got := door.read(t, row, "dept"); got != pmMaskSSN {
				t.Errorf("SET dept = ssn stored %q; the stored ssn must never reach a "+
					"column the identity can read", got)
			}

			// A DENIED column does not exist: in a predicate, as a SET target,
			// in a SET expression, or as an INSERT target.
			for _, sql := range []string{
				fmt.Sprintf(`UPDATE e7emp SET dept = 'zz' WHERE salary = %d`, 700000+row),
				`UPDATE e7emp SET salary = 1 WHERE id = 1`,
				`UPDATE e7emp SET dept = salary WHERE id = 1`,
				`DELETE FROM e7emp WHERE salary > 0`,
				`INSERT INTO e7emp (id, salary) VALUES (98, 1)`,
			} {
				tag, err := door.exec(t, "writer-key", sql)
				if err == nil {
					t.Errorf("%q ran (%s); a denied column must not resolve", sql, tag)
					continue
				}
				if !strings.Contains(err.Error(), "salary") {
					t.Errorf("%q: %v\n  want an error naming the denied column", sql, err)
				}
			}

			// A DELETE whose predicate names the stored value deletes nothing.
			before := door.read(t, row, "id")
			if _, err := door.exec(t, "writer-key",
				fmt.Sprintf(`DELETE FROM e7emp WHERE ssn = 'true-ssn-%02d'`, row)); err != nil {
				t.Fatalf("delete on the stored ssn: %v", err)
			}
			if got := door.read(t, row, "id"); got != before {
				t.Errorf("row %d was DELETED by a predicate naming the stored ssn", row)
			}
		})
	}
}

// TestDMLUnderNoPolicyIsUnchanged — the control. An identity with no
// obligations writes exactly as before.
func TestDMLUnderNoPolicyIsUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	_, _, doors := dmlRig(t, ctx)

	for i, door := range doors {
		t.Run(door.name, func(t *testing.T) {
			row := 9 + i
			if _, err := door.exec(t, "admin-key",
				fmt.Sprintf(`UPDATE e7emp SET dept = ssn WHERE id = %d`, row)); err != nil {
				t.Fatalf("admin update: %v", err)
			}
			if got := door.read(t, row, "dept"); got != fmt.Sprintf("true-ssn-%02d", row) {
				t.Errorf("an unpoliced identity's SET dept = ssn stored %q, want the stored ssn", got)
			}
			if _, err := door.exec(t, "admin-key",
				fmt.Sprintf(`UPDATE e7emp SET salary = 1 WHERE id = %d`, row)); err != nil {
				t.Errorf("an unpoliced identity must still write the column a POLICY denies: %v", err)
			}
		})
	}
}
