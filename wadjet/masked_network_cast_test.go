// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestACastOverAPolicedColumnAnswersTheMaskOrSaysWhyNot pins what an ABAC
// column policy does to a network column now that a CAST to a network type
// PARSES its operand (#1092).
//
// No value leaks in either direction, and this gate asserts that first: a
// `deny_column` is 42703 at every door including the two that WRITE, and a
// mask whose text IS a value of the type flows through every door unchanged
// and never lets the real address out. What CHANGED when the cast started
// parsing is the third row: a mask whose text is NOT a value of the type —
// `'***'` — stops a cast that answered the mask before. PostgreSQL agrees
// with the refusal (`'***'::inet` is 22P02), so the semantics are right and
// the cost is that a report which casts a policed column stops answering.
//
// The structural fix is at the POLICY boundary, not here: a mask that cannot
// produce a value of the column's declared type is an unenforceable mask, and
// `auth.plan_enforce` already refuses other kinds. Making the cast hand a
// non-value back instead would be exactly the pass-through #1092 closed, so
// the behaviour is pinned rather than patched, and the filing candidate is in
// the landing notes. ADR-0012 records it.
func TestACastOverAPolicedColumnAnswersTheMaskOrSaysWhyNot(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T, obligation string) (*DB, context.Context) {
		t.Helper()
		db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64, Nullable: true},
			{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
		}}
		for _, name := range []string{"t", "t2"} {
			if err := db.CreateTable(ctx, name, schema, nil); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Query(ctx, "INSERT INTO t (id, ip) VALUES (1, '10.0.0.1')"); err != nil {
			t.Fatal(err)
		}
		var obl []auth.Obligation
		switch obligation {
		case "mask":
			obl = []auth.Obligation{{Type: "mask_column", Target: "ip", Value: "'***'"}}
		case "mask-address":
			obl = []auth.Obligation{{Type: "mask_column", Target: "ip", Value: "'0.0.0.0'"}}
		case "deny":
			obl = []auth.Obligation{{Type: "deny_column", Target: "ip"}}
		}
		ev := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
			Name: "p", Version: 1, Enabled: true,
			Rules: []auth.PolicyRule{{
				ID: "r", EffectStr: "allow", Priority: 10,
				Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:     []auth.Action{auth.ActionRead, auth.ActionWrite},
				Obligations: obl,
			}},
		}})
		authn, authz := auth.New(auth.Config{
			Enabled: true,
			APIKeys: []auth.APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
			Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}}},
		})
		p := auth.NewProvider(authn, authz, nil, nil)
		p.UpdateWithEvaluator(authn, authz, nil, ev)
		db.SetAuthProvider(p)
		return db, auth.ContextWithIdentity(ctx, &auth.Identity{Name: "analyst", Role: "analyst",
			Method: "apikey", Tables: []string{"*"}, Perms: []string{"read", "write"}})
	}

	// The nine doors a policed column can reach through a CAST, plus the two
	// that do not cast, so a leak would show whichever way it came.
	doors := []struct{ name, sql, read string }{
		{"project", "SELECT ip AS v FROM t", ""},
		{"project-cast", "SELECT CAST(ip AS IPV4) AS v FROM t", ""},
		{"project-cast-text", "SELECT CAST(CAST(ip AS IPV4) AS TEXT) AS v FROM t", ""},
		{"where", "SELECT count(*) AS v FROM t WHERE ip = '10.0.0.1'", ""},
		{"where-cast", "SELECT count(*) AS v FROM t WHERE CAST(ip AS IPV4) = '10.0.0.1'", ""},
		{"group", "SELECT CAST(ip AS IPV4) AS v, count(*) AS n FROM t GROUP BY CAST(ip AS IPV4)", ""},
		{"order", "SELECT CAST(ip AS IPV4) AS v FROM t ORDER BY CAST(ip AS IPV4)", ""},
		{"join", "SELECT CAST(a.ip AS IPV4) AS v FROM t a JOIN t b ON CAST(a.ip AS IPV4) = CAST(b.ip AS IPV4)", ""},
		{"ctas-cast", "CREATE TABLE u AS SELECT CAST(ip AS IPV4) AS v FROM t", "SELECT v FROM u"},
		{"insert-select-cast", "INSERT INTO t2 (id, ip) SELECT id, CAST(ip AS IPV4) FROM t",
			"SELECT ip AS v FROM t2"},
	}

	for _, c := range []struct {
		obligation string
		// want is the value every answering door publishes; "" means the door
		// is expected to refuse with state.
		want  string
		state string
		// answering doors, when only some answer.
		refuse map[string]bool
	}{
		{obligation: "none", want: "10.0.0.1"},
		{obligation: "mask-address", want: "0.0.0.0"},
		{obligation: "deny", state: "42703"},
		{obligation: "mask", want: "***", state: "22P02", refuse: map[string]bool{
			"project-cast": true, "project-cast-text": true, "where-cast": true,
			"group": true, "order": true, "join": true,
			"ctas-cast": true, "insert-select-cast": true,
		}},
	} {
		for _, d := range doors {
			t.Run(c.obligation+"/"+d.name, func(t *testing.T) {
				db, actx := open(t, c.obligation)
				res, err := db.Query(actx, d.sql)
				if err == nil && d.read != "" {
					res, err = db.Query(actx, d.read)
				}
				refuses := c.want == "" || (c.refuse != nil && c.refuse[d.name])
				if refuses {
					if err == nil {
						t.Fatalf("%s answered %v; want %s", d.sql, res.Rows, c.state)
					}
					if st := sqlerr.StateOf(err); st != c.state {
						t.Errorf("SQLSTATE %q, want %q (%v)", st, c.state, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s refused: %v", d.sql, err)
				}
				// A counting door answers a number; the rest answer the
				// column. Either way the REAL address must not appear unless
				// the policy allows it.
				got := ntCellText(res)
				if strings.HasPrefix(d.name, "where") {
					return
				}
				if got != c.want {
					t.Errorf("%s = %q, want %q", d.sql, got, c.want)
				}
				if c.obligation != "none" && got == "10.0.0.1" {
					t.Fatalf("%s published the policed value", d.sql)
				}
			})
		}
	}
}
