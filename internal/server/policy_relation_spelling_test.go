package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// ---------------------------------------------------------------------------
// #882 — a policy binds to the RELATION, not to a spelling of it.
//
// A relation has two legitimate spellings for one table: the catalog's — the
// spelling a parquet dataset or an Iceberg import brings, where CamelCase is
// ordinary — and the folded one an unquoted reference arrives in, because the
// lexer folds unquoted identifiers (#731). `catalog.ResolveTableName`
// reconciles them, so `FROM Hits` and `FROM hits` read the SAME table.
//
// The policy layer did not reconcile them, and it failed OPEN in both
// directions at once:
//
//   - ABAC matched `resource.name` through the generic attribute comparator
//     (byte-exact `fmt.Sprintf("%v")`). With catalog `Hits` and a rule scoped
//     `resource.name eq "hits"` the scoped rule did not match — and an
//     unmatched scoped rule is not a refusal. The broad allow every
//     `roles:`-to-ABAC migration emits still matched, so the decision came
//     back Allowed with NO obligations: the masked column in plaintext, the
//     denied column present, and a DML predicate on the masked column a
//     working oracle for its own stored value.
//   - the legacy `PolicySet` keyed its map by the YAML's bytes and looked it
//     up with the statement's folded name, so `table: Hits` bound to NOTHING
//     and the row filter was silently absent.
//
// The two directions are OPPOSITE, which is the part that makes this
// unfixable by an operator: on a CamelCase table there was no single spelling
// they could write that bound on both paths. Whichever they chose, one path
// was open.
//
// This gate is the matrix the fix has to satisfy: both policy spellings, both
// enforcement paths, all three doors, read and write.
// ---------------------------------------------------------------------------

const prsTable = "Hits"

func prsSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "Region", Type: parquet.TypeString},
		{Name: "Secret", Type: parquet.TypeString},
		{Name: "Salary", Type: parquet.TypeInt64},
	}}
}

// prsSecret is row 1's stored secret. A statement that can still SELECT it, or
// match it in a predicate, has defeated the mask.
const prsSecret = "true-secret-01"

func prsRows() []map[string]any {
	out := make([]map[string]any, 0, 6)
	for i := 1; i <= 6; i++ {
		reg := "us"
		if i%2 == 0 {
			reg = "eu"
		}
		out = append(out, map[string]any{
			"WatchID": int64(i),
			"Region":  reg,
			"Secret":  fmt.Sprintf("true-secret-%02d", i),
			"Salary":  int64(700000 + i),
		})
	}
	return out
}

// prsProvider builds the shape a real deployment has: one rule SCOPED to the
// relation carrying the obligations, beside the BROAD allow that every
// `roles:`-to-ABAC migration emits. The broad allow is what turns a scoped
// rule that fails to match into a silent grant rather than a refusal, so it is
// load-bearing for this gate — without it the failure mode is a availability
// break (default deny), which is loud, instead of a disclosure, which is not.
func prsProvider(t *testing.T, resourceName string) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "prs", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "prs-scoped", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: resourceName}},
				Actions:   []auth.Action{auth.ActionRead, auth.ActionWrite},
				Obligations: []auth.Obligation{
					{Type: "deny_column", Target: "Salary"},
					{Type: "mask_column", Target: "Secret", Value: "'***'"},
				},
			},
			{
				ID: "prs-broad", EffectStr: "allow", Priority: 20,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}}},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

type prsRig struct {
	db       *wadjet.DB
	ctxAuth  context.Context
	pgAddr   string
	httpBase string
}

func prsUp(t *testing.T, ctx context.Context, provider *auth.Provider, legacy *auth.PolicySet) prsRig {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	// Through the CATALOG so the CamelCase spelling survives: the DDL door
	// folds a name it MINTS, so a CamelCase relation is one a dataset brought.
	if err := db.Catalog().CreateTable(ctx, prsTable, prsSchema(), nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester(prsTable, prsSchema(), nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	if err := ing.Ingest(ctx, prsRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	db.SetAuthProvider(provider)

	id, err := provider.Authenticator().AuthenticateToken("analyst-key")
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	actx := auth.ContextWithIdentity(ctx, id)

	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, logger)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)

	h := New(Config{Addr: ":0", Catalog: db.Catalog(), Provider: provider, Policies: legacy}, logger)
	hs := httptest.NewServer(h.Mux())
	t.Cleanup(hs.Close)

	return prsRig{db: db, ctxAuth: actx, pgAddr: pg.Addr(), httpBase: hs.URL}
}

// prsAnswer renders one door's answer as text. The three doors are three
// different transports and this census is about what each DISCLOSES, so the
// rendering is deliberately coarse: what matters is whether the plaintext
// secret or the denied salary appears in it at all.
func prsEmbedded(r prsRig, sql string) string {
	res, err := r.db.Query(r.ctxAuth, sql)
	if err != nil {
		return "ERR: " + err.Error()
	}
	s := fmt.Sprintf("cols=%v ", res.Columns)
	for i := range res.Rows {
		s += fmt.Sprintf("%v", res.Cells(i))
	}
	return s
}

func prsExec(r prsRig, sql string) string {
	res, err := r.db.Execute(r.ctxAuth, sql)
	if err != nil {
		return "ERR: " + err.Error()
	}
	return fmt.Sprintf("OK rows=%d", res.RowsAffected)
}

func prsPG(ctx context.Context, r prsRig, sql string) string {
	dsn := fmt.Sprintf("postgres://wadjet:analyst-key@%s/wadjet?sslmode=disable", r.pgAddr)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "CONNERR: " + err.Error()
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return "ERR: " + err.Error()
	}
	defer rows.Close()
	var cols []string
	for _, fd := range rows.FieldDescriptions() {
		cols = append(cols, fd.Name)
	}
	s := fmt.Sprintf("cols=%v ", cols)
	for rows.Next() {
		v, _ := rows.Values()
		s += fmt.Sprintf("%v", v)
	}
	if err := rows.Err(); err != nil {
		return "ERR: " + err.Error()
	}
	return s
}

func prsPGExec(ctx context.Context, r prsRig, sql string) string {
	dsn := fmt.Sprintf("postgres://wadjet:analyst-key@%s/wadjet?sslmode=disable", r.pgAddr)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "CONNERR: " + err.Error()
	}
	defer conn.Close(ctx)
	tag, err := conn.Exec(ctx, sql)
	if err != nil {
		return "ERR: " + err.Error()
	}
	return "OK " + tag.String()
}

func prsHTTP(ctx context.Context, r prsRig, sql string) string {
	body, _ := json.Marshal(map[string]string{"sql": sql})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.httpBase+"/v1/queries", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer analyst-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "ERR: " + err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return fmt.Sprintf("[%d] %s", resp.StatusCode, string(b))
}

// prsDoors runs one statement on all three doors and returns their answers,
// labelled. A policy that binds must bind on every door: the enforcement is
// plan-time and door-independent by construction (#859), and a census that
// checks one door cannot see a door that lost it.
func prsDoors(ctx context.Context, r prsRig, sql string, write bool) map[string]string {
	if write {
		return map[string]string{
			"embedded": prsExec(r, sql),
			"pgwire":   prsPGExec(ctx, r, sql),
			"http":     prsHTTP(ctx, r, sql),
		}
	}
	return map[string]string{
		"embedded": prsEmbedded(r, sql),
		"pgwire":   prsPG(ctx, r, sql),
		"http":     prsHTTP(ctx, r, sql),
	}
}

// prsSpellings are the two spellings of ONE relation an operator may write in
// a policy. Both must bind: the catalog's own, and the folded one — which is
// what an unquoted reference to that relation IS, and therefore the spelling
// an operator reading their own query is most likely to copy.
var prsSpellings = []struct{ name, resource string }{
	{"policy names the catalog spelling", "Hits"},
	{"policy names the folded spelling", "hits"},
}

// TestPolicyBindsToTheRelationNotToASpellingOfIt is #882's gate.
func TestPolicyBindsToTheRelationNotToASpellingOfIt(t *testing.T) {
	for _, sp := range prsSpellings {
		t.Run(sp.name, func(t *testing.T) {
			ctx := context.Background()
			r := prsUp(t, ctx, prsProvider(t, sp.resource), nil)

			t.Run("read doors mask and deny", func(t *testing.T) {
				// Both spellings of the relation IN THE STATEMENT too: the
				// policy must bind however the query names the table.
				for _, sql := range []string{
					`SELECT * FROM Hits ORDER BY WatchID`,
					`SELECT * FROM hits ORDER BY WatchID`,
				} {
					for door, got := range prsDoors(ctx, r, sql, false) {
						if strings.Contains(got, prsSecret) {
							t.Errorf("[%s] %s DISCLOSED the masked column in plaintext:\n  %s", door, sql, got)
						}
						if strings.Contains(got, "700001") || strings.Contains(strings.ToLower(got), "salary") {
							t.Errorf("[%s] %s returned the DENIED column:\n  %s", door, sql, got)
						}
						if !strings.Contains(got, "***") {
							t.Errorf("[%s] %s did not carry the mask:\n  %s", door, sql, got)
						}
					}
				}
			})

			t.Run("a denied column cannot be selected", func(t *testing.T) {
				for door, got := range prsDoors(ctx, r, `SELECT Salary FROM Hits`, false) {
					if !strings.Contains(got, "ERR") && !strings.Contains(got, "[4") {
						t.Errorf("[%s] SELECT of a denied column was answered:\n  %s", door, got)
					}
					if strings.Contains(got, "700001") {
						t.Errorf("[%s] SELECT of a denied column returned values:\n  %s", door, got)
					}
				}
			})

			// The write half. ADR-0033 rule 2: a read INSIDE a statement that
			// writes sees what a SELECT would see.
			for _, tc := range []struct{ name, sql string }{
				{"UPDATE a denied column", `UPDATE Hits SET Salary = 1 WHERE WatchID = 1`},
				{"DELETE on a denied column", `DELETE FROM hits WHERE Salary = 700001`},
				{"INSERT into a denied column", `INSERT INTO hits (WatchID, Region, Secret, Salary) VALUES (99, 'us', 's', 1)`},
				{"MERGE setting a denied column", `MERGE INTO hits USING (SELECT 1 AS k) s ON hits.WatchID = s.k ` +
					`WHEN MATCHED THEN UPDATE SET Salary = 5`},
			} {
				t.Run(tc.name+" is refused", func(t *testing.T) {
					for door, got := range prsDoors(ctx, r, tc.sql, true) {
						if strings.HasPrefix(got, "OK") || strings.HasPrefix(got, "[200") {
							t.Errorf("[%s] %s was PERMITTED on a denied column:\n  %s", door, tc.sql, got)
						}
					}
				})
			}

			t.Run("a masked column is not an oracle for its own value", func(t *testing.T) {
				// The disclosure ADR-0033 rule 2 exists to close: with the
				// policy bound the predicate compares the MASK, so it matches
				// nothing; unbound it compares the STORED secret and matches
				// exactly the row that holds it.
				sql := `UPDATE Hits SET Region = 'zz' WHERE Secret = '` + prsSecret + `'`
				for door, got := range prsDoors(ctx, r, sql, true) {
					if strings.Contains(got, "rows=1") || strings.Contains(got, "UPDATE 1") {
						t.Errorf("[%s] a masked column matched its STORED value — the mask is "+
							"a working oracle:\n  %s", door, got)
					}
				}
			})
		})
	}

	// The counter-cell. The concession is CASE, not spelling: a rule scoped to
	// some other relation must stay unbound, or the fix would have made every
	// policy global — which would read as "all the gates pass" while meaning
	// the enforcement no longer targets anything.
	t.Run("a policy scoped to a different relation does not bind", func(t *testing.T) {
		ctx := context.Background()
		r := prsUp(t, ctx, prsProvider(t, "SomeOtherTable"), nil)
		got := prsEmbedded(r, `SELECT * FROM Hits ORDER BY WatchID`)
		if !strings.Contains(got, prsSecret) {
			t.Fatalf("a policy naming ANOTHER relation bound to this one: %s", got)
		}
	})
}

// TestLegacyPolicySetBindsToTheRelationNotToASpellingOfIt is the same question
// for the non-ABAC deployment: a `policies:` YAML block with no `roles:` and
// no `abac_policies:`, which the HTTP door enforces through `PolicySet.Lookup`.
//
// This path failed open in the OPPOSITE direction to the ABAC one — the YAML's
// bytes were the map key and the statement's folded name was the lookup — so
// before the fix there was no single `table:` spelling that bound on both.
func TestLegacyPolicySetBindsToTheRelationNotToASpellingOfIt(t *testing.T) {
	for _, spelling := range []string{"Hits", "hits"} {
		t.Run("policy table: "+spelling, func(t *testing.T) {
			ctx := context.Background()
			ps, err := auth.ParsePolicies([]auth.PolicyConfig{{
				Table: spelling, Role: "analyst", RowFilter: "Region = 'us'",
			}})
			if err != nil {
				t.Fatal(err)
			}
			authn, authz := auth.New(auth.Config{
				Enabled: true,
				APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
				Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
			})
			provider := auth.NewProvider(authn, authz, ps, nil)
			r := prsUp(t, ctx, provider, ps)

			got := prsHTTP(ctx, r, `SELECT WatchID, Region FROM Hits ORDER BY WatchID`)
			// Region='us' keeps rows 1, 3, 5 of six.
			if strings.Contains(got, `"region":"eu"`) {
				t.Fatalf("the row filter was NOT applied — every row came back:\n  %s", got)
			}
			if !strings.Contains(got, "Filter:") {
				t.Fatalf("the plan carries no Filter node, so no row filter bound:\n  %s", got)
			}
		})
	}
}
