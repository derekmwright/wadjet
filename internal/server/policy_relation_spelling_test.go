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

// prsOther is a second, real relation — see prsUp.
const prsOther = "Other"

// prsExpectBindRefusal reports whether this provider is one the unbindable
// cells deliberately built, so prsUp can attach it and let the DOORS report
// the refusal rather than failing the fixture.
func prsExpectBindRefusal(p *auth.Provider) bool { return p.BindError() != nil }

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
	// A SECOND, real relation. The counter-cell needs a policy scoped
	// somewhere that EXISTS: since the bind refuses a name the catalog does
	// not hold, "scoped to another relation" can only be tested against a
	// relation there actually is.
	if err := db.Catalog().CreateTable(ctx, prsOther, prsSchema(), nil); err != nil {
		t.Fatalf("create %s: %v", prsOther, err)
	}
	oing := db.NewIngester(prsOther, prsSchema(), nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	if err := oing.Ingest(ctx, prsRows()); err != nil {
		t.Fatal(err)
	}
	if err := oing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	// Attaching the provider is what BINDS its names to this catalog. The
	// harness does it exactly the way a door does, which is the half round 2
	// found missing: a gate whose harness does not attach the way production
	// attaches cannot see what production does.
	if err := db.SetAuthProvider(provider); err != nil && !prsExpectBindRefusal(provider) {
		t.Fatalf("attaching the policy set: %v", err)
	}

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

// prsUnbindableSpellings are the spellings that name NO relation. Each is
// neither the catalog's nor the folded one, so the fold-aware comparison — the
// floor, which reconciles exactly that one pair — cannot reach them; they are
// the bind's job, and the bind must REFUSE them.
//
// This is the shape #882 round 2 found still open: with binding wired into two
// `serve` call sites and nowhere else, a policy set installed through the
// embedded API or through a server the caller stands up itself kept only the
// floor, and every spelling below yielded "no policy applies" — which beside
// the broad allow is a grant. `HITS` and `hItS` are delimited-looking names
// that a reasonable operator writes; `Hitz` is the plain typo. All three must
// be refused, and refused the same way, because to the catalog they are the
// same thing: a name it does not hold.
var prsUnbindableSpellings = []struct{ name, resource string }{
	{"an upper-case spelling of the relation", "HITS"},
	{"a mixed-case spelling that is neither", "hItS"},
	{"a plain typo", "Hitz"},
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
	// a DIFFERENT relation — one that exists, so the set binds — must not
	// reach this one, or the fix would have made every policy global, which
	// would read as "all the gates pass" while meaning the enforcement no
	// longer targets anything.
	t.Run("a policy scoped to a different relation does not reach this one", func(t *testing.T) {
		ctx := context.Background()
		r := prsUp(t, ctx, prsProvider(t, prsOther), nil)
		got := prsEmbedded(r, `SELECT * FROM Hits ORDER BY WatchID`)
		if strings.Contains(got, "ERR") {
			t.Fatalf("a policy scoped to another EXISTING relation refused this query: %s", got)
		}
		if !strings.Contains(got, prsSecret) {
			t.Fatalf("a policy naming ANOTHER relation was applied to this one: %s", got)
		}
		// ...and it DOES reach the relation it names.
		other := prsEmbedded(r, `SELECT * FROM Other ORDER BY WatchID`)
		if strings.Contains(other, prsSecret) {
			t.Fatalf("the policy did not apply to the relation it names: %s", other)
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

// TestAnAttachedPolicySetThatCannotBindRefusesEveryQuery is B1(r2)'s gate.
//
// ADR-0033 rule 3 says a policy naming a relation that does not resolve is
// refused AT LOAD, and rule 4 that there is no code path where a spelling
// mismatch yields "no policy applies". Round 2 measured otherwise: the bind
// was wired into `runStandalone` and `runCoordinator` and nowhere else, so an
// embedded caller — and this file's own harness — ran on the fold-aware
// comparison alone. That floor reconciles the catalog spelling with the folded
// one and NOTHING else, so a policy spelled `HITS` or `hItS` against a catalog
// `Hits` matched nothing and returned the masked column in plaintext with the
// denied column writable, on all three doors.
//
// Binding is now a property of ATTACHING a set to a catalog, so this harness
// binds exactly as production does — which is the other half of the finding:
// a gate whose harness does not attach the way production attaches cannot see
// what production does.
func TestAnAttachedPolicySetThatCannotBindRefusesEveryQuery(t *testing.T) {
	for _, sp := range prsUnbindableSpellings {
		t.Run(sp.name, func(t *testing.T) {
			ctx := context.Background()
			r := prsUp(t, ctx, prsProvider(t, sp.resource), nil)

			// Every door refuses, and nothing leaks on the way to refusing.
			for _, sql := range []string{
				`SELECT * FROM Hits ORDER BY WatchID`,
				`SELECT Salary FROM Hits`,
			} {
				for door, got := range prsDoors(ctx, r, sql, false) {
					if strings.Contains(got, prsSecret) {
						t.Errorf("[%s] %s DISCLOSED the masked column in plaintext under an "+
							"unbindable policy:\n  %s", door, sql, got)
					}
					if strings.Contains(got, "700001") {
						t.Errorf("[%s] %s returned the DENIED column under an unbindable "+
							"policy:\n  %s", door, sql, got)
					}
					if !strings.Contains(got, "ERR") && !strings.Contains(got, "[4") {
						t.Errorf("[%s] %s was ANSWERED under a policy set that could not be "+
							"bound:\n  %s", door, sql, got)
					}
				}
			}
			for _, sql := range []string{
				`UPDATE Hits SET Salary = 1 WHERE WatchID = 1`,
				`UPDATE Hits SET Region = 'zz' WHERE Secret = '` + prsSecret + `'`,
				`DELETE FROM hits WHERE Salary = 700001`,
			} {
				for door, got := range prsDoors(ctx, r, sql, true) {
					if strings.HasPrefix(got, "OK") || strings.HasPrefix(got, "[200") {
						t.Errorf("[%s] %s was PERMITTED under a policy set that could not be "+
							"bound:\n  %s", door, sql, got)
					}
				}
			}
		})
	}

	t.Run("the refusal names the relation it could not resolve", func(t *testing.T) {
		ctx := context.Background()
		r := prsUp(t, ctx, prsProvider(t, "Hitz"), nil)
		got := prsEmbedded(r, `SELECT WatchID FROM Hits`)
		if !strings.Contains(got, "Hitz") {
			t.Fatalf("the refusal does not name the unresolvable relation: %s", got)
		}
	})

	t.Run("a bindable set still answers", func(t *testing.T) {
		// The control: the refusal must be caused by the unbindable NAME and
		// not by the guard refusing everything.
		ctx := context.Background()
		r := prsUp(t, ctx, prsProvider(t, "hits"), nil)
		got := prsEmbedded(r, `SELECT * FROM Hits ORDER BY WatchID`)
		if strings.Contains(got, "ERR") {
			t.Fatalf("a bindable policy set refused the query: %s", got)
		}
		if !strings.Contains(got, "***") {
			t.Fatalf("a bindable policy set did not mask: %s", got)
		}
	})
}

// TestTheEmbeddedAttachBindsOnItsOwn isolates ONE attach site.
//
// The three doors share one *auth.Provider, so a bind performed at any of them
// marks it bound for all — good defence in depth, and it means the matrix
// above cannot attribute its result to a single call site. This cell builds
// only the embedded DB: no pgwire server, no HTTP server, so
// `wadjet.DB.SetAuthProvider` is the only attach that could have bound. Revert
// its bind and this fails while the matrix above still passes.
func TestTheEmbeddedAttachBindsOnItsOwn(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Catalog().CreateTable(ctx, prsTable, prsSchema(), nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester(prsTable, prsSchema(), nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	if err := ing.Ingest(ctx, prsRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	provider := prsProvider(t, "HITS") // neither the catalog spelling nor the folded one
	attachErr := db.SetAuthProvider(provider)
	if attachErr == nil {
		t.Fatal("attaching a policy set that names no relation returned no error — " +
			"the attach did not bind")
	}
	if provider.BindError() == nil {
		t.Fatal("the failed attach was not remembered, so enforcement would run unbound")
	}

	id, err := provider.Authenticator().AuthenticateToken("analyst-key")
	if err != nil {
		t.Fatal(err)
	}
	actx := auth.ContextWithIdentity(ctx, id)
	if _, err := db.Query(actx, `SELECT * FROM Hits ORDER BY WatchID`); err == nil {
		t.Fatal("the embedded door ANSWERED under a policy set that could not be bound")
	}
	if _, err := db.Execute(actx, `UPDATE Hits SET Salary = 1 WHERE WatchID = 1`); err == nil {
		t.Fatal("the embedded door PERMITTED a write under a policy set that could not be bound")
	}
}

// TestTheHTTPServerAttachBindsOnItsOwn isolates the OTHER attach site.
//
// `prsUp` installs ONE provider through three entry points —
// `wadjet.DB.SetAuthProvider`, `pgwire.NewServer` and `server.New` — so a bind
// performed at any of them marks it bound for all three doors, and the matrix
// above therefore cannot attribute its result to a call site.
// `TestTheEmbeddedAttachBindsOnItsOwn` pins the embedded attach; this pins
// `server.New`, which is what a caller standing the HTTP door up over a
// catalog it already holds uses. The DB here NEVER sees the provider, so the
// constructor is the only attach that could have bound: revert its bind and
// the unbindable cells below disclose while the matrix above still passes.
//
// The READ half is the isolating one — `handleQuery` enforces with this
// server's own provider. The write half is asserted because that is where the
// disclosure was measured, but it does not attribute: the DML door attaches
// its own `wadjet.Attach`ed DB to the same provider, so it binds by the same
// rule at a different call site.
func TestTheHTTPServerAttachBindsOnItsOwn(t *testing.T) {
	for _, tc := range []struct {
		name, resource string
		bindable       bool
	}{
		{"the catalog spelling binds", prsTable, true},
		{"the folded spelling binds", "hits", true},
		{"an upper-case spelling of the relation refuses at attach", "HITS", false},
		{"a mixed-case spelling that is neither refuses at attach", "hItS", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
			db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { db.Close() })
			if err := db.Catalog().CreateTable(ctx, prsTable, prsSchema(), nil); err != nil {
				t.Fatal(err)
			}
			ing := db.NewIngester(prsTable, prsSchema(), nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
			if err := ing.Ingest(ctx, prsRows()); err != nil {
				t.Fatal(err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}

			provider := prsProvider(t, tc.resource)
			// Deliberately NOT db.SetAuthProvider: this cell is about the
			// constructor.
			h := New(Config{Addr: ":0", Catalog: db.Catalog(), Provider: provider}, logger)
			hs := httptest.NewServer(h.Mux())
			t.Cleanup(hs.Close)

			// Errorf, not Fatalf: when the constructor does not bind, the
			// interesting failure is the one the doors report next.
			if bound := provider.BindError() == nil; bound != tc.bindable {
				t.Errorf("after server.New the set is bound=%v, want %v (bind error: %v)",
					bound, tc.bindable, provider.BindError())
			}
			r := prsRig{httpBase: hs.URL}

			got := prsHTTP(ctx, r, `SELECT * FROM Hits ORDER BY WatchID`)
			if strings.Contains(got, prsSecret) {
				t.Errorf("the HTTP door DISCLOSED the masked column in plaintext:\n  %s", got)
			}
			if strings.Contains(got, "700001") {
				t.Errorf("the HTTP door returned the DENIED column:\n  %s", got)
			}
			if tc.bindable {
				if !strings.Contains(got, "***") {
					t.Errorf("a bindable set attached at server.New did not mask:\n  %s", got)
				}
			} else if !strings.Contains(got, "[4") {
				t.Errorf("the HTTP door ANSWERED under a policy set that could not be bound:\n  %s", got)
			}

			for _, sql := range []string{
				`UPDATE Hits SET Salary = 1 WHERE WatchID = 1`,
				`DELETE FROM Hits WHERE Salary = 700001`,
			} {
				if w := prsHTTP(ctx, r, sql); strings.HasPrefix(w, "[200") {
					t.Errorf("the HTTP door PERMITTED %s on a denied column:\n  %s", sql, w)
				}
			}
			oracle := prsHTTP(ctx, r, `UPDATE Hits SET Region = 'zz' WHERE Secret = '`+prsSecret+`'`)
			if strings.Contains(oracle, "UPDATE 1") {
				t.Errorf("a masked column matched its STORED value through the HTTP door — "+
					"the mask is a working oracle:\n  %s", oracle)
			}
		})
	}
}
