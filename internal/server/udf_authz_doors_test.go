package server

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// #940 + #942. UDF mutation had TWO decisions and both were wrong.
//
// `wadjet.DB.Query` — the boundary the embedded caller and pgwire reach —
// registered with an EMPTY owner and the literal `isAdmin = true`, and asked
// for no permission; the HTTP handlers asked for no permission either and read
// `isAdmin` off the role's NAME. Measured at 672bb5e1:
//
//	embedded  reader   CREATE FUNCTION f(x) AS x+1 WITH LOCK   created, owner ""
//	pgwire    reader   DROP FUNCTION f                          dropped
//	http      reader   CREATE FUNCTION f(x) … WITH LOCK         200, installed
//	http      role NAMED "admin", allow [read]                  DROPPED another owner's locked f
//	http      role "ops" that HOLDS admin                       400, REFUSED the same drop
//
// `expr.DefaultUDFs` is process-global, so each of those changes what every
// other session's queries mean.
//
// The gate is a door census over one shared boundary, and it asserts the
// REGISTRY after every refusal — the definition that must still be there, with
// the owner and body it had.
func TestUDFMutationAuthorizationHoldsOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)
	cleanupUDFs(t, "u940_emb", "u940_pgwire", "u940_http", "u940_owned", "u940_ifexists")

	// --- an identity holding only `read` may not mutate the registry ---
	for _, d := range rig.doors {
		d := d
		t.Run("reader-refused/"+d.name, func(t *testing.T) {
			name := "u940_" + d.name
			_, class, err := d.run(t, sec4Reader,
				"CREATE FUNCTION "+name+"(x) AS x + 1 WITH LOCK")
			sec4Refused(t, d, "CREATE FUNCTION", class, err, "42501")
			if _, ok := expr.DefaultUDFs.Get(name); ok {
				t.Errorf("%s: CREATE FUNCTION was refused but %q is in the registry", d.name, name)
			}
		})
	}

	// --- ownership: writer A locks it, writer B may not take it over ---
	emb := rig.door("embedded")
	if _, _, err := emb.run(t, sec4Writer, "CREATE FUNCTION u940_owned(x) AS x + 1 WITH LOCK"); err != nil {
		t.Fatalf("writer CREATE FUNCTION: %v", err)
	}
	def, ok := expr.DefaultUDFs.Get("u940_owned")
	if !ok {
		t.Fatal("writer CREATE FUNCTION reported success but the function is not registered")
	}
	// The owner is the IDENTITY's name, not empty.
	if def.Owner != "writer" {
		t.Errorf("owner = %q, want %q", def.Owner, "writer")
	}
	if !def.Locked {
		t.Error("WITH LOCK did not record Locked")
	}

	type ownershipCell struct {
		key, why, wantClass string
		wantOwnerAfter      string
		wantBodyAfter       string
	}
	for _, d := range rig.doors {
		d := d
		t.Run("ownership/"+d.name, func(t *testing.T) {
			for _, c := range []ownershipCell{
				{sec4WriterB, "a second identity in the writer role holds `write`, not this function", "42501", "writer", "x + 1"},
				{sec4NamedAdmin, "a role NAMED admin that holds only `read`", "42501", "writer", "x + 1"},
				{sec4Reader, "a read-only identity", "42501", "writer", "x + 1"},
			} {
				_, class, err := d.run(t, c.key, "CREATE OR REPLACE FUNCTION u940_owned(x) AS x + 99")
				sec4Refused(t, d, "REPLACE locked ("+c.why+")", class, err, c.wantClass)
				_, class, err = d.run(t, c.key, "DROP FUNCTION u940_owned")
				sec4Refused(t, d, "DROP locked ("+c.why+")", class, err, c.wantClass)

				got, ok := expr.DefaultUDFs.Get("u940_owned")
				if !ok {
					t.Fatalf("%s: %s — the refusal removed the function", d.name, c.why)
				}
				if got.Owner != c.wantOwnerAfter || got.Body != c.wantBodyAfter {
					t.Fatalf("%s: %s — refused, but the definition changed to owner=%q body=%q",
						d.name, c.why, got.Owner, got.Body)
				}
			}
		})
	}

	// The owner itself may replace it, and the definition follows.
	if _, _, err := emb.run(t, sec4Writer, "CREATE OR REPLACE FUNCTION u940_owned(x) AS x + 7 WITH LOCK"); err != nil {
		t.Fatalf("owner REPLACE: %v", err)
	}
	if got, _ := expr.DefaultUDFs.Get("u940_owned"); got.Body != "x + 7" {
		t.Errorf("owner REPLACE: body = %q, want %q", got.Body, "x + 7")
	}

	// A role that HOLDS `admin` — and is not named "admin" — may override the
	// lock. This is the direction the role-name test had backwards.
	if _, _, err := emb.run(t, sec4Ops, "DROP FUNCTION u940_owned"); err != nil {
		t.Fatalf("a role holding the admin permission must be able to drop another owner's locked function, got %v", err)
	}
	if _, ok := expr.DefaultUDFs.Get("u940_owned"); ok {
		t.Error("ops DROP reported success but the function is still registered")
	}

	// --- DROP FUNCTION IF EXISTS is a REFUSAL for an unauthorized caller ---
	// It is not the "does not exist (no-op)" answer: that reports registry
	// contents to a caller whose statement was never going to run.
	for _, d := range rig.doors {
		d := d
		t.Run("ifexists/"+d.name, func(t *testing.T) {
			_, class, err := d.run(t, sec4Reader, "DROP FUNCTION IF EXISTS u940_ifexists")
			sec4Refused(t, d, "DROP FUNCTION IF EXISTS", class, err, "42501")
		})
	}
	// For an AUTHORIZED caller it still forgives an absent function.
	if _, _, err := emb.run(t, sec4Writer, "DROP FUNCTION IF EXISTS u940_ifexists"); err != nil {
		t.Fatalf("writer DROP FUNCTION IF EXISTS on an absent function: %v", err)
	}
	// ...and does NOT forgive one that is present and locked by someone else.
	if _, _, err := emb.run(t, sec4Writer, "CREATE FUNCTION u940_ifexists(x) AS x + 1 WITH LOCK"); err != nil {
		t.Fatalf("writer CREATE: %v", err)
	}
	_, class, err := emb.run(t, sec4WriterB, "DROP FUNCTION IF EXISTS u940_ifexists")
	sec4RefusedWith(t, "embedded", "DROP IF EXISTS over another owner's lock", class, err, "42501")
	if _, ok := expr.DefaultUDFs.Get("u940_ifexists"); !ok {
		t.Error("DROP FUNCTION IF EXISTS removed another owner's locked function")
	}
}

// SHOW FUNCTIONS is readable by any AUTHENTICATED identity, including one that
// may not mutate anything — PostgreSQL hands `pg_proc.prosrc` and `\sf` to a
// role with no privileges on the function (verified on the oracle server).
// With auth enabled and NO identity it is refused.
func TestShowFunctionsIsReadableByAnyAuthenticatedIdentity(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)
	cleanupUDFs(t, "u940_shown")

	emb := rig.door("embedded")
	if _, _, err := emb.run(t, sec4Writer, "CREATE FUNCTION u940_shown(x) AS x + 3"); err != nil {
		t.Fatalf("writer CREATE FUNCTION: %v", err)
	}
	// The embedded and HTTP doors only. `SHOW FUNCTIONS` never reaches this
	// statement through pgwire: `pgConn.matchShow` treats any `SHOW <word>` it
	// does not know as a session VARIABLE and answers one empty row under that
	// name, so the statement is swallowed before `DB.Query` sees it. That is
	// so at 672bb5e1 too — it is a routing gap in the SHOW fallback, not an
	// authorization one, and it is recorded rather than changed here.
	for _, d := range rig.doors {
		if d.name == "pgwire" {
			continue
		}
		rows, _, err := d.run(t, sec4Reader, "SHOW FUNCTIONS")
		if err != nil {
			t.Errorf("%s: reader SHOW FUNCTIONS: %v", d.name, err)
			continue
		}
		if !anyContains(rows, "u940_shown") {
			t.Errorf("%s: reader SHOW FUNCTIONS did not list u940_shown: %v", d.name, rows)
		}
	}
	// No identity at all, auth ENABLED: refused on the door that can express it.
	_, class, err := emb.run(t, "", "SHOW FUNCTIONS")
	sec4RefusedWith(t, "embedded", "SHOW FUNCTIONS/nil identity", class, err, "42501")
}

// The boundary claim: with NO provider, UDF mutation behaves exactly as it did
// — every existing embedded and CLI test creates, replaces and drops functions
// with no identity at all.
func TestUDFMutationIsUnchangedWithoutAnAuthProvider(t *testing.T) {
	ctx := context.Background()
	db := sec4DB(t, ctx)
	cleanupUDFs(t, "u940_noauth")

	if _, err := db.Query(ctx, "CREATE FUNCTION u940_noauth(x) AS x + 1 WITH LOCK"); err != nil {
		t.Fatalf("no-provider CREATE FUNCTION: %v", err)
	}
	def, ok := expr.DefaultUDFs.Get("u940_noauth")
	if !ok {
		t.Fatal("no-provider CREATE FUNCTION did not register the function")
	}
	if def.Owner != "" {
		t.Errorf("no-provider owner = %q, want empty", def.Owner)
	}
	// Locked, with no provider: still replaceable and droppable, exactly as
	// before — there is no identity to own it and nothing to enforce.
	if _, err := db.Query(ctx, "CREATE OR REPLACE FUNCTION u940_noauth(x) AS x + 2 WITH LOCK"); err != nil {
		t.Fatalf("no-provider REPLACE of a locked function: %v", err)
	}
	if _, err := db.Query(ctx, "SHOW FUNCTIONS"); err != nil {
		t.Fatalf("no-provider SHOW FUNCTIONS: %v", err)
	}
	if _, err := db.Query(ctx, "DROP FUNCTION u940_noauth"); err != nil {
		t.Fatalf("no-provider DROP FUNCTION: %v", err)
	}
	if _, ok := expr.DefaultUDFs.Get("u940_noauth"); ok {
		t.Error("no-provider DROP FUNCTION did not remove the function")
	}
}

// cleanupUDFs removes the named functions from the PROCESS-GLOBAL store before
// and after the test. The store outlives any one test, so a name a test leaves
// behind changes what the next one measures.
func cleanupUDFs(t *testing.T, names ...string) {
	t.Helper()
	drop := func() {
		for _, n := range names {
			_ = expr.DefaultUDFs.Unregister(n, "", true)
		}
	}
	drop()
	t.Cleanup(drop)
}

func anyContains(rows []string, want string) bool {
	for _, r := range rows {
		if strings.Contains(r, want) {
			return true
		}
	}
	return false
}

// P2. With NO provider — or a provider whose auth is DISABLED — the caller is
// not an administrator.
//
// The UDF registry is PERSISTED (`cmd/wadjet` wires `UDFStore.SetPersister` and
// `LoadDefs`), so it can hold a definition whose owner was recorded while auth
// was ON. Returning `IsAdmin: true` for a caller with no identity let that
// caller replace and drop such a definition — which the HTTP door refused
// before this batch, because it computed `isAdmin = identity.Role == "admin"`
// and a nil identity is not "admin". Unifying the two doors on `DB.Query`'s old
// unconditional `true` would have unified them on half of #940.
//
// The other half of the claim is what keeps every no-auth deployment working:
// a definition CREATED without an identity carries the empty owner, and
// `UDFStore`'s lock check fires only on a non-empty owner, so the same caller
// still replaces and drops everything it made.
func TestWithoutAProviderTheCallerIsNotAnAdministrator(t *testing.T) {
	ctx := context.Background()
	cleanupUDFs(t, "u940_loaded", "u940_selfmade")

	// Seeded exactly as LoadDefs would from persisted state: an owner recorded
	// under a previous, authenticated configuration.
	if err := expr.DefaultUDFs.Register(expr.UDFDef{
		Name: "u940_loaded", Params: []string{"x"}, Body: "x + 1",
		Owner: "writer", Locked: true,
	}, true); err != nil {
		t.Fatal(err)
	}

	db := sec4DB(t, ctx) // no provider at all
	for _, sql := range []string{
		"CREATE OR REPLACE FUNCTION u940_loaded(x) AS x + 99 WITH LOCK",
		"DROP FUNCTION u940_loaded",
	} {
		if _, err := db.Query(ctx, sql); err == nil {
			t.Errorf("no-provider caller must not override an owner recorded while auth was on: %q succeeded", sql)
		}
	}
	got, ok := expr.DefaultUDFs.Get("u940_loaded")
	if !ok {
		t.Fatal("the locked, owned function was removed by an unauthenticated caller")
	}
	if got.Owner != "writer" || got.Body != "x + 1" {
		t.Fatalf("the definition changed: owner=%q body=%q", got.Owner, got.Body)
	}

	// ...and nothing an unauthenticated deployment does for itself breaks.
	if _, err := db.Query(ctx, "CREATE FUNCTION u940_selfmade(x) AS x + 1 WITH LOCK"); err != nil {
		t.Fatalf("no-provider CREATE … WITH LOCK: %v", err)
	}
	if d, _ := expr.DefaultUDFs.Get("u940_selfmade"); d.Owner != "" {
		t.Errorf("no-provider owner = %q, want empty", d.Owner)
	}
	if _, err := db.Query(ctx, "CREATE OR REPLACE FUNCTION u940_selfmade(x) AS x + 2 WITH LOCK"); err != nil {
		t.Fatalf("no-provider REPLACE of its OWN locked function: %v", err)
	}
	if _, err := db.Query(ctx, "DROP FUNCTION u940_selfmade"); err != nil {
		t.Fatalf("no-provider DROP of its OWN locked function: %v", err)
	}
}

// The same rule on the HTTP door, with a provider that is present but DISABLED
// — the configuration that reaches `ProviderMiddleware`'s `!provider.Enabled()`
// passthrough, so the request arrives with no identity.
func TestHTTPWithAuthDisabledCannotOverrideARecordedOwner(t *testing.T) {
	ctx := context.Background()
	cleanupUDFs(t, "u940_httpdisabled")

	if err := expr.DefaultUDFs.Register(expr.UDFDef{
		Name: "u940_httpdisabled", Params: []string{"x"}, Body: "x + 1",
		Owner: "writer", Locked: true,
	}, true); err != nil {
		t.Fatal(err)
	}

	// auth.New with no keys and no roles yields an Authenticator that is not
	// Enabled(), which is how a deployment turns auth off.
	authn, authz := auth.New(auth.Config{Enabled: false})
	provider := auth.NewProvider(authn, authz, nil, nil)
	if provider.Enabled() {
		t.Fatal("fixture error: the provider must be disabled for this cell")
	}
	rig := sec4RigWithProvider(t, ctx, provider)
	http := rig.door("http")

	for _, sql := range []string{
		"CREATE OR REPLACE FUNCTION u940_httpdisabled(x) AS x + 99 WITH LOCK",
		"DROP FUNCTION u940_httpdisabled",
	} {
		if _, _, err := http.run(t, "", sql); err == nil {
			t.Errorf("HTTP with auth disabled must not override a recorded owner: %q succeeded", sql)
		}
	}
	if got, ok := expr.DefaultUDFs.Get("u940_httpdisabled"); !ok || got.Body != "x + 1" {
		t.Fatalf("the definition changed or was removed: %+v", got)
	}
}
