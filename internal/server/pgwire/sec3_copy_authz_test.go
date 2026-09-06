package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// COPY FROM STDIN is a WRITE and authorizes before it invites the client to
// stream (#938, ADR-0034).
//
// At 672bb5e1 it authorized nowhere: `handleCopyIn` resolved the table and
// constructed an `ingest.Ingester` directly, so a role holding only `read`
// received CopyInResponse and its rows landed — the bulk-ingest path was the
// one write on this door with no decision on it, and the ABAC write rule
// INSERT obeys was never consulted.
//
// Every cell below drives a REAL authenticated pgwire connection and asserts
// the SIDE EFFECT after the refusal, not only the message: a refused COPY
// leaves the table exactly as it was.

const sec3CopyTable = "emp"

// sec3CopyDB is an empty three-column table; `salary` is the column the ABAC
// fixture denies.
func sec3CopyDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "salary", Type: parquet.TypeFloat64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, sec3CopyTable, schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

// sec3RowCount reads what is STORED, so the assertion sees the table rather
// than what a policy would show.
//
// It reads under the WRITER identity, not a bare context: with an auth
// provider installed, a context carrying no identity is refused on the plan
// path, which is the fail-closed rule and not something a fixture should route
// around. `writer` holds `read` on this table in both provider shapes and
// carries no column obligation a `count(*)` would meet, so what it counts is
// what is stored. Pass nil where the server has no provider.
func sec3RowCount(t *testing.T, db *wadjet.DB, provider *auth.Provider) int64 {
	t.Helper()
	ctx := context.Background()
	if provider != nil {
		// AUTHENTICATE the key rather than hand-building an Identity: the
		// legacy checks read `id.Perms` and `id.Tables`, which only the
		// authenticator fills in from the role, so a hand-built identity is
		// refused by the very rule this fixture is not testing.
		id, err := provider.Authenticator().AuthenticateToken("writer-key")
		if err != nil {
			t.Fatalf("authenticating the fixture's reader: %v", err)
		}
		ctx = auth.ContextWithIdentity(ctx, id)
	}
	res, err := db.Query(ctx, "SELECT count(*) AS n FROM "+sec3CopyTable)
	if err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("count returned %d rows", len(res.Rows))
	}
	switch v := res.Rows[0]["n"].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	default:
		t.Fatalf("count is %T, not an integer", res.Rows[0]["n"])
		return 0
	}
}

// sec3LegacyProvider is roles only — no ABAC evaluator installed, which is
// the configuration most deployments still run.
func sec3LegacyProvider() *auth.Provider {
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader-user", Role: "reader"},
			{Key: "writer-key", Name: "writer-user", Role: "writer"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
		},
	})
	return auth.NewProvider(authn, authz, nil, nil)
}

// sec3ABACProvider installs an evaluator: `reader` may only READ emp, and
// `writer` may write it but carries a deny_column on `salary`.
func sec3ABACProvider() *auth.Provider {
	deny := []auth.Obligation{{Type: "deny_column", Target: "salary"}}
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "sec3-copy", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "reader", EffectStr: "allow", Priority: 10,
				Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: sec3CopyTable}},
				Actions:     []auth.Action{auth.ActionRead},
				Obligations: deny,
			},
			{
				ID: "writer", EffectStr: "allow", Priority: 10,
				Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "writer"}},
				Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: sec3CopyTable}},
				Actions:     []auth.Action{auth.ActionRead, auth.ActionWrite},
				Obligations: deny,
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader-user", Role: "reader"},
			{Key: "writer-key", Name: "writer-user", Role: "writer"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// sec3CopyOutcome is what one COPY attempt got back.
type sec3CopyOutcome struct {
	msgType byte   // 'G' = invited to stream, 'E' = refused
	code    string // SQLSTATE when refused
	msg     string
	tag     string // command tag when the whole COPY completed
}

// sec3Copy runs one COPY FROM STDIN over a real authenticated connection. When
// the server invites the client to stream, it streams one row and completes.
func sec3Copy(t *testing.T, addr, user, key, sql, row string) sec3CopyOutcome {
	t.Helper()
	client := newPGClient(t, addr)
	if key == "" {
		client.startup(user, "testdb")
	} else if errMsg := client.startupWithPassword(user, "testdb", key); errMsg != "" {
		t.Fatalf("authenticating %s: %s", user, errMsg)
	}
	defer client.terminate()

	client.writeMsg('Q', append([]byte(sql), 0))
	typ, payload, err := client.readMsg()
	if err != nil {
		t.Fatalf("reading COPY response: %v", err)
	}
	out := sec3CopyOutcome{msgType: typ}
	if typ == 'E' {
		out.code, out.msg = parseErrorFields(payload)
		// The connection stays in the ordinary message loop: the refusal is
		// followed by ReadyForQuery, not by a dropped connection.
		for {
			mt, _, rerr := client.readMsg()
			if rerr != nil {
				t.Fatalf("no ReadyForQuery after the COPY refusal: %v", rerr)
			}
			if mt == 'Z' {
				break
			}
		}
		return out
	}
	if typ != 'G' {
		t.Fatalf("unexpected COPY response %q", typ)
	}
	client.writeMsg('d', []byte(row))
	client.writeMsg('c', nil)
	for {
		mt, data, rerr := client.readMsg()
		if rerr != nil {
			t.Fatalf("reading after CopyDone: %v", rerr)
		}
		switch mt {
		case 'C':
			out.tag = readCString(data)
		case 'E':
			out.code, out.msg = parseErrorFields(data)
		case 'Z':
			return out
		}
	}
}

func TestPGWireCopyRequiresWritePermission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider func() *auth.Provider
	}{
		{"legacy roles", sec3LegacyProvider},
		{"ABAC evaluator", sec3ABACProvider},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sec3CopyDB(t)
			provider := tc.provider()
			db.SetAuthProvider(provider)
			srv := startTestServerWithAuth(t, db, provider)

			// The read-only identity is refused BEFORE CopyInResponse, and
			// the table is untouched afterwards.
			before := sec3RowCount(t, db, provider)
			got := sec3Copy(t, srv.Addr(), "reader-user", "reader-key",
				"COPY emp (id, name) FROM STDIN", "99\tmallory\n")
			if got.msgType != 'E' {
				t.Fatalf("read-only COPY got message %q (tag %q); want 'E'", got.msgType, got.tag)
			}
			if got.code != "42501" {
				t.Errorf("read-only COPY refused with SQLSTATE %q; want 42501 (msg %q)", got.code, got.msg)
			}
			if want := `permission denied for table "emp"`; got.msg != want {
				t.Errorf("refusal message %q; want %q — the same text INSERT's refusal carries", got.msg, want)
			}
			if after := sec3RowCount(t, db, provider); after != before {
				t.Fatalf("the refused COPY appended rows: %d -> %d", before, after)
			}

			// The writer identity still works, and its row lands.
			got = sec3Copy(t, srv.Addr(), "writer-user", "writer-key",
				"COPY emp (id, name) FROM STDIN", "1\talice\n")
			if got.msgType != 'G' {
				t.Fatalf("writer COPY got message %q (%s: %s); want 'G'", got.msgType, got.code, got.msg)
			}
			if got.tag != "COPY 1" {
				t.Errorf("writer COPY tag %q; want %q", got.tag, "COPY 1")
			}
			if after := sec3RowCount(t, db, provider); after != before+1 {
				t.Fatalf("the authorized COPY did not land: %d -> %d", before, after)
			}
		})
	}
}

// A column the identity's policy DENIES does not exist for it, in a COPY
// column list exactly as in an INSERT target list.
func TestPGWireCopyRefusesADeniedColumn(t *testing.T) {
	db := sec3CopyDB(t)
	provider := sec3ABACProvider()
	db.SetAuthProvider(provider)
	srv := startTestServerWithAuth(t, db, provider)

	before := sec3RowCount(t, db, provider)
	got := sec3Copy(t, srv.Addr(), "writer-user", "writer-key",
		"COPY emp (id, salary) FROM STDIN", "1\t100.0\n")
	if got.msgType != 'E' {
		t.Fatalf("COPY naming a denied column got message %q; want 'E'", got.msgType)
	}
	if got.code != "42703" {
		t.Errorf("SQLSTATE %q; want 42703 — a denied column does not exist (msg %q)", got.code, got.msg)
	}
	if after := sec3RowCount(t, db, provider); after != before {
		t.Fatalf("the refused COPY appended rows: %d -> %d", before, after)
	}
}

// With no provider nothing is enforced and nothing changes.
func TestPGWireCopyIsUnchangedWithoutAuth(t *testing.T) {
	db := sec3CopyDB(t)
	srv := startTestServer(t, db)

	got := sec3Copy(t, srv.Addr(), "anyone", "", "COPY emp (id, name) FROM STDIN", "7\tbob\n")
	if got.msgType != 'G' || got.tag != "COPY 1" {
		t.Fatalf("unauthenticated COPY got %q tag %q (%s: %s); want 'G' and COPY 1",
			got.msgType, got.tag, got.code, got.msg)
	}
	if n := sec3RowCount(t, db, nil); n != 1 {
		t.Fatalf("row count %d; want 1", n)
	}
}

// The write decision comes BEFORE the relation's existence is reported, so a
// COPY refusal is not an existence or column oracle (round-1 review P3).
//
// `handleCopyIn` used to resolve the name (42P01), then the column list
// (42703), and only then ask the table decision — three distinguishable
// answers, so a caller that may not write the relation could still decide
// whether it exists and whether a guessed column exists on it.
func TestPGWireCopyRefusalIsNotAnExistenceOracle(t *testing.T) {
	db := sec3CopyDB(t)
	provider := sec3LegacyProvider()
	db.SetAuthProvider(provider)
	srv := startTestServerWithAuth(t, db, provider)

	// One answer for the unauthorized identity, whatever it names.
	for _, tc := range []struct{ name, sql string }{
		{"a real relation", "COPY emp (id) FROM STDIN"},
		{"a real relation, a column it does not have", "COPY emp (nosuchcol) FROM STDIN"},
		{"a relation that does not exist", "COPY nosuchtable (id) FROM STDIN"},
	} {
		t.Run("reader/"+tc.name, func(t *testing.T) {
			got := sec3Copy(t, srv.Addr(), "reader-user", "reader-key", tc.sql, "")
			if got.msgType != 'E' {
				t.Fatalf("got message %q; want a refusal", got.msgType)
			}
			if got.code != "42501" {
				t.Errorf("SQLSTATE %q (%s); want 42501 for every relation — a different "+
					"class here tells an unauthorized caller what exists", got.code, got.msg)
			}
			if strings.Contains(got.msg, "nosuchcol") {
				t.Errorf("the refusal named a column of a relation the caller may not "+
					"write: %s", got.msg)
			}
		})
	}

	// The identity that MAY write still gets the real, useful classes.
	for _, tc := range []struct{ name, sql, wantCode string }{
		{"a column the relation does not have", "COPY emp (nosuchcol) FROM STDIN", "42703"},
		{"a relation that does not exist", "COPY nosuchtable (id) FROM STDIN", "42P01"},
	} {
		t.Run("writer/"+tc.name, func(t *testing.T) {
			got := sec3Copy(t, srv.Addr(), "writer-user", "writer-key", tc.sql, "")
			if got.msgType != 'E' || got.code != tc.wantCode {
				t.Errorf("got %q %s (%s); want an 'E' with %s — an authorized caller still "+
					"gets the class it needs", got.msgType, got.code, got.msg, tc.wantCode)
			}
		})
	}
}

// A refused COPY ends the STATEMENT, not the connection (round-1 review P4).
//
// The refusal arrives instead of CopyInResponse, but a client that had already
// queued its rows sends them anyway. Those CopyData/CopyDone messages used to
// fall into handleMessage's default case — 08P01 AND `skipUntilSync` — and a
// simple-protocol client never sends Sync, so nothing it sent afterwards was
// ever answered. PostgreSQL accepts and ignores `d`/`c`/`f` outside copy mode
// for exactly this case.
func TestConnectionSurvivesAStreamAfterARefusedCopy(t *testing.T) {
	db := sec3CopyDB(t)
	provider := sec3LegacyProvider()
	db.SetAuthProvider(provider)
	srv := startTestServerWithAuth(t, db, provider)

	client := newPGClient(t, srv.Addr())
	defer client.terminate()
	if errMsg := client.startupWithPassword("reader-user", "testdb", "reader-key"); errMsg != "" {
		t.Fatalf("authenticating: %s", errMsg)
	}

	client.writeMsg('Q', append([]byte("COPY emp (id, name) FROM STDIN"), 0))
	typ, payload, err := client.readMsg()
	if err != nil {
		t.Fatalf("reading the COPY response: %v", err)
	}
	if typ != 'E' {
		t.Fatalf("got message %q; want the refusal", typ)
	}
	if code, _ := parseErrorFields(payload); code != "42501" {
		t.Fatalf("SQLSTATE %q; want 42501", code)
	}
	for {
		mt, _, rerr := client.readMsg()
		if rerr != nil {
			t.Fatalf("no ReadyForQuery after the refusal: %v", rerr)
		}
		if mt == 'Z' {
			break
		}
	}

	// The client streams what it had already queued, as libpq would.
	client.writeMsg('d', []byte("99\tmallory\n"))
	client.writeMsg('c', nil)

	// And the connection answers the next statement.
	_, rows, tag := client.simpleQuery("SELECT id FROM emp")
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("the connection did not survive the refused COPY: %s", tag)
	}
	if len(rows) != 0 {
		t.Errorf("the ignored CopyData landed rows: %v", rows)
	}
	if n := sec3RowCount(t, db, provider); n != 0 {
		t.Errorf("row count %d after a refused COPY; want 0", n)
	}
}
