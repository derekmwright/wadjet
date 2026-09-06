package pgwire

import (
	"context"
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

// sec3RowCount reads what is STORED, under no identity, so the assertion sees
// the table rather than what a policy would show.
func sec3RowCount(t *testing.T, db *wadjet.DB) int64 {
	t.Helper()
	res, err := db.Query(context.Background(), "SELECT count(*) AS n FROM "+sec3CopyTable)
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
			before := sec3RowCount(t, db)
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
			if after := sec3RowCount(t, db); after != before {
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
			if after := sec3RowCount(t, db); after != before+1 {
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

	before := sec3RowCount(t, db)
	got := sec3Copy(t, srv.Addr(), "writer-user", "writer-key",
		"COPY emp (id, salary) FROM STDIN", "1\t100.0\n")
	if got.msgType != 'E' {
		t.Fatalf("COPY naming a denied column got message %q; want 'E'", got.msgType)
	}
	if got.code != "42703" {
		t.Errorf("SQLSTATE %q; want 42703 — a denied column does not exist (msg %q)", got.code, got.msg)
	}
	if after := sec3RowCount(t, db); after != before {
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
	if n := sec3RowCount(t, db); n != 1 {
		t.Fatalf("row count %d; want 1", n)
	}
}
