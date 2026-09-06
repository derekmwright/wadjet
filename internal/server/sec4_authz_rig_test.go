package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The three-door rig the SEC4 authorization gates share.
//
// A door census is the only thing that can say "the SAME operation refuses
// with the SAME class everywhere": the defects this file gates were each
// visible at one door and invisible at another, because the frontends had
// forked the decision. `wadjet.DB.Query` is the boundary the embedded caller
// and pgwire both reach, and the HTTP mux is driven through
// `auth.ProviderMiddleware` so an authenticated-but-unauthorized identity is a
// real request and not a synthesized context.
//
// Identities (all AUTHENTICATED, differing only in what they are AUTHORIZED
// to do):
//
//	reader-key      role reader,     tables [users], allow [read]
//	writer-key      role writer,     tables [*],     allow [read write]
//	writerb-key     role writer      — a SECOND identity in the writer role
//	ops-key         role ops,        tables [*],     allow [admin]
//	namedadmin-key  role "admin",    tables [*],     allow [read]   ← the name is not the permission
const (
	sec4Reader     = "reader-key"
	sec4Writer     = "writer-key"
	sec4WriterB    = "writerb-key"
	sec4Ops        = "ops-key"
	sec4NamedAdmin = "namedadmin-key"
)

// sec4Provider builds the provider with an ABAC evaluator installed. The
// reader's rule is SCOPED to `users`, so `secret` is default-denied for it,
// which is what separates the metadata doors from the data door.
func sec4Provider(t *testing.T) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "sec4", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "reader-users", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "users"}},
				Actions:   []auth.Action{auth.ActionRead},
			},
			{
				ID: "writer-all", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "writer"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
			{
				ID: "ops-all", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "ops"}},
				Actions: []auth.Action{auth.ActionRead, auth.ActionWrite, auth.ActionAdmin,
					auth.ActionCreate, auth.ActionDrop, auth.ActionDescribe},
			},
			{
				ID: "namedadmin-read", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions:  []auth.Action{auth.ActionRead},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: sec4Reader, Name: "reader", Role: "reader"},
			{Key: sec4Writer, Name: "writer", Role: "writer"},
			{Key: sec4WriterB, Name: "writerb", Role: "writer"},
			{Key: sec4Ops, Name: "ops", Role: "ops"},
			{Key: sec4NamedAdmin, Name: "namedadmin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"users"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
			{Name: "ops", Tables: []string{"*"}, Allow: []string{"admin"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// sec4DB opens the embedded engine with `users` and `secret`.
func sec4DB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	t.Cleanup(db.Close)
	mk := func(name string, sch parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, name, sch, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mk("users", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}}, []map[string]any{{"id": int64(1), "name": "alice"}})
	mk("secret", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "sensitive_column", Type: parquet.TypeString},
	}}, []map[string]any{{"id": int64(1), "sensitive_column": "top"}})
	return db
}

// sec4Door runs one statement through one door.
//
// `class` is what a client branches on: the SQLSTATE on the embedded and
// pgwire doors, and on HTTP the `sqlstate` field the JSON error carries (the
// status code is asserted separately through `status`).
type sec4Door struct {
	name   string
	run    func(t *testing.T, key, sql string) (rows []string, class string, err error)
	status func() int // HTTP status of the last run, 0 for the non-HTTP doors
}

type sec4Rig struct {
	db       *wadjet.DB
	provider *auth.Provider
	doors    []sec4Door
	// idCtx builds an embedded context carrying the identity behind an API key.
	idCtx   func(key string) context.Context
	pgAddr  string
	httpURL string
}

// sec4NewRig stands up the embedded DB, a real pgwire server over it and a
// real HTTP mux over its catalog, all sharing one provider.
func sec4NewRig(t *testing.T, ctx context.Context) *sec4Rig {
	t.Helper()
	provider := sec4Provider(t)
	db := sec4DB(t, ctx)
	if err := db.SetAuthProvider(provider); err != nil {
		t.Fatalf("SetAuthProvider: %v", err)
	}
	idCtx := func(key string) context.Context {
		if key == "" {
			return ctx
		}
		id, err := provider.Authenticator().AuthenticateToken(key)
		if err != nil {
			t.Fatalf("authenticate %q: %v", key, err)
		}
		return auth.ContextWithIdentity(ctx, id)
	}

	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("start pgwire: %v", err)
	}
	t.Cleanup(pg.Shutdown)

	srv := New(Config{Addr: ":0", Catalog: db.Catalog(), Provider: provider}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	var lastStatus int
	rig := &sec4Rig{db: db, provider: provider, idCtx: idCtx, pgAddr: pg.Addr(), httpURL: hs.URL}
	rig.doors = []sec4Door{
		{
			name: "embedded",
			run: func(t *testing.T, key, sql string) ([]string, string, error) {
				res, err := db.Query(idCtx(key), sql)
				if err != nil {
					return nil, sqlerr.StateOf(err), err
				}
				return sec4RenderRows(res), "", nil
			},
			status: func() int { return 0 },
		},
		{
			name: "pgwire",
			run: func(t *testing.T, key, sql string) ([]string, string, error) {
				conn, err := pgx.Connect(ctx, fmt.Sprintf(
					"postgres://wadjet:%s@%s/wadjet?sslmode=disable", key, pg.Addr()))
				if err != nil {
					return nil, "", fmt.Errorf("connect: %w", err)
				}
				defer conn.Close(ctx)
				rows, err := conn.Query(ctx, sql)
				if err != nil {
					return nil, sec4PGState(err), err
				}
				var out []string
				for rows.Next() {
					vals, verr := rows.Values()
					if verr != nil {
						rows.Close()
						return nil, "", verr
					}
					parts := make([]string, len(vals))
					for i, v := range vals {
						parts[i] = fmt.Sprint(v)
					}
					out = append(out, strings.Join(parts, "|"))
				}
				if rerr := rows.Err(); rerr != nil {
					return nil, sec4PGState(rerr), rerr
				}
				return out, "", nil
			},
			status: func() int { return 0 },
		},
		{
			name: "http",
			run: func(t *testing.T, key, sql string) ([]string, string, error) {
				body, _ := json.Marshal(map[string]string{"sql": sql})
				req, err := http.NewRequestWithContext(ctx, http.MethodPost,
					hs.URL+"/v1/queries", bytes.NewReader(body))
				if err != nil {
					return nil, "", err
				}
				req.Header.Set("Content-Type", "application/json")
				if key != "" {
					req.Header.Set("Authorization", "Bearer "+key)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return nil, "", err
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				lastStatus = resp.StatusCode
				var out struct {
					Columns  []string         `json:"columns"`
					Rows     []map[string]any `json:"rows"`
					Error    string           `json:"error"`
					SQLState string           `json:"sqlstate"`
				}
				if uerr := json.Unmarshal(raw, &out); uerr != nil {
					return nil, "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
				}
				if resp.StatusCode != http.StatusOK || out.Error != "" {
					return nil, out.SQLState, fmt.Errorf("HTTP %d: %s", resp.StatusCode, out.Error)
				}
				return sec4RenderJSONRows(out.Columns, out.Rows), "", nil
			},
			status: func() int { return lastStatus },
		},
	}
	return rig
}

// door returns the named door, so a cell that applies to one door only does
// not have to loop.
func (r *sec4Rig) door(name string) sec4Door {
	for _, d := range r.doors {
		if d.name == name {
			return d
		}
	}
	panic("no such door: " + name)
}

func (r *sec4Rig) tableExists(t *testing.T, ctx context.Context, name string) bool {
	t.Helper()
	_, err := r.db.Catalog().GetTable(ctx, name)
	return err == nil
}

func sec4RenderRows(res *wadjet.QueryResult) []string {
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		parts := make([]string, len(res.Columns))
		for i, c := range res.Columns {
			parts[i] = fmt.Sprint(row[c])
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

func sec4RenderJSONRows(cols []string, rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = fmt.Sprint(row[c])
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

func sec4PGState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// sec4RefusedWith asserts that err is a refusal carrying SQLSTATE want.
func sec4RefusedWith(t *testing.T, door, cell string, class string, err error, want string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s/%s: expected refusal %s, got success", door, cell, want)
		return
	}
	if class != want {
		t.Errorf("%s/%s: expected SQLSTATE %s, got %q (err: %v)", door, cell, want, class, err)
	}
}

// sec4Refused is the door census assertion: an authorization refusal is 42501
// on the embedded and pgwire doors, and HTTP 403 on the HTTP door.
//
// The HTTP door's 403 is asserted by STATUS rather than by SQLSTATE for the
// statements whose HTTP handler answers with a bare message (`writeError`
// rather than `writeSQLError`); those handlers live in this package's
// `server.go` outside this arc's territory. Where the HTTP handler does carry
// a class, pass wantClass and it is checked too.
func sec4Refused(t *testing.T, d sec4Door, cell, class string, err error, wantClass string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s/%s: expected a refusal, got success", d.name, cell)
		return
	}
	if d.name == "http" {
		if got := d.status(); got != http.StatusForbidden {
			t.Errorf("%s/%s: expected HTTP 403, got %d (err: %v)", d.name, cell, got, err)
		}
		if wantClass != "" && class != "" && class != wantClass {
			t.Errorf("%s/%s: expected SQLSTATE %s, got %q", d.name, cell, wantClass, class)
		}
		return
	}
	if class != wantClass {
		t.Errorf("%s/%s: expected SQLSTATE %s, got %q (err: %v)", d.name, cell, wantClass, class, err)
	}
}
