package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestCoordinatorABACEnforcement is the parity gate for coordinator-side
// ABAC: with an auth provider wired (SetAuthProvider), ExecuteSQL must
// enforce the same policies as the embedded engine (wadjet.DB.Query, the
// pre-existing reference) — table denial, row filters, column deny and
// mask — on BOTH coordinator executions: the local fast path and the
// distributed stage DAG. Rows are compared against the embedded engine's
// output per query, so any enforcement gap on either path fails loudly.
func TestCoordinatorABACEnforcement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)
	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("js: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("cat init: %v", err)
	}

	// findings: the policy-restricted table, split into 3 files so the
	// distributed path genuinely fans out. denied_tbl: access-denied table.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "severity", Type: parquet.TypeString},
		{Name: "owner", Type: parquet.TypeString},
		{Name: "secret_col", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "denied_tbl", schema, nil); err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	sevs := []string{"high", "low", "medium"}
	for i := 0; i < 900; i++ {
		rows = append(rows, map[string]any{
			"id":         int64(i),
			"severity":   sevs[i%3],
			"owner":      fmt.Sprintf("owner-%d", i%7),
			"secret_col": fmt.Sprintf("classified-%d", i),
		})
	}
	writeChunk := func(table string, chunk int, data []map[string]any) {
		t.Helper()
		var buf bytes.Buffer
		pw, _ := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err := pw.WriteRows(data); err != nil {
			t.Fatal(err)
		}
		pw.Close()
		fp := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, chunk)
		pd := buf.Bytes()
		store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
		cat.AddFiles(ctx, table, map[string]string{}, "tables/"+table+"/", []catalog.FileEntry{{
			Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(len(data)), CreatedAt: time.Now(),
		}})
	}
	for c := 0; c < 3; c++ {
		writeChunk("findings", c, rows[c*300:(c+1)*300])
	}
	writeChunk("denied_tbl", 0, rows[:10])

	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{NATSUrl: embeddedNATS.ClientURL(), MaxConcurrent: 4, CacheBytes: 64 << 20}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}

	// Policy: analyst may read findings with severity='high' only, never
	// sees secret_col, and owner is masked. denied_tbl is not covered by
	// any allow rule → denied.
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "test-policy", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "restrict-findings", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "findings"}},
			Actions:   []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "deny_column", Target: "secret_col"},
				{Type: "mask_column", Target: "owner", Value: "'***'"},
				{Type: "row_filter", Value: "severity = 'high'"},
			},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "test-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	newCoord := func(fastBytes int64) *Coordinator {
		c := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test",
			LocalFastPathBytes: fastBytes}, cat, nc, js, logger)
		c.SetAuthProvider(provider)
		for i := 0; i < 3; i++ {
			c.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
		}
		return c
	}
	fast := newCoord(DefaultLocalFastPathBytes)
	dag := newCoord(0)

	// Reference: the embedded engine over the same store/catalog with the
	// same provider — the enforcement behavior all paths must match.
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv, Logger: logger})
	if err != nil {
		t.Fatalf("wadjet.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetAuthProvider(provider)

	identity := &auth.Identity{Name: "analyst", Role: "analyst", Method: "apikey"}
	authCtx := auth.ContextWithIdentity(ctx, identity)

	canonRows := func(rs []map[string]any, cols []string) []string {
		out := make([]string, len(rs))
		for i, r := range rs {
			s := ""
			for _, c := range cols {
				s += fmt.Sprintf("%v|", r[c])
			}
			out[i] = s
		}
		sort.Strings(out)
		return out
	}

	queries := []struct {
		name string
		sql  string
		cols []string
	}{
		{"row filter", "SELECT id, severity FROM findings", []string{"id", "severity"}},
		{"mask", "SELECT id, owner FROM findings", []string{"id", "owner"}},
		{"count under filter", "SELECT COUNT(*) AS c FROM findings", []string{"c"}},
		{"group by masked", "SELECT owner, COUNT(*) AS c FROM findings GROUP BY owner", []string{"owner", "c"}},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			ref, err := db.Query(authCtx, q.sql)
			if err != nil {
				t.Fatalf("embedded: %v", err)
			}
			want := canonRows(ref.Rows, q.cols)
			for path, coord := range map[string]*Coordinator{"fastpath": fast, "dag": dag} {
				res, err := coord.ExecuteSQL(authCtx, q.sql)
				if err != nil || res.Error != "" {
					t.Fatalf("%s: %v %s", path, err, res.Error)
				}
				got := canonRows(mustRows(t, res), q.cols)
				if len(got) != len(want) {
					t.Fatalf("%s: %d rows, embedded reference %d", path, len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s row %d: got %s want %s", path, i, got[i], want[i])
					}
				}
			}
		})
	}

	// Enforcement sanity independent of the reference: no low/medium rows,
	// no raw owner values, secret_col absent.
	t.Run("no policy leakage", func(t *testing.T) {
		for path, coord := range map[string]*Coordinator{"fastpath": fast, "dag": dag} {
			res, err := coord.ExecuteSQL(authCtx, "SELECT * FROM findings")
			if err != nil || res.Error != "" {
				t.Fatalf("%s: %v %s", path, err, res.Error)
			}
			rs := mustRows(t, res)
			if len(rs) != 300 {
				t.Errorf("%s: got %d rows, want 300 (severity='high' only)", path, len(rs))
			}
			for _, r := range rs {
				if _, present := r["secret_col"]; present {
					t.Fatalf("%s: denied column secret_col present in result", path)
				}
				if sev := fmt.Sprint(r["severity"]); sev != "high" {
					t.Fatalf("%s: row filter leaked severity=%s", path, sev)
				}
				if own := fmt.Sprint(r["owner"]); own != "***" {
					t.Fatalf("%s: mask leaked owner=%s", path, own)
				}
			}
		}
	})

	// Table denial errors on every path.
	t.Run("table denial", func(t *testing.T) {
		if _, err := db.Query(authCtx, "SELECT id FROM denied_tbl"); err == nil {
			t.Error("embedded: expected denial")
		}
		for path, coord := range map[string]*Coordinator{"fastpath": fast, "dag": dag} {
			if _, err := coord.ExecuteSQL(authCtx, "SELECT id FROM denied_tbl"); err == nil {
				t.Errorf("%s: expected denial", path)
			}
		}
	})

	// Without an identity, the provider does not restrict (matches the
	// embedded engine's contract) — full table visible.
	t.Run("no identity unrestricted", func(t *testing.T) {
		res, err := dag.ExecuteSQL(ctx, "SELECT COUNT(*) AS c FROM findings")
		if err != nil || res.Error != "" {
			t.Fatalf("%v %s", err, res.Error)
		}
		if got := fmt.Sprint(mustRows(t, res)[0]["c"]); got != "900" {
			t.Errorf("unauthed count = %s, want 900", got)
		}
	})
}
