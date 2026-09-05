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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// ---------------------------------------------------------------------------
// #859 — the policy-masking matrix: every DOOR, every ARM, every SHAPE a
// masked value can travel through.
//
// The fixture is one table with three policed columns, chosen so a leak is
// unmistakable in the rendered cell: `ssn` masks to '***' from a value that
// starts with "true-", `acct` masks to 0 from a value in the 900000s, and
// `salary` is DENIED outright and must not exist above the scan at all.
// ---------------------------------------------------------------------------

const (
	pmTable = "e7emp"
	// pmOther is a SECOND, UNPOLICED table, present so the matrix can attack
	// the policy's own targeting: a policy binds to a RELATION, and a scan of
	// e7other must be untouched however the statement names it — including
	// when the statement aliases it to the policed table's own name.
	pmOther    = "e7other"
	pmRows     = 12
	pmMaskSSN  = "***"
	pmMaskAcct = "0"
)

// pmBal is a third table with ONE masked numeric column whose stored values
// have both signs. It exists to make a per-row disclosure VISIBLE: with `bal`
// masked to 0, a predicate that reads the MASK answers the same for every row,
// and one that reads the STORED column answers the SIGN of each row — so the
// row set itself is arithmetic on the value the policy hides (#859 round 2).
const pmBal = "e7bal"

func pmBalSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "bal", Type: parquet.TypeInt64},
	}}
}

func pmBalFixture() []map[string]any {
	rows := make([]map[string]any, 0, 8)
	for i := 1; i <= 8; i++ {
		v := int64(100 * i)
		if i%2 == 1 {
			v = -v
		}
		rows = append(rows, map[string]any{"id": int64(i), "bal": v})
	}
	return rows
}

func pmOtherSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "note", Type: parquet.TypeString},
	}}
}

func pmOtherFixture() []map[string]any {
	rows := make([]map[string]any, 0, 3)
	for i := 1; i <= 3; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "note": fmt.Sprintf("n%d", i)})
	}
	return rows
}

func pmSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "dept", Type: parquet.TypeString},
		{Name: "ssn", Type: parquet.TypeString},
		{Name: "acct", Type: parquet.TypeInt64},
		{Name: "salary", Type: parquet.TypeInt64},
		{Name: "amt", Type: parquet.TypeInt64},
	}}
}

func pmFixture() []map[string]any {
	rows := make([]map[string]any, 0, pmRows)
	for i := 1; i <= pmRows; i++ {
		rows = append(rows, map[string]any{
			"id":     int64(i),
			"dept":   fmt.Sprintf("d%d", i%3),
			"ssn":    fmt.Sprintf("true-ssn-%02d", i),
			"acct":   int64(900000 + i),
			"salary": int64(700000 + i),
			"amt":    int64(i),
		})
	}
	return rows
}

// pmTrueValues is every literal that must NEVER appear in any cell of any
// door under the analyst identity. The census's leak test is textual and
// blunt on purpose: a masked column's true value reaching a client is the
// defect, whatever shape carried it.
func pmTrueValues() []string {
	var out []string
	for i := 1; i <= pmRows; i++ {
		out = append(out, fmt.Sprintf("true-ssn-%02d", i))
		out = append(out, fmt.Sprintf("%d", 900000+i))
		out = append(out, fmt.Sprintf("%d", 700000+i))
	}
	for _, r := range pmBalFixture() {
		out = append(out, fmt.Sprint(r["bal"]))
	}
	return out
}

func pmProvider(t *testing.T) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "e7-policy", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "analyst-masks", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "deny_column", Target: "salary"},
					{Type: "mask_column", Target: "ssn", Value: "'" + pmMaskSSN + "'"},
					{Type: "mask_column", Target: "acct", Value: pmMaskAcct},
				},
			},
			{
				ID: "analyst-bal", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmBal}},
				Actions:   []auth.Action{auth.ActionRead},
				Obligations: []auth.Obligation{
					{Type: "mask_column", Target: "bal", Value: "0"},
				},
			},
			{
				// e7other is readable and carries NO obligation. It is the
				// control for the policy's targeting.
				ID: "analyst-other-open", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmOther}},
				Actions:   []auth.Action{auth.ActionRead},
			},
			{
				ID: "admin-raw", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions:  []auth.Action{auth.ActionRead},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "analyst-key", Name: "analyst", Role: "analyst"},
			{Key: "admin-key", Name: "admin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// ---------------------------------------------------------------------------
// Doors
// ---------------------------------------------------------------------------

// pmResult is one door's answer, normalised so three very different
// transports compare. Values are rendered as text: this census is about
// WHICH value came back, and the declared type is gated separately.
type pmResult struct {
	cols []string
	rows []map[string]string
}

func (r pmResult) canon() []string {
	out := make([]string, 0, len(r.rows))
	names := append([]string(nil), r.cols...)
	sort.Strings(names)
	for _, row := range r.rows {
		parts := make([]string, 0, len(names))
		for _, c := range names {
			parts = append(parts, c+"="+row[c])
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out
}

func pmRender(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case string:
		return t
	case []byte:
		return string(t)
	case pgtype.Numeric:
		// pgx boxes a NUMERIC column; render its digits so the three doors
		// compare as text.
		f, err := t.Float64Value()
		if err != nil || !f.Valid {
			return "NULL"
		}
		if f.Float64 == float64(int64(f.Float64)) {
			return fmt.Sprintf("%d", int64(f.Float64))
		}
		return fmt.Sprintf("%g", f.Float64)
	case float64:
		// The HTTP door's JSON turns every number into a float64; render
		// integral values without the ".0" so all three doors agree.
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%g", t)
	default:
		return fmt.Sprint(v)
	}
}

func pmFromRows(cols []string, rows []map[string]any) pmResult {
	out := pmResult{cols: cols}
	if len(out.cols) == 0 && len(rows) > 0 {
		for k := range rows[0] {
			out.cols = append(out.cols, k)
		}
		sort.Strings(out.cols)
	}
	for _, r := range rows {
		m := make(map[string]string, len(r))
		for k, v := range r {
			m[k] = pmRender(v)
		}
		out.rows = append(out.rows, m)
	}
	return out
}

type pmDoor struct {
	name string
	run  func(t *testing.T, key, sql string) (pmResult, error)
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

type pmRig struct {
	doors    []pmDoor
	provider *auth.Provider
	// asyncBase is the HTTP base URL of the coordinator-backed server, the
	// one that carries POST /v1/queries/async.
	asyncBase string
}

func pmWriteFixture(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog) {
	t.Helper()
	pmWriteTable(t, ctx, store, cat, pmTable, pmSchema(), pmFixture())
	pmWriteTable(t, ctx, store, cat, pmOther, pmOtherSchema(), pmOtherFixture())
	pmWriteTable(t, ctx, store, cat, pmBal, pmBalSchema(), pmBalFixture())
}

func pmWriteTable(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog,
	table string, sch parquet.Schema, rows []map[string]any) {
	t.Helper()
	if err := cat.CreateTable(ctx, table, sch, nil); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	const chunks = 3
	per := (len(rows) + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, min(c*per+per, len(rows))
		if lo >= hi {
			break
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, sch, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, c)
		payload := buf.Bytes()
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(payload),
			int64(len(payload)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: int64(len(payload)),
			NumRows: int64(hi - lo), CreatedAt: time.Now(),
		})
	}
	if err := cat.AddFiles(ctx, table, map[string]string{}, "tables/"+table+"/", entries); err != nil {
		t.Fatal(err)
	}
}

func pmEmbeddedDB(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ingest1 := func(table string, sch parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, table, sch, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(table, sch, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 4})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ingest1(pmTable, pmSchema(), pmFixture())
	ingest1(pmOther, pmOtherSchema(), pmOtherFixture())
	ingest1(pmBal, pmBalSchema(), pmBalFixture())
	return db
}

// pmRigUp stands up every door over the arc's mask/deny provider.
func pmRigUp(t *testing.T, ctx context.Context) pmRig {
	t.Helper()
	return pmRigUpWith(t, ctx, pmProvider(t))
}

// pmRigUpWith stands up every door over the given policy provider.
func pmRigUpWith(t *testing.T, ctx context.Context, provider *auth.Provider) pmRig {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// The in-process doors are handed the identity directly; resolve it
	// through the provider's own authenticator so all three doors carry the
	// identity the API key really names.
	ctxFor := func(key string) context.Context {
		if key == "" {
			return ctx
		}
		id, err := provider.Authenticator().AuthenticateToken(key)
		if err != nil {
			t.Fatalf("authenticate %q: %v", key, err)
		}
		return auth.ContextWithIdentity(ctx, id)
	}

	var doors []pmDoor

	// --- embedded: single and spilled -------------------------------------
	single := pmEmbeddedDB(t, ctx, 0)
	single.SetAuthProvider(provider)
	spilled := pmEmbeddedDB(t, ctx, 512*1024)
	spilled.SetAuthProvider(provider)
	embed := func(db *wadjet.DB) func(*testing.T, string, string) (pmResult, error) {
		return func(t *testing.T, key, sql string) (res pmResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := db.Query(ctxFor(key), sql)
			if qerr != nil {
				return pmResult{}, qerr
			}
			return pmFromRows(out.Columns, out.Rows), nil
		}
	}
	doors = append(doors,
		pmDoor{"embedded/single", embed(single)},
		pmDoor{"embedded/spilled", embed(spilled)})

	// --- distributed infra shared by the DAG arms -------------------------
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	t.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	pmWriteFixture(t, ctx, store, cat)

	ids := make([]string, 3)
	for i := range ids {
		ids[i] = fmt.Sprintf("e7-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID: ids[i], NATSUrl: embedded.ClientURL(),
			MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
		}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}
	newCoord := func(opts ...func(*coordinator.Config)) *coordinator.Coordinator {
		cfg := coordinator.Config{NATSUrl: embedded.ClientURL(), ResultBucket: "test", LocalFastPathBytes: 0}
		for _, o := range opts {
			o(&cfg)
		}
		c := coordinator.New(cfg, cat, nc, js, logger)
		c.SetAuthProvider(provider)
		return c
	}
	dag := newCoord()
	dagShuffled := newCoord(func(c *coordinator.Config) { c.BroadcastBytesOverride = 1 })
	// Heartbeat on the workers' behalf so planning sees the cluster now.
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, id := range ids {
			hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 4, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
				t.Fatal(err)
			}
		}
		nc.Flush()
		if dag.Workers().Count() >= 3 && dagShuffled.Workers().Count() >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers did not register: dag=%d shuffled=%d",
				dag.Workers().Count(), dagShuffled.Workers().Count())
		}
		time.Sleep(50 * time.Millisecond)
	}

	dagRun := func(c *coordinator.Coordinator) func(*testing.T, string, string) (pmResult, error) {
		return func(t *testing.T, key, sql string) (res pmResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := c.ExecuteSQL(ctxFor(key), sql)
			if qerr != nil {
				return pmResult{}, qerr
			}
			if out.Error != "" {
				return pmResult{}, fmt.Errorf("%s", out.Error)
			}
			rows, rerr := out.Rows()
			if rerr != nil {
				return pmResult{}, rerr
			}
			return pmFromRows(out.Columns, rows), nil
		}
	}
	doors = append(doors,
		pmDoor{"embedded/dag", dagRun(dag)},
		pmDoor{"embedded/dag-shuffled", dagRun(dagShuffled)})

	// --- pgwire: the single-process door and the DAG door -----------------
	pgSingle := pgwire.NewServer(single, pgwire.Config{AuthProvider: provider}, logger)
	if err := pgSingle.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pgSingle.Shutdown)
	pgDAGdb, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pgDAGdb.Close() })
	pgDAGdb.SetAuthProvider(provider)
	pgDAG := pgwire.NewServer(pgDAGdb, pgwire.Config{AuthProvider: provider}, logger)
	pgDAG.SetCoordinator(dag)
	if err := pgDAG.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pgDAG.Shutdown)

	pgRun := func(addr string) func(*testing.T, string, string) (pmResult, error) {
		return func(t *testing.T, key, sql string) (pmResult, error) {
			pw := key
			if pw == "" {
				pw = "none"
			}
			dsn := fmt.Sprintf("postgres://wadjet:%s@%s/wadjet?sslmode=disable", pw, addr)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return pmResult{}, err
			}
			defer conn.Close(ctx)
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				return pmResult{}, err
			}
			defer rows.Close()
			var out pmResult
			for _, fd := range rows.FieldDescriptions() {
				out.cols = append(out.cols, fd.Name)
			}
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return pmResult{}, err
				}
				m := make(map[string]string, len(vals))
				for i, v := range vals {
					if i < len(out.cols) {
						m[out.cols[i]] = pmRender(v)
					}
				}
				out.rows = append(out.rows, m)
			}
			if err := rows.Err(); err != nil {
				return pmResult{}, err
			}
			return out, nil
		}
	}
	doors = append(doors,
		pmDoor{"pgwire/single", pgRun(pgSingle.Addr())},
		pmDoor{"pgwire/dag", pgRun(pgDAG.Addr())})

	// --- HTTP: the local (standalone) door and the coordinator door -------
	httpLocal := New(Config{Addr: ":0", Catalog: single.Catalog(), Provider: provider}, logger)
	hsLocal := httptest.NewServer(httpLocal.Mux())
	t.Cleanup(hsLocal.Close)
	httpDAG := New(Config{Addr: ":0", Catalog: cat, Coordinator: dag, Provider: provider}, logger)
	hsDAG := httptest.NewServer(httpDAG.Mux())
	t.Cleanup(hsDAG.Close)

	httpRun := func(base string) func(*testing.T, string, string) (pmResult, error) {
		return func(t *testing.T, key, sql string) (pmResult, error) {
			body, _ := json.Marshal(map[string]string{"sql": sql})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				base+"/v1/queries", bytes.NewReader(body))
			if err != nil {
				return pmResult{}, err
			}
			req.Header.Set("Content-Type", "application/json")
			if key != "" {
				req.Header.Set("Authorization", "Bearer "+key)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return pmResult{}, err
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				return pmResult{}, err
			}
			var parsed struct {
				Columns []string         `json:"columns"`
				Rows    []map[string]any `json:"rows"`
				Error   string           `json:"error"`
			}
			if err := json.Unmarshal(raw, &parsed); err != nil {
				return pmResult{}, fmt.Errorf("status %d, unparseable body %q", resp.StatusCode, raw)
			}
			if parsed.Error != "" {
				return pmResult{}, fmt.Errorf("%s", parsed.Error)
			}
			if resp.StatusCode != http.StatusOK {
				return pmResult{}, fmt.Errorf("status %d: %s", resp.StatusCode, raw)
			}
			return pmFromRows(parsed.Columns, parsed.Rows), nil
		}
	}
	doors = append(doors,
		pmDoor{"http/local", httpRun(hsLocal.URL)},
		pmDoor{"http/dag", httpRun(hsDAG.URL)})

	return pmRig{doors: doors, provider: provider, asyncBase: hsDAG.URL}
}

// ---------------------------------------------------------------------------
// The shapes
// ---------------------------------------------------------------------------

type pmCell struct {
	name string
	sql  string
	// want is the canonical row list under the ANALYST identity, columns
	// sorted alphabetically inside each row.
	want []string
	// wantErrLike, when set, is what the analyst identity must be REFUSED
	// with instead of an answer.
	wantErrLike string
	// There is deliberately NO per-arm expectation here. Every cell asserts
	// ONE answer on all eight runners. An earlier revision carried a
	// `wantDAG` pin for a shape the DAG answered differently; the review
	// measured that difference and it was a per-row disclosure, not a path
	// quirk. A pin is never the disposition for a leak: the shape either
	// answers what the in-process pipeline answers, or it refuses.
	//
	// deniedLike names the DENIED column in sql. The cell then asserts that
	// the analyst is refused with exactly the error the SAME STATEMENT gets
	// when that identifier names a column the table really does not have —
	// the whole content of "a denied column does not exist".
	deniedLike string
	// noSalary asserts the denied column is absent from the result schema.
	noSalary bool
}

func pmCells() []pmCell {
	all := func(f func(i int) string) []string {
		out := make([]string, 0, pmRows)
		for i := 1; i <= pmRows; i++ {
			out = append(out, f(i))
		}
		sort.Strings(out)
		return out
	}
	return []pmCell{
		{name: "select_masked_column", sql: `SELECT id, ssn FROM e7emp`,
			want: all(func(i int) string { return fmt.Sprintf("id=%d|ssn=%s", i, pmMaskSSN) })},
		{name: "select_star", sql: `SELECT * FROM e7emp`, noSalary: true,
			want: all(func(i int) string {
				return fmt.Sprintf("acct=0|amt=%d|dept=d%d|id=%d|ssn=%s", i, i%3, i, pmMaskSSN)
			})},
		{name: "sum_of_a_masked_numeric", sql: `SELECT SUM(acct) AS s FROM e7emp`,
			want: []string{"s=0"}},
		{name: "min_max_of_a_masked_column", sql: `SELECT MIN(ssn) AS lo, MAX(ssn) AS hi FROM e7emp`,
			want: []string{"hi=" + pmMaskSSN + "|lo=" + pmMaskSSN}},
		{name: "count_distinct_masked", sql: `SELECT COUNT(DISTINCT ssn) AS c FROM e7emp`,
			want: []string{"c=1"}},
		{name: "group_by_masked", sql: `SELECT ssn, COUNT(*) AS c FROM e7emp GROUP BY ssn`,
			want: []string{fmt.Sprintf("c=%d|ssn=%s", pmRows, pmMaskSSN)}},
		{name: "order_by_masked", sql: `SELECT ssn FROM e7emp ORDER BY ssn`,
			want: all(func(int) string { return "ssn=" + pmMaskSSN })},
		{name: "self_join_on_masked", sql: `SELECT COUNT(*) AS c FROM e7emp a JOIN e7emp b ON a.ssn = b.ssn`,
			want: []string{fmt.Sprintf("c=%d", pmRows*pmRows)}},
		{name: "where_equals_the_true_value", sql: `SELECT COUNT(*) AS c FROM e7emp WHERE ssn = 'true-ssn-01'`,
			want: []string{"c=0"}},
		{name: "where_equals_the_mask", sql: `SELECT COUNT(*) AS c FROM e7emp WHERE ssn = '***'`,
			want: []string{fmt.Sprintf("c=%d", pmRows)}},
		{name: "derived_table", sql: `SELECT d.ssn AS s, COUNT(*) AS c FROM (SELECT ssn FROM e7emp) d GROUP BY d.ssn`,
			want: []string{fmt.Sprintf("c=%d|s=%s", pmRows, pmMaskSSN)}},
		{name: "cte", sql: `WITH u AS (SELECT ssn, acct FROM e7emp) SELECT ssn, SUM(acct) AS s FROM u GROUP BY ssn`,
			want: []string{fmt.Sprintf("s=0|ssn=%s", pmMaskSSN)}},
		{name: "union_all_arm", sql: `SELECT ssn FROM e7emp UNION ALL SELECT ssn FROM e7emp`,
			want: func() []string {
				out := all(func(int) string { return "ssn=" + pmMaskSSN })
				return append(out, out...)
			}()},
		{name: "window_partition_by_masked",
			sql: `SELECT id, SUM(amt) OVER (PARTITION BY ssn) AS w FROM e7emp`,
			want: all(func(i int) string {
				return fmt.Sprintf("id=%d|w=%d", i, pmRows*(pmRows+1)/2)
			})},
		{name: "scalar_subquery_over_masked",
			sql:  `SELECT id, (SELECT MAX(ssn) FROM e7emp) AS m FROM e7emp WHERE id = 1`,
			want: []string{"id=1|m=" + pmMaskSSN}},
		// A DENIED column does not exist for this identity, so every one of
		// these must be the SAME refusal the engine gives for a column that
		// really is not there — asserted by substitution against
		// `nosuchcol`, not by a literal message this gate would then own.
		{name: "select_denied_column", sql: `SELECT salary FROM e7emp`, deniedLike: "salary"},
		{name: "aggregate_over_denied_column", sql: `SELECT SUM(salary) AS s FROM e7emp`, deniedLike: "salary"},
		{name: "where_on_denied_column", sql: `SELECT COUNT(*) AS c FROM e7emp WHERE salary > 0`, deniedLike: "salary"},
		{name: "qualified_denied_column", sql: `SELECT a.salary FROM e7emp a`, deniedLike: "salary"},
		{name: "denied_column_in_a_derived_table",
			sql: `SELECT d.salary AS s FROM (SELECT salary FROM e7emp) d`, deniedLike: "salary"},
		{name: "denied_column_in_a_cte",
			sql: `WITH u AS (SELECT salary FROM e7emp) SELECT salary FROM u`, deniedLike: "salary"},
		{name: "denied_column_in_a_scalar_subquery",
			sql:        `SELECT id, (SELECT MAX(salary) FROM e7emp) AS m FROM e7emp WHERE id = 1`,
			deniedLike: "salary"},
		{name: "denied_column_in_a_union_arm",
			sql: `SELECT id FROM e7emp UNION ALL SELECT salary FROM e7emp`, deniedLike: "salary"},

		// ------------------------------------------------------------------
		// The policy's TARGETING. A policy binds to a RELATION — the catalog
		// table the scan reads — and never to a name the statement minted.
		// Matching an alias made `FROM e7other AS e7emp` get e7emp's schema
		// projected over a scan of e7other: its own `note` column vanished
		// and every row came back NULL, a silently wrong answer for a query
		// that does not read the policed table at all.
		{name: "unpoliced_table_aliased_to_the_policed_name",
			sql:  `SELECT e7emp.id, e7emp.note FROM e7other AS e7emp ORDER BY e7emp.id`,
			want: []string{"id=1|note=n1", "id=2|note=n2", "id=3|note=n3"}},
		{name: "unpoliced_table_aliased_to_the_policed_name_beside_it",
			sql:  `SELECT e7emp.id, e7emp.note FROM e7other AS e7emp, e7emp AS z WHERE z.id = 1 ORDER BY e7emp.id`,
			want: []string{"id=1|note=n1", "id=2|note=n2", "id=3|note=n3"}},
		{name: "unpoliced_table_aliased_to_the_policed_name_case_folded",
			sql:  `SELECT e7emp.note AS n FROM e7other AS E7EMP ORDER BY e7emp.id`,
			want: []string{"n=n1", "n=n2", "n=n3"}},
		{name: "a_cte_named_like_the_policed_table",
			sql:  `WITH e7emp AS (SELECT id, note FROM e7other) SELECT note FROM e7emp ORDER BY id`,
			want: []string{"note=n1", "note=n2", "note=n3"}},
		{name: "the_policed_table_under_two_aliases",
			sql:  `SELECT a.ssn AS x, b.ssn AS y FROM e7emp a JOIN e7emp b ON a.id = b.id WHERE a.id = 1`,
			want: []string{"x=***|y=***"}},
		{name: "the_policed_table_joined_to_an_unpoliced_one",
			sql:  `SELECT o.note AS n, e.ssn AS s FROM e7other o JOIN e7emp e ON o.id = e.id ORDER BY o.id`,
			want: []string{"n=n1|s=***", "n=n2|s=***", "n=n3|s=***"}},

		// ------------------------------------------------------------------
		// Round-1 review: the shapes the first matrix did not carry. Thirteen
		// of them leaked at af6f18db on all eight runners — EXCEPT, INTERSECT
		// and UNION-distinct over a masked column, and a correlated scalar
		// subquery's OUTER reference, among them.
		{name: "window_order_by_masked",
			sql:  `SELECT id, SUM(amt) OVER (ORDER BY ssn) AS w FROM e7emp`,
			want: all(func(i int) string { return fmt.Sprintf("id=%d|w=78", i) })},
		{name: "window_lag_over_masked",
			sql: `SELECT id, LAG(ssn) OVER (ORDER BY id) AS p FROM e7emp`,
			want: all(func(i int) string {
				if i == 1 {
					return "id=1|p=NULL"
				}
				return fmt.Sprintf("id=%d|p=%s", i, pmMaskSSN)
			})},
		{name: "case_over_masked",
			sql:  `SELECT id, CASE WHEN ssn = 'true-ssn-01' THEN 'HIT' ELSE ssn END AS c FROM e7emp WHERE id = 1`,
			want: []string{"c=" + pmMaskSSN + "|id=1"}},
		{name: "cast_of_masked",
			sql: `SELECT CAST(ssn AS VARCHAR) AS c FROM e7emp WHERE id = 1`, want: []string{"c=" + pmMaskSSN}},
		{name: "substring_of_masked",
			sql: `SELECT SUBSTRING(ssn, 1, 8) AS c FROM e7emp WHERE id = 1`, want: []string{"c=" + pmMaskSSN}},
		{name: "concat_of_masked",
			sql: `SELECT ssn || '-x' AS c FROM e7emp WHERE id = 1`, want: []string{"c=" + pmMaskSSN + "-x"}},
		{name: "having_on_masked",
			sql:  `SELECT dept, MAX(ssn) AS m FROM e7emp GROUP BY dept HAVING MAX(ssn) = 'true-ssn-03'`,
			want: nil},
		{name: "having_on_masked_numeric",
			sql:  `SELECT dept, SUM(acct) AS s FROM e7emp GROUP BY dept HAVING SUM(acct) > 0`,
			want: nil},
		{name: "in_subquery_masked",
			sql:  `SELECT COUNT(*) AS c FROM e7emp a WHERE a.ssn IN (SELECT ssn FROM e7emp b WHERE b.id < 3)`,
			want: []string{"c=12"}},
		{name: "correlated_exists_masked",
			sql:  `SELECT COUNT(*) AS c FROM e7emp a WHERE EXISTS (SELECT 1 FROM e7emp b WHERE b.ssn = a.ssn AND b.id = 1)`,
			want: []string{"c=12"}},
		{name: "correlated_scalar_outer_ref",
			sql:  `SELECT a.id, (SELECT MIN(b.ssn) FROM e7emp b WHERE b.id = a.id) AS m FROM e7emp a WHERE a.id < 3`,
			want: []string{"id=1|m=" + pmMaskSSN, "id=2|m=" + pmMaskSSN}},
		{name: "distinct_masked", sql: `SELECT DISTINCT ssn FROM e7emp`, want: []string{"ssn=" + pmMaskSSN}},
		{name: "mixed_conjunction_pushdown",
			sql: `SELECT COUNT(*) AS c FROM e7emp WHERE dept = 'd1' AND ssn = 'true-ssn-01'`, want: []string{"c=0"}},
		{name: "like_on_masked",
			sql: `SELECT COUNT(*) AS c FROM e7emp WHERE ssn LIKE 'true%'`, want: []string{"c=0"}},
		{name: "in_list_on_masked",
			sql: `SELECT COUNT(*) AS c FROM e7emp WHERE ssn IN ('true-ssn-01','true-ssn-02')`, want: []string{"c=0"}},
		{name: "numeric_eq_on_masked",
			sql: `SELECT COUNT(*) AS c FROM e7emp WHERE acct = 900001`, want: []string{"c=0"}},
		{name: "numeric_range_on_masked",
			sql: `SELECT COUNT(*) AS c FROM e7emp WHERE acct > 500000`, want: []string{"c=0"}},
		{name: "order_by_masked_limit",
			sql:  `SELECT id, ssn FROM e7emp ORDER BY ssn DESC, id LIMIT 3`,
			want: []string{"id=1|ssn=" + pmMaskSSN, "id=2|ssn=" + pmMaskSSN, "id=3|ssn=" + pmMaskSSN}},
		{name: "top_n_on_masked_numeric",
			// The VALUE, not which row won it: every key is the mask, so the
			// tie is ADR-0013 legal nondeterminism.
			sql: `SELECT acct FROM e7emp ORDER BY acct DESC LIMIT 1`, want: []string{"acct=0"}},
		{name: "union_distinct_arm",
			sql: `SELECT ssn FROM e7emp UNION SELECT ssn FROM e7emp`, want: []string{"ssn=" + pmMaskSSN}},
		{name: "except_arm",
			sql: `SELECT ssn FROM e7emp EXCEPT SELECT 'zzz'`, want: []string{"ssn=" + pmMaskSSN}},
		{name: "intersect_arm",
			sql: `SELECT ssn FROM e7emp INTERSECT SELECT ssn FROM e7emp`, want: []string{"ssn=" + pmMaskSSN}},
		{name: "nested_derived_two_deep",
			sql:  `SELECT x.s AS s FROM (SELECT d.ssn AS s FROM (SELECT ssn FROM e7emp) d) x`,
			want: all(func(int) string { return "s=" + pmMaskSSN })},
		{name: "cte_referenced_twice",
			sql:  `WITH u AS (SELECT ssn FROM e7emp) SELECT COUNT(*) AS c FROM u a JOIN u b ON a.ssn = b.ssn`,
			want: []string{"c=144"}},
		{name: "join_masked_to_unmasked",
			sql: `SELECT COUNT(*) AS c FROM e7emp a JOIN e7emp b ON a.ssn = b.dept`, want: []string{"c=0"}},
		{name: "case_insensitive_table_name",
			sql:  `SELECT ssn FROM E7EMP`,
			want: all(func(int) string { return "ssn=" + pmMaskSSN })},
		{name: "quoted_column",
			sql:  `SELECT "ssn" FROM e7emp`,
			want: all(func(int) string { return "ssn=" + pmMaskSSN })},
		{name: "count_star_with_a_denied_column",
			sql: `SELECT COUNT(*) AS c FROM e7emp`, want: []string{"c=12"}},
		{name: "qualified_star", sql: `SELECT a.* FROM e7emp a`, noSalary: true,
			want: all(func(i int) string {
				return fmt.Sprintf("acct=0|amt=%d|dept=d%d|id=%d|ssn=%s", i, i%3, i, pmMaskSSN)
			})},
		{name: "aggregate_of_a_case_over_masked",
			sql:  `SELECT SUM(CASE WHEN ssn = 'true-ssn-01' THEN 1 ELSE 0 END) AS c FROM e7emp`,
			want: []string{"c=0"}},
		{name: "coalesce_masked",
			sql: `SELECT COALESCE(ssn, 'x') AS c FROM e7emp WHERE id = 1`, want: []string{"c=" + pmMaskSSN}},
		{name: "group_by_an_expression_over_masked",
			sql:  `SELECT SUBSTRING(ssn,1,4) AS g, COUNT(*) AS c FROM e7emp GROUP BY SUBSTRING(ssn,1,4)`,
			want: []string{"c=12|g=" + pmMaskSSN}},
		// The oracle probe goes THROUGH the deferred-scalar producer: a
		// table-less `(SELECT 'true-ssn-01')` folds before planning and
		// answers c=0 under every hypothesis, so it could not fail (round-2
		// P1). Over the policed table it discriminates: if the outer column
		// were read stored, `ssn = (SELECT MIN('true-ssn-01') FROM e7emp)`
		// would match the one row whose stored ssn is that value.
		{name: "scalar_subquery_oracle_probe",
			sql:  `SELECT COUNT(*) AS c FROM e7emp WHERE ssn = (SELECT MIN('true-ssn-01') FROM e7emp)`,
			want: []string{"c=0"}},
		{name: "scalar_subquery_oracle_probe_numeric",
			sql:  `SELECT COUNT(*) AS c FROM e7emp WHERE acct = (SELECT MIN(900001) FROM e7emp)`,
			want: []string{"c=0"}},
		{name: "scalar_subquery_in_where",
			sql:  `SELECT COUNT(*) AS c FROM e7emp WHERE ssn = (SELECT MIN(ssn) FROM e7emp)`,
			want: []string{"c=12"}},

		// ------------------------------------------------------------------
		// THE PER-ROW BIT. `bal` is masked to 0 and its stored values have
		// both signs, so a predicate that reads the MASK answers the same for
		// every row and one that reads the STORED column answers each row's
		// SIGN — the row set IS the hidden value. On the DAG the scan
		// fragment used to run OpScan → OpFilter → SecurityProject, one
		// filter slot for the policy's row filter AND the user's predicate,
		// so `WHERE bal > (SELECT MIN(bal) FROM e7bal)` returned exactly the
		// rows whose stored balance was positive (ids 2 4 6 8) where the
		// in-process pipeline returned none. Every cell below asserts the
		// in-process answer on all eight runners.
		{name: "per_row_bit_scalar_subquery_gt",
			sql:  `SELECT id FROM e7bal WHERE bal > (SELECT MIN(bal) FROM e7bal) ORDER BY id`,
			want: nil},
		{name: "per_row_bit_scalar_subquery_lt",
			sql:  `SELECT id FROM e7bal WHERE bal < (SELECT MIN(bal) FROM e7bal) ORDER BY id`,
			want: nil},
		{name: "per_row_bit_scalar_subquery_ge",
			sql:  `SELECT COUNT(*) AS c FROM e7bal WHERE bal >= (SELECT MIN(bal) FROM e7bal)`,
			want: []string{"c=8"}},
		{name: "per_row_bit_literal_predicate",
			sql: `SELECT id FROM e7bal WHERE bal > 0 ORDER BY id`, want: nil},
		{name: "per_row_bit_literal_predicate_negative",
			sql: `SELECT id FROM e7bal WHERE bal < 0 ORDER BY id`, want: nil},
		{name: "per_row_bit_in_subquery",
			sql:  `SELECT COUNT(*) AS c FROM e7bal WHERE bal IN (SELECT bal FROM e7bal)`,
			want: []string{"c=8"}},
		{name: "per_row_bit_in_list",
			sql:  `SELECT COUNT(*) AS c FROM e7bal WHERE bal IN (-100, 200, 0)`,
			want: []string{"c=8"}},
		{name: "per_row_bit_having",
			sql:  `SELECT COUNT(*) AS c FROM e7bal GROUP BY bal HAVING MIN(bal) < 0`,
			want: nil},
		{name: "per_row_bit_having_on_the_mask",
			sql:  `SELECT COUNT(*) AS c FROM e7bal GROUP BY bal HAVING MIN(bal) = 0`,
			want: []string{"c=8"}},
		{name: "per_row_bit_sum_and_min",
			sql:  `SELECT SUM(bal) AS s, MIN(bal) AS lo, MAX(bal) AS hi FROM e7bal`,
			want: []string{"hi=0|lo=0|s=0"}},

		// ------------------------------------------------------------------
		// The predicate INSIDE a subquery. The matrix's older IN/EXISTS cells
		// put the inner predicate on an UNPOLICED column, so they passed with
		// the inner scan's security projection missing entirely — and it was:
		// a decorrelated semi-join's inner side was built with no projection,
		// its predicate read the STORED column, and `… IN (SELECT id FROM t
		// WHERE bal > 300)` returned exactly the rows above the threshold the
		// CLIENT chose. Five such probes recover every hidden value
		// (#859 round 3). Mixed signs are load-bearing: over a same-sign
		// column a mask of 0 hides the defect.
		{name: "inner_predicate_over_masked_gt_zero",
			sql:  `SELECT id FROM e7bal WHERE id IN (SELECT id FROM e7bal WHERE bal > 0) ORDER BY id`,
			want: nil},
		{name: "inner_predicate_over_masked_lt_zero",
			sql:  `SELECT id FROM e7bal WHERE id IN (SELECT id FROM e7bal WHERE bal < 0) ORDER BY id`,
			want: nil},
		{name: "inner_predicate_over_masked_threshold",
			sql:  `SELECT id FROM e7bal WHERE id IN (SELECT id FROM e7bal WHERE bal > 300) ORDER BY id`,
			want: nil},
		{name: "inner_predicate_over_masked_not_in",
			sql:  `SELECT COUNT(*) AS c FROM e7bal WHERE id NOT IN (SELECT id FROM e7bal WHERE bal > 0)`,
			want: []string{"c=8"}},
		{name: "inner_predicate_over_masked_exists",
			sql: `SELECT id FROM e7bal a WHERE EXISTS (` +
				`SELECT 1 FROM e7bal b WHERE b.id = a.id AND b.bal > 0) ORDER BY id`,
			want: nil},
		{name: "inner_predicate_over_masked_not_exists",
			sql: `SELECT COUNT(*) AS c FROM e7bal a WHERE NOT EXISTS (` +
				`SELECT 1 FROM e7bal b WHERE b.id = a.id AND b.bal > 0)`,
			want: []string{"c=8"}},
		{name: "inner_predicate_over_masked_scalar_subquery",
			sql: `SELECT id FROM e7bal WHERE bal > (` +
				`SELECT MAX(bal) FROM e7bal WHERE bal > 0) ORDER BY id`,
			want: nil},
		// LATERAL's decorrelated inner keeps its predicate below the
		// projection in a shape the planner cannot reorder, so the invariant
		// REFUSES on every arm and every door rather than answer. That is the
		// branch's doctrine for a shape it cannot express safely, and it is
		// uniform — the same 0A000 everywhere, not an arm divergence.
		{name: "inner_predicate_over_masked_lateral",
			sql: `SELECT COUNT(*) AS c FROM e7bal a, LATERAL (` +
				`SELECT bal FROM e7bal b WHERE b.id = a.id AND b.bal > 0) x`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "inner_predicate_over_masked_derived_table",
			sql:  `SELECT id FROM (SELECT id, bal FROM e7bal WHERE bal > 0) t ORDER BY id`,
			want: nil},
		{name: "inner_predicate_over_masked_cte",
			sql:  `WITH u AS (SELECT id, bal FROM e7bal WHERE bal > 0) SELECT id FROM u ORDER BY id`,
			want: nil},

		// A DERIVED TABLE inside an `IN` list. The planner folds it into the
		// outer plan, so there is one plan to order and the predicate reads
		// the mask: `t.bal > 0` is false on every row where the stored column
		// would answer the row's SIGN. Both constants discriminate — a stored
		// read answers `id=2` under the unpoliced outer and `2 4 6 8` under
		// the policed one, and the empty answer is the mask's.
		//
		// This spelling REFUSED until v0.18.36's set-operation work changed
		// how a derived table inside an IN list is planned; the refusal is
		// recorded in the round-5 report because a shape moving from 0A000 to
		// an answer is a change in what the branch promises.
		{name: "derived_table_inside_in_reads_the_mask",
			sql: `SELECT d.id FROM e7other d WHERE d.id IN (` +
				`SELECT t.id FROM (SELECT id, bal FROM e7bal) t WHERE t.bal > 0) ORDER BY d.id`,
			want: nil},
		{name: "derived_table_inside_in_outer_reads_it",
			sql: `SELECT id FROM e7bal WHERE id IN (` +
				`SELECT t.id FROM (SELECT id, bal FROM e7bal) t WHERE t.bal > 0) ORDER BY id`,
			want: nil},

		// The inner predicate names the MASK ITSELF. Under masking every row's
		// value IS the mask, so `mask = mask` holds on every row and the
		// answer is every row. This was the arc's one arm split: in process
		// the semi-join's build side yielded NOTHING for a predicate over a
		// COMPUTED projection output — a predicate over a PASSTHROUGH column
		// of the same barrier was already right — so the outer answered 0
		// while the DAG answered the full count (#859 round 5, P2).
		{name: "semi_join_inner_predicate_names_the_mask",
			sql: `SELECT COUNT(*) AS c FROM e7emp a WHERE a.id IN (` +
				`SELECT b.id FROM e7emp b WHERE b.ssn = '***')`,
			want: []string{"c=12"}},
		{name: "semi_join_inner_predicate_names_the_mask_exists",
			sql: `SELECT COUNT(*) AS c FROM e7emp a WHERE EXISTS (` +
				`SELECT 1 FROM e7emp b WHERE b.id = a.id AND b.ssn = '***')`,
			want: []string{"c=12"}},
		{name: "semi_join_inner_predicate_names_the_mask_not_in",
			sql: `SELECT COUNT(*) AS c FROM e7emp a WHERE a.id NOT IN (` +
				`SELECT b.id FROM e7emp b WHERE b.ssn = '***')`,
			want: []string{"c=0"}},
		{name: "semi_join_inner_predicate_names_the_numeric_mask",
			sql: `SELECT COUNT(*) AS c FROM e7bal a WHERE a.id IN (` +
				`SELECT b.id FROM e7bal b WHERE b.bal = 0)`,
			want: []string{"c=8"}},
		// The passthrough twin, so the pair LOCALIZES the defect rather than
		// merely detecting it: same shape, inner predicate over an unpoliced
		// column of the same barrier.
		{name: "semi_join_inner_predicate_names_a_passthrough_column",
			sql: `SELECT COUNT(*) AS c FROM e7emp a WHERE a.id IN (` +
				`SELECT b.id FROM e7emp b WHERE b.dept = 'd1')`,
			want: []string{"c=4"}},

		// ------------------------------------------------------------------
		// The relation named ONLY inside a subquery, in the spellings a client
		// chooses. Each of these reached the stored column on every arm until
		// the invariant was asked over the subquery's OWN plan: the inner
		// query is SQL TEXT when enforcement runs, so what it contains — a
		// derived table, a set operation, a correlation — is the client's
		// choice and no per-shape teaching can enumerate it (#859 round 4).
		// Every one of them REFUSES 0A000, uniformly on all eight runners: the
		// planner cannot show that the subquery's own plan keeps its
		// predicates above the projection, and the branch's doctrine for a
		// shape it cannot order safely is to refuse.
		//
		// The line is drawn by the INNER PLAN, not by the outer statement's
		// FROM list. A subquery the optimizer folds into the outer plan — a
		// plain `IN (SELECT col FROM policed WHERE …)`, an `EXISTS`, a
		// derived table or CTE in the FROM clause — is one plan, ordered and
		// answered: that is the ten `inner_predicate_over_masked_*` cells
		// above. A subquery that keeps a plan of its own refuses, and it
		// refuses whether or not the outer statement reads the same relation
		// — the three `*_outer_reads_it` cells below are the same three
		// spellings with `e7bal` on both sides, and they refuse identically.
		{name: "hidden_relation_union_all_inside_in",
			sql: `SELECT d.id FROM e7other d WHERE d.id IN (` +
				`SELECT id FROM e7bal WHERE bal > 300 UNION ALL SELECT id FROM e7bal WHERE bal > 500) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		// UNION-distinct inside the subquery. Spelled with IN rather than
		// EXISTS: an EXISTS over a set operation is refused on the DAG arms
		// for a pre-existing reason of its own ("EXISTS subquery requires a
		// SubqueryRunner"), so that spelling cannot assert ONE disposition on
		// all eight runners — and a cell that accepts two messages asserts
		// neither.
		{name: "hidden_relation_union_distinct_inside_in",
			sql: `SELECT d.id FROM e7other d WHERE d.id IN (` +
				`SELECT id FROM e7bal WHERE bal > 300 UNION SELECT id FROM e7bal WHERE bal > 500) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "hidden_relation_intersect_inside_in",
			sql: `SELECT d.id FROM e7other d WHERE d.id IN (` +
				`SELECT id FROM e7bal WHERE bal > 300 INTERSECT SELECT id FROM e7bal) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "hidden_relation_except_inside_in",
			sql: `SELECT d.id FROM e7other d WHERE d.id IN (` +
				`SELECT id FROM e7bal EXCEPT SELECT id FROM e7bal WHERE bal > 300) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "hidden_relation_correlated_scalar",
			sql: `SELECT d.id FROM e7other d WHERE d.id = (` +
				`SELECT b.id FROM e7bal b WHERE b.id = d.id AND b.bal > 300) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "hidden_relation_derived_inside_union_inside_in",
			sql: `SELECT d.id FROM e7other d WHERE d.id IN (` +
				`SELECT t.id FROM (SELECT id, bal FROM e7bal) t WHERE t.bal > 300 ` +
				`UNION ALL SELECT id FROM e7bal WHERE bal > 500) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "hidden_relation_correlated_scalar_over_a_cte",
			sql: `WITH u AS (SELECT id, bal FROM e7bal) SELECT d.id FROM e7other d WHERE d.id = (` +
				`SELECT u.id FROM u WHERE u.id = d.id AND u.bal > 300) ORDER BY d.id`,
			wantErrLike: "could not be placed above the security projection"},
		// The same spellings with the POLICED relation on BOTH sides. An
		// earlier revision of docs/security.md drew the boundary at the outer
		// statement's FROM list — "a subquery over a table the outer statement
		// also reads answers normally" — and these cells are why that sentence
		// is gone (#859 round 5, review P1).
		{name: "hidden_relation_union_all_inside_in_outer_reads_it",
			sql: `SELECT id FROM e7bal WHERE id IN (` +
				`SELECT id FROM e7bal WHERE bal > 300 UNION ALL SELECT id FROM e7bal WHERE bal > 500) ORDER BY id`,
			wantErrLike: "could not be placed above the security projection"},
		{name: "hidden_relation_correlated_scalar_outer_reads_it",
			sql: `SELECT a.id FROM e7bal a WHERE a.id = (` +
				`SELECT b.id FROM e7bal b WHERE b.id = a.id AND b.bal > 300) ORDER BY a.id`,
			wantErrLike: "could not be placed above the security projection"},

		{name: "denied_in_order_by", sql: `SELECT id FROM e7emp ORDER BY salary`, deniedLike: "salary"},
		{name: "denied_in_group_by",
			sql: `SELECT salary, COUNT(*) AS c FROM e7emp GROUP BY salary`, deniedLike: "salary"},
		{name: "denied_in_having",
			sql: `SELECT dept FROM e7emp GROUP BY dept HAVING MAX(salary) > 0`, deniedLike: "salary"},
		{name: "denied_in_case",
			sql: `SELECT CASE WHEN salary > 0 THEN 1 ELSE 0 END AS c FROM e7emp`, deniedLike: "salary"},
		{name: "denied_in_join_on",
			sql: `SELECT COUNT(*) AS c FROM e7emp a JOIN e7emp b ON a.salary = b.salary`, deniedLike: "salary"},
		{name: "denied_in_an_aggregate_argument",
			sql: `SELECT COUNT(salary) AS c FROM e7emp`, deniedLike: "salary"},
	}
}

// ---------------------------------------------------------------------------
// The census / gate
// ---------------------------------------------------------------------------

// TestPolicyMaskingIsPlanTimeOnEveryDoor is #859's gate: with a mask policy
// on `ssn` and `acct` and a deny policy on `salary`, every door and every arm
// answers the MASKED value or refuses — and never the true one — for every
// shape a value can travel through.
//
// Set WADJET_E7_CENSUS=1 to print the per-cell census instead of failing on
// it; that is how the before-state at the base commit was recorded.
func TestPolicyMaskingIsPlanTimeOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	census := os.Getenv("WADJET_E7_CENSUS") == "1"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	for _, cell := range pmCells() {
		for _, door := range rig.doors {
			name := cell.name + "/" + door.name
			t.Run(name, func(t *testing.T) {
				got, err := door.run(t, "analyst-key", cell.sql)
				verdict := ""
				switch {
				case err != nil:
					verdict = "loud: " + firstLine(err.Error())
				default:
					verdict = "rows: " + strings.Join(got.canon(), " ; ")
				}
				// The leak test first: it is the one that says "wrong
				// answer", not "missing coverage".
				leaked := ""
				if err == nil {
					for _, row := range got.rows {
						for c, v := range row {
							// CONTAINS, not equality: a PARTIAL disclosure —
							// `SUBSTRING(ssn,1,8)`, a concatenation, a
							// timestamp rendered inside a longer string — is
							// the same defect, and an exact-match predicate
							// reads stronger than it is.
							for _, bad := range leaks {
								if strings.Contains(v, bad) {
									leaked = fmt.Sprintf("%s=%s", c, v)
								}
							}
						}
					}
					for _, c := range got.cols {
						if c == "salary" {
							leaked = "column salary present"
						}
					}
				}
				if census {
					state := "right"
					if leaked != "" {
						state = "LEAKED " + leaked
					} else if err != nil {
						state = "loud"
					}
					t.Logf("CENSUS %-34s %-22s %-14s %s", cell.name, door.name, state, verdict)
					return
				}
				if leaked != "" {
					t.Fatalf("%s: the analyst identity received the TRUE value (%s)\n  SQL: %s\n  got: %s",
						name, leaked, cell.sql, verdict)
				}
				if cell.deniedLike != "" {
					if err == nil {
						t.Fatalf("%s: answered %s, but %q is DENIED for this identity "+
							"and must not resolve\n  SQL: %s", name, verdict, cell.deniedLike, cell.sql)
					}
					// The control: the same statement with a name the table
					// really does not carry. Both must be the same refusal.
					ctrlSQL := strings.ReplaceAll(cell.sql, cell.deniedLike, "e7nosuchcol")
					_, ctrlErr := door.run(t, "analyst-key", ctrlSQL)
					if ctrlErr == nil {
						t.Fatalf("%s: the control %q ANSWERED; the census cannot compare refusals",
							name, ctrlSQL)
					}
					want := strings.ReplaceAll(ctrlErr.Error(), "e7nosuchcol", cell.deniedLike)
					if err.Error() != want {
						t.Fatalf("%s: a DENIED column must refuse exactly as a MISSING one does\n"+
							"  denied:  %v\n  missing: %v\n  SQL: %s", name, err, ctrlErr, cell.sql)
					}
					return
				}
				if cell.wantErrLike != "" {
					if err == nil {
						t.Fatalf("%s: answered %s, want a refusal containing %q\n  SQL: %s",
							name, verdict, cell.wantErrLike, cell.sql)
					}
					if !strings.Contains(err.Error(), cell.wantErrLike) {
						t.Fatalf("%s: error %v\n  want one containing %q\n  SQL: %s",
							name, err, cell.wantErrLike, cell.sql)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s: %v\n  SQL: %s", name, err, cell.sql)
				}
				if cell.noSalary {
					for _, c := range got.cols {
						if c == "salary" {
							t.Fatalf("%s: denied column salary is in the result schema %v", name, got.cols)
						}
					}
				}
				want := append([]string(nil), cell.want...)
				sort.Strings(want)
				gotRows := got.canon()
				if len(gotRows) != len(want) {
					t.Fatalf("%s: %d rows, want %d\n  got  %v\n  want %v\n  SQL: %s",
						name, len(gotRows), len(want), gotRows, want, cell.sql)
				}
				for i := range want {
					if gotRows[i] != want[i] {
						t.Fatalf("%s: row %d\n  got  %s\n  want %s\n  SQL: %s",
							name, i, gotRows[i], want[i], cell.sql)
					}
				}
			})
		}
	}
}

// TestPolicyMaskingLeavesUnpolicedIdentitiesAlone is the other half of the
// claim: an admin identity, and a connection with NO identity at all, keep
// seeing the raw table. A masking layer that masks everybody is not
// enforcement, and the no-identity behaviour is the historical contract
// EnforcePlanPolicies documents.
func TestPolicyMaskingLeavesUnpolicedIdentitiesAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)

	for _, key := range []string{"admin-key", ""} {
		who := key
		if who == "" {
			who = "no-identity"
		}
		for _, door := range rig.doors {
			if key == "" && !strings.HasPrefix(door.name, "embedded/") {
				// A network door with a provider wired REFUSES an
				// unauthenticated caller before any planning happens — 401 on
				// HTTP, 28P01 on pgwire — so "no identity" is a state only the
				// in-process callers (the embedded API and the coordinator's
				// ExecuteSQL, which are handed the context directly) can
				// present. That is the historical contract
				// EnforcePlanPolicies documents and this arc keeps.
				continue
			}
			t.Run(who+"/"+door.name, func(t *testing.T) {
				got, err := door.run(t, key, `SELECT ssn, acct, salary FROM e7emp WHERE id = 3`)
				if err != nil {
					t.Fatalf("%s: %v", door.name, err)
				}
				if len(got.rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", door.name, len(got.rows))
				}
				r := got.rows[0]
				if r["ssn"] != "true-ssn-03" || r["acct"] != "900003" || r["salary"] != "700003" {
					t.Fatalf("%s: %s identity got %v, want the raw row", door.name, who, r)
				}
			})
		}
	}
}

// TestAsyncDoorRefusesAPolicedStatement — the async door
// (`POST /v1/queries/async`) dispatches ONE pipeline task carrying the SQL
// TEXT, and the worker parses, builds and optimizes it again with no policy in
// reach. At af6f18db and at the arc's own first tip it therefore answered a
// policed identity with every stored value, including the DENIED column, while
// the coordinator's own plan string showed the security projection.
//
// A policy that cannot be delivered refuses. The control is the same door
// under an identity with no obligations, which must still work.
func TestAsyncDoorRefusesAPolicedStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)

	post := func(key, sql string) (int, string) {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			rig.asyncBase+"/v1/queries/async", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	for _, sql := range []string{
		`SELECT id, ssn FROM e7emp`,
		`SELECT * FROM e7emp`,
		`SELECT SUM(acct) AS s FROM e7emp`,
		`SELECT ssn, COUNT(*) AS c FROM e7emp GROUP BY ssn`,
	} {
		status, body := post("analyst-key", sql)
		if status == http.StatusOK || status == http.StatusAccepted {
			t.Errorf("async %q was ACCEPTED for a policed identity: %s", sql, body)
			continue
		}
		if !strings.Contains(body, "security policy") {
			t.Errorf("async %q refused with %d %s; want the policy refusal", sql, status, body)
		}
		// A POLICY refusal is 403 on this door, the status
		// `access denied to table …` gets from POST /v1/queries. 400 said
		// "your SQL is malformed", which a client cannot act on.
		if status != http.StatusForbidden {
			t.Errorf("async %q refused with %d; a policy refusal on this door is %d",
				sql, status, http.StatusForbidden)
		}
		if !strings.Contains(body, `"sqlstate":"0A000"`) {
			t.Errorf("async %q carries no 0A000: %s", sql, body)
		}
		for _, bad := range pmTrueValues() {
			if strings.Contains(body, bad) {
				t.Errorf("async %q leaked %q in its refusal: %s", sql, bad, body)
			}
		}
	}

	// A denied column is refused by NAME before the door's own refusal — the
	// 42703 the rest of the matrix asserts, on this door too.
	if _, body := post("analyst-key", `SELECT salary FROM e7emp`); !strings.Contains(body, "salary") {
		t.Errorf("async SELECT salary: %s", body)
	}

	// The control: an identity with no obligations still gets async.
	if status, body := post("admin-key", `SELECT id FROM e7emp`); status != http.StatusAccepted &&
		status != http.StatusOK {
		t.Errorf("async for an unpoliced identity: %d %s — the refusal must be about the POLICY, "+
			"not about the door", status, body)
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// TestLegacyYAMLCellPoliciesAreEnforcedAtThePlanOnEveryDoor is position 2's
// proof: a `policies:` block — the YAML form docs/security.md documents, with
// the doc's own `mask` / `deny` / `row_filter` spellings — still works on every
// door now that the result-row path (AccessPolicy.ApplyToRows, the HTTP door's
// applyColumnPolicies) is gone. MigrateRBACToABAC turns it into obligations and
// auth.EnforcePlanPolicies injects them at the scan.
//
// The aggregate is the cell that could not have passed before: a result-row
// mask cannot reach a value the aggregate already consumed.
func TestLegacyYAMLCellPoliciesAreEnforcedAtThePlanOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	abacPolicies, err := auth.MigrateRBACToABAC(
		[]auth.RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
		[]auth.PolicyConfig{{
			Table: pmTable, Role: "reader",
			Columns:   map[string]string{"ssn": "mask", "acct": "mask", "salary": "deny"},
			RowFilter: "dept = 'd1'",
		}})
	if err != nil {
		t.Fatalf("MigrateRBACToABAC: %v", err)
	}
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "reader", Role: "reader"}},
		Roles:   []auth.RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator(abacPolicies))

	rig := pmRigUpWith(t, ctx, provider)
	// dept='d1' selects ids 1,4,7,10 out of 12.
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			got, err := door.run(t, "analyst-key", `SELECT ssn, SUM(acct) AS s, COUNT(*) AS c FROM e7emp GROUP BY ssn`)
			if err != nil {
				t.Fatalf("%v", err)
			}
			want := []string{"c=4|s=0|ssn=***"}
			if diff := got.canon(); len(diff) != 1 || diff[0] != want[0] {
				t.Fatalf("got %v, want %v — a YAML `mask` must reach the AGGREGATE, "+
					"and the row filter must still select four rows", diff, want)
			}
			// `deny` still removes the column from the schema.
			star, err := door.run(t, "analyst-key", `SELECT * FROM e7emp`)
			if err != nil {
				t.Fatalf("SELECT *: %v", err)
			}
			for _, c := range star.cols {
				if c == "salary" {
					t.Fatalf("denied column salary in the result schema %v", star.cols)
				}
			}
		})
	}
}

// TestARowFilterSeesTheTrueValueAndTheUserPredicateSeesTheMask pins the
// ordering the ADR records: the security projection goes ABOVE the row filter,
// so the POLICY's predicate reads the row as stored (PostgreSQL's RLS
// semantics) while a predicate the USER writes reads the mask.
func TestARowFilterSeesTheTrueValueAndTheUserPredicateSeesTheMask(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and three servers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "e7-rls", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "filter-on-a-masked-column", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: pmTable}},
			Actions:   []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "mask_column", Target: "ssn", Value: "'" + pmMaskSSN + "'"},
				{Type: "row_filter", Value: "ssn = 'true-ssn-03'"},
			},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	rig := pmRigUpWith(t, ctx, provider)
	for _, door := range rig.doors {
		t.Run(door.name, func(t *testing.T) {
			// The POLICY's predicate names the TRUE value and selects one row.
			got, err := door.run(t, "analyst-key", `SELECT id, ssn FROM e7emp`)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if rows := got.canon(); len(rows) != 1 || rows[0] != "id=3|ssn=***" {
				t.Fatalf("got %v, want [id=3|ssn=***]: the row filter must be BELOW the "+
					"security projection, so it compares the stored value while the "+
					"SELECT list shows the mask", rows)
			}
			// The USER's predicate is above the projection: the true value
			// matches nothing, the mask matches the filtered row.
			for _, c := range []struct {
				sql  string
				want string
			}{
				{`SELECT COUNT(*) AS c FROM e7emp WHERE ssn = 'true-ssn-03'`, "c=0"},
				{`SELECT COUNT(*) AS c FROM e7emp WHERE ssn = '***'`, "c=1"},
			} {
				out, err := door.run(t, "analyst-key", c.sql)
				if err != nil {
					t.Fatalf("%s: %v", c.sql, err)
				}
				if rows := out.canon(); len(rows) != 1 || rows[0] != c.want {
					t.Fatalf("%s: got %v, want [%s]", c.sql, rows, c.want)
				}
			}
		})
	}
}
