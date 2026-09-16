// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// One binary, one statement, one plan: wadjetd's two EXPLAIN doors must agree.
//
// They did not. Once the local planner stopped emitting stages (ADR-0037), the
// PostgreSQL wire door — which answered EXPLAIN from the embedded database —
// printed "Single-stage local execution" while the SAME server's HTTP door
// printed the stage DAG it was about to dispatch. An operator reading EXPLAIN
// over psql against a coordinator was told the query runs in one process.
//
// EXPLAIN now routes to the coordinator on any server that has one, and both
// doors render through Coordinator.StagePlanTextForExplain. This gate is that
// agreement, byte for byte, over the shapes the review measured diverging:
// a bare scan, an aggregate, an aggregate with ORDER BY (where the local
// emitter used to produce sort/merge_sort stages the DAG does not) and a join
// (where the DAG rewires the build edge through a replicate exchange).
//
// The MIT binary is the other half of the same decision and is gated in
// internal/cli: TestTheEmbeddedExplainPrintsNoStageList requires its EXPLAIN
// to name no stage at all, because it dispatches none.
func TestTheTwoExplainDoorsAgreeOnWadjetd(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up an embedded NATS, three workers, a pgwire server and an HTTP mux")
	}
	ctx := context.Background()
	pgAddr, httpURL := explainDoors(t, ctx)

	shapes := []struct{ name, sql string }{
		{"scan", "EXPLAIN VERBOSE SELECT * FROM m"},
		{"aggregate", "EXPLAIN VERBOSE SELECT s, count(*) FROM m GROUP BY s"},
		{"aggregate_order_by", "EXPLAIN VERBOSE SELECT s, count(*) FROM m GROUP BY s ORDER BY s"},
		{"join", "EXPLAIN VERBOSE SELECT a.id FROM m a JOIN m b ON a.id = b.id"},
		{"not_verbose", "EXPLAIN SELECT s, count(*) FROM m GROUP BY s"},
		// Whitespace spellings, because psql sends a multi-line statement as
		// typed: EXPLAIN on its own line used to take the unrouted path and
		// print the pipeline's line while the HTTP door printed the DAG
		// (round-3 review P1).
		{"newline_separator", "EXPLAIN\nVERBOSE SELECT s, count(*) FROM m GROUP BY s"},
		{"tab_separator", "EXPLAIN\tVERBOSE SELECT s, count(*) FROM m GROUP BY s"},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			wire := explainOverPgwire(t, ctx, pgAddr, sh.sql)
			over := explainOverHTTP(t, httpURL, sh.sql)
			if wire != over {
				t.Fatalf("the two doors of one binary describe the same statement differently.\n"+
					"--- pgwire ---\n%s\n--- http ---\n%s", wire, over)
			}
			if sh.name != "not_verbose" && !strings.Contains(wire, "Stage ") {
				t.Errorf("a server with a coordinator printed no stage line for %s:\n%s", sh.sql, wire)
			}
		})
	}
}

// explainOverPgwire runs the statement on the wire door and joins its rows.
func explainOverPgwire(t *testing.T, ctx context.Context, addr, sql string) string {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", addr))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s over pgwire: %v", sql, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 1 {
			t.Fatalf("EXPLAIN returned %d columns, want 1", len(vals))
		}
		out = append(out, fmt.Sprint(vals[0]))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading EXPLAIN rows: %v", err)
	}
	return strings.Join(out, "\n")
}

// explainOverHTTP runs the same statement on the HTTP door.
func explainOverHTTP(t *testing.T, baseURL, sql string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+"/v1/queries", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("%s over HTTP: %v", sql, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s over HTTP: status %d", sql, resp.StatusCode)
	}
	var out struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, r := range out.Rows {
		lines = append(lines, fmt.Sprint(r["plan"]))
	}
	// The HTTP door answers EXPLAIN as ONE row holding the whole text; the
	// wire door answers one row per line. Compare the text, not the framing.
	return strings.Join(lines, "\n")
}

// explainDoors stands up one coordinator with three workers and puts BOTH
// doors on it: a pgwire server with the coordinator as its router, and the
// HTTP mux with the same coordinator.
func explainDoors(t *testing.T, ctx context.Context) (pgAddr, httpURL string) {
	t.Helper()
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, nil)
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
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "m", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 8)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "s": fmt.Sprintf("k%d", i%3)}
	}
	ing := db.NewIngester("m", schema, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 16})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Three workers, so the DAG the coordinator plans is a multi-worker one —
	// the shape an operator's EXPLAIN is about.
	ids := make([]string, 3)
	for i := range ids {
		ids[i] = fmt.Sprintf("explain-worker-%d", i)
	}
	coord := coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test", LocalFastPathBytes: 0,
	}, cat, nc, js, nil)
	deadline := time.Now().Add(30 * time.Second)
	for coord.Workers().Count() < 3 {
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
		if time.Now().After(deadline) {
			t.Fatalf("workers did not register: %d of 3", coord.Workers().Count())
		}
		time.Sleep(50 * time.Millisecond)
	}

	pg := pgwire.NewServer(db, pgwire.Config{}, nil)
	pg.SetRouter(coordinator.NewQueryRouter(coord))
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)

	srv := New(Config{Addr: ":0", Catalog: cat, Coordinator: coord}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	return pg.Addr(), hs.URL
}
