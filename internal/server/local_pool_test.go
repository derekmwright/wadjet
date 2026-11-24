// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The HTTP API's local path (no coordinator) runs its requests on ONE pool
// per server (1685e, ADR-0006's 2026-11-24 amendment). With a coordinator the
// requests are the coordinator's and this pool is never built.

const httpPoolDeadline = 2 * time.Minute

type httpPoolGate struct {
	once             sync.Once
	reached, release chan struct{}
}

var (
	httpPoolGatesMu sync.Mutex
	httpPoolGates   = map[string]*httpPoolGate{}
)

func armHTTPPoolGates(n int) []*httpPoolGate {
	httpPoolGatesMu.Lock()
	defer httpPoolGatesMu.Unlock()
	out := make([]*httpPoolGate, n)
	for i := range out {
		g := &httpPoolGate{reached: make(chan struct{}), release: make(chan struct{})}
		name := fmt.Sprintf("http_pool_gate_%d", i)
		httpPoolGates[name] = g
		out[i] = g
		expr.RegisterFunc(name, func(args []any) any {
			httpPoolGatesMu.Lock()
			g := httpPoolGates[name]
			httpPoolGatesMu.Unlock()
			g.once.Do(func() {
				close(g.reached)
				select {
				case <-g.release:
				case <-time.After(httpPoolDeadline):
				}
			})
			return args[0]
		}, expr.RetTypeOf(batch.TypeInt64))
	}
	return out
}

func httpPoolServer(t *testing.T, budget int64) *Server {
	t.Helper()
	ctx := context.Background()
	kv := catalog.NewMemKV()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "wide", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 4000)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i), "pad": fmt.Sprintf("%08d", i) + strings.Repeat("x", 504)}
	}
	ing := db.NewIngester("wide", schema, nil, ingest.Config{MaxBufferRows: 4001, RowGroupSize: 512})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(Config{Addr: ":0", Catalog: cat}, logger)
	if budget > 0 {
		dir := t.TempDir()
		s.localPoolOnce.Do(func() { s.localPool = memory.NewExecutionPool("query", budget, dir) })
	}
	return s
}

func httpPoolQuery(t *testing.T, url, sql string) (string, error) {
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	resp, err := http.Post(url+"/v1/queries", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var qr QueryResponse
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&qr); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return fmt.Sprint(qr.Rows), nil
}

func httpPoolSQL(gate int) string {
	return fmt.Sprintf("SELECT count(*) AS n, sum(http_pool_gate_%d(a.k)) AS s FROM wide a JOIN wide b ON a.k = b.k", gate)
}

// TestHTTPLocalPoolTopology: the detected default budget (no configured
// figure on this path), one pool per server, every request's planner carries
// it.
func TestHTTPLocalPoolTopology(t *testing.T) {
	s := httpPoolServer(t, 0)
	pool := s.localExecutionPool()
	if pool != s.localExecutionPool() {
		t.Fatal("two pools from one server")
	}
	if want := memory.ResolveExecutionBudget(0); pool.Budget() != want {
		t.Fatalf("HTTP local pool budget %d, want the detected default %d", pool.Budget(), want)
	}
	p := s.newPlanner(httptest.NewRequest("POST", "/v1/queries", nil))
	if p.SharedTracker != pool.Tracker() || p.SharedSpillMgr != pool.SpillManager() || p.SharedSpillMgr == nil {
		t.Fatal("a request's planner does not carry the server's pool")
	}
	var _ *physical.Planner = p
}

// TestHTTPLocalRequestsShareOnePool: two requests held with their builds
// retained at a pool one of them fits in; the second spills; both answer
// PostgreSQL's 4000 | 7998000 (1685e_evidence/pg_oracle.txt).
func TestHTTPLocalRequestsShareOnePool(t *testing.T) {
	const budget = 614268 // 1.5 × one statement's peak (409512, measured alone)
	s := httpPoolServer(t, budget)
	ts := httptest.NewServer(s.Mux())
	defer ts.Close()
	gates := armHTTPPoolGates(2)
	answers := make([]string, 2)
	errs := make([]error, 2)
	evicted := make([]int64, 2)
	done := make([]chan struct{}, 2)
	for i := 0; i < 2; i++ {
		done[i] = make(chan struct{})
		before := exec.JoinPartitionsEvicted.Load()
		go func(i int) {
			defer close(done[i])
			answers[i], errs[i] = httpPoolQuery(t, ts.URL, httpPoolSQL(i))
		}(i)
		select {
		case <-gates[i].reached:
		case <-done[i]:
		case <-time.After(httpPoolDeadline):
			t.Fatalf("request %d neither reached its gate nor ended", i)
		}
		evicted[i] = exec.JoinPartitionsEvicted.Load() - before
	}
	for _, g := range gates {
		close(g.release)
	}
	for i := range done {
		<-done[i]
	}
	const want = "[map[n:4000 s:7998000]]"
	for i := range answers {
		if errs[i] != nil || answers[i] != want {
			t.Errorf("request %d: %s %v, PostgreSQL 17.11 %s", i, answers[i], errs[i], want)
		}
	}
	if evicted[0] != 0 || evicted[1] == 0 {
		t.Errorf("the second request's build should spill against the first's (evicted %d, %d)", evicted[0], evicted[1])
	}
	pool := s.localExecutionPool()
	if u := pool.Tracker().Used(); u != 0 {
		t.Errorf("HTTP local pool used = %d after both requests", u)
	}
	if c := pool.SpillManager().Census(); c.Live != 0 || c.Peaks != 0 || c.Cooldowns != 0 {
		t.Errorf("registry not quiescent: %+v", c)
	}
}
