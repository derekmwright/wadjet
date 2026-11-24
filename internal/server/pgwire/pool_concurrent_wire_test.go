// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// `wadjet serve`'s shape: pgwire over one embedded DB. Its connections'
// statements share the DB's ONE query-execution pool (1685e, ADR-0006's
// 2026-11-24 amendment). Two connections, each statement held at a gate in
// its probe phase with its hash-join build retained (see the embedded test,
// wadjet/pool_concurrent_test.go, for the device).

const wirePoolDeadline = 2 * time.Minute

type wirePoolGate struct {
	once             sync.Once
	reached, release chan struct{}
}

var (
	wirePoolGatesMu sync.Mutex
	wirePoolGates   = map[string]*wirePoolGate{}
)

func armWirePoolGates(n int) []*wirePoolGate {
	wirePoolGatesMu.Lock()
	defer wirePoolGatesMu.Unlock()
	out := make([]*wirePoolGate, n)
	for i := range out {
		g := &wirePoolGate{reached: make(chan struct{}), release: make(chan struct{})}
		name := fmt.Sprintf("wire_pool_gate_%d", i)
		wirePoolGates[name] = g
		out[i] = g
		expr.RegisterFunc(name, func(args []any) any {
			wirePoolGatesMu.Lock()
			g := wirePoolGates[name]
			wirePoolGatesMu.Unlock()
			g.once.Do(func() {
				close(g.reached)
				select {
				case <-g.release:
				case <-time.After(wirePoolDeadline):
				}
			})
			return args[0]
		}, expr.RetTypeOf(batch.TypeInt64))
	}
	return out
}

// wirePoolBudget is 1.5 × one statement's pool peak (409,512 bytes, measured
// alone): one statement's build fits, two do not.
const wirePoolBudget = 614268

func wirePoolServer(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: wirePoolBudget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
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
	return startTestServer(t, db)
}

func wirePoolSQL(gate int) string {
	return fmt.Sprintf("SELECT count(*), sum(wire_pool_gate_%d(a.k)) FROM wide a JOIN wide b ON a.k = b.k", gate)
}

// PostgreSQL 17.11 over the same rows answers 4000 | 7998000
// (1685e_evidence/pg_oracle.txt).
const wirePoolWant = "4000|7998000"

func wireRun(ctx context.Context, conn *pgconn.PgConn, sql string) (string, error) {
	res, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return "", err
	}
	var parts []string
	for _, r := range res {
		for _, row := range r.Rows {
			for _, v := range row {
				parts = append(parts, string(v))
			}
		}
	}
	return strings.Join(parts, "|"), nil
}

// TestWirePoolConnectionsShareOneBudget: before the pool each connection's
// statement had the whole budget, and both builds stayed in memory. Now the
// second connection's build spills against the first's, both connections get
// PostgreSQL's answer, and a statement run alone afterwards finds the whole
// budget again (it builds without spilling: nothing was stranded).
func TestWirePoolConnectionsShareOneBudget(t *testing.T) {
	srv := wirePoolServer(t)
	ctx := context.Background()
	alone := connectPgconn(t, srv.Addr())
	armWirePoolGates(1)[0].openNow()
	before := exec.JoinPartitionsEvicted.Load()
	if got, err := wireRun(ctx, alone, wirePoolSQL(0)); err != nil || got != wirePoolWant {
		t.Fatalf("alone: %q %v", got, err)
	}
	if d := exec.JoinPartitionsEvicted.Load() - before; d != 0 {
		t.Fatalf("alone, the build already spilled %d partitions: the budget is not one statement's", d)
	}

	gates := armWirePoolGates(2)
	conns := []*pgconn.PgConn{connectPgconn(t, srv.Addr()), connectPgconn(t, srv.Addr())}
	answers := make([]string, 2)
	errs := make([]error, 2)
	evicted := make([]int64, 2)
	done := make([]chan struct{}, 2)
	for i := 0; i < 2; i++ {
		done[i] = make(chan struct{})
		before := exec.JoinPartitionsEvicted.Load()
		go func(i int) {
			defer close(done[i])
			answers[i], errs[i] = wireRun(ctx, conns[i], wirePoolSQL(i))
		}(i)
		select {
		case <-gates[i].reached:
		case <-done[i]:
		case <-time.After(wirePoolDeadline):
			t.Fatalf("connection %d neither reached its gate nor ended", i)
		}
		evicted[i] = exec.JoinPartitionsEvicted.Load() - before
	}
	for _, g := range gates {
		g.openNow()
	}
	for i := range done {
		<-done[i]
	}
	t.Logf("order: connection 0 held, then connection 1 built (evicted %d, %d)", evicted[0], evicted[1])
	for i := range answers {
		if errs[i] != nil || answers[i] != wirePoolWant {
			t.Errorf("connection %d: %q %v, PostgreSQL 17.11 %q", i, answers[i], errs[i], wirePoolWant)
		}
	}
	if evicted[0] != 0 || evicted[1] == 0 {
		t.Errorf("the second connection's build should spill against the first's (evicted %d, %d): "+
			"the connections are not sharing one budget", evicted[0], evicted[1])
	}

	armWirePoolGates(1)[0].openNow()
	before = exec.JoinPartitionsEvicted.Load()
	if got, err := wireRun(ctx, alone, wirePoolSQL(0)); err != nil || got != wirePoolWant {
		t.Fatalf("alone afterwards: %q %v", got, err)
	}
	if d := exec.JoinPartitionsEvicted.Load() - before; d != 0 {
		t.Errorf("alone afterwards the build spilled %d partitions: the earlier statements left charges on the pool", d)
	}
}

func (g *wirePoolGate) openNow() {
	select {
	case <-g.release:
	default:
		close(g.release)
	}
}
