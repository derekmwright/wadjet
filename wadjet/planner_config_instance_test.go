// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// bjExpandingChain is the four-relation shape whose optimal order only a
// BUSHY plan can express: fact_a ⋈ fact_b is a many-to-many explosion (both
// key NDVs 10 over 1000 rows), and each fact carries a small dimension, so
// the cheapest order attaches BOTH dimensions before the exploding edge —
// (fact_a ⋈ dim_x) ⋈ (fact_b ⋈ dim_y). It is the same fixture
// logical.TestBushyReorder_ExpandingJoinDeferred plans on synthetic stats,
// here as real tables with ANALYZE-computed NDV so the whole embedded
// entry — Config → DB → logical.Optimize → the reorder — is what decides.
const bjExpandingChain = `SELECT count(*) AS n FROM fact_a ` +
	`JOIN dim_x ON a_x = x_id ` +
	`JOIN fact_b ON a_id = b_id ` +
	`JOIN dim_y ON b_y = y_id`

// bjExpandingChainRows is the answer: ten fact_a rows survive a_x = x_id,
// ten fact_b rows survive b_y = y_id, and their keys line up one-to-one.
const bjExpandingChainRows = int64(10)

// bjOpen opens a DB with its own MemStore and loads the fixture. Every DB in
// a test holds its own store, so the only thing two of them share is the
// process — which is the whole point of the gate.
func bjOpen(t *testing.T, bushy bool) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{
		Store:            objstore.NewMemStore(),
		Bucket:           "test",
		BushyJoinReorder: bushy,
	})
	if err != nil {
		t.Fatalf("Open(BushyJoinReorder=%v): %v", bushy, err)
	}
	t.Cleanup(db.Close)
	bjLoadFixture(t, db)
	return db
}

func bjLoadFixture(t *testing.T, db *DB) {
	t.Helper()
	ctx := context.Background()
	two := func(a, b string) parquet.Schema {
		return parquet.Schema{Columns: []parquet.Column{
			{Name: a, Type: parquet.TypeInt64},
			{Name: b, Type: parquet.TypeInt64},
		}}
	}
	load := func(table string, schema parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatalf("CreateTable %s: %v", table, err)
		}
		ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", table, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", table, err)
		}
		if _, err := db.Query(ctx, "ANALYZE TABLE "+table); err != nil {
			t.Fatalf("ANALYZE %s: %v", table, err)
		}
	}

	const factRows = 1000
	factA := make([]map[string]any, factRows)
	factB := make([]map[string]any, factRows)
	for i := 0; i < factRows; i++ {
		factA[i] = map[string]any{"a_id": int64(i % 10), "a_x": int64(i)}
		factB[i] = map[string]any{"b_id": int64(i % 10), "b_y": int64(i)}
	}
	dimX := make([]map[string]any, 10)
	dimY := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		dimX[i] = map[string]any{"x_id": int64(i), "x_v": int64(i)}
		dimY[i] = map[string]any{"y_id": int64(i), "y_v": int64(i)}
	}
	load("fact_a", two("a_id", "a_x"), factA)
	load("dim_x", two("x_id", "x_v"), dimX)
	load("fact_b", two("b_id", "b_y"), factB)
	load("dim_y", two("y_id", "y_v"), dimY)
}

// bjPlanAndCount runs the expanding-chain query on db and reports how many
// bushy join orders the planner chose while doing it. The counter is the
// mechanism marker docs/design/bushy-join-cbo.md §3.2 installed for exactly
// this question; it advances only when the FINAL plan contains a join of two
// composite intermediates.
func bjPlanAndCount(t *testing.T, db *DB) int64 {
	t.Helper()
	before := logical.BushyJoinsPlanned.Load()
	res, err := db.Query(context.Background(), bjExpandingChain)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expanding chain returned %d rows, want 1", len(res.Rows))
	}
	if got := toInt64(t, res.Rows[0]["n"]); got != bjExpandingChainRows {
		t.Fatalf("expanding chain count = %d, want %d", got, bjExpandingChainRows)
	}
	return logical.BushyJoinsPlanned.Load() - before
}

func toInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		t.Fatalf("count came back as %T (%v), want an integer", v, v)
		return 0
	}
}

// TestPlannerConfigIsInstanceScoped is #1223's acceptance criterion 2: two
// DBs open at once in one process with OPPOSITE BushyJoinReorder settings,
// each planning by its own. It fails at 6960cd27, where Config.BushyJoinReorder
// is stored into a package variable that nothing ever stores false: whichever
// DB asked for bushy enables it for the other one too, and the default
// instance silently plans in the regime docs/design/bushy-join-cbo.md keeps
// opt-in.
//
// Both Open orders are run because the package variable is sticky in one
// direction only — a bushy DB opened SECOND poisons a default DB that was
// already open, and a bushy DB opened FIRST poisons every later one.
func TestPlannerConfigIsInstanceScoped(t *testing.T) {
	t.Run("bushy opened first", func(t *testing.T) {
		bushyDB := bjOpen(t, true)
		plainDB := bjOpen(t, false)
		bjAssertPair(t, bushyDB, plainDB)
	})
	t.Run("default opened first", func(t *testing.T) {
		plainDB := bjOpen(t, false)
		bushyDB := bjOpen(t, true)
		bjAssertPair(t, bushyDB, plainDB)
	})
}

// bjAssertPair interleaves the two instances so neither ordering of the
// QUERIES can be what makes the cells agree.
func bjAssertPair(t *testing.T, bushyDB, plainDB *DB) {
	t.Helper()
	for round := 0; round < 2; round++ {
		if got := bjPlanAndCount(t, bushyDB); got != 1 {
			t.Fatalf("round %d: the BushyJoinReorder:true instance planned %d bushy joins, want 1"+
				" — its own setting did not reach the optimizer", round, got)
		}
		if got := bjPlanAndCount(t, plainDB); got != 0 {
			t.Fatalf("round %d: the BushyJoinReorder:false instance planned %d bushy joins, want 0"+
				" — it is planning by another instance's setting", round, got)
		}
	}
}

// TestCloseThenOpenRestoresTheDefault is #1223's second half of criterion 2:
// Close is not a release of anything the next Open inherits. At 6960cd27 the
// package variable outlives the DB that set it, so a process that opened one
// bushy DB plans bushy for the rest of its life.
func TestCloseThenOpenRestoresTheDefault(t *testing.T) {
	bushyDB := bjOpen(t, true)
	if got := bjPlanAndCount(t, bushyDB); got != 1 {
		t.Fatalf("the bushy instance planned %d bushy joins, want 1 — the fixture proves nothing", got)
	}
	bushyDB.Close()

	plainDB := bjOpen(t, false)
	if got := bjPlanAndCount(t, plainDB); got != 0 {
		t.Fatalf("after Close, a default instance planned %d bushy joins, want 0"+
			" — the closed instance's setting outlived it", got)
	}
}
