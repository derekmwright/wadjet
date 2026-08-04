package worker

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// makeKVWshf writes a two-int64-column .wshf payload with the given
// column names.
func makeKVWshf(t *testing.T, colA, colB string, rows [][2]int64) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: colA, Type: parquet.TypeInt64, Nullable: true},
		{Name: colB, Type: parquet.TypeInt64, Nullable: true},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].Int64Data[i] = r[0]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].Int64Data[i] = r[1]
		b.Columns[1].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(rows)); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

// makeDecimalLineitemWshf writes (l_orderkey int64, l_quantity
// decimal(15,2)) — the real Q18 payload types. Quantities are scaled
// by 100 (Int128From(q*100)).
func makeDecimalLineitemWshf(t *testing.T, rows [][2]int64) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64, Nullable: true},
		{Name: "l_quantity", Type: parquet.TypeDecimal, Nullable: true, Precision: 15, Scale: 2},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].Int64Data[i] = r[0]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].DecimalData.Data[i] = batch.Int128From(r[1] * 100)
		b.Columns[1].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(rows)); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

// TestExecuteShuffle_PartialAggThroughJoinChain reproduces the SF100 Q18
// composed-stack failure shape (results/20260803-223926, Q18 = 0 rows):
// a partial-agg'd exchange whose stream flows through a join, is
// materialized as that join's stage output, and is then consumed as the
// probe of a SECOND join keyed on a column that came from the FIRST
// join's build side. The final join must still match — on the broken
// build the second join emitted zero rows while the plain (raw) arm was
// correct.
//
// Shape: lineitem(l_orderkey, l_quantity) --shuffle+partialagg-->
// J1 probe x orders(o_orderkey, o_custkey) --> out
// J2: out probe x customer(c_custkey, c_nationkey) on o_custkey.
func TestExecuteShuffle_PartialAggThroughJoinChain(t *testing.T) {
	for _, arm := range []struct {
		name       string
		partialAgg bool
		decimal    bool
		j1Rows     int64 // lineitem-side rows surviving J1
	}{
		{"raw", false, false, 60},
		{"partial-agg", true, false, 20},
		{"raw-decimal", false, true, 60},
		{"partial-agg-decimal", true, true, 20},
		// The exchange's declared payload is an over-approximated UNION by
		// planner convention ("the reader ignores columns that don't
		// exist"). PartialAggKeys derived from it can therefore name
		// columns absent from the runtime stream. Before the first-batch
		// intersection fix, HashAggregate materialized those phantoms as
		// all-null STRING columns in every flush; the ghost then collided
		// with the REAL same-named column on J1's build side, forcing
		// alias qualification ("orders.o_custkey") — and J2, resolving
		// bare "o_custkey", found the null ghost and emitted 0 rows
		// (SF100 Q18 composed-stack failure, results/20260803-223926).
		{"partial-agg-phantom-keys", true, false, 20},
	} {
		t.Run(arm.name, func(t *testing.T) {
			ctx := context.Background()
			const bucket = "test-pagg-chain"
			const numOrders = 20

			store := objstore.NewMemStore()
			if err := store.MakeBucket(ctx, bucket); err != nil {
				t.Fatalf("MakeBucket: %v", err)
			}

			// lineitem: 3 rows per orderkey.
			var li [][2]int64
			for ok := 0; ok < numOrders; ok++ {
				for j := 0; j < 3; j++ {
					li = append(li, [2]int64{int64(ok), int64(ok*100 + j)})
				}
			}
			var liData []byte
			if arm.decimal {
				liData = makeDecimalLineitemWshf(t, li)
			} else {
				liData = makeKVWshf(t, "l_orderkey", "l_quantity", li)
			}
			liKey := "in/lineitem.wshf"
			if _, err := store.Put(ctx, bucket, liKey, bytes.NewReader(liData), int64(len(liData)), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}

			// orders: one row per orderkey, custkey = 1000+ok.
			var ord [][2]int64
			for ok := 0; ok < numOrders; ok++ {
				ord = append(ord, [2]int64{int64(ok), int64(1000 + ok)})
			}
			ordData := makeKVWshf(t, "o_orderkey", "o_custkey", ord)
			ordKey := "in/orders.wshf"
			if _, err := store.Put(ctx, bucket, ordKey, bytes.NewReader(ordData), int64(len(ordData)), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}

			// customer: covers every custkey J1 can emit.
			var cust [][2]int64
			for ok := 0; ok < numOrders; ok++ {
				cust = append(cust, [2]int64{int64(1000 + ok), int64(ok % 25)})
			}
			custData := makeKVWshf(t, "c_custkey", "c_nationkey", cust)
			custKey := "in/customer.wshf"
			if _, err := store.Put(ctx, bucket, custKey, bytes.NewReader(custData), int64(len(custData)), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}

			cache := NewLRUCache(4 * 1024 * 1024)
			executor := NewExecutor(store, cache, nil)

			// Stage 1: shuffle sender over lineitem, partial agg per arm.
			shTask := distributed.Task{
				ID:            "sh-" + arm.name,
				QueryID:       "q-pagg-chain-" + arm.name,
				StageID:       "rp-1",
				Type:          distributed.TaskTypeShuffle,
				DataBucket:    bucket,
				ResultBucket:  bucket,
				ResultPrefix:  "out/" + arm.name + "/rp-1/",
				Files:         []string{liKey},
				Columns:       []string{"l_orderkey", "l_quantity"},
				ShuffleKeys:   []string{"l_orderkey"},
				NumPartitions: 2,
			}
			if arm.partialAgg {
				shTask.PartialAggKeys = []string{"l_orderkey"}
				if arm.name == "partial-agg-phantom-keys" {
					// Union-declared payload names that do NOT exist in the
					// runtime stream, exactly as the planner ships them.
					shTask.PartialAggKeys = []string{"l_orderkey", "o_orderkey", "o_custkey", "o_totalprice", "o_orderdate"}
				}
				shTask.PartialAggSpecs = []distributed.AggSpec{
					{Func: "sum", InputCol: "l_quantity", OutputCol: "l_quantity"},
				}
			}
			shRes := &distributed.ResultNotification{TaskID: shTask.ID}
			if err := executor.executeShuffle(ctx, shTask, shRes); err != nil {
				t.Fatalf("executeShuffle: %v", err)
			}
			if len(shRes.ResultFiles) == 0 {
				t.Fatal("shuffle produced no partition files")
			}

			// Stage 2 (J1): partial stream ⋈ orders on orderkey.
			j1 := distributed.Task{
				ID:           "j1-" + arm.name,
				QueryID:      "q-pagg-chain-" + arm.name,
				StageID:      "join-1",
				Type:         distributed.TaskTypeStage,
				DataBucket:   bucket,
				ResultBucket: bucket,
				ResultPrefix: "out/" + arm.name + "/join-1/",
				Operators: []distributed.OpSpec{
					{
						Type:        distributed.OpShuffleSource,
						InputAlias:  "lineitem",
						InputFiles:  shRes.ResultFiles,
						InputBucket: bucket,
					},
					{
						Type:        distributed.OpBroadcastProbe,
						JoinType:    "inner",
						LeftKeys:    []string{"l_orderkey"},
						RightKeys:   []string{"o_orderkey"},
						BuildAlias:  "orders",
						BuildFiles:  []string{ordKey},
						BuildBucket: bucket,
					},
					{Type: distributed.OpUnpartitionedSink},
				},
			}
			j1Res := &distributed.ResultNotification{TaskID: j1.ID}
			if err := executor.executeStage(ctx, j1, j1Res); err != nil {
				t.Fatalf("J1 executeStage: %v", err)
			}
			if j1Res.NumRows != arm.j1Rows {
				t.Fatalf("J1 NumRows = %d, want %d", j1Res.NumRows, arm.j1Rows)
			}

			// Stage 3 (J2): J1 output ⋈ customer on the BUILD-side column
			// o_custkey that J1 appended.
			j2 := distributed.Task{
				ID:           "j2-" + arm.name,
				QueryID:      "q-pagg-chain-" + arm.name,
				StageID:      "join-2",
				Type:         distributed.TaskTypeStage,
				DataBucket:   bucket,
				ResultBucket: bucket,
				ResultPrefix: "out/" + arm.name + "/join-2/",
				Operators: []distributed.OpSpec{
					{
						Type:        distributed.OpShuffleSource,
						InputAlias:  "j1",
						InputFiles:  j1Res.ResultFiles,
						InputBucket: bucket,
					},
					{
						Type:        distributed.OpBroadcastProbe,
						JoinType:    "inner",
						LeftKeys:    []string{"o_custkey"},
						RightKeys:   []string{"c_custkey"},
						BuildAlias:  "customer",
						BuildFiles:  []string{custKey},
						BuildBucket: bucket,
					},
					{Type: distributed.OpUnpartitionedSink},
				},
			}
			j2Res := &distributed.ResultNotification{TaskID: j2.ID}
			if err := executor.executeStage(ctx, j2, j2Res); err != nil {
				t.Fatalf("J2 executeStage: %v", err)
			}
			if j2Res.NumRows != arm.j1Rows {
				t.Fatalf("J2 NumRows = %d, want %d (customer join dropped partial-agg-derived rows: the SF100 Q18 0-rows signature)",
					j2Res.NumRows, arm.j1Rows)
			}
			_ = strconv.Itoa
		})
	}
}
