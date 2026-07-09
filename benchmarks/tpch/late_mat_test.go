package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestTPCHQueriesLateMaterializationForced is the phase-2 gate from
// docs/design/late-materialization.md §8: with --late-materialization on,
// every inner/left hash-join probe emits view-column output, and all 22
// queries must return rows IDENTICAL to the default configuration — unlike
// the SMJ gate, no accumulation-order tolerance applies anywhere, because
// the deferred gather produces the same values in the same order as the
// eager one.
//
// Engagement is asserted through both counters (the dynamic-filter lesson):
// LateMatJoinsPlanned proves the planner stamped probes, and
// LateMatBatchesEmitted proves probes actually emitted views at runtime.
func TestTPCHQueriesLateMaterializationForced(t *testing.T) {
	ctx := context.Background()

	// Two independent DBs, each loaded from the deterministic generator: a
	// DB's catalog metadata is in-memory per instance, so sharing one store
	// between two Opens leaves the second catalog empty.
	openAndLoad := func(lateMat bool) *wadjet.DB {
		db, err := wadjet.Open(ctx, wadjet.Config{
			Store:               objstore.NewMemStore(),
			Bucket:              "tpch",
			LateMaterialization: lateMat,
		})
		if err != nil {
			t.Fatal(err)
		}
		data := Generate(SF001)
		for tableName, schema := range AllTables {
			if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
				t.Fatalf("creating table %s: %v", tableName, err)
			}
			rows := data[tableName]
			if len(rows) == 0 {
				continue
			}
			ing := db.NewIngester(tableName, schema, nil, ingest.Config{
				MaxBufferRows: len(rows) + 1,
				RowGroupSize:  max(100, len(rows)/4),
			})
			if err := ing.Ingest(ctx, rows); err != nil {
				t.Fatalf("ingesting %s: %v", tableName, err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatalf("flushing %s: %v", tableName, err)
			}
		}
		return db
	}

	baseline := openAndLoad(false)
	defer baseline.Close()
	forced := openAndLoad(true)
	defer forced.Close()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	plannedBefore := physical.LateMatJoinsPlanned.Load()
	emittedBefore := exec.LateMatBatchesEmitted.Load()
	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			baseCount := physical.LateMatJoinsPlanned.Load()
			baseEmitted := exec.LateMatBatchesEmitted.Load()
			want, err := baseline.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("baseline Q%d failed: %v", qNum, err)
			}
			if got := physical.LateMatJoinsPlanned.Load(); got != baseCount {
				t.Fatalf("dormancy broken: default config planned %d late-mat joins", got-baseCount)
			}
			if got := exec.LateMatBatchesEmitted.Load(); got != baseEmitted {
				t.Fatalf("dormancy broken: default config emitted %d view batches", got-baseEmitted)
			}

			got, err := forced.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("late-mat Q%d failed: %v", qNum, err)
			}

			w, g := canonicalRows(want.Rows), canonicalRows(got.Rows)
			if len(w) != len(g) {
				t.Fatalf("Q%d row count: late-mat %d vs baseline %d", qNum, len(g), len(w))
			}
			for i := range w {
				if w[i] != g[i] {
					t.Fatalf("Q%d row %d differs:\n  baseline %s\n  late-mat %s", qNum, i, w[i], g[i])
				}
			}
		})
	}
	if planned := physical.LateMatJoinsPlanned.Load() - plannedBefore; planned == 0 {
		t.Fatal("forced flag planned zero late-mat joins — the gate never fired and this test proved nothing")
	} else {
		t.Logf("late-mat joins planned across the suite: %d", planned)
	}
	if emitted := exec.LateMatBatchesEmitted.Load() - emittedBefore; emitted == 0 {
		t.Fatal("zero view batches emitted — planner stamped probes but runtime never engaged")
	} else {
		t.Logf("view batches emitted across the suite: %d (flattens: %d)", emitted, exec.LateMatFlattens.Load())
	}
}
