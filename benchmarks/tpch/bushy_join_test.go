package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestTPCHQueriesBushyForced is the Layer B parity gate from
// docs/design/bushy-join-cbo.md §4: with BushyJoinReorder enabled, every
// query must return the same rows as the left-deep default over identical
// data. One DB serves both runs — the flag is process-wide, so it is
// toggled around each query pair.
//
// Q02/Q22 compare row counts with the same tolerance as TestTPCHQueries:
// their float-threshold predicates admit borderline rows that legitimately
// shift with accumulation order, which differs between join orders.
func TestTPCHQueriesBushyForced(t *testing.T) {
	ctx := context.Background()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  objstore.NewMemStore(),
		Bucket: "tpch",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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

	defer logical.BushyJoinReorder.Store(false)

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	plannedBefore := logical.BushyJoinsPlanned.Load()
	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			logical.BushyJoinReorder.Store(false)
			baseCount := logical.BushyJoinsPlanned.Load()
			want, err := db.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("baseline Q%d failed: %v", qNum, err)
			}
			if got := logical.BushyJoinsPlanned.Load(); got != baseCount {
				t.Fatalf("dormancy broken: flag-off run planned %d bushy joins", got-baseCount)
			}

			logical.BushyJoinReorder.Store(true)
			got, err := db.Query(ctx, q.SQL)
			logical.BushyJoinReorder.Store(false)
			if err != nil {
				t.Fatalf("bushy-forced Q%d failed: %v", qNum, err)
			}

			if qNum == 2 || qNum == 22 {
				diff := len(got.Rows) - len(want.Rows)
				if diff < -4 || diff > 4 {
					t.Fatalf("Q%d row count: bushy %d vs baseline %d (tolerance 4)", qNum, len(got.Rows), len(want.Rows))
				}
				return
			}

			w, g := canonicalRows(want.Rows), canonicalRows(got.Rows)
			if len(w) != len(g) {
				t.Fatalf("Q%d row count: bushy %d vs baseline %d", qNum, len(g), len(w))
			}
			for i := range w {
				if w[i] != g[i] {
					t.Fatalf("Q%d row %d differs:\n  baseline %s\n  bushy    %s", qNum, i, w[i], g[i])
				}
			}
		})
	}
	if planned := logical.BushyJoinsPlanned.Load() - plannedBefore; planned == 0 {
		t.Fatal("bushy flag planned zero bushy joins across the suite — the enumeration never fired and this test proved nothing")
	} else {
		t.Logf("bushy join orders chosen across the suite: %d", planned)
	}
}
