// SPDX-License-Identifier: MIT

package tpch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestTPCHQueriesBushyForced is the Layer B parity gate from
// docs/design/bushy-join-cbo.md §4: with BushyJoinReorder enabled, every
// query must return the same rows as the left-deep default over identical
// data. TWO DBs serve the two arms — same MemStore, same bucket, same
// rows, opposite Config.BushyJoinReorder — because the setting is the
// instance's, so the arms are two instances rather than one instance and a
// package flag toggled between queries (#1223). The dormancy assertion is
// then a live property of the default instance while the bushy one is open
// beside it, not a property of the moment between two Stores.
//
// Q02/Q22 compare row counts with the same tolerance as TestTPCHQueries:
// their float-threshold predicates admit borderline rows that legitimately
// shift with accumulation order, which differs between join orders.
func TestTPCHQueriesBushyForced(t *testing.T) {
	ctx := context.Background()

	// One store and one catalog, so the two instances below differ in
	// exactly one thing. A nil MetaKV would give each DB its own in-memory
	// catalog and the second would see no tables at all.
	store := objstore.NewMemStore()
	meta := catalog.NewMemKV()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "tpch",
		MetaKV: meta,
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

	// The second instance over the same data, asking for bushy. Opened
	// after ingest so its catalog sees every table the first one wrote.
	bushyDB, err := wadjet.Open(ctx, wadjet.Config{
		Store:            store,
		Bucket:           "tpch",
		MetaKV:           meta,
		BushyJoinReorder: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bushyDB.Close()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	plannedBefore := logical.BushyJoinsPlanned.Load()
	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			baseCount := logical.BushyJoinsPlanned.Load()
			want, err := db.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("baseline Q%d failed: %v", qNum, err)
			}
			if got := logical.BushyJoinsPlanned.Load(); got != baseCount {
				t.Fatalf("dormancy broken: the default instance planned %d bushy joins", got-baseCount)
			}

			got, err := bushyDB.Query(ctx, q.SQL)
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
		t.Fatal("the bushy instance planned zero bushy joins across the suite — the enumeration never fired and this test proved nothing")
	} else {
		t.Logf("bushy join orders chosen across the suite: %d", planned)
	}
}
