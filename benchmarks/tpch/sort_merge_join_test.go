package tpch

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestTPCHQueriesSortMergeJoinForced is the phase-2 gate from
// docs/design/sort-merge-join.md §4: with --sort-merge-join-bytes forced to 1,
// every gate-eligible inner equi-join in the suite routes through
// SortMergeJoin, and all 22 queries must return the same rows as the default
// (hash join) configuration over identical data.
//
// Q02/Q22 compare row counts with the same tolerance as TestTPCHQueries:
// their float-threshold predicates admit borderline rows that legitimately
// shift with accumulation order, which differs between join algorithms.
func TestTPCHQueriesSortMergeJoinForced(t *testing.T) {
	ctx := context.Background()

	// Two independent DBs, each loaded from the deterministic generator: a
	// DB's catalog metadata is in-memory per instance, so sharing one store
	// between two Opens leaves the second catalog empty.
	openAndLoad := func(smjBytes int64) *wadjet.DB {
		db, err := wadjet.Open(ctx, wadjet.Config{
			Store:              objstore.NewMemStore(),
			Bucket:             "tpch",
			SortMergeJoinBytes: smjBytes,
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

	baseline := openAndLoad(0)
	defer baseline.Close()
	forced := openAndLoad(1)
	defer forced.Close()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	plannedBefore := physical.SortMergeJoinsPlanned.Load()
	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			baseCount := physical.SortMergeJoinsPlanned.Load()
			want, err := baseline.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("baseline Q%d failed: %v", qNum, err)
			}
			if got := physical.SortMergeJoinsPlanned.Load(); got != baseCount {
				t.Fatalf("dormancy broken: default config planned %d sort-merge joins", got-baseCount)
			}

			got, err := forced.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("forced-SMJ Q%d failed: %v", qNum, err)
			}

			// Q02/Q22: float-threshold borderline rows shift with
			// accumulation order — count tolerance, matching TestTPCHQueries.
			if qNum == 2 || qNum == 22 {
				diff := len(got.Rows) - len(want.Rows)
				if diff < -4 || diff > 4 {
					t.Fatalf("Q%d row count: forced %d vs baseline %d (tolerance 4)", qNum, len(got.Rows), len(want.Rows))
				}
				return
			}

			w, g := canonicalRows(want.Rows), canonicalRows(got.Rows)
			if len(w) != len(g) {
				t.Fatalf("Q%d row count: forced %d vs baseline %d", qNum, len(g), len(w))
			}
			for i := range w {
				if w[i] != g[i] {
					t.Fatalf("Q%d row %d differs:\n  baseline %s\n  forced   %s", qNum, i, w[i], g[i])
				}
			}
		})
	}
	if planned := physical.SortMergeJoinsPlanned.Load() - plannedBefore; planned == 0 {
		t.Fatal("forced threshold planned zero sort-merge joins — the gate never fired and this test proved nothing")
	} else {
		t.Logf("sort-merge joins planned across the suite: %d", planned)
	}
}

// canonicalRows renders rows order-independently with floats at 6 significant
// digits, absorbing accumulation-order noise between join algorithms.
func canonicalRows(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	var sb strings.Builder
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.Reset()
		for _, k := range keys {
			switch v := r[k].(type) {
			case float64:
				sb.WriteString(k + "=" + strconv.FormatFloat(v, 'g', 6, 64) + "|")
			case float32:
				sb.WriteString(k + "=" + strconv.FormatFloat(float64(v), 'g', 6, 32) + "|")
			default:
				fmt.Fprintf(&sb, "%s=%v|", k, v)
			}
		}
		out = append(out, sb.String())
	}
	sort.Strings(out)
	return out
}
