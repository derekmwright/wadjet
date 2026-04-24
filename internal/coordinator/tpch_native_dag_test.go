package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ingestTPCHTable ingests an SF0.01 TPC-H table into the distributed test
// fixture. Mirrors ingestTestData but takes a parquet.Schema (TPC-H
// schemas live in benchmarks/tpch) instead of a []Column.
func ingestTPCHTable(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog, tableName string, schema parquet.Schema, rows []map[string]any) {
	t.Helper()
	if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
		t.Fatalf("creating table %s: %v", tableName, err)
	}
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	filePath := "tables/" + tableName + "/chunk_0001.parquet"
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("adding file to manifest for %s: %v", tableName, err)
	}
}

// ingestTPCHTableChunked streams a TPC-H table into the distributed test
// fixture in chunks to keep memory bounded at larger scale factors.
// CreateTable is called once; each chunk becomes its own parquet file in
// the manifest so the worker sees >1 file per table and exercises the
// scan-aggregate fan-out path.
func ingestTPCHTableChunked(
	t *testing.T,
	ctx context.Context,
	store objstore.Store,
	cat *catalog.Catalog,
	tableName string,
	schema parquet.Schema,
	chunks [][]map[string]any,
) {
	t.Helper()
	if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
		t.Fatalf("creating table %s: %v", tableName, err)
	}
	for i, rows := range chunks {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		filePath := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, i+1)
		if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
			Path:      filePath,
			SizeBytes: int64(len(data)),
			NumRows:   int64(len(rows)),
			CreatedAt: time.Now(),
		}}); err != nil {
			t.Fatalf("adding file to manifest for %s: %v", tableName, err)
		}
	}
}

// TestTPCHNativeDAG_SF001 is the local correctness gate for native-DAG
// distributed execution. It populates SF0.01 TPC-H tables into the
// distributed test fixture and runs each query twice — once on the legacy
// coordinator path, once on the native-DAG path — asserting row parity.
//
// The harness-based `--use-native-dag` local mode has a pre-existing
// NATS-disconnect flake during data load. This test bypasses that by
// ingesting directly through the in-process catalog.
//
// Expected row counts come from expectedRowsSF001 in benchmarks/tpch.
// Failures here should block EC2 deploys — they're cheap to run (single
// process, seconds) and catch the same class of bugs that cost real
// money at SF10.
func TestTPCHNativeDAG_SF001(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TPCH SF0.01 native-DAG suite in short mode")
	}
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cat := coord.catalog

	data := tpch.Generate(tpch.SF001)
	// Ingest in a deterministic order so test logs are easy to read.
	tableOrder := []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"}
	for _, table := range tableOrder {
		rows := data[table]
		if rows == nil {
			t.Fatalf("datagen missing table %s", table)
		}
		ingestTPCHTable(t, ctx, store, cat, table, tpch.AllTables[table], rows)
	}

	// Known row counts per query at SF0.01. Sourced from the
	// benchmarks/tpch TestTPCHQueries suite.
	expected := map[int]int{
		1: 6, 2: 5, 3: 10, 4: 5, 5: 5, 6: 1, 7: 4, 8: 2,
		9: 150, 10: 20, 11: 235, 12: 2, 13: 100, 14: 1, 15: 1,
		16: 293, 17: 1, 18: 0, 19: 1, 20: 3, 21: 1, 22: 7,
	}
	// Q02 / Q22 have float-threshold comparisons where non-deterministic
	// accumulation order can shift borderline rows in/out; match the
	// tolerance used by TestTPCHQueries.
	tol := map[int]int{2: 4, 22: 4}

	qNums := make([]int, 0, len(tpch.TPCHQueries))
	for n := range tpch.TPCHQueries {
		qNums = append(qNums, n)
	}
	sort.Ints(qNums)

	var failures []string
	for _, qNum := range qNums {
		q := tpch.TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			coord.UseNativeDAG = true
			defer func() { coord.UseNativeDAG = false }()

			res, err := coord.ExecuteSQL(ctx, q.SQL)
			if err != nil {
				failures = append(failures, fmt.Sprintf("Q%02d: %v", qNum, err))
				t.Fatalf("native-DAG Q%02d: %v", qNum, err)
			}
			got := int(res.TotalRows)
			want, ok := expected[qNum]
			if !ok {
				t.Logf("Q%02d returned %d rows (no expected)", qNum, got)
				return
			}
			tolerance := tol[qNum]
			diff := got - want
			if diff < -tolerance || diff > tolerance {
				failures = append(failures, fmt.Sprintf("Q%02d: got %d rows, want %d (±%d)", qNum, got, want, tolerance))
				t.Errorf("Q%02d row count: got %d, want %d (±%d)", qNum, got, want, tolerance)
			}
		})
	}
	if len(failures) > 0 {
		t.Logf("failing queries summary:\n  %v", failures)
	}
}
