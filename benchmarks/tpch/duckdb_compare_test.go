package tpch

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

const (
	duckdbBin   = "/tmp/duckdb"
	tpchDataDir = "/tmp/tpch-sf001"
	floatEps    = 1e-4 // accept ULP-level drift across engines
)

// TestDuckDBCompare runs each TPC-H query against Wadjet and DuckDB on the
// same SF0.01 dataset (DuckDB-generated, exported in Wadjet shape) and
// reports per-query divergences. Gated by env var to keep the regular
// suite fast and to avoid requiring DuckDB on every machine.
//
// Setup (one-time):
//
//	1. /tmp/duckdb < /tmp/export-wadjet-shape.sql  → /tmp/tpch-sf001/*.parquet
//	2. /tmp/duckdb < /tmp/export-json.sql          → /tmp/tpch-sf001/*.json
//
// To run: WADJET_DUCKDB_COMPARE=1 go test -run TestDuckDBCompare -v ./benchmarks/tpch/
func TestDuckDBCompare(t *testing.T) {
	if os.Getenv("WADJET_DUCKDB_COMPARE") != "1" {
		t.Skip("set WADJET_DUCKDB_COMPARE=1 to enable")
	}
	if _, err := os.Stat(duckdbBin); err != nil {
		t.Fatalf("duckdb binary not found at %s: %v", duckdbBin, err)
	}
	for _, name := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
		if _, err := os.Stat(filepath.Join(tpchDataDir, name+".json")); err != nil {
			t.Fatalf("missing %s/%s.json — run /tmp/duckdb < /tmp/export-json.sql first: %v", tpchDataDir, name, err)
		}
		if _, err := os.Stat(filepath.Join(tpchDataDir, name+".parquet")); err != nil {
			t.Fatalf("missing %s/%s.parquet — run /tmp/duckdb < /tmp/export-wadjet-shape.sql first: %v", tpchDataDir, name, err)
		}
	}

	// Bring up Wadjet on the DuckDB-generated data.
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "tpch"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for tableName, schema := range AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("create table %s: %v", tableName, err)
		}
		rows, err := loadJSONRows(filepath.Join(tpchDataDir, tableName+".json"), schema)
		if err != nil {
			t.Fatalf("load %s: %v", tableName, err)
		}
		ing := db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(100, len(rows)/4),
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", tableName, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tableName, err)
		}
	}

	// DuckDB setup: register views over the same parquet files.
	duckdbSetup := ""
	for _, name := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
		duckdbSetup += fmt.Sprintf(
			"CREATE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet');\n",
			name, tpchDataDir, name)
	}

	queryNums := []int{}
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	matches := 0
	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			wRows, wCols, wErr := runWadjet(ctx, db, q.SQL)
			dRows, dCols, dErr := runDuckDB(duckdbSetup, q.SQL)

			if wErr != nil && dErr == nil {
				t.Errorf("Wadjet error: %v\nDuckDB ran fine, %d rows", wErr, len(dRows))
				return
			}
			if dErr != nil && wErr == nil {
				t.Errorf("DuckDB error: %v\nWadjet ran fine, %d rows", dErr, len(wRows))
				return
			}
			if wErr != nil && dErr != nil {
				t.Logf("Q%02d both errored: wadjet=%v duckdb=%v", qNum, wErr, dErr)
				return
			}

			if len(wRows) != len(dRows) {
				t.Errorf("row count: wadjet=%d duckdb=%d", len(wRows), len(dRows))
				if len(wRows) > 0 {
					t.Logf("Wadjet first row: %v", wRows[0])
				}
				if len(dRows) > 0 {
					t.Logf("DuckDB first row: %v", dRows[0])
				}
				return
			}
			if len(wRows) == 0 {
				t.Logf("Q%02d 0 rows (both)", qNum)
				matches++
				return
			}

			cols := wCols
			if len(cols) == 0 {
				cols = dCols
			}
			diff := 0
			for i := range wRows {
				for _, col := range cols {
					if !cellEqual(wRows[i][col], dRows[i][col]) {
						diff++
						if diff <= 3 {
							t.Errorf("row %d col %s: wadjet=%v duckdb=%v",
								i, col, wRows[i][col], dRows[i][col])
						}
					}
				}
			}
			if diff > 0 {
				t.Errorf("Q%02d: %d cell divergences (showing first 3)", qNum, diff)
				return
			}
			t.Logf("Q%02d MATCH (%d rows)", qNum, len(wRows))
			matches++
		})
	}
	t.Logf("Summary: %d/22 queries matched DuckDB", matches)
}

// runWadjet runs sql via wadjet.DB and returns the typed row slice.
func runWadjet(ctx context.Context, db *wadjet.DB, sql string) ([]map[string]any, []string, error) {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	cols := append([]string(nil), res.Columns...)
	return res.Rows, cols, nil
}

// runDuckDB runs sql via the duckdb CLI subprocess and returns rows as
// (col → string) maps. The DuckDB output is CSV with header.
func runDuckDB(setup, sql string) ([]map[string]any, []string, error) {
	script := setup + "\n.mode csv\n.headers on\n" + sql + ";\n"
	cmd := exec.Command(duckdbBin)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("duckdb: %v\nscript: %s", err, script)
	}
	r := csv.NewReader(strings.NewReader(string(out)))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("parse duckdb csv: %v", err)
	}
	if len(records) == 0 {
		return nil, nil, nil
	}
	cols := records[0]
	rows := make([]map[string]any, 0, len(records)-1)
	for _, rec := range records[1:] {
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if i < len(rec) {
				row[col] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, cols, nil
}

// cellEqual compares one cell from Wadjet (typed) against one from DuckDB
// (string from CSV). Both are normalised via canonicalString and compared
// with float-aware tolerance.
func cellEqual(w, d any) bool {
	if w == nil && d == nil {
		return true
	}
	// DuckDB CSV emits "" for NULL by default. Treat empty-string == nil.
	if w == nil {
		if ds, ok := d.(string); ok && ds == "" {
			return true
		}
		return false
	}
	if d == nil {
		if ws, ok := w.(string); ok && ws == "" {
			return true
		}
		return false
	}
	ws := canonicalString(w)
	ds := canonicalString(d)
	if ws == ds {
		return true
	}
	wf, wOk := parseFloat(ws)
	df, dOk := parseFloat(ds)
	if wOk && dOk {
		if wf == 0 && df == 0 {
			return true
		}
		denom := math.Max(math.Abs(wf), math.Abs(df))
		if denom == 0 {
			return wf == df
		}
		return math.Abs(wf-df)/denom < floatEps
	}
	return false
}

func canonicalString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(x)
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// loadJSONRows reads a DuckDB-exported `[{...}, {...}]` JSON array and
// coerces each cell to the Go type matching the supplied schema, ready
// for Wadjet's Ingester. Numeric JSON values arrive as float64 and are
// re-typed to int32 / int64 / float64 according to schema.Type.
func loadJSONRows(path string, schema parquet.Schema) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw []map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", path, err)
	}
	for _, row := range raw {
		for _, col := range schema.Columns {
			v, ok := row[col.Name]
			if !ok || v == nil {
				continue
			}
			switch col.Type {
			case parquet.TypeInt32:
				if f, ok := v.(float64); ok {
					row[col.Name] = int32(f)
				}
			case parquet.TypeInt64:
				if f, ok := v.(float64); ok {
					row[col.Name] = int64(f)
				}
			case parquet.TypeFloat32:
				if f, ok := v.(float64); ok {
					row[col.Name] = float32(f)
				}
			case parquet.TypeFloat64:
				// already float64 from JSON
			case parquet.TypeString:
				// already string from JSON
			}
		}
	}
	return raw, nil
}
