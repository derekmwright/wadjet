package tpch

import (
	"bytes"
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

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

const (
	duckdbBin              = "/tmp/duckdb"
	duckdbDataDir          = "duckdb-data"            // committed: benchmarks/tpch/duckdb-data/*.parquet
	duckdbBaselineFile     = "baseline-duckdb-sf001.json" // stored DuckDB-output checksums
	floatEps               = 1e-4                     // ULP-level drift between engines
)

// TestDuckDBCompare runs each TPC-H query against Wadjet on the SF0.01
// DuckDB-generated parquet committed under benchmarks/tpch/duckdb-data/, and
// gates Wadjet's output against the stored DuckDB-output checksums in
// benchmarks/tpch/baseline-duckdb-sf001.json. This catches silent value
// drift in cross-engine output (e.g., Q06 returning revenue=0 against
// externally-written parquet) without requiring DuckDB on the runner.
//
// Modes:
//
//	default       — run Wadjet, compare each row against the stored DuckDB
//	                checksum. No DuckDB binary required. This is the CI gate.
//	WADJET_DUCKDB_COMPARE=1 — additionally shell out to DuckDB and verify
//	                live cross-engine cell-by-cell agreement (catches drift
//	                in the stored baseline against current DuckDB output).
//	                Requires /tmp/duckdb.
//	WADJET_REGENERATE_DUCKDB_BASELINE=1 — regenerate the stored baseline by
//	                running DuckDB live and writing each query's row digest
//	                to disk. Requires /tmp/duckdb.
//
// Setup for the live / regenerate paths:
//
//	wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip
//	unzip -d /tmp /tmp/duckdb.zip
//
// The committed parquet fixtures were produced by
// benchmarks/tpch/duckdb-setup/export-wadjet-shape.sql at SF0.01.
func TestDuckDBCompare(t *testing.T) {
	regenerate := os.Getenv("WADJET_REGENERATE_DUCKDB_BASELINE") == "1"
	liveCompare := regenerate || os.Getenv("WADJET_DUCKDB_COMPARE") == "1"

	dataDir := filepath.Join(".", duckdbDataDir)
	for _, name := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
		if _, err := os.Stat(filepath.Join(dataDir, name+".parquet")); err != nil {
			t.Fatalf("missing fixture %s/%s.parquet — regenerate via duckdb-setup/export-wadjet-shape.sql: %v",
				dataDir, name, err)
		}
	}
	if liveCompare {
		if _, err := os.Stat(duckdbBin); err != nil {
			t.Fatalf("WADJET_DUCKDB_COMPARE / REGENERATE requires %s: %v", duckdbBin, err)
		}
	}

	// Wadjet ingest from the committed DuckDB-shaped parquet. Reading via
	// scan.ReadFileBatches → batch.ToRows exercises the parquet reader
	// against externally-written parquet, which is what the audit is for.
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
		rows, err := loadParquetRows(filepath.Join(dataDir, tableName+".parquet"), schema)
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

	// DuckDB setup: register views over the same parquet fixtures.
	duckdbSetup := ""
	if liveCompare {
		for _, name := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
			absDir, err := filepath.Abs(dataDir)
			if err != nil {
				t.Fatalf("abs %s: %v", dataDir, err)
			}
			duckdbSetup += fmt.Sprintf(
				"CREATE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet');\n",
				name, absDir, name)
		}
	}

	stored := loadDuckDBBaseline(t, duckdbBaselineFile)
	updated := make(map[int]duckdbBaselineEntry, len(stored))

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
			if wErr != nil {
				t.Errorf("wadjet error: %v", wErr)
				return
			}
			wEntry := duckdbBaselineEntry{
				RowCount: len(wRows),
				Checksum: queryChecksum(wRows),
			}

			if liveCompare {
				dRows, dCols, dErr := runDuckDB(duckdbSetup, q.SQL)
				if dErr != nil {
					t.Errorf("duckdb error: %v", dErr)
					return
				}
				if regenerate {
					updated[qNum] = duckdbBaselineEntry{
						RowCount: len(dRows),
						Checksum: queryChecksumStringRows(dCols, dRows),
					}
					t.Logf("Q%02d (regen): rows=%d", qNum, len(dRows))
					return
				}
				if !rowsEqualLive(wRows, dRows, wCols, dCols) {
					t.Errorf("live cross-engine divergence — see verbose output")
					reportLiveDiff(t, qNum, wRows, dRows, wCols, dCols)
					return
				}
			}

			want, ok := stored[qNum]
			if !ok {
				t.Errorf("Q%02d: no stored DuckDB baseline. Regenerate with WADJET_REGENERATE_DUCKDB_BASELINE=1 (rows=%d)",
					qNum, wEntry.RowCount)
				return
			}
			if wEntry.RowCount != want.RowCount {
				t.Errorf("Q%02d row count: got %d, want %d (DuckDB)", qNum, wEntry.RowCount, want.RowCount)
				return
			}
			if wEntry.Checksum != want.Checksum {
				t.Errorf("Q%02d content drift vs stored DuckDB baseline:\n  got %s\n  want %s\nfirst row: %v",
					qNum, wEntry.Checksum, want.Checksum, firstRow(wRows))
				return
			}
			t.Logf("Q%02d MATCH (%d rows, checksum %s)", qNum, wEntry.RowCount, wEntry.Checksum)
			matches++
		})
	}
	if regenerate {
		writeDuckDBBaseline(t, duckdbBaselineFile, updated)
		t.Logf("wrote %s with %d entries", duckdbBaselineFile, len(updated))
		return
	}
	t.Logf("Summary: %d/%d queries matched stored DuckDB baseline", matches, len(queryNums))
}

// rowsEqualLive compares Wadjet rows (typed) vs DuckDB rows (string CSV) cell
// by cell using cellEqual; column order is taken from the Wadjet result.
func rowsEqualLive(wRows []map[string]any, dRows []map[string]string, wCols, dCols []string) bool {
	if len(wRows) != len(dRows) {
		return false
	}
	cols := wCols
	if len(cols) == 0 {
		cols = dCols
	}
	for i := range wRows {
		for _, col := range cols {
			if !cellEqual(wRows[i][col], dRows[i][col]) {
				return false
			}
		}
	}
	return true
}

func reportLiveDiff(t *testing.T, qNum int, wRows []map[string]any, dRows []map[string]string, wCols, dCols []string) {
	t.Helper()
	if len(wRows) != len(dRows) {
		t.Logf("Q%02d row count: wadjet=%d duckdb=%d", qNum, len(wRows), len(dRows))
		if len(wRows) > 0 {
			t.Logf("  Wadjet first row: %v", wRows[0])
		}
		if len(dRows) > 0 {
			t.Logf("  DuckDB first row: %v", dRows[0])
		}
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
					t.Logf("Q%02d row %d col %s: wadjet=%v duckdb=%v",
						qNum, i, col, wRows[i][col], dRows[i][col])
				}
			}
		}
	}
	t.Logf("Q%02d %d cell divergences total", qNum, diff)
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
//
// NULL is encoded as the literal string "<NULL>" (instead of an empty field)
// so the CSV reader doesn't drop rows whose only column is NULL — the Go
// encoding/csv reader skips records with zero fields, which it treats a bare
// "\r\n" as. cellEqual recognises "<NULL>" as Wadjet's typed nil.
func runDuckDB(setup, sql string) ([]map[string]string, []string, error) {
	script := setup + "\n.mode csv\n.headers on\n.nullvalue <NULL>\n" + sql + ";\n"
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
	rows := make([]map[string]string, 0, len(records)-1)
	for _, rec := range records[1:] {
		row := make(map[string]string, len(cols))
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
func cellEqual(w any, d string) bool {
	if w == nil {
		return d == "" || d == "<NULL>"
	}
	if d == "<NULL>" {
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

// loadParquetRows reads a DuckDB-written parquet file via Wadjet's reader and
// returns rows ready for Wadjet's Ingester. Replaces the prior JSON path so
// the test exercises Wadjet's parquet decoder against externally-written data
// (the whole point of the cross-engine audit) and removes the 27 MB JSON
// dependency from the committed fixture.
func loadParquetRows(path string, schema parquet.Schema) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("parquet reader %s: %w", path, err)
	}
	batches, err := scan.ReadFileBatches(reader, schema.Columns, nil)
	if err != nil {
		return nil, fmt.Errorf("parquet scan %s: %w", path, err)
	}
	var rows []map[string]any
	for _, b := range batches {
		rows = append(rows, b.ToRows()...)
	}
	return rows, nil
}

// duckdbBaselineEntry is one row of baseline-duckdb-sf001.json — the row
// count and content checksum captured from a live DuckDB run on the
// committed fixture.
type duckdbBaselineEntry struct {
	RowCount int    `json:"row_count"`
	Checksum string `json:"checksum"`
}

func loadDuckDBBaseline(t *testing.T, file string) map[int]duckdbBaselineEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".", file))
	if err != nil {
		if os.IsNotExist(err) {
			return map[int]duckdbBaselineEntry{}
		}
		t.Fatalf("read duckdb baseline: %v", err)
	}
	var raw map[string]duckdbBaselineEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse duckdb baseline: %v", err)
	}
	out := make(map[int]duckdbBaselineEntry, len(raw))
	for k, v := range raw {
		n, err := strconv.Atoi(k)
		if err != nil {
			t.Fatalf("parse duckdb baseline key %q: %v", k, err)
		}
		out[n] = v
	}
	return out
}

func writeDuckDBBaseline(t *testing.T, file string, b map[int]duckdbBaselineEntry) {
	t.Helper()
	raw := make(map[string]duckdbBaselineEntry, len(b))
	for k, v := range b {
		raw[fmt.Sprintf("%d", k)] = v
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Fatalf("marshal duckdb baseline: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(".", file), data, 0o644); err != nil {
		t.Fatalf("write duckdb baseline: %v", err)
	}
}

// queryChecksumStringRows produces a checksum compatible with queryChecksum
// for DuckDB's CSV output (where every cell is a string). cols carries the
// column iteration order so the digest matches Wadjet's typed-row ordering.
func queryChecksumStringRows(cols []string, rows []map[string]string) string {
	typed := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(cols))
		for _, c := range cols {
			v := r[c]
			if v == "<NULL>" || v == "" {
				m[c] = nil
				continue
			}
			if f, ok := parseFloat(v); ok {
				m[c] = f
				continue
			}
			m[c] = v
		}
		typed[i] = m
	}
	return queryChecksum(typed)
}
