package tpch

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// Generation of the SF100 ground truth.
//
// This test is the ONLY writer of a ground-truth fingerprint file, and every
// entry it writes is stamped with the engine that produced it. It never reads
// a Wadjet result: the whole value of the file is that it was computed
// somewhere else, over the same bytes.
//
// DuckDB is the reference rather than Trino, for three reasons. It is already
// this repo's reference engine (benchmarks/tpch/duckdb-setup/, the SF0.01 gate
// and its 175 stored fingerprints), so the two scales agree on what "ground
// truth" means and on how a cell is rendered. It is a single binary with no
// cluster to stand up, which matters for a job that has to run inside a
// benchmark window. And it reads the benchmark's own parquet in place through
// httpfs, so no data is re-generated and the "same bytes" property is
// structural rather than a claim. Trino remains the second opinion — it
// produced the externally-validated row counts in baseline-sf100.json — and
// the loader accepts entries stamped "trino" for exactly that reason.
//
// SF100 IS NOT RUN LOCALLY. The bucket is 280 GiB; generation belongs on an
// in-region instance during a benchmark window. See
// docs/design/sf100-value-fingerprints.md for the runbook.
//
//	# SF100, in-region, against the bucket the benchmark reads:
//	WADJET_FP_GENERATE=1 \
//	WADJET_FP_DATA=s3://wadjet-bench-sf100-use2/tables/ \
//	WADJET_FP_S3_REGION=us-east-2 \
//	WADJET_FP_DUCKDB_MEMORY=48GB WADJET_FP_DUCKDB_TMP=/mnt/nvme/duckdb-tmp \
//	  go test -run TestGenerateFingerprintGroundTruth -timeout 6h -v ./benchmarks/tpch/
//
//	# Smoke test of the same path over the committed SF0.01 fixtures:
//	WADJET_FP_GENERATE=1 WADJET_FP_DATA=duckdb-data WADJET_FP_SCALE=0.01 \
//	WADJET_FP_OUT=/tmp/fingerprint-sf001.json \
//	  go test -run TestGenerateFingerprintGroundTruth -v ./benchmarks/tpch/
func TestGenerateFingerprintGroundTruth(t *testing.T) {
	if os.Getenv("WADJET_FP_GENERATE") != "1" {
		t.Skip("set WADJET_FP_GENERATE=1 (plus WADJET_FP_DATA) to capture reference fingerprints")
	}
	data := os.Getenv("WADJET_FP_DATA")
	if data == "" {
		t.Fatal("WADJET_FP_DATA must name the parquet the BENCHMARK reads: an s3:// prefix, or a local directory")
	}
	if _, err := os.Stat(duckdbBin); err != nil {
		t.Fatalf("the generator needs the DuckDB CLI at %s: %v", duckdbBin, err)
	}
	versionOut, err := exec.Command(duckdbBin, "--version").Output()
	if err != nil {
		t.Fatalf("duckdb --version: %v", err)
	}
	version := strings.TrimSpace(string(versionOut))

	sf := fpGenScale(t)
	out := os.Getenv("WADJET_FP_OUT")
	if out == "" {
		out = "fingerprint-sf100.json"
		if sf != SF100 {
			t.Fatalf("refusing to write %s from an SF%g capture: set WADJET_FP_OUT", out, float64(sf))
		}
	}

	setup := fpDuckDBSetup(t, data)
	corpus := CorrectnessQueries(sf)
	stored := loadStoredForVerify(t)

	entries := make(map[string]FPEntry, len(corpus))
	capturedAt := time.Now().UTC().Format(time.RFC3339)
	for _, q := range corpus {
		start := time.Now()
		rows, cols, err := runDuckDB(setup, q.SQL)
		if err != nil {
			t.Fatalf("%s: duckdb: %v", q.Name, err)
		}
		typed := make([]map[string]any, len(rows))
		for i, r := range rows {
			m := make(map[string]any, len(cols))
			for _, col := range cols {
				m[col] = oracle.TextCell(r[col], duckdbNull)
			}
			typed[i] = m
		}
		sig := SignatureOf(&oracle.Result{Columns: cols, Rows: typed}, q)
		entries[q.Name] = NewEntry("duckdb", version, data, capturedAt, q, sig)
		t.Logf("%s: %d rows in %v (%s)", q.Name, len(rows), time.Since(start).Round(time.Second), sig)

		// Verify arm: the stored file must still be what DuckDB answers. A
		// divergence here means the data, the query text, or the DuckDB
		// version moved under the baseline — never that Wadjet is wrong.
		if e, ok := stored[q.Name]; ok {
			if match, detail := e.Match(sig); !match {
				t.Errorf("STORED FINGERPRINT IS NOT DUCKDB'S ANSWER: %s %s", q.Name, detail)
			}
		}
	}

	f := &FingerprintFile{
		Version: 1,
		Kind:    KindGroundTruth,
		Scale:   fmt.Sprintf("SF%g", float64(sf)),
		Generator: fmt.Sprintf("duckdb %s over %s, captured %s (WADJET_FP_GENERATE=1)",
			version, data, capturedAt),
		Note: "Ground truth. Every entry is DuckDB output over the parquet the benchmark reads; nothing here " +
			"is derived from Wadjet, and the loader refuses an entry stamped otherwise. Values are deliberately " +
			"absent: only row counts, column names, and opaque digests are stored.",
		Queries: entries,
	}
	blob, err := MarshalFingerprintFile(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Round-trip through the loader before writing: a file the gate would
	// refuse is not worth committing.
	loaded, err := ParseFingerprintFile(blob, KindGroundTruth)
	if err != nil {
		t.Fatalf("generated file does not load: %v", err)
	}
	if err := loaded.CheckCoversCorpus(corpus); err != nil {
		t.Fatalf("generated file does not cover the corpus: %v", err)
	}
	if err := os.WriteFile(out, blob, 0o644); err != nil {
		t.Fatalf("write %s: %v", out, err)
	}
	t.Logf("wrote %s: %d entries from duckdb %s over %s", out, len(entries), version, data)
}

// loadStoredForVerify returns the committed entries when they exist, so a
// regeneration also re-checks the file it is replacing.
func loadStoredForVerify(t *testing.T) map[string]FPEntry {
	t.Helper()
	f, err := GroundTruthSF100()
	if err != nil {
		return nil
	}
	return f.Queries
}

func fpGenScale(t *testing.T) ScaleFactor {
	t.Helper()
	switch v := os.Getenv("WADJET_FP_SCALE"); v {
	case "", "100":
		return SF100
	case "0.01":
		return SF001
	case "0.1":
		return SF01
	case "1":
		return SF1
	case "10":
		return SF10
	default:
		t.Fatalf("WADJET_FP_SCALE=%q: want 0.01, 0.1, 1, 10 or 100", v)
		return 0
	}
}

// fpDuckDBSetup builds the DuckDB preamble: one view per TPC-H table over the
// benchmark's own parquet.
//
// Date columns are cast to VARCHAR. The SF100 bucket stores them as parquet
// DATE while the local fixtures store them as strings, and Wadjet renders a
// DATE cell as YYYY-MM-DD (batch.FormatDate) either way — the cast makes
// DuckDB's rendering identical instead of type-dependent, and it is a no-op
// where the column is already text.
func fpDuckDBSetup(t *testing.T, data string) string {
	t.Helper()
	var sb strings.Builder
	if strings.HasPrefix(data, "s3://") {
		region := os.Getenv("WADJET_FP_S3_REGION")
		if region == "" {
			t.Fatal("WADJET_FP_S3_REGION is required for an s3:// dataset")
		}
		sb.WriteString("INSTALL httpfs; LOAD httpfs;\n")
		fmt.Fprintf(&sb, "SET s3_region='%s';\n", region)
		// CREDENTIAL_CHAIN picks up the instance role, which is how the
		// benchmark hosts reach the bucket.
		sb.WriteString("CREATE SECRET IF NOT EXISTS fpgen (TYPE S3, PROVIDER CREDENTIAL_CHAIN);\n")
	}
	if mem := os.Getenv("WADJET_FP_DUCKDB_MEMORY"); mem != "" {
		fmt.Fprintf(&sb, "SET memory_limit='%s';\n", mem)
	}
	if tmp := os.Getenv("WADJET_FP_DUCKDB_TMP"); tmp != "" {
		fmt.Fprintf(&sb, "SET temp_directory='%s';\n", tmp)
	}

	names := make([]string, 0, len(AllTables))
	for name := range AllTables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(&sb, "CREATE VIEW %s AS SELECT *%s FROM read_parquet('%s');\n",
			name, fpDateReplace(name), fpParquetGlob(t, data, name))
	}
	return sb.String()
}

// fpDateColumns are the TPC-H columns the SF100 parquet stores as DATE.
var fpDateColumns = map[string][]string{
	"lineitem": {"l_shipdate", "l_commitdate", "l_receiptdate"},
	"orders":   {"o_orderdate"},
}

func fpDateReplace(table string) string {
	cols := fpDateColumns[table]
	if len(cols) == 0 {
		return ""
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("CAST(%s AS VARCHAR) AS %s", c, c)
	}
	return " REPLACE (" + strings.Join(parts, ", ") + ")"
}

// fpParquetGlob resolves one table's parquet location under data, which is
// either an s3:// prefix, a directory of <table>.parquet files (the committed
// fixtures), or a directory of <table>/ subdirectories (the benchmark layout).
func fpParquetGlob(t *testing.T, data, table string) string {
	t.Helper()
	if strings.HasPrefix(data, "s3://") {
		return strings.TrimSuffix(data, "/") + "/" + table + "/*.parquet"
	}
	abs, err := filepath.Abs(data)
	if err != nil {
		t.Fatalf("abs %s: %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(abs, table+".parquet")); err == nil {
		return filepath.Join(abs, table+".parquet")
	}
	if _, err := os.Stat(filepath.Join(abs, table)); err == nil {
		return filepath.Join(abs, table, "*.parquet")
	}
	t.Fatalf("no parquet for table %s under %s", table, abs)
	return ""
}
