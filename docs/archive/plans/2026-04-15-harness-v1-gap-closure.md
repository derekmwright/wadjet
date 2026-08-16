# Harness V1 Gap Closure Implementation Plan


**Goal:** Close the three remaining gaps in the harness v1 branch: goroutine dump on hang via pprof, real micro-benchmarks with synthetic data, and worker metrics port isolation.

**Architecture:** Register `net/http/pprof` on the coordinator's HTTP server and the worker's metrics server. Track per-worker metrics ports in cluster.go. Add synthetic micro table generation to loadSampleData. Replace the micro_reverse_bloom stub and add two new micros.

**Tech Stack:** Go stdlib (`net/http/pprof`, `math/rand`), existing harness/parquet/catalog packages.

---

### Task 1: Register pprof on the coordinator's HTTP server

**Files:**
- Modify: `internal/server/server.go:1-114`

- [ ] **Step 1: Write the failing test**

Create `internal/server/pprof_test.go`:

```go
package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPprofGoroutineEndpoint(t *testing.T) {
	s := New(Config{}, nil)
	ts := httptest.NewServer(s.mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/pprof/goroutine?debug=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "goroutine") {
		t.Error("response does not contain goroutine dump")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestPprofGoroutineEndpoint -v ./internal/server/`
Expected: FAIL — 404 on `/debug/pprof/goroutine`

- [ ] **Step 3: Register pprof handlers**

In `internal/server/server.go`, add to imports:

```go
"net/http/pprof"
```

At the end of the `New` function (after line 111, before `return s`), add:

```go
	s.mux.HandleFunc("/debug/pprof/", pprof.Index)
	s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	s.mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	s.mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	s.mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -run TestPprofGoroutineEndpoint -v ./internal/server/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/server/server.go internal/server/pprof_test.go
git commit -m "feat(server): register pprof handlers on coordinator HTTP server"
```

---

### Task 2: Register pprof on the worker metrics server

**Files:**
- Modify: `cmd/wadjet/main.go:1130-1145`

- [ ] **Step 1: Add pprof to the worker metrics mux**

In `cmd/wadjet/main.go`, add to the import block (there's already a `"net/http"` import):

```go
"net/http/pprof"
```

After line 1138 (`metricsMux.Handle("/metrics", m.Handler())`), add:

```go
	metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
	metricsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	metricsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	metricsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	metricsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	metricsMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	metricsMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	metricsMux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
```

- [ ] **Step 2: Verify build**

Run: `go build ./cmd/wadjet`
Expected: compiles cleanly

- [ ] **Step 3: Commit**

```bash
git add cmd/wadjet/main.go
git commit -m "feat(worker): register pprof handlers on worker metrics server"
```

---

### Task 3: Track per-worker metrics ports in cluster.go

**Files:**
- Modify: `internal/harness/cluster.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/harness/fake_cluster_test.go`:

```go
func TestDebugPortsMapping(t *testing.T) {
	c := &Cluster{
		cfg:      ClusterConfig{PgAddr: ":15433"},
		httpPort: 8080,
	}
	c.workers = []*managedProcess{
		{role: "worker-0", debugPort: 9200},
		{role: "worker-1", debugPort: 9201},
	}
	ports := c.DebugPorts()
	if ports["coord"] != 8080 {
		t.Errorf("coord port: want 8080, got %d", ports["coord"])
	}
	if ports["worker-0"] != 9200 {
		t.Errorf("worker-0 port: want 9200, got %d", ports["worker-0"])
	}
	if ports["worker-1"] != 9201 {
		t.Errorf("worker-1 port: want 9201, got %d", ports["worker-1"])
	}
	if len(ports) != 3 {
		t.Errorf("want 3 entries, got %d", len(ports))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestDebugPortsMapping -v ./internal/harness/`
Expected: FAIL — `debugPort` field and `DebugPorts` method undefined

- [ ] **Step 3: Add debugPort field and DebugPorts method**

In `internal/harness/cluster.go`, add `debugPort` to the `managedProcess` struct (after the `exitErr` field at line 54):

```go
type managedProcess struct {
	role      string // "coord" or "worker-N"
	cmd       *exec.Cmd
	logFile   *os.File
	exitedC   chan struct{} // closed when the process exits
	exitErr   error
	debugPort int // HTTP port for pprof (coordinator httpPort, worker metricsPort)
}
```

Add the `DebugPorts` method after the `PgAddr` method (after line 71):

```go
// DebugPorts returns a map of role → HTTP port for pprof access.
// Only valid after StartCoordinator and StartWorkers have been called.
func (c *Cluster) DebugPorts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	ports := make(map[string]int, 1+len(c.workers))
	if c.httpPort > 0 {
		ports["coord"] = c.httpPort
	}
	for _, w := range c.workers {
		if w.debugPort > 0 {
			ports[w.role] = w.debugPort
		}
	}
	return ports
}
```

- [ ] **Step 4: Assign per-worker metrics ports in StartWorkers**

In `StartWorkers` (around line 139), allocate a random metrics port per worker and pass it as `--metrics-addr`. Replace the worker spawn loop:

```go
	for i := 0; i < c.cfg.NumWorkers; i++ {
		role := fmt.Sprintf("worker-%d", i)
		metricsPort := freePort()
		workerArgs := []string{
			"serve",
			"--mode=worker",
			"--nats-url=" + c.natsURL,
			"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", role),
			"--metrics-addr=:" + strconv.Itoa(metricsPort),
		}
		if c.cfg.DataDir != "" {
			workerArgs = append(workerArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
		}
		w, err := c.spawn(role, workerArgs)
		if err != nil {
			return fmt.Errorf("spawning %s: %w", role, err)
		}
		w.debugPort = metricsPort
		c.workers = append(c.workers, w)
	}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -run TestDebugPortsMapping -v ./internal/harness/`
Expected: PASS

- [ ] **Step 6: Run all harness tests**

Run: `go test -short -v ./internal/harness/...`
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add internal/harness/cluster.go internal/harness/fake_cluster_test.go
git commit -m "feat(harness): track per-worker metrics ports, expose DebugPorts()"
```

---

### Task 4: Capture goroutine dumps on hang

**Files:**
- Modify: `internal/harness/harness.go`

- [ ] **Step 1: Write the captureGoroutineDumps function**

Add to `internal/harness/harness.go`, before the `runOneQuery` function:

```go
// captureGoroutineDumps fetches /debug/pprof/goroutine?debug=2 from every
// process in the cluster and writes each dump to a file in runDir/logs/.
// Returns the directory containing the dumps. Errors are logged, not fatal —
// a missing dump is acceptable, a harness hang is not.
func captureGoroutineDumps(cluster *Cluster, query string, runDir string, logger *slog.Logger) string {
	if cluster == nil {
		return ""
	}
	dumpDir := filepath.Join(runDir, "logs")
	ports := cluster.DebugPorts()
	client := &http.Client{Timeout: 5 * time.Second}
	for role, port := range ports {
		url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/goroutine?debug=2", port)
		resp, err := client.Get(url)
		if err != nil {
			logger.Warn("pprof fetch failed", "role", role, "query", query, "err", err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		dumpPath := filepath.Join(dumpDir, fmt.Sprintf("hang-%s-%s.txt", query, role))
		if err := os.WriteFile(dumpPath, body, 0644); err != nil {
			logger.Warn("writing dump", "path", dumpPath, "err", err)
		} else {
			logger.Info("goroutine dump captured", "path", dumpPath, "bytes", len(body))
		}
	}
	return dumpDir
}
```

Add `"io"` and `"net/http"` to the imports in harness.go if not already present.

- [ ] **Step 2: Wire captureGoroutineDumps into runOneQuery**

`runOneQuery` needs access to `cluster`, `runDir`, and `logger`. Update its signature and the call site.

Update the `runOneQuery` signature (line 222) to:

```go
func runOneQuery(
	ctx context.Context,
	coordURL string,
	name string,
	collector *MeasurementCollector,
	hangDetector *HangDetector,
	baseline *BaselineFile,
	_ SliceConfig,
	cluster *Cluster,
	runDir string,
	logger *slog.Logger,
) (QueryMeasurement, error) {
```

After the query timeout fires (just before `m := collector.EndWindow(name)` at line 278), add hang dump capture. Replace the existing result collection block. The full updated function body after the `rows.Close()` + error check should be:

```go
	if err := rows.Err(); err != nil {
		// Check if this was a timeout — if so, capture goroutine dumps.
		if queryCtx.Err() != nil {
			dumpPath := captureGoroutineDumps(cluster, name, runDir, logger)
			m := collector.EndWindow(name)
			m.Hung = true
			m.HangDumpPath = dumpPath
			return m, err
		}
		return collector.EndWindow(name), err
	}

	m := collector.EndWindow(name)
	m.RowCount = rowCount
	m.RowChecksum = hex.EncodeToString(hash.Sum(nil))
	return m, nil
```

Update the call site in the `Run` function (around line 107) to pass the new args:

```go
		m, err := runOneQuery(ctx, coordURL, qname, collector, hangDetector, baseline, sliceCfg, cluster, runDir, logger)
```

- [ ] **Step 3: Verify build and tests**

Run: `go build ./cmd/tpch-harness && go test -short -v ./internal/harness/...`
Expected: compiles, all tests pass

- [ ] **Step 4: Commit**

```bash
git add internal/harness/harness.go
git commit -m "feat(harness): capture goroutine dumps via pprof on hang detection"
```

---

### Task 5: Synthetic micro data generation

**Files:**
- Modify: `internal/harness/micros.go`
- Create: `internal/harness/micros_test.go`

- [ ] **Step 1: Write the failing test for generateMicroData**

Create `internal/harness/micros_test.go`:

```go
package harness

import "testing"

func TestGenerateMicroData(t *testing.T) {
	data := generateMicroData()

	cases := []struct {
		table string
		rows  int
		cols  int
	}{
		{"micro_lineitem", 200_000, 3},
		{"micro_orders", 20_000, 2},
		{"micro_build", 500_000, 3},
		{"micro_probe", 50_000, 2},
		{"micro_agg", 200_000, 2},
	}
	for _, tc := range cases {
		mt, ok := data[tc.table]
		if !ok {
			t.Errorf("missing table %s", tc.table)
			continue
		}
		if len(mt.rows) != tc.rows {
			t.Errorf("%s: want %d rows, got %d", tc.table, tc.rows, len(mt.rows))
		}
		if len(mt.schema.Columns) != tc.cols {
			t.Errorf("%s: want %d cols, got %d", tc.table, tc.cols, len(mt.schema.Columns))
		}
	}
}

func TestGenerateMicroDataDeterministic(t *testing.T) {
	d1 := generateMicroData()
	d2 := generateMicroData()
	// Check a sample value from micro_agg is identical across runs.
	r1 := d1["micro_agg"].rows[0]["group_key"]
	r2 := d2["micro_agg"].rows[0]["group_key"]
	if r1 != r2 {
		t.Errorf("non-deterministic: %v != %v", r1, r2)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestGenerateMicroData -v ./internal/harness/`
Expected: FAIL — `generateMicroData` undefined

- [ ] **Step 3: Implement generateMicroData**

Replace the contents of `internal/harness/micros.go` with:

```go
package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/jackc/pgx/v5"
)

// microTable holds a synthetic table's schema and generated rows.
type microTable struct {
	schema parquet.Schema
	rows   []map[string]any
}

// microSchemas defines the schemas for all synthetic micro tables.
var microSchemas = map[string]parquet.Schema{
	"micro_lineitem": {Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
	}},
	"micro_orders": {Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
	}},
	"micro_build": {Columns: []parquet.Column{
		{Name: "build_key", Type: parquet.TypeInt64},
		{Name: "build_val", Type: parquet.TypeInt64},
		{Name: "build_pad", Type: parquet.TypeString},
	}},
	"micro_probe": {Columns: []parquet.Column{
		{Name: "probe_key", Type: parquet.TypeInt64},
		{Name: "probe_val", Type: parquet.TypeInt64},
	}},
	"micro_agg": {Columns: []parquet.Column{
		{Name: "group_key", Type: parquet.TypeString},
		{Name: "value", Type: parquet.TypeInt64},
	}},
}

// generateMicroData creates deterministic synthetic data for all micro tables.
func generateMicroData() map[string]microTable {
	rng := rand.New(rand.NewSource(42))
	data := make(map[string]microTable, len(microSchemas))

	// micro_lineitem: 200K rows, l_orderkey in [1, 20000] (matches micro_orders)
	{
		rows := make([]map[string]any, 200_000)
		for i := range rows {
			rows[i] = map[string]any{
				"l_orderkey": int64(rng.Intn(20_000) + 1),
				"l_partkey":  int64(rng.Intn(100_000) + 1),
				"l_quantity": float64(rng.Intn(50) + 1),
			}
		}
		data["micro_lineitem"] = microTable{schema: microSchemas["micro_lineitem"], rows: rows}
	}

	// micro_orders: 20K rows, o_orderkey in [1, 20000] (unique keys)
	{
		rows := make([]map[string]any, 20_000)
		for i := range rows {
			rows[i] = map[string]any{
				"o_orderkey":   int64(i + 1),
				"o_totalprice": float64(rng.Intn(500_000)) / 100.0,
			}
		}
		data["micro_orders"] = microTable{schema: microSchemas["micro_orders"], rows: rows}
	}

	// micro_build: 500K rows, high-cardinality keys, padded strings to inflate memory
	{
		const pad = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" // 64 bytes
		rows := make([]map[string]any, 500_000)
		for i := range rows {
			rows[i] = map[string]any{
				"build_key": int64(rng.Intn(50_000) + 1),
				"build_val": int64(rng.Int63()),
				"build_pad": fmt.Sprintf("%s-%d", pad, i),
			}
		}
		data["micro_build"] = microTable{schema: microSchemas["micro_build"], rows: rows}
	}

	// micro_probe: 50K rows, keys in [1, 50000] (overlap with micro_build)
	{
		rows := make([]map[string]any, 50_000)
		for i := range rows {
			rows[i] = map[string]any{
				"probe_key": int64(i + 1),
				"probe_val": int64(rng.Int63()),
			}
		}
		data["micro_probe"] = microTable{schema: microSchemas["micro_probe"], rows: rows}
	}

	// micro_agg: 200K rows, 100K distinct group keys (2 rows per key on average)
	{
		rows := make([]map[string]any, 200_000)
		for i := range rows {
			rows[i] = map[string]any{
				"group_key": fmt.Sprintf("grp_%06d", rng.Intn(100_000)),
				"value":     int64(rng.Intn(10_000)),
			}
		}
		data["micro_agg"] = microTable{schema: microSchemas["micro_agg"], rows: rows}
	}

	return data
}

// runMicroQuery is the shared execution logic for all micro-benchmarks.
// It opens a pgx connection, runs the query, collects row count + checksum,
// and returns the measurement from the collector window.
func runMicroQuery(ctx context.Context, coordURL string, name string, sql string, collector *MeasurementCollector) (QueryMeasurement, error) {
	collector.StartWindow(name)

	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	conn, err := pgx.Connect(queryCtx, coordURL)
	if err != nil {
		return collector.EndWindow(name), fmt.Errorf("pgx connect: %w", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(queryCtx, sql)
	if err != nil {
		return collector.EndWindow(name), err
	}
	defer rows.Close()

	hash := sha256.New()
	var rowCount int64
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return collector.EndWindow(name), err
		}
		fmt.Fprintf(hash, "%v\n", vals)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return collector.EndWindow(name), err
	}

	m := collector.EndWindow(name)
	m.RowCount = rowCount
	m.RowChecksum = hex.EncodeToString(hash.Sum(nil))
	return m, nil
}

// RunMicroReverseBloom forces the reverseBloomBridge into its spill path
// by joining a large build side (micro_lineitem, 200K rows) against a small
// probe side (micro_orders, 20K rows), then asserts spill occurred.
func RunMicroReverseBloom(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql := `SELECT o.o_orderkey, SUM(l.l_quantity)
FROM micro_lineitem l
JOIN micro_orders o ON l.l_orderkey = o.o_orderkey
GROUP BY o.o_orderkey`

	m, err := runMicroQuery(ctx, coordURL, "micro_reverse_bloom", sql, collector)
	if err != nil {
		return m, err
	}
	if m.RowCount == 0 {
		return m, fmt.Errorf("micro_reverse_bloom: expected rows, got 0")
	}
	return m, nil
}

// RunMicroGraceHashJoin forces grace hash join partitioning by joining a
// memory-heavy build side (micro_build, 500K padded rows) against a smaller
// probe side (micro_probe, 50K rows), then asserts spill occurred.
func RunMicroGraceHashJoin(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql := `SELECT b.build_key, b.build_val, p.probe_val
FROM micro_build b
JOIN micro_probe p ON b.build_key = p.probe_key`

	m, err := runMicroQuery(ctx, coordURL, "micro_grace_hash_join", sql, collector)
	if err != nil {
		return m, err
	}
	if m.RowCount == 0 {
		return m, fmt.Errorf("micro_grace_hash_join: expected rows, got 0")
	}
	return m, nil
}

// RunMicroHashAggHighCard runs a high-cardinality GROUP BY (100K distinct keys)
// and asserts allocation discipline — no per-row allocation leak.
func RunMicroHashAggHighCard(ctx context.Context, coordURL string, collector *MeasurementCollector) (QueryMeasurement, error) {
	sql := `SELECT group_key, COUNT(*), SUM(value)
FROM micro_agg
GROUP BY group_key`

	m, err := runMicroQuery(ctx, coordURL, "micro_hash_agg_high_card", sql, collector)
	if err != nil {
		return m, err
	}
	if m.RowCount == 0 {
		return m, fmt.Errorf("micro_hash_agg_high_card: expected rows, got 0")
	}
	return m, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestGenerateMicroData -v ./internal/harness/`
Expected: both tests PASS

- [ ] **Step 5: Commit**

```bash
git add internal/harness/micros.go internal/harness/micros_test.go
git commit -m "feat(harness): synthetic micro data generation and real micro-benchmark implementations"
```

---

### Task 6: Seed micro tables in loadSampleData

**Files:**
- Modify: `internal/harness/harness.go`

- [ ] **Step 1: Add micro table seeding to loadSampleData**

In `internal/harness/harness.go`, inside the `loadSampleData` function, after the TPC-H table loop (after line 377, before the final `return nil`), add:

```go
	// Seed synthetic micro tables for micro-benchmarks.
	microData := generateMicroData()
	for tableName, mt := range microData {
		if err := cat.CreateTable(ctx, tableName, mt.schema, nil); err != nil {
			return fmt.Errorf("creating micro table %s: %w", tableName, err)
		}
		if len(mt.rows) == 0 {
			continue
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, mt.schema, parquet.DefaultWriterConfig())
		if err != nil {
			return fmt.Errorf("parquet writer for %s: %w", tableName, err)
		}
		if err := pw.WriteRows(mt.rows); err != nil {
			return fmt.Errorf("writing %s: %w", tableName, err)
		}
		if err := pw.Close(); err != nil {
			return fmt.Errorf("closing %s: %w", tableName, err)
		}
		filePath := fmt.Sprintf("tables/%s/chunk_0001.parquet", tableName)
		pdata := buf.Bytes()
		if _, err := store.Put(ctx, bucketName, filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
			return fmt.Errorf("storing %s: %w", tableName, err)
		}
		entries := []catalog.FileEntry{{
			Path:      filePath,
			SizeBytes: int64(len(pdata)),
			NumRows:   int64(len(mt.rows)),
			CreatedAt: time.Now(),
		}}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", entries); err != nil {
			return fmt.Errorf("adding %s files to catalog: %w", tableName, err)
		}
		logger.Info("loaded micro table", "table", tableName, "rows", len(mt.rows))
	}
```

- [ ] **Step 2: Update the load_data_test to expect micro tables**

In `internal/harness/load_data_test.go`, update the expected table map (around line 90) to include the 5 micro tables:

```go
	expected := map[string]bool{
		"region": true, "nation": true, "supplier": true, "part": true,
		"partsupp": true, "customer": true, "orders": true, "lineitem": true,
		"micro_lineitem": true, "micro_orders": true, "micro_build": true,
		"micro_probe": true, "micro_agg": true,
	}
```

- [ ] **Step 3: Verify build and tests**

Run: `go build ./cmd/tpch-harness && go test -short -v ./internal/harness/...`
Expected: compiles, all tests pass (load_data_test is skipped with -short, but unit tests pass)

- [ ] **Step 4: Commit**

```bash
git add internal/harness/harness.go internal/harness/load_data_test.go
git commit -m "feat(harness): seed synthetic micro tables in loadSampleData"
```

---

### Task 7: Wire new micros into suite.go and runOneQuery dispatch

**Files:**
- Modify: `internal/harness/suite.go`
- Modify: `internal/harness/harness.go`
- Modify: `internal/harness/suite_test.go`

- [ ] **Step 1: Update SelectQueries to include all three micros**

In `internal/harness/suite.go`, update `SelectQueries` (line 72):

```go
func SelectQueries(requested []string) []string {
	if len(requested) == 0 {
		out := AllTPCHQueries()
		out = append(out, "micro_reverse_bloom", "micro_grace_hash_join", "micro_hash_agg_high_card")
		return out
	}
	return requested
}
```

- [ ] **Step 2: Update the suite_test**

In `internal/harness/suite_test.go`, update `TestSelectQueriesEmpty` (line 14):

```go
func TestSelectQueriesEmpty(t *testing.T) {
	got := SelectQueries(nil)
	if len(got) != 25 { // 22 TPC-H + 3 micros
		t.Errorf("want 22+3 queries, got %d", len(got))
	}
}
```

- [ ] **Step 3: Update runOneQuery micro dispatch**

In `internal/harness/harness.go`, update the micro dispatch in `runOneQuery` (around line 235). Replace:

```go
	sql, err := LoadQuery(name)
	if err != nil {
		if name == "micro_reverse_bloom" {
			return RunMicroReverseBloom(ctx, coordURL, collector)
		}
		return collector.EndWindow(name), err
	}
```

With:

```go
	sql, err := LoadQuery(name)
	if err != nil {
		switch name {
		case "micro_reverse_bloom":
			return RunMicroReverseBloom(ctx, coordURL, collector)
		case "micro_grace_hash_join":
			return RunMicroGraceHashJoin(ctx, coordURL, collector)
		case "micro_hash_agg_high_card":
			return RunMicroHashAggHighCard(ctx, coordURL, collector)
		default:
			return collector.EndWindow(name), err
		}
	}
```

- [ ] **Step 4: Run all tests**

Run: `go test -short -v ./internal/harness/...`
Expected: all pass (TestSelectQueriesEmpty now expects 25)

- [ ] **Step 5: Commit**

```bash
git add internal/harness/suite.go internal/harness/suite_test.go internal/harness/harness.go
git commit -m "feat(harness): wire all three micro-benchmarks into suite and query dispatch"
```

---

### Task 8: Final verification

- [ ] **Step 1: Run full harness test suite**

Run: `go test -short -count=1 -v ./internal/harness/...`
Expected: all tests pass

- [ ] **Step 2: Run server pprof test**

Run: `go test -run TestPprofGoroutineEndpoint -v ./internal/server/`
Expected: PASS

- [ ] **Step 3: Build both binaries**

Run: `go build ./cmd/wadjet && go build ./cmd/tpch-harness`
Expected: both compile cleanly

- [ ] **Step 4: Run go vet on changed packages**

Run: `go vet ./internal/harness/ ./internal/server/ ./cmd/wadjet/ ./cmd/tpch-harness/`
Expected: no warnings
