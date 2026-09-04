> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Harness S3 Mode — PR A Implementation Plan


**Goal:** Extend `cmd/tpch-harness` with an `--source=s3` mode so it can drive a local coordinator+workers cluster against the real SF10 TPC-H bucket (`wadjet-bench-sf10-use2`). Unblocks local reproduction of the distributed-execution regression class that currently requires EC2 to observe.

**Architecture:** Reuse the existing harness cluster-spawn machinery; the only changes are (a) configuring the coordinator to use the S3 object store instead of `FileStore`, (b) skipping `loadSampleData` when data is already in S3, and (c) priming the catalog by listing table parquet files under a configurable S3 prefix — the same pattern `cmd/tpch-bench` already uses.

**Tech Stack:** Go, embedded NATS, NATS KV catalog, minio-go S3 client, existing `tpch-bench` catalog discovery.

**Spec:** `docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md` — "Testing" section.

**Base branch:** `feat/distribution-property-phase-2` (HEAD `ba9c9ac`). This PR lands first; PR B (native-DAG runtime) depends on it.

---

## File Structure

- `cmd/tpch-harness/main.go` — add `--source`, `--bucket`, `--region`, `--endpoint`, `--ssl`, `--data-prefix` flags
- `internal/harness/types.go` — `Config` gains `Source`, `Bucket`, `Region`, `Endpoint`, `SSL`, `DataPrefix`
- `internal/harness/cluster.go` — `ClusterConfig` gains `StorageType`, `Bucket`, `Region`, `Endpoint`, `SSL`; coordinator and worker arg construction branches on `StorageType`
- `internal/harness/harness.go` — `Run` branches: if `Source == "s3"`, skip `loadSampleData`, call new `primeS3Catalog` instead
- `internal/harness/s3_catalog.go` (new) — `primeS3Catalog` lists parquet files under `<bucket>/<dataPrefix>/<table>/` and calls `db.CreateTable` + `db.RegisterFiles` the same way `cmd/tpch-bench` does in `discoverData`

---

## Task A1: Add `--source` flag + Config field

**Files:**
- Modify: `cmd/tpch-harness/main.go`
- Modify: `internal/harness/types.go`

- [ ] **Step 1: Add flag to main.go**

Append after the `pgAddr` flag declaration (around line 28):

```go
		source     = flag.String("source", "local", "data source: local (files under --data-dir) or s3 (coordinator reads from --bucket)")
		bucket     = flag.String("bucket", "wadjet-bench-sf10-use2", "S3 bucket name (source=s3)")
		region     = flag.String("region", "us-east-2", "S3 region (source=s3)")
		endpoint   = flag.String("endpoint", "s3.us-east-2.amazonaws.com", "S3 endpoint (source=s3)")
		ssl        = flag.Bool("ssl", true, "use SSL for S3 (source=s3)")
		dataPrefix = flag.String("data-prefix", "tables/", "S3 prefix under --bucket containing table data (source=s3)")
```

- [ ] **Step 2: Wire flags into Config construction**

In the same file where `cfg := harness.Config{...}` is built, add:

```go
		Source:     *source,
		Bucket:     *bucket,
		Region:     *region,
		Endpoint:   *endpoint,
		SSL:        *ssl,
		DataPrefix: *dataPrefix,
```

- [ ] **Step 3: Extend `harness.Config` struct**

In `internal/harness/types.go`, append to `Config`:

```go
	// S3 source (Source=="s3" only)
	Source     string // "local" or "s3" (default "local")
	Bucket     string
	Region     string
	Endpoint   string
	SSL        bool
	DataPrefix string // prefix under Bucket containing table data
```

- [ ] **Step 4: Validate source value**

In `main.go` `main()`, after `flag.Parse()`, insert:

```go
	if *source != "local" && *source != "s3" {
		fmt.Fprintln(os.Stderr, "ERROR: --source must be 'local' or 's3'")
		os.Exit(harness.ExitSetup)
	}
```

- [ ] **Step 5: Build + smoke test**

Run: `go build ./cmd/tpch-harness && ./tpch-harness --help 2>&1 | grep -E "source|bucket|data-prefix"`
Expected: each of the six new flags appears in the help output.

- [ ] **Step 6: Commit**

```bash
git add cmd/tpch-harness/main.go internal/harness/types.go
git commit -m "feat(harness): add --source/--bucket/--region/--endpoint/--ssl/--data-prefix flags

Plumbs the S3-mode configuration through the Config struct. No runtime
behavior change yet — Source defaults to 'local' and s3 mode is a no-op
until Task A2 branches the cluster spawn on Source.

Phase 3 harness spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task A2: Extend `ClusterConfig` + coordinator/worker arg construction

**Files:**
- Modify: `internal/harness/cluster.go`

- [ ] **Step 1: Add fields to ClusterConfig**

In `internal/harness/cluster.go`, extend `ClusterConfig` (around line 22-32):

```go
type ClusterConfig struct {
	WadjetBin  string
	RunDir     string
	NumWorkers int
	GoMemLimit int64
	PgAddr     string
	DataDir    string // FileStore dir (StorageType=="file")

	// S3 storage mode (StorageType=="s3")
	StorageType string // "file" (default) or "s3"
	Bucket      string
	Region      string
	Endpoint    string
	SSL         bool

	Logger *slog.Logger
}
```

- [ ] **Step 2: Branch coordinator args on StorageType**

Find the coordinator args construction (around line 114-125) that currently has:

```go
	if c.cfg.DataDir != "" {
		coordArgs = append(coordArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
	}
```

Replace with:

```go
	switch c.cfg.StorageType {
	case "s3":
		coordArgs = append(coordArgs,
			"--storage-type=s3",
			"--bucket="+c.cfg.Bucket,
			"--region="+c.cfg.Region,
			"--endpoint="+c.cfg.Endpoint,
		)
		if c.cfg.SSL {
			coordArgs = append(coordArgs, "--ssl")
		}
	default: // "file" or empty
		if c.cfg.DataDir != "" {
			coordArgs = append(coordArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
		}
	}
```

- [ ] **Step 3: Apply same branch to worker args**

Find the worker args construction (around line 167-169):

```go
		if c.cfg.DataDir != "" {
			workerArgs = append(workerArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
		}
```

Replace with the same switch as Step 2, appending to `workerArgs` instead of `coordArgs`.

- [ ] **Step 4: Build**

Run: `go build ./internal/harness/...`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add internal/harness/cluster.go
git commit -m "feat(harness): branch coordinator/worker args on StorageType

ClusterConfig gains StorageType/Bucket/Region/Endpoint/SSL fields. When
StorageType=='s3', coordinator and workers are spawned with --storage-type=s3
and the S3 connection args. FileStore path retained as default.

Phase 3 harness spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task A3: Create `primeS3Catalog` helper

**Files:**
- Create: `internal/harness/s3_catalog.go`

- [ ] **Step 1: Write the helper**

Create `internal/harness/s3_catalog.go`:

```go
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/derekmwright/wadjet"
	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// primeS3Catalog lists parquet files under bucket/dataPrefix/<table>/ for each
// of the 8 TPC-H tables and registers them in the coordinator's catalog via
// CreateTable + RegisterFiles. Mirrors cmd/tpch-bench/main.go's discoverData.
//
// This path assumes data is already staged in S3 in the layout produced by
// cmd/tpch-seed (or equivalent). It does not write sample parquet files.
func primeS3Catalog(
	ctx context.Context,
	db *wadjet.DB,
	endpoint, region, bucket string,
	ssl bool,
	dataPrefix string,
	logger *slog.Logger,
) error {
	if !strings.HasSuffix(dataPrefix, "/") && dataPrefix != "" {
		dataPrefix = dataPrefix + "/"
	}

	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		return fmt.Errorf("s3 store: %w", err)
	}

	for name, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create table %s: %w", name, err)
		}
		prefix := dataPrefix + name + "/"
		keys, err := store.List(ctx, bucket, prefix)
		if err != nil {
			return fmt.Errorf("list %s: %w", bucket+"/"+prefix, err)
		}
		var files []string
		for _, k := range keys {
			if strings.HasSuffix(k, ".parquet") {
				files = append(files, k)
			}
		}
		if len(files) == 0 {
			return fmt.Errorf("no parquet files found under s3://%s/%s", bucket, prefix)
		}
		if err := db.RegisterFiles(ctx, name, files); err != nil {
			return fmt.Errorf("register files for %s: %w", name, err)
		}
		logger.Info("primed table", "table", name, "files", len(files))
	}
	return nil
}
```

Before committing, verify `objstore.MinIOStore.List`, `wadjet.DB.CreateTable`, and `wadjet.DB.RegisterFiles` exist and match these signatures. If any differs, update the call. Grep first:

```bash
grep -n "func (s \*MinIOStore) List\|func (d \*DB) CreateTable\|func (d \*DB) RegisterFiles" internal/storage/objstore/ wadjet/
```

If `List` has a different name (e.g. `ListObjects`, `Keys`) or takes different args (e.g. a `prefix string` only without a separate `bucket`), match the real signature.

- [ ] **Step 2: Build**

Run: `go build ./internal/harness/...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/harness/s3_catalog.go
git commit -m "feat(harness): add primeS3Catalog helper

Lists parquet files under s3://<bucket>/<dataPrefix>/<table>/ for each
TPC-H table and registers them via DB.CreateTable + DB.RegisterFiles.
Mirrors cmd/tpch-bench's discoverData pattern. Used by S3-mode harness
in place of loadSampleData.

Phase 3 harness spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task A4: Branch `harness.Run` on Source

**Files:**
- Modify: `internal/harness/harness.go`

- [ ] **Step 1: Read current Run() to locate branch point**

Run: `grep -n "loadSampleData\|ModeLocal\|ModeGolden" internal/harness/harness.go` — the `ModeLocal` case (around line 59-90) is where the branch goes.

- [ ] **Step 2: Branch in ModeLocal based on Source**

Inside `case ModeLocal:` in `Run()`, find the block that calls `loadSampleData`. Replace:

```go
		if err := loadSampleData(ctx, cluster, cfg.DataDir, sliceCfg, logger); err != nil {
			return result, fmt.Errorf("loading sample data: %w", err)
		}
```

with:

```go
		switch cfg.Source {
		case "s3":
			// data lives in S3; skip local seed. Prime catalog from bucket
			// listing once the coordinator is up.
		default: // "local" or empty
			if err := loadSampleData(ctx, cluster, cfg.DataDir, sliceCfg, logger); err != nil {
				return result, fmt.Errorf("loading sample data: %w", err)
			}
		}
```

- [ ] **Step 3: Configure ClusterConfig for S3 when requested**

Find the `cluster = NewCluster(ClusterConfig{...})` block. Replace with:

```go
		clusterCfg := ClusterConfig{
			WadjetBin:  cfg.WadjetBin,
			RunDir:     runDir,
			NumWorkers: numWorkers,
			GoMemLimit: sliceCfg.GoMemLimit,
			PgAddr:     cfg.PgAddr,
			DataDir:    cfg.DataDir,
			Logger:     logger,
		}
		if cfg.Source == "s3" {
			clusterCfg.StorageType = "s3"
			clusterCfg.Bucket = cfg.Bucket
			clusterCfg.Region = cfg.Region
			clusterCfg.Endpoint = cfg.Endpoint
			clusterCfg.SSL = cfg.SSL
			clusterCfg.DataDir = "" // ignore — S3 is the source
		}
		cluster = NewCluster(clusterCfg)
```

- [ ] **Step 4: Prime catalog after coordinator + workers are up (S3 path)**

After `cluster.StartWorkers(ctx)` and before the line `coordURL = fmt.Sprintf(...)`, add:

```go
		if cfg.Source == "s3" {
			pgURL := fmt.Sprintf("postgres://wadjet@localhost%s/wadjet?sslmode=disable", cluster.PgAddr())
			db, err := openDBFromPgURL(ctx, pgURL) // see Step 5
			if err != nil {
				return result, fmt.Errorf("opening DB for S3 prime: %w", err)
			}
			defer db.Close()
			if err := primeS3Catalog(ctx, db, cfg.Endpoint, cfg.Region, cfg.Bucket, cfg.SSL, cfg.DataPrefix, logger); err != nil {
				return result, fmt.Errorf("priming S3 catalog: %w", err)
			}
		}
```

- [ ] **Step 5: Implement or reuse openDBFromPgURL**

Grep for an existing helper that opens a `*wadjet.DB` over pgwire. If it doesn't exist, add this to `internal/harness/s3_catalog.go`:

```go
// openDBFromPgURL opens a Wadjet DB handle through the coordinator's pgwire
// endpoint. Used by primeS3Catalog so we go through the same CREATE TABLE /
// REGISTER FILES path as any other client.
func openDBFromPgURL(ctx context.Context, pgURL string) (*wadjet.DB, error) {
	// If wadjet.DB doesn't support pgwire opening directly, the simpler
	// path is to talk pgwire via pgx and issue CREATE TABLE / INSERT INTO
	// or the Wadjet-specific extension statements. Verify by grepping for
	// wadjet.Open callers that use pgwire.
	return nil, fmt.Errorf("TODO: inspect wadjet.Open signature to decide on pgx.Connect + SQL path vs in-process DB handle")
}
```

**If that TODO blocks you, BLOCK and report.** Grep for existing patterns: `grep -rn "wadjet.Open\|postgres://wadjet" --include='*.go' | head -20` — if the codebase has a "connect by pg URL" pattern, use it; otherwise switch to using `pgx.Connect(pgURL)` and executing `CREATE TABLE ...` + `REGISTER FILES ...` SQL directly (Wadjet pgwire accepts both). The implementer is expected to figure out the cleanest option; if neither works, use a direct in-process `catalog.New(...)` + `CreateTable` path instead (matching how the FileStore `loadSampleData` does it).

- [ ] **Step 6: Build + smoke test**

```bash
go build ./...
./tpch-harness --mode=local --source=s3 --bucket=wadjet-bench-sf10-use2 \
               --region=us-east-2 --endpoint=s3.us-east-2.amazonaws.com --ssl \
               --data-prefix=tables/ --pg-addr=:15499 \
               --wadjet-bin=./wadjet_bin --no-compare --queries=q01 2>&1 | tail -20
```

Expected: coordinator starts, primes 8 tables from S3, runs Q01, returns 6 rows. Requires `AWS_PROFILE=citc` or equivalent credentials in env. If the profile isn't available, the command fails with a clear S3 auth error.

- [ ] **Step 7: Commit**

```bash
git add internal/harness/harness.go internal/harness/s3_catalog.go
git commit -m "feat(harness): route S3 source through primeS3Catalog

When --source=s3, skip loadSampleData and register tables by listing
parquet files under s3://<bucket>/<dataPrefix>/. Coordinator and workers
are spawned with --storage-type=s3 so they read from S3 directly.

Enables local reproduction of distributed-execution regressions without
EC2 cost — the harness drives a real 2-worker cluster against a real
S3 bucket.

Phase 3 harness spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task A5: SF0.01 sanity check (no bucket needed yet)

**Files:** none

- [ ] **Step 1: Verify FileStore path still works (regression check for Task A4)**

```bash
./tpch-harness --mode=local --slice=small --wadjet-bin=./wadjet_bin \
               --pg-addr=:15499 --no-compare 2>&1 | tail -30
```

Expected: same output as before Task A4 (22 queries + 3 micros, Q03 fails with `invalid length for int8: 4`, row counts unchanged). Regression failure here means the FileStore branch got broken by the switch added in Task A2.

- [ ] **Step 2: If no AWS credentials available, skip the S3 smoke test**

```bash
if ! aws --profile citc sts get-caller-identity > /dev/null 2>&1; then
    echo "skipping S3 smoke (no citc credentials)"
    exit 0
fi
```

If credentials present, Task A4 Step 6's command should have already proven the S3 path. Nothing to do here.

---

## Task A6: Integration test — reproduce the 2026-04-21 Phase 2b regression (manual)

This task is a one-time validation. It confirms the harness CAN catch the Phase 2b regression class; once done, the harness becomes part of the Phase 3 (PR B) gate.

- [ ] **Step 1: Run the harness against the SF10 bucket on the current feat branch**

```bash
./tpch-harness --mode=local --source=s3 --bucket=wadjet-bench-sf10-use2 \
               --region=us-east-2 --endpoint=s3.us-east-2.amazonaws.com --ssl \
               --data-prefix=tables/ --pg-addr=:15499 \
               --wadjet-bin=./wadjet_bin --no-compare --queries=q18,q20,q21 2>&1 | tail -40
```

Expected (Phase 2b branch state, broken): Q18 hangs or returns 0 rows; Q20 returns significantly fewer than 6194 rows; Q21 returns 0 or 1 row. This matches the SF10 EC2 run we captured in `docs/_archive/research/sf10-phase2b-2026-04-21/feat-b2d205e.txt`.

- [ ] **Step 2: Document the reproduction in a short session note**

Append to `memory/feedback_tpch_harness_local_gate.md` (using the Edit tool, not Bash):

Find the "KNOWN LIMITATION" section and add at the end of it:

```markdown
**2026-04-22 update:** `--source=s3` mode lifts the small-slice limitation. The harness now reproduces the Phase 2b SF10 regression locally in ~2 minutes against the wadjet-bench-sf10-use2 bucket. PR A (harness S3 mode) is the pre-deploy gate for PR B (native-DAG runtime) and any future distribution-layer change.
```

- [ ] **Step 3: Commit the memory update**

```bash
# memory is under ~/.claude — use Edit tool output, no git commit here.
```

---

## Self-review

**Spec coverage:** Testing section of the spec called for either S3 mode or SF1-sample generator. S3 mode selected; implemented in A1-A4. ✓

**Placeholder scan:** Task A4 Step 5 has a `TODO` inside the proposed code snippet — but it's explicitly gated with "BLOCK and report" guidance and the implementer is told to inspect the real codebase shape. That's defensible, not a placeholder masquerading as implementation. Acceptable.

**Type consistency:** `ClusterConfig.StorageType`, `Config.Source` are consistent through A1-A4. `primeS3Catalog` signature matches its one caller. ✓

**Remaining uncertainty:** `openDBFromPgURL` (Task A4 Step 5) is the one real unknown — it depends on Wadjet's existing DB-open APIs which I haven't fully traced. Flagged for BLOCK escalation. The implementer can resolve in context.

---

## Execution handoff

Two execution options when you're ready:

1. **Subagent-driven** (recommended) — fresh subagent per task, review checkpoints
2. **Inline** — execute in this session with batch checkpoints

PR A is 6 tasks, mostly mechanical. Inline execution might be fastest; subagent-driven gives cleaner review per task. Your call.
