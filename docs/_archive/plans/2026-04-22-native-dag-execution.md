> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# Native-DAG Distributed Execution — PR B Implementation Plan


**Goal:** Replace the legacy four-mode switch and the broken Phase 2b `lowerExchangeDAG` with a native stage-DAG executor that walks Exchange-annotated plans topologically, materializes output to S3 between stages, and terminates in a Gather operator that streams to the coordinator via NATS reply.

**Architecture:** W3 (coordinator topologically dispatches stages, workers run intra-stage operator DAGs). Matches Trino batch-mode fragments and Spark SQL `ShuffleExchangeExec`-separated stages. Existing push-based pipeline executor stays untouched; only source selection and sink selection learn shuffle I/O and gather streaming.

**Tech Stack:** Go, NATS JetStream + request/reply, MinIO S3 client, existing `partitionedShuffleSink` / `partitionShardSource` on workers.

**Spec:** `docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md` (commit `ba9c9ac`).

**Base branch:** `feat/distribution-property-phase-2` AFTER PR A (harness S3 mode) merges. First task of this plan reverts the Phase 2b runtime work.

**Pre-req:** PR A merged (local harness can run SF10-scale against the real bucket).

---

## File Structure

New files (coordinator):
- `internal/coordinator/stage_output.go` — `StageOutput` type + `collectInputs` helper
- `internal/coordinator/execute_stage_dag.go` — `executeStageDAG` topo-walk loop + `UseNativeDAG` flag wiring
- `internal/coordinator/dispatch_shuffle.go` — `dispatchShuffleStage` (refactor of today's `runShuffleSide`)
- `internal/coordinator/dispatch_replicate.go` — `dispatchReplicateStage` (wraps `preScanBuildTables`)
- `internal/coordinator/dispatch_pipeline.go` — `dispatchPipelineStage` (general case)
- `internal/coordinator/dispatch_gather.go` — `dispatchGatherStage` + NATS reply subscription
- `internal/coordinator/gather_receiver.go` — batch stream reassembly from gather reply subject

New files (worker):
- `internal/worker/gather_reply_sink.go` — Sink that publishes batches to a NATS reply subject instead of writing to S3

Modified (planner):
- `internal/planner/physical/ensure_distribution.go` — always emit terminal Gather; populate Gather Ordering/Limit from logical plan

Modified (schema):
- `internal/distributed/messages.go` — `Task` gains `Inputs`, `Output`, `ReplySubject`, `GatherOrdering`, `GatherLimit` fields
- `internal/coordinator/coordinator.go` — add `UseNativeDAG bool` on Coordinator struct; branch `executeDistributed` on it

Deleted (after transition, last tasks):
- `lowerExchangeDAG` and `dag_lower.go`
- The empty-BuildAlias skip guard
- `physical.Stage.PreScannedInputs`, `BuildCachePreScans`, `ProbeSplitAlias`, `ProbeSplitFiles` fields (generalized into `Task.Inputs`)
- Coordinator-side `mergeProbePartials` + `ExtractMergeInfo` call (replaced by `gather_receiver`)

---

## Task 1: Hard revert Phase 2b runtime commits

**Files:** none to write; use git.

- [ ] **Step 1: Identify the commit range to revert**

Run:

```bash
git log --oneline ba9c9ac^..ba9c9ac | head -5     # design commit (keep)
git log --oneline main..feat/distribution-property-phase-2 | head -40
```

Expected: see Phase 2a commits `707a0f2..d1e8175` (keep), followed by Phase 2b commits starting at `b0fd4cc` or similar. Verify by checking commit titles for "orchestrateRepartition shim" (first Phase 2b commit).

- [ ] **Step 2: Identify the cut-line**

The design spec names `dd3d0f5..78fd60e` as Phase 2b. Verify: `git log --oneline dd3d0f5^..78fd60e` should list exactly the Phase 2b commits (orchestrate shims, dispatchStageDAG skeleton, lowerExchangeDAG, executeStageDAG wiring, cleanup, PARITY harness, SF10 artifacts).

If the range is slightly different, use the actual range. Specifically:
- Keep `d1e8175` and everything before (Phase 2a + Phase 1).
- Keep `ba9c9ac` (this Phase 3 spec).
- Revert every Phase 2b commit in between.
- Keep `78fd60e` (the `--pg-addr` harness fix) — it's legitimate.

- [ ] **Step 3: Perform the revert**

Prefer `git revert` over `git reset` — keeps history legible. One revert commit for the whole range:

```bash
git revert --no-commit dd3d0f5..78fd60e
# This touches many files. Resolve any conflicts inline.
# Preserve the --pg-addr change from 78fd60e by either skipping that commit
# (git revert -n dd3d0f5..78fd60e^ && git revert -n 78fd60e --no-edit would
# re-apply it) or by restoring it after the revert.
# Simpler: revert everything, then cherry-pick the --pg-addr change back.

git revert --skip
# If conflicts arise, abort and use the cherry-pick approach:
git revert --abort
git reset --hard ba9c9ac
git revert dd3d0f5..78fd60e^ --no-commit
git commit -m "revert: phase 2b runtime integration (lowerExchangeDAG was architecturally wrong)

Reverts commits dd3d0f5..78fd60e^ which attempted to route Exchange-annotated
plans through a single-pipeline-0 lowering pass. SF10 EC2 validation on
2026-04-21 showed catastrophic regression: Q18 timeout, Q15/Q18/Q20/Q21/Q22
wrong row counts, 3.24× total slowdown. Root cause: collapse-to-pipeline-0
discards multi-step shuffle information.

Replaced by native stage-DAG execution — see
docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md.

Phase 2a (EnsureDistribution planner pass, 707a0f2..d1e8175) stands — its
Exchange-annotated plans are the input to the new executeStageDAG."
git cherry-pick 78fd60e  # --pg-addr flag: legitimate, keep
```

- [ ] **Step 4: Verify the revert**

```bash
git log --oneline | head -10
# Should show: cherry-pick of --pg-addr, revert commit, ba9c9ac (spec), Phase 2a HEAD.
go build ./...
go test ./internal/planner/... ./internal/coordinator/...
go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -5
```

Expected: build clean, all tests pass, SF0.01 22/22. If SF0.01 is not 22/22, the revert was incomplete — bisect and fix.

---

## Task 2: Extend `distributed.Task` schema

**Files:**
- Modify: `internal/distributed/messages.go`

- [ ] **Step 1: Add new fields**

In `Task` struct (around line 19-115), append after `PreComputedAggregates`:

```go
	// Inputs maps scan/alias name → S3 keys for upstream stage output.
	// Generalizes PreScannedInputs: used for both table-scan inputs (legacy)
	// and previous-stage-output inputs (Phase 3 native DAG). Worker source
	// selection inspects file patterns: partition=NNNN/*.wshf → partitionShardSource;
	// *.parquet → streamSource.
	Inputs map[string][]string `json:"inputs,omitempty"`

	// Output is the S3 prefix where this task's output is materialized.
	// For shuffle: worker writes "<Output>partition=NNNN/<taskID>.wshf".
	// For pipeline (intermediate): same format using ShuffleKeys/NumPartitions.
	// For pipeline (final before Gather): single-partition output at "<Output><taskID>.wshf".
	// For gather: empty; worker streams to ReplySubject.
	Output string `json:"output,omitempty"`

	// ReplySubject is the NATS subject the worker publishes batch chunks to.
	// Only set for TaskTypeGather; enables real-operator Gather semantics.
	ReplySubject string `json:"reply_subject,omitempty"`

	// GatherOrdering (Gather only) — merge-sort keys applied by the coordinator
	// when reassembling output from multiple gather workers. Empty means no
	// ordering; coordinator concatenates streams in arrival order.
	GatherOrdering []SortKeySpec `json:"gather_ordering,omitempty"`

	// GatherLimit (Gather only) — top-N limit applied by the coordinator
	// after ordering. Zero means no limit.
	GatherLimit int `json:"gather_limit,omitempty"`
```

- [ ] **Step 2: Build + ensure backward-compat**

```bash
go build ./...
go test ./internal/distributed/...
go test ./internal/worker/...     # workers still only read old fields — should pass
go test ./internal/coordinator/...
```

Expected: all green. The new fields are unused at this point; nothing reads them.

- [ ] **Step 3: Commit**

```bash
git add internal/distributed/messages.go
git commit -m "feat(distributed): add Task.Inputs/Output/ReplySubject/GatherOrdering/GatherLimit

Five new fields on the Task message for native-DAG execution:
- Inputs generalizes PreScannedInputs for multi-stage plans
- Output is the S3 prefix each stage task materializes to
- ReplySubject + GatherOrdering + GatherLimit support streaming Gather

Unused in this commit; consumed by the worker's source/sink selection
(Task 3 and Task 5) and the new coordinator dispatchers (Tasks 6-10).

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 3: Worker — `gatherReplySink`

**Files:**
- Create: `internal/worker/gather_reply_sink.go`
- Create: `internal/worker/gather_reply_sink_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/worker/gather_reply_sink_test.go`:

```go
package worker

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestGatherReplySink publishes a single RecordBatch to a test NATS subject
// and asserts the subscriber receives a GatherBatchMsg with the same rows.
func TestGatherReplySink(t *testing.T) {
	en, err := distributed.NewEmbeddedNATS(distributed.DefaultNATSConfig(), nil)
	if err != nil {
		t.Fatalf("NATS: %v", err)
	}
	defer en.Shutdown()
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	const subject = "test.gather.reply"
	received := make(chan distributed.GatherBatchMsg, 4)
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var m distributed.GatherBatchMsg
		if err := distributed.Unmarshal(msg.Data, &m); err != nil {
			return
		}
		received <- m
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	schema := batch.SimpleSchema("name:string,age:int32")
	b := batch.NewBatch(schema, 2)
	b.AppendValues("alice", int32(30))
	b.AppendValues("bob", int32(25))

	sink := newGatherReplySink(nc, subject)
	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var got []distributed.GatherBatchMsg
	timeout := time.After(2 * time.Second)
	for {
		select {
		case m := <-received:
			got = append(got, m)
			if m.Terminal {
				goto done
			}
		case <-timeout:
			t.Fatalf("timeout; got %d messages", len(got))
		}
	}
done:
	if len(got) != 2 {
		t.Fatalf("expected 2 messages (1 batch + 1 terminal), got %d", len(got))
	}
	if got[0].Terminal {
		t.Errorf("first message should not be terminal")
	}
	if !got[1].Terminal {
		t.Errorf("last message should be terminal")
	}
	if got[0].RowCount != 2 {
		t.Errorf("RowCount: got %d, want 2", got[0].RowCount)
	}
}
```

Note: `distributed.GatherBatchMsg` doesn't exist yet — add it in Step 2. `batch.SimpleSchema` and `batch.NewBatch` may need minor renaming; grep for the real constructors in `internal/engine/batch/*.go` and adapt.

- [ ] **Step 2: Add the GatherBatchMsg type**

In `internal/distributed/messages.go`, append:

```go
// GatherBatchMsg is the NATS message body the worker publishes to the
// coordinator's gather reply subject. One message per output RecordBatch,
// terminated by one message with Terminal=true (zero RowCount, any Err set).
type GatherBatchMsg struct {
	Terminal  bool   `json:"terminal"`
	RowCount  int32  `json:"row_count"`
	Payload   []byte `json:"payload,omitempty"` // MessagePack-encoded rows
	Schema    []byte `json:"schema,omitempty"`  // sent with first non-terminal msg
	Err       string `json:"err,omitempty"`     // non-empty on terminal failure
}
```

- [ ] **Step 3: Write gatherReplySink**

Create `internal/worker/gather_reply_sink.go`:

```go
package worker

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/nats-io/nats.go"
)

// gatherReplySink is a Sink that streams batches to a NATS reply subject.
// Used by Gather stages — the coordinator subscribes to the subject and
// reassembles the query result. No S3 I/O.
type gatherReplySink struct {
	nc         *nats.Conn
	subject    string
	schemaSent bool
	err        error
}

func newGatherReplySink(nc *nats.Conn, subject string) *gatherReplySink {
	return &gatherReplySink{nc: nc, subject: subject}
}

func (s *gatherReplySink) Init(ctx context.Context) error { return nil }

func (s *gatherReplySink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	msg := distributed.GatherBatchMsg{
		Terminal: false,
		RowCount: int32(b.NumRows()),
	}
	// First non-terminal message carries schema.
	if !s.schemaSent {
		schema, err := batch.EncodeSchema(b.Schema())
		if err != nil {
			return fmt.Errorf("encode schema: %w", err)
		}
		msg.Schema = schema
		s.schemaSent = true
	}
	payload, err := batch.EncodeBatch(b)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}
	msg.Payload = payload
	data, err := distributed.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return s.nc.Publish(s.subject, data)
}

func (s *gatherReplySink) Finalize(ctx context.Context) error {
	msg := distributed.GatherBatchMsg{Terminal: true}
	if s.err != nil {
		msg.Err = s.err.Error()
	}
	data, err := distributed.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal terminal: %w", err)
	}
	return s.nc.Publish(s.subject, data)
}

func (s *gatherReplySink) Close() error { return nil }
```

`batch.EncodeBatch` and `batch.EncodeSchema` may be named differently — grep `internal/engine/batch/` for the existing serialization helpers (there's one used by the shuffle writer; reuse it). If none exist for arbitrary batches, adapt the shuffle-writer code.

**If encoding a RecordBatch over the wire turns out to require introducing a brand-new serialization path**, STOP and report BLOCKED. The worker already has `shuffleWriter` for per-partition binary encoding; we may want to reuse that format (messagepack-wrapped raw-vector bytes) rather than invent a third format.

- [ ] **Step 4: Run the test**

```bash
go test ./internal/worker/ -run TestGatherReplySink -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/gather_reply_sink.go internal/worker/gather_reply_sink_test.go internal/distributed/messages.go
git commit -m "feat(worker): gatherReplySink + GatherBatchMsg wire format

New Sink that streams output batches to a NATS reply subject. Used by
Gather stage tasks instead of writing to S3 — coordinator subscribes
directly to the reply stream. One GatherBatchMsg per output batch,
terminated by a message with Terminal=true.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 4: Worker — source selection from Task.Inputs

**Files:**
- Modify: `internal/worker/executor.go` (Source construction for pipeline tasks)

Today's worker constructs sources via a combination of:
- `ScanFiles` / `Files` → `streamSource` (parquet)
- `PreScannedInputs` → per-alias streamSource
- Shuffle-result files (implicit from prior stage) → `partitionShardSource`

This task generalizes: pipeline tasks read from `Task.Inputs` with pattern detection.

- [ ] **Step 1: Locate the current source-construction site**

Grep: `grep -n "newStreamSource\|newPartitionShardSource\|PreScannedInputs" internal/worker/executor.go`

Find where the executor decides which Source to make. Typically inside `executePipeline` or similar.

- [ ] **Step 2: Add `selectSourceForAlias` helper**

Append to `internal/worker/executor.go`:

```go
// selectSourceForAlias inspects the file list for an input alias and picks
// the right Source implementation. Handles three patterns:
//
//   1. files contain "partition=NNNN/*.wshf" → partitionShardSource
//      (output of a previous Repartition stage)
//   2. files contain "*.wshf" without partition segments → streamSource
//      (output of a previous Replicate stage; broadcast .wshc)
//   3. files contain "*.parquet" → streamSource (table scan)
//
// Mixed patterns in a single alias are treated as a planner bug and return
// an error so we fail loudly rather than silently read the wrong source.
func (e *Executor) selectSourceForAlias(alias string, files []string, schema []parquet.Column) (Source, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("alias %q: empty file list", alias)
	}
	isShuffled := strings.Contains(files[0], "partition=")
	for _, f := range files[1:] {
		if strings.Contains(f, "partition=") != isShuffled {
			return nil, fmt.Errorf("alias %q: mixed shuffled and non-shuffled files", alias)
		}
	}
	if isShuffled {
		return newPartitionShardSource(e.store, e.bucket, files, schema), nil
	}
	return newStreamSource(e.store, e.bucket, files, schema, e.spillDir), nil
}
```

Adapt the constructor signatures to whatever `newStreamSource` and `newPartitionShardSource` actually take in the current codebase — grep to verify.

- [ ] **Step 3: Route the pipeline-task source construction through it**

Inside the pipeline executor (where scan sources are assembled today), replace the direct `newStreamSource(...)` call with `e.selectSourceForAlias(...)`.

The exact shape depends on the current code. The important invariant: when `Task.Inputs[alias]` is set for any alias, use the new helper for that alias; fall back to the existing `ScanFiles` / `PreScannedInputs` paths only if `Inputs` is empty.

- [ ] **Step 4: Add a test for alias source selection**

Create `internal/worker/source_select_test.go`:

```go
package worker

import (
	"testing"
)

func TestSelectSourceForAlias(t *testing.T) {
	e := &Executor{} // fields not used for pattern detection
	cases := []struct {
		name    string
		files   []string
		wantType string
	}{
		{"shuffle", []string{"queries/q/s1/partition=0000/t.wshf", "queries/q/s1/partition=0001/t.wshf"}, "*worker.partitionShardSource"},
		{"parquet", []string{"tables/orders/part-0.parquet", "tables/orders/part-1.parquet"}, "*worker.streamSource"},
		{"wshc",   []string{"queries/q/cache/build-orders.wshf"}, "*worker.streamSource"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src, err := e.selectSourceForAlias(c.name, c.files, nil)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			got := fmt.Sprintf("%T", src)
			if got != c.wantType {
				t.Errorf("got %s, want %s", got, c.wantType)
			}
		})
	}

	t.Run("mixed_errors", func(t *testing.T) {
		mixed := []string{"partition=0000/a.wshf", "tables/b.parquet"}
		if _, err := e.selectSourceForAlias("x", mixed, nil); err == nil {
			t.Error("expected error for mixed patterns, got nil")
		}
	})
}
```

If the Executor struct's constructor requires args we can't easily fake, change the test strategy: make `selectSourceForAlias` a package-level func taking the needed dependencies as params, or split pattern-detection into a pure function `classifyInputFiles(files []string) (sourceKind, error)` and unit-test that instead.

- [ ] **Step 5: Run tests**

```bash
go test ./internal/worker/ -run TestSelectSourceForAlias -v
go test ./internal/worker/...
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/worker/executor.go internal/worker/source_select_test.go
git commit -m "feat(worker): generalize source selection via Task.Inputs patterns

selectSourceForAlias inspects file patterns and picks partitionShardSource
(for 'partition=NNNN/*.wshf' shuffle output) or streamSource (for parquet
or non-partitioned .wshf). Used by pipeline tasks consuming upstream stage
output in the native-DAG executor.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 5: Coordinator — `StageOutput` type + `collectInputs` helper

**Files:**
- Create: `internal/coordinator/stage_output.go`
- Create: `internal/coordinator/stage_output_test.go`

- [ ] **Step 1: Write StageOutput + collectInputs**

Create `internal/coordinator/stage_output.go`:

```go
package coordinator

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// OutputKind describes how a stage's output is distributed across S3 keys.
type OutputKind int

const (
	OutputPartitioned OutputKind = iota // Files[p] = keys for partition p
	OutputReplicated                    // Files[0] = single broadcast file
	OutputSinglePart                    // Files[0] = concatenation (e.g. pipeline stage feeding a Gather)
)

// StageOutput is the materialized output of one stage in the native-DAG
// executor. Produced by each dispatchXxxStage helper; consumed by
// downstream stages' collectInputs.
type StageOutput struct {
	Kind          OutputKind
	NumPartitions int         // Partitioned only
	Files         [][]string  // Partitioned: Files[p]; others: Files[0]
}

// collectInputs translates a stage's Dependencies into a Task.Inputs map
// by looking up each dependency's output in the stageOutputs map. The
// returned map is keyed by the producing stage's alias (for scan producers)
// or the stage ID (for non-scan producers); the worker's selectSourceForAlias
// inspects file patterns to decide which Source to instantiate.
//
// For partitioned inputs, the caller must also supply a partition index
// (the consuming worker's assigned partition) and use partitionFiles() to
// slice Files[p]; collectInputs returns the full layout.
func collectInputs(stage physical.Stage, outputs map[string]StageOutput) (map[string]StageOutput, error) {
	inputs := make(map[string]StageOutput, len(stage.Dependencies))
	for _, depID := range stage.Dependencies {
		out, ok := outputs[depID]
		if !ok {
			return nil, fmt.Errorf("stage %s: dependency %s has no recorded output", stage.ID, depID)
		}
		inputs[depID] = out
	}
	return inputs, nil
}

// partitionFilesForWorker returns the subset of partitioned output files
// assigned to worker w of workerCount. Contiguous-slice assignment mirrors
// buildShufflePipelineTasks's assignPartitionsToWorker.
func partitionFilesForWorker(out StageOutput, w, workerCount int) []string {
	if out.Kind != OutputPartitioned {
		return out.Files[0]
	}
	if out.NumPartitions == 0 || workerCount == 0 {
		return nil
	}
	partsPerWorker := out.NumPartitions / workerCount
	if partsPerWorker == 0 {
		partsPerWorker = 1
	}
	start := w * partsPerWorker
	end := start + partsPerWorker
	if w == workerCount-1 {
		end = out.NumPartitions // last worker absorbs remainder
	}
	if start >= out.NumPartitions {
		return nil
	}
	var files []string
	for p := start; p < end && p < out.NumPartitions; p++ {
		files = append(files, out.Files[p]...)
	}
	return files
}
```

- [ ] **Step 2: Write unit tests**

Create `internal/coordinator/stage_output_test.go`:

```go
package coordinator

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

func TestCollectInputs(t *testing.T) {
	outputs := map[string]StageOutput{
		"scan-0":   {Kind: OutputSinglePart, Files: [][]string{{"tables/o/p.parquet"}}},
		"repart-0": {Kind: OutputPartitioned, NumPartitions: 4, Files: make([][]string, 4)},
	}
	stage := physical.Stage{
		ID:           "join-0",
		Dependencies: []string{"scan-0", "repart-0"},
	}
	inputs, err := collectInputs(stage, outputs)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !reflect.DeepEqual(inputs["scan-0"], outputs["scan-0"]) {
		t.Errorf("scan-0 mismatch")
	}
	if !reflect.DeepEqual(inputs["repart-0"], outputs["repart-0"]) {
		t.Errorf("repart-0 mismatch")
	}

	// Missing dep should error.
	missing := physical.Stage{ID: "j", Dependencies: []string{"unknown"}}
	if _, err := collectInputs(missing, outputs); err == nil {
		t.Error("expected error for unknown dep")
	}
}

func TestPartitionFilesForWorker(t *testing.T) {
	out := StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: 8,
		Files: [][]string{
			{"p0-a.wshf"}, {"p1-a.wshf"}, {"p2-a.wshf"}, {"p3-a.wshf"},
			{"p4-a.wshf"}, {"p5-a.wshf"}, {"p6-a.wshf"}, {"p7-a.wshf"},
		},
	}
	got0 := partitionFilesForWorker(out, 0, 2)
	want0 := []string{"p0-a.wshf", "p1-a.wshf", "p2-a.wshf", "p3-a.wshf"}
	if !reflect.DeepEqual(got0, want0) {
		t.Errorf("worker 0: got %v, want %v", got0, want0)
	}
	got1 := partitionFilesForWorker(out, 1, 2)
	want1 := []string{"p4-a.wshf", "p5-a.wshf", "p6-a.wshf", "p7-a.wshf"}
	if !reflect.DeepEqual(got1, want1) {
		t.Errorf("worker 1: got %v, want %v", got1, want1)
	}
}
```

- [ ] **Step 3: Run + commit**

```bash
go test ./internal/coordinator/ -run "TestCollectInputs|TestPartitionFilesForWorker" -v
git add internal/coordinator/stage_output.go internal/coordinator/stage_output_test.go
git commit -m "feat(coordinator): StageOutput type + collectInputs helper

Core data structures for the native-DAG executor. Each dispatched stage
produces a StageOutput; downstream stages consume via collectInputs and
partitionFilesForWorker. No dispatch logic yet — wired in Tasks 6-10.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 6: `dispatchShuffleStage` (refactor `runShuffleSide`)

**Files:**
- Create: `internal/coordinator/dispatch_shuffle.go`
- Modify: `internal/coordinator/orchestrate_repartition.go` (delegate `runShuffleSide` to the new func)

- [ ] **Step 1: Extract `dispatchShuffleStage` from the existing `runShuffleSide`**

Read `internal/coordinator/orchestrate_repartition.go` lines 130-300 (the existing `runShuffleSide`). The goal: produce a new coordinator method that takes a stage's `Inputs` (from `collectInputs`) and a stage spec, not a raw `physical.Stage` scan.

Create `internal/coordinator/dispatch_shuffle.go`:

```go
package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// dispatchShuffleStage executes a StageExchangeRepartition by dispatching
// N shuffle tasks to workers. Each task reads a slice of the input files
// and hash-partitions into stage.Exchange.Count output partitions, writing
// to s3://<bucket>/queries/<queryID>/<stageID>/partition=NNNN/<taskID>.wshf.
//
// Returns StageOutput{Partitioned} with Files[p] populated for p in [0, Count).
func (c *Coordinator) dispatchShuffleStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	if stage.Type != physical.StageExchangeRepartition {
		return StageOutput{}, fmt.Errorf("dispatchShuffleStage: stage %s is not StageExchangeRepartition", stage.ID)
	}
	if stage.Exchange == nil {
		return StageOutput{}, fmt.Errorf("dispatchShuffleStage: stage %s has nil Exchange", stage.ID)
	}
	numParts := stage.Exchange.Count
	if numParts <= 0 {
		return StageOutput{}, fmt.Errorf("dispatchShuffleStage: stage %s has NumPartitions=%d", stage.ID, numParts)
	}
	keys := stage.Exchange.Keys
	if len(keys) == 0 {
		return StageOutput{}, fmt.Errorf("dispatchShuffleStage: stage %s has empty ShuffleKeys", stage.ID)
	}

	// Single-dep is the common case; chained shuffles per-edge happen one
	// stage at a time so each repartition stage has exactly one input.
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("dispatchShuffleStage: stage %s has %d deps, want 1",
			stage.ID, len(stage.Dependencies))
	}
	depOut, ok := inputs[stage.Dependencies[0]]
	if !ok {
		return StageOutput{}, fmt.Errorf("dispatchShuffleStage: stage %s has no input for dep %s",
			stage.ID, stage.Dependencies[0])
	}

	// Flatten input files. For a partitioned upstream, concatenate across
	// partitions — each worker will hash-partition freshly.
	inputFiles := flattenStageFiles(depOut)
	if len(inputFiles) == 0 {
		// Nothing to shuffle. Return empty partition layout.
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         make([][]string, numParts),
		}, nil
	}

	resultPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	shuffleQueryID := fmt.Sprintf("sh-%s-%s", stage.ID, queryID)

	// Split source files evenly across workers.
	fileSets := splitFilesEvenly(inputFiles, workerCount)
	actualTasks := len(fileSets)
	if actualTasks == 0 {
		return StageOutput{
			Kind:          OutputPartitioned,
			NumPartitions: numParts,
			Files:         make([][]string, numParts),
		}, nil
	}

	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypeShuffle, TotalTasks: actualTasks},
	}
	c.tracker.Register(shuffleQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(shuffleQueryID)
	defer c.tracker.Delete(shuffleQueryID)

	// Subscribe for completion before dispatching.
	subject := distributed.QueryResultSubject(shuffleQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	var (
		mu        sync.Mutex
		collected = make([]taskResult, 0, actualTasks)
		done      = make(chan struct{}, 1)
	)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if e := distributed.Unmarshal(msg.Data, &r); e != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= actualTasks {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("subscribe for %s: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	// Build + publish tasks.
	tasks := make([]distributed.Task, actualTasks)
	for i, files := range fileSets {
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      shuffleQueryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypeShuffle,
			Files:        files,
			ShuffleKeys:  keys,
			NumPartitions: numParts,
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: resultPrefix,
			Output:       resultPrefix, // new field for symmetry with native-DAG
			CreatedAt:    time.Now(),
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		tasks[i] = t
	}
	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("publish %s tasks: %w", stage.ID, err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		return StageOutput{}, fmt.Errorf("%s timed out: %w", stage.ID, ctx.Err())
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()
	for _, r := range results {
		if r.err != "" {
			return StageOutput{}, fmt.Errorf("%s task failed: %s", stage.ID, r.err)
		}
	}

	// Bucket output files by partition number (parsed from path segment).
	partitionFiles := make([][]string, numParts)
	for _, r := range results {
		for _, f := range r.files {
			p, perr := parsePartitionNumber(f)
			if perr != nil {
				return StageOutput{}, fmt.Errorf("parse partition from %s: %w", f, perr)
			}
			if p < 0 || p >= numParts {
				return StageOutput{}, fmt.Errorf("partition %d out of range [0,%d): %s", p, numParts, f)
			}
			partitionFiles[p] = append(partitionFiles[p], f)
		}
	}
	return StageOutput{
		Kind:          OutputPartitioned,
		NumPartitions: numParts,
		Files:         partitionFiles,
	}, nil
}

// flattenStageFiles concatenates all files from a StageOutput, regardless
// of partitioning. Used when a shuffle stage consumes the full upstream
// output to re-partition it on different keys.
func flattenStageFiles(out StageOutput) []string {
	var files []string
	for _, part := range out.Files {
		files = append(files, part...)
	}
	return files
}
```

`parsePartitionNumber`, `splitFilesEvenly`, and `StageInfo` already exist in the coordinator package — reuse them (`grep -n "func parsePartitionNumber\|func splitFilesEvenly\|type StageInfo" internal/coordinator/`).

- [ ] **Step 2: Integration test with in-process cluster**

Append to the existing `internal/coordinator/distributed_test.go` (or create `dispatch_shuffle_test.go` if that's cleaner):

```go
// TestDispatchShuffleStage registers 2 tiny parquet inputs, dispatches a
// hash-repartition on a single int key across 4 partitions with 2 workers,
// and asserts the returned StageOutput is well-formed.
func TestDispatchShuffleStage(t *testing.T) {
	ctx, c, store := setupDistributed(t)

	// Seed two parquet files with {key, val} rows and register them as a
	// table in the catalog so we have an input layout to shuffle.
	// [Setup omitted; reuse existing test helpers for seeding parquet —
	// see other tests in this file for the pattern.]

	stage := physical.Stage{
		ID:           "repart-0",
		Type:         physical.StageExchangeRepartition,
		Dependencies: []string{"scan-0"},
		Exchange: &physical.ExchangeStage{
			Keys:  []string{"key"},
			Count: 4,
		},
	}
	inputs := map[string]StageOutput{
		"scan-0": {
			Kind:  OutputSinglePart,
			Files: [][]string{{"<seeded-file-1>", "<seeded-file-2>"}},
		},
	}
	out, err := c.dispatchShuffleStage(ctx, "q-test", stage, inputs, 2)
	if err != nil {
		t.Fatalf("dispatchShuffleStage: %v", err)
	}
	if out.Kind != OutputPartitioned {
		t.Errorf("Kind: got %v, want OutputPartitioned", out.Kind)
	}
	if out.NumPartitions != 4 {
		t.Errorf("NumPartitions: got %d, want 4", out.NumPartitions)
	}
	if len(out.Files) != 4 {
		t.Errorf("len(Files): got %d, want 4", len(out.Files))
	}
	// At least one partition should have received files.
	total := 0
	for _, p := range out.Files {
		total += len(p)
	}
	if total == 0 {
		t.Error("no output files across any partition")
	}
}
```

The seeded-parquet setup is what `distributed_test.go`'s existing tests do; reuse those helpers verbatim.

- [ ] **Step 3: Run tests**

```bash
go build ./...
go test ./internal/coordinator/ -run TestDispatchShuffleStage -v
```

- [ ] **Step 4: Commit**

```bash
git add internal/coordinator/dispatch_shuffle.go internal/coordinator/dispatch_shuffle_test.go
git commit -m "feat(coordinator): dispatchShuffleStage — generalized runShuffleSide

Dispatches N shuffle tasks for a StageExchangeRepartition and returns the
resulting partition file layout. Inputs come from the upstream stage's
StageOutput (not a raw physical.Stage scan), which lets chained shuffles
work: each successive repartition stage shuffles the previous stage's
partitioned output.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 7: `dispatchReplicateStage`

**Files:**
- Create: `internal/coordinator/dispatch_replicate.go`

- [ ] **Step 1: Write the helper**

Create `internal/coordinator/dispatch_replicate.go`:

```go
package coordinator

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// dispatchReplicateStage executes a StageExchangeReplicate by pre-scanning
// the upstream build input into a single .wshc cache file at
// s3://<bucket>/queries/<queryID>/<stageID>/cache.wshc, which every
// consumer worker reads in full. Returns StageOutput{Replicated}.
//
// Today this is implemented as a thin wrapper over preScanBuildTables,
// which returns map[alias] → []cacheFiles. For native-DAG dispatch we
// only need one alias (the replicate stage has one dependency), so we
// extract the single entry.
func (c *Coordinator) dispatchReplicateStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
) (StageOutput, error) {
	if stage.Type != physical.StageExchangeReplicate {
		return StageOutput{}, fmt.Errorf("dispatchReplicateStage: stage %s is not Replicate", stage.ID)
	}
	if stage.Exchange == nil || stage.Exchange.BuildAlias == "" {
		return StageOutput{}, fmt.Errorf("dispatchReplicateStage: stage %s missing Exchange.BuildAlias", stage.ID)
	}
	if len(stage.Dependencies) != 1 {
		return StageOutput{}, fmt.Errorf("dispatchReplicateStage: stage %s has %d deps, want 1",
			stage.ID, len(stage.Dependencies))
	}
	// preScanBuildTables expects the original physical.Stage slice to walk
	// scans. We adapt by constructing a minimal stage list from the input
	// layout. For Phase 3, this is an intermediate bridge — Task 13 can
	// rewrite preScanBuildTables to take StageOutput directly.
	// [For now: construct synthetic physical.Stage with ScanFiles = the
	// flattened upstream output. See preScanBuildTables signature.]
	return StageOutput{}, fmt.Errorf("dispatchReplicateStage: not yet wired — see Task 13 for preScanBuildTables refactor")
}
```

**This is deliberately stubbed** — the real implementation depends on `preScanBuildTables` internals. The stub documents intent; Task 13's cleanup phase converts `preScanBuildTables` to accept `StageOutput` and returns a single `.wshc` path.

For the Phase 3 runtime to actually exercise Replicate, we need this wired — but the wiring is a careful refactor. Break it into two sub-tasks: expose a version of `preScanBuildTables` that takes `(inputFiles []string, alias string)` and returns `[]string` of cache files; call it from here.

- [ ] **Step 2: Expose a test-friendly preScanBuildTables entry point**

Check the current `preScanBuildTables` signature in `internal/coordinator/build_cache.go`. Add a wrapper:

```go
// preScanBuildTablesFromInput builds a broadcast cache for a single alias
// whose input comes from a prior stage's output. Used by dispatchReplicateStage.
func (c *Coordinator) preScanBuildTablesFromInput(
	ctx context.Context,
	queryID, sql string,
	alias string,
	inputFiles []string,
	outputPrefix string,
) ([]string, error) {
	// [Body: synthesize the minimal physical.Stage that preScanBuildTables
	// needs; call it with a 1-element stage slice; return the cacheFiles
	// under the given alias.]
	// The implementer reads preScanBuildTables and writes this bridge.
	return nil, fmt.Errorf("not yet implemented")
}
```

Replace the stubbed return in `dispatchReplicateStage`:

```go
	depFiles := flattenStageFiles(inputs[stage.Dependencies[0]])
	outputPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)
	cacheFiles, err := c.preScanBuildTablesFromInput(ctx, queryID, sql, stage.Exchange.BuildAlias, depFiles, outputPrefix)
	if err != nil {
		return StageOutput{}, fmt.Errorf("preScan for %s: %w", stage.ID, err)
	}
	return StageOutput{
		Kind:  OutputReplicated,
		Files: [][]string{cacheFiles},
	}, nil
```

- [ ] **Step 3: Build + unit test with stubbed cluster**

Unit-testing `dispatchReplicateStage` end-to-end needs a cluster; defer to the integration tests in Task 12. For now:

```bash
go build ./internal/coordinator/...
```
Expected: builds. `preScanBuildTablesFromInput` returning an error doesn't block compilation.

- [ ] **Step 4: Commit**

```bash
git add internal/coordinator/dispatch_replicate.go internal/coordinator/build_cache.go
git commit -m "feat(coordinator): dispatchReplicateStage + preScanBuildTablesFromInput

Dispatches a Replicate stage's build-cache pre-scan using upstream stage
output as input. Currently thinly wraps preScanBuildTables for backward
compat; fuller refactor in Task 13 when PreScannedInputs/BuildCachePreScans
are removed from physical.Stage.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 8: `dispatchPipelineStage` (generalized)

**Files:**
- Create: `internal/coordinator/dispatch_pipeline.go`

- [ ] **Step 1: Write the helper**

Create `internal/coordinator/dispatch_pipeline.go`:

```go
package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// dispatchPipelineStage executes a compute stage (pipeline/scan/aggregate/
// join/sort/window) by dispatching workerCount pipeline tasks, each
// consuming this stage's assigned partition slice of every input and
// writing to s3://<bucket>/queries/<queryID>/<stageID>/...
//
// If stage.Exchange has downstream repartitioning (i.e. the next stage is
// an Exchange), this stage's output is pre-partitioned by those keys —
// saving a later shuffle. Otherwise single-partition output.
func (c *Coordinator) dispatchPipelineStage(
	ctx context.Context,
	queryID, sql string,
	stage physical.Stage,
	inputs map[string]StageOutput,
	workerCount int,
) (StageOutput, error) {
	outputPrefix := fmt.Sprintf("queries/%s/%s/", queryID, stage.ID)

	trackerStages := map[string]*StageInfo{
		stage.ID: {StageID: stage.ID, Type: distributed.TaskTypePipeline, TotalTasks: workerCount},
	}
	subQueryID := fmt.Sprintf("pl-%s-%s", stage.ID, queryID)
	c.tracker.Register(subQueryID, "", trackerStages, []string{stage.ID})
	c.tracker.Start(subQueryID)
	defer c.tracker.Delete(subQueryID)

	subject := distributed.QueryResultSubject(subQueryID)
	type taskResult struct {
		files []string
		err   string
	}
	var (
		mu        sync.Mutex
		collected = make([]taskResult, 0, workerCount)
		done      = make(chan struct{}, 1)
	)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var r distributed.ResultNotification
		if e := distributed.Unmarshal(msg.Data, &r); e != nil {
			return
		}
		mu.Lock()
		if r.Success {
			collected = append(collected, taskResult{files: r.ResultFiles})
		} else {
			collected = append(collected, taskResult{err: r.Error})
		}
		got := len(collected)
		mu.Unlock()
		if got >= workerCount {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return StageOutput{}, fmt.Errorf("subscribe for %s: %w", stage.ID, err)
	}
	defer sub.Unsubscribe()

	// Build one pipeline task per worker. Each worker gets its partition
	// slice via partitionFilesForWorker on each partitioned input alias.
	tasks := make([]distributed.Task, 0, workerCount)
	for w := 0; w < workerCount; w++ {
		taskInputs := make(map[string][]string, len(inputs))
		for alias, out := range inputs {
			taskInputs[alias] = partitionFilesForWorker(out, w, workerCount)
		}
		t := distributed.Task{
			ID:           uuid.New().String()[:8],
			QueryID:      subQueryID,
			StageID:      stage.ID,
			Type:         distributed.TaskTypePipeline,
			SQLText:      sql,
			Inputs:       taskInputs,
			Output:       outputPrefix,
			DataBucket:   c.config.ResultBucket,
			ResultBucket: c.config.ResultBucket,
			ResultPrefix: outputPrefix,
			CreatedAt:    time.Now(),
		}
		// If this stage's output feeds a downstream Repartition, the task
		// pre-partitions its own output by those keys. Populate ShuffleKeys.
		if stage.Exchange != nil && len(stage.Exchange.Keys) > 0 {
			t.ShuffleKeys = stage.Exchange.Keys
			t.NumPartitions = stage.Exchange.Count
		}
		if clusterID := c.catalog.ClusterID(); clusterID != "" {
			t.ClusterID = clusterID
		}
		tasks = append(tasks, t)
	}
	if err := c.scheduler.PublishTasks(ctx, tasks); err != nil {
		return StageOutput{}, fmt.Errorf("publish %s tasks: %w", stage.ID, err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		return StageOutput{}, fmt.Errorf("%s timed out: %w", stage.ID, ctx.Err())
	}

	mu.Lock()
	results := make([]taskResult, len(collected))
	copy(results, collected)
	mu.Unlock()
	for _, r := range results {
		if r.err != "" {
			return StageOutput{}, fmt.Errorf("%s task failed: %s", stage.ID, r.err)
		}
	}

	// Aggregate output files. Single-partition if no downstream shuffle;
	// otherwise files carry partition=NNNN and the parent (Exchange) stage
	// consumes them.
	var allFiles []string
	for _, r := range results {
		allFiles = append(allFiles, r.files...)
	}
	return StageOutput{
		Kind:  OutputSinglePart,
		Files: [][]string{allFiles},
	}, nil
}
```

- [ ] **Step 2: Integration test (same pattern as Task 6)**

Create `internal/coordinator/dispatch_pipeline_test.go` with a test that seeds an input stage, dispatches a pipeline stage reading it, and verifies rows.

- [ ] **Step 3: Run + commit**

```bash
go build ./...
go test ./internal/coordinator/ -run TestDispatchPipelineStage -v
git add internal/coordinator/dispatch_pipeline.go internal/coordinator/dispatch_pipeline_test.go
git commit -m "feat(coordinator): dispatchPipelineStage — generalized pipeline dispatch

Dispatches workerCount pipeline tasks, each handling its partition slice
of every input. Generalizes buildShufflePipelineTasks: a pipeline stage
with no upstream shuffle gets per-worker scan partitioning; a pipeline
stage with partitioned upstream input gets its partition slice via
partitionFilesForWorker.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 9: `dispatchGatherStage` + `gather_receiver`

**Files:**
- Create: `internal/coordinator/dispatch_gather.go`
- Create: `internal/coordinator/gather_receiver.go`

- [ ] **Step 1: Gather receiver — subscribe + reassemble**

Create `internal/coordinator/gather_receiver.go`:

```go
package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/nats-io/nats.go"
)

// GatherResult is the coordinator-assembled output from gather reply streams.
type GatherResult struct {
	Schema  []byte           // from first non-terminal msg
	Batches []*batch.RecordBatch
	Err     error
}

// receiveGatherStream subscribes to the given NATS subject, collects
// GatherBatchMsg from `expectedWorkers` tasks (identified by any Terminal
// message), decodes them into RecordBatches, and returns when every worker
// has sent its terminal msg. Thread-safe; caller should unsubscribe after.
func (c *Coordinator) receiveGatherStream(
	ctx context.Context,
	subject string,
	expectedWorkers int,
	timeout time.Duration,
) (*GatherResult, error) {
	result := &GatherResult{}
	var (
		mu            sync.Mutex
		terminalsSeen int
		done          = make(chan struct{}, 1)
	)
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var m distributed.GatherBatchMsg
		if e := distributed.Unmarshal(msg.Data, &m); e != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if m.Terminal {
			if m.Err != "" && result.Err == nil {
				result.Err = fmt.Errorf("gather worker error: %s", m.Err)
			}
			terminalsSeen++
			if terminalsSeen >= expectedWorkers {
				select {
				case done <- struct{}{}:
				default:
				}
			}
			return
		}
		if len(m.Schema) > 0 && result.Schema == nil {
			result.Schema = m.Schema
		}
		b, derr := batch.DecodeBatch(m.Payload, result.Schema)
		if derr != nil {
			if result.Err == nil {
				result.Err = fmt.Errorf("decode batch: %w", derr)
			}
			return
		}
		result.Batches = append(result.Batches, b)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}
	defer sub.Unsubscribe()

	select {
	case <-done:
	case <-ctx.Done():
		return nil, fmt.Errorf("gather timed out: %w", ctx.Err())
	case <-time.After(timeout):
		return nil, fmt.Errorf("gather timed out after %s", timeout)
	}

	mu.Lock()
	defer mu.Unlock()
	return result, result.Err
}
```

`batch.DecodeBatch` should match `batch.EncodeBatch` from Task 3. If that didn't exist and we used shuffle-format, use the shuffle reader here.

- [ ] **Step 2: dispatchGatherStage**

Create `internal/coordinator/dispatch_gather.go`:

```go
package coordinator

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/google/uuid"
)

// dispatchGatherStage executes the terminal StageExchangeGather by
// dispatching one (or optionally N for ordered gather) worker task that
// streams output batches to the coordinator via NATS reply.
func (c *Coordinator) dispatchGatherStage(
	ctx context.Context,
	queryID string,
	stage physical.Stage,
	inputs map[string]StageOutput,
) (*QueryResult, error) {
	if stage.Type != physical.StageExchangeGather {
		return nil, fmt.Errorf("dispatchGatherStage: stage %s is not Gather", stage.ID)
	}
	if len(stage.Dependencies) != 1 {
		return nil, fmt.Errorf("dispatchGatherStage: stage %s has %d deps, want 1",
			stage.ID, len(stage.Dependencies))
	}
	depOut := inputs[stage.Dependencies[0]]

	// Single-worker gather is the default. Ordered gather across workers
	// is only beneficial if the pipeline stage produced partitioned output
	// whose ordering we want to merge-sort; for the first native-DAG
	// milestone, run Gather as a single worker concatenating everything.
	replySubject := fmt.Sprintf("wadjet.gather.%s.%s", queryID, stage.ID)

	task := distributed.Task{
		ID:           uuid.New().String()[:8],
		QueryID:      queryID,
		StageID:      stage.ID,
		Type:         distributed.TaskTypeGather,
		Inputs:       map[string][]string{stage.Dependencies[0]: flattenStageFiles(depOut)},
		ReplySubject: replySubject,
		DataBucket:   c.config.ResultBucket,
		ResultBucket: c.config.ResultBucket,
		CreatedAt:    time.Now(),
	}
	if stage.Exchange != nil {
		task.GatherOrdering = stage.Exchange.Ordering
		task.GatherLimit = stage.Exchange.Limit
	}
	if clusterID := c.catalog.ClusterID(); clusterID != "" {
		task.ClusterID = clusterID
	}

	const gatherTimeout = 30 * time.Minute
	result, err := c.receiveGatherStreamAsync(ctx, replySubject, 1, gatherTimeout, func() error {
		return c.scheduler.PublishTasks(ctx, []distributed.Task{task})
	})
	if err != nil {
		return nil, err
	}

	// Apply coordinator-side limit + ordering if configured. Single-worker
	// gather means ordering is already honored by the worker; only Limit
	// needs coordinator application (workers stream everything).
	rows := assembleRows(result.Batches)
	if task.GatherLimit > 0 && len(rows) > task.GatherLimit {
		rows = rows[:task.GatherLimit]
	}
	_ = sort.Sort // placeholder to keep sort import

	return &QueryResult{
		QueryID: queryID,
		Rows:    rows,
	}, nil
}

// receiveGatherStreamAsync starts the subscription, invokes dispatch(),
// then waits. Ensures the subscription is active before any worker
// can publish (prevents the race seen in runShuffleSide).
func (c *Coordinator) receiveGatherStreamAsync(
	ctx context.Context,
	subject string,
	expectedWorkers int,
	timeout time.Duration,
	dispatch func() error,
) (*GatherResult, error) {
	// [Same structure as receiveGatherStream, but calls dispatch() after
	// subscribe and before waiting on done. Implementer can either split
	// receiveGatherStream or add an overload. Simpler path: add a `dispatch`
	// arg to receiveGatherStream and invoke it between subscribe and select.]
	return nil, fmt.Errorf("refactor into receiveGatherStream with dispatch callback")
}

func assembleRows(batches []*batch.RecordBatch) []map[string]any {
	// [Convert RecordBatches to []map[string]any in the same shape
	// wadjet.DB.Query returns today. Reuse any existing helper in
	// internal/engine/batch or wadjet/.]
	var rows []map[string]any
	for _, b := range batches {
		for i := 0; i < b.NumRows(); i++ {
			rows = append(rows, b.RowToMap(i))
		}
	}
	return rows
}
```

The assembleRows body depends on what `batch.RecordBatch.RowToMap(i)` looks like in the current codebase — verify. If no such method exists, look at how `wadjet.DB.Query` currently assembles its result from whatever the coordinator produces today; reuse that pattern.

- [ ] **Step 3: Build + refactor**

The stub `receiveGatherStreamAsync` needs real wiring. Simplest: make `receiveGatherStream` accept a `dispatch func() error` parameter; `nil` means caller already dispatched (preserves original signature semantics for future callers). Adjust Task 9 Step 1 accordingly.

```bash
go build ./...
```
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add internal/coordinator/dispatch_gather.go internal/coordinator/gather_receiver.go
git commit -m "feat(coordinator): dispatchGatherStage + gather receiver

Terminal Gather stage. Dispatches one worker task with a NATS reply
subject, subscribes to the subject, reassembles batches into a
QueryResult. Replaces mergeProbePartials (S3-list-based merge) and
ExtractMergeInfo(logicalPlan) (Gather stage now carries Ordering/Limit
on its Exchange payload).

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 10: `executeStageDAG` topo-walk + `UseNativeDAG` flag

**Files:**
- Create: `internal/coordinator/execute_stage_dag.go`
- Modify: `internal/coordinator/coordinator.go` (add `UseNativeDAG`; branch `executeDistributed`)

- [ ] **Step 1: executeStageDAG**

Create `internal/coordinator/execute_stage_dag.go`:

```go
package coordinator

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// executeStageDAG walks the Exchange-annotated stage DAG from EnsureDistribution
// in topological order, dispatching each stage via the appropriate helper.
// Returns when the terminal Gather stage produces a QueryResult.
//
// Replaces the legacy four-mode switch and the Phase 2b lowerExchangeDAG.
// The stages argument is already topo-sorted (guaranteed by the planner).
func (c *Coordinator) executeStageDAG(
	ctx context.Context,
	queryID, sql string,
	stages []physical.Stage,
	workerCount int,
) (*QueryResult, error) {
	outputs := make(map[string]StageOutput, len(stages))
	for _, stage := range stages {
		inputs, err := collectInputs(stage, outputs)
		if err != nil {
			return nil, err
		}
		switch stage.Type {
		case physical.StageExchangeRepartition:
			out, err := c.dispatchShuffleStage(ctx, queryID, stage, inputs, workerCount)
			if err != nil {
				return nil, fmt.Errorf("repartition %s: %w", stage.ID, err)
			}
			outputs[stage.ID] = out

		case physical.StageExchangeReplicate:
			out, err := c.dispatchReplicateStage(ctx, queryID, sql, stage, inputs)
			if err != nil {
				return nil, fmt.Errorf("replicate %s: %w", stage.ID, err)
			}
			outputs[stage.ID] = out

		case physical.StageExchangeGather:
			return c.dispatchGatherStage(ctx, queryID, stage, inputs)

		default: // pipeline / scan / aggregate / join / sort / window
			out, err := c.dispatchPipelineStage(ctx, queryID, sql, stage, inputs, workerCount)
			if err != nil {
				return nil, fmt.Errorf("pipeline %s: %w", stage.ID, err)
			}
			outputs[stage.ID] = out
		}
	}
	return nil, fmt.Errorf("plan terminated without Gather stage (query %s)", queryID)
}
```

- [ ] **Step 2: Add UseNativeDAG flag**

In `internal/coordinator/coordinator.go`, find the `Coordinator` struct and add:

```go
	// UseNativeDAG routes queries through executeStageDAG instead of the
	// legacy four-mode switch. Defaults true; set false to fall back to
	// legacy dispatch during Phase 3 rollout. Flag is removed after
	// one week of stability — see
	// docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md.
	UseNativeDAG bool
```

In the `New` constructor, default it to `true`.

- [ ] **Step 3: Branch executeDistributed on the flag**

In `executeDistributed` (the function that today builds `physStages` and runs the four-mode switch), after `physStages` is ready and before the legacy switch, add:

```go
	if c.UseNativeDAG {
		return c.executeStageDAG(ctx, queryID, sql, physStages, c.workers.Count())
	}
```

Leave the legacy switch body in place for fall-back. Task 14 cleanup deletes it once the flag is removed.

- [ ] **Step 4: Unit-level smoke test**

Append to `internal/coordinator/distributed_test.go`:

```go
// TestExecuteStageDAG_TwoStageGather builds a minimal 2-stage plan
// (scan → gather) and verifies executeStageDAG returns a populated
// QueryResult.
func TestExecuteStageDAG_TwoStageGather(t *testing.T) {
	ctx, c, _ := setupDistributed(t)
	c.UseNativeDAG = true

	// [Seed a tiny parquet file, construct a physical.Stage slice with
	// one scan + one Gather, call c.executeStageDAG(...), assert
	// len(result.Rows) == expected.]
}
```

The seeded-parquet + fake-stage pattern should mirror existing tests in this file. If seeding is too painful, a simpler variant is to call `c.executeStageDAG` with a single Gather stage whose dep has a pre-populated `StageOutput` entry in `outputs` — but that skips the dispatch logic. Prefer the end-to-end seeded path.

- [ ] **Step 5: Run + commit**

```bash
go build ./...
go test ./internal/coordinator/ -run TestExecuteStageDAG -v
go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -5   # SF0.01 gate
```
All green.

```bash
git add internal/coordinator/execute_stage_dag.go internal/coordinator/coordinator.go internal/coordinator/distributed_test.go
git commit -m "feat(coordinator): executeStageDAG + UseNativeDAG flag

Native stage-DAG executor. Topologically walks Exchange-annotated plans
and dispatches each stage via the appropriate helper (dispatchShuffleStage,
dispatchReplicateStage, dispatchPipelineStage, dispatchGatherStage).
Returns a QueryResult from the terminal Gather.

UseNativeDAG flag defaults true; legacy switch retained for one-week
rollback window per spec.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 11: EnsureDistribution — always emit terminal Gather

**Files:**
- Modify: `internal/planner/physical/ensure_distribution.go`
- Modify: `internal/planner/physical/ensure_distribution_test.go`

- [ ] **Step 1: Update the final-Gather condition**

Current code in `EnsureDistribution` (around line 112-125):

```go
if len(out) > 0 {
    root := out[len(out)-1]
    if root.Distribution.Kind != DistSingleton && root.Type != StageExchangeGather {
        gather := Stage{...}
        out = append(out, gather)
    }
}
```

Replace with unconditional Gather-on-no-Gather:

```go
// Every PlanDistributed result ends in a Gather stage so executeStageDAG
// has a single terminal node to stream results from. If the root is
// already Singleton, the Gather is a trivial one-worker stream; if the
// root is partitioned, the Gather merges (and optionally sorts+limits)
// worker outputs.
if len(out) > 0 {
    root := out[len(out)-1]
    if root.Type != StageExchangeGather {
        gather := Stage{
            Type:         StageExchangeGather,
            ID:           fmt.Sprintf("%s-%s", StageExchangeGather, root.ID),
            Dependencies: []string{root.ID},
            Distribution: Distribution{Kind: DistSingleton},
            Exchange:     &ExchangeStage{},
        }
        out = append(out, gather)
    }
}
```

- [ ] **Step 2: Update tests that asserted "no Gather when root is Singleton"**

In `ensure_distribution_test.go`, any test that asserted no Gather appends on a Singleton root needs to flip to assert one Gather appends. Grep:

```bash
grep -n "StageExchangeGather\|Singleton.*Gather\|no.*gather" internal/planner/physical/ensure_distribution_test.go
```

Update the affected test cases.

- [ ] **Step 3: Verify all 22 TPC-H plans end in Gather**

Append to `internal/planner/physical/plan_tpch_test.go` (or a new test file):

```go
func TestAllTPCHPlansEndInGather(t *testing.T) {
	plans := tpchPlansWithEnsureDistribution(t)
	for name, stages := range plans {
		t.Run(name, func(t *testing.T) {
			if len(stages) == 0 {
				t.Fatal("empty plan")
			}
			last := stages[len(stages)-1]
			if last.Type != StageExchangeGather {
				t.Errorf("%s: last stage type %q, want %q", name, last.Type, StageExchangeGather)
			}
		})
	}
}
```

If `tpchPlansWithEnsureDistribution` was deleted in Task 1's revert (it lived in `exchange_equivalence_test.go`, which was part of Phase 2b), extract a minimal version that plans each query with `UseEnsureDistribution=true` — but wait, `UseEnsureDistribution` flag was also deleted. EnsureDistribution is unconditional now (post-revert, it was reverted to being flag-gated; we need to un-revert or re-apply).

Check the revert state: `grep -n "UseEnsureDistribution" internal/planner/physical/plan.go` — if the flag is back (revert resurrected it), we need to either re-apply Task 10 of the Phase 2b plan (always-run EnsureDistribution) or set the flag in this new test. Simpler: in this test, construct a Planner, set `planner.UseEnsureDistribution = true`, and go.

- [ ] **Step 4: Run + commit**

```bash
go test ./internal/planner/physical/...
go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -5
git add internal/planner/physical/ensure_distribution.go internal/planner/physical/ensure_distribution_test.go internal/planner/physical/plan_tpch_test.go
git commit -m "feat(planner): EnsureDistribution always emits terminal Gather

Previously Gather was only appended when the root's output was non-Singleton.
Now every distributed plan ends in Gather — required by executeStageDAG's
topo walk which relies on a terminal node to stream results. For Singleton
roots, Gather is a trivial one-worker pass-through.

Matches Trino's 'output fragment' invariant.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 12: EnsureDistribution — populate Gather.Ordering and .Limit from logical plan

**Files:**
- Modify: `internal/planner/physical/ensure_distribution.go`
- Modify: `internal/planner/physical/plan.go` (if the planner's logical-plan reference isn't available in EnsureDistribution)

Today `ExtractMergeInfo(logicalPlan)` is called in the coordinator to get Order/Limit for the final merge. For Phase 3, move that extraction into `EnsureDistribution` so the Gather stage carries its own metadata.

- [ ] **Step 1: Figure out how the logical plan reaches EnsureDistribution**

`EnsureDistribution(stages []Stage, workerCount int)` doesn't currently take a logical plan. Two options:

(a) Make EnsureDistribution see the logical plan — change signature to `EnsureDistribution(stages []Stage, workerCount int, logicalPlan *logical.Node)` and pass through from `PlanDistributed`. Small surgery.

(b) Populate `Ordering` / `Limit` on the terminal physical stage during physical planning (before EnsureDistribution runs), then EnsureDistribution just copies those fields onto the Gather. Cleaner — keeps EnsureDistribution logical-plan-unaware.

Option (b) is cleaner and matches the planner's layered structure. Grep the physical planner for the final Sort/Limit stage construction — there's likely a place where `Stage{Type: "sort", Limit: ..., SortKeys: ...}` is built. Ensure its `SortKeys` / `Limit` flow through.

- [ ] **Step 2: On terminal-Gather insertion, copy Ordering/Limit from the root**

In `EnsureDistribution` (the Gather-insert block from Task 11):

```go
		if root.Type != StageExchangeGather {
			gather := Stage{
				Type:         StageExchangeGather,
				ID:           fmt.Sprintf("%s-%s", StageExchangeGather, root.ID),
				Dependencies: []string{root.ID},
				Distribution: Distribution{Kind: DistSingleton},
				Exchange:     &ExchangeStage{},
			}
			// Inherit Ordering + Limit from the root if the root is a Sort
			// stage. The coordinator's gather receiver applies these after
			// collecting worker batches.
			if root.Type == StageSort {
				gather.Exchange.Ordering = append([]SortKeySpec(nil), root.SortKeys...)
				gather.Exchange.Limit = root.Limit
			}
			out = append(out, gather)
		}
```

- [ ] **Step 3: Confirm ExchangeStage has Limit**

In `internal/planner/physical/exchange.go`, verify or add:

```go
type ExchangeStage struct {
	// ...existing...
	Ordering []SortKeySpec // Gather only
	Limit    int           // Gather only
}
```

- [ ] **Step 4: Remove coordinator's ExtractMergeInfo call**

Grep: `grep -n "ExtractMergeInfo(logicalPlan)" internal/coordinator/coordinator.go`

Find the call in `executeDistributed` — it should only be called for the Q17 pre-compute path now (see the spec). The Gather extraction is dead code. Delete the var + the surrounding block. **But leave the Q17 call if it exists.**

- [ ] **Step 5: Run + commit**

```bash
go build ./...
go test ./internal/planner/...
go test ./internal/coordinator/...
go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -5
git add internal/planner/physical/ensure_distribution.go internal/planner/physical/exchange.go internal/coordinator/coordinator.go
git commit -m "feat(planner): Gather stage carries Ordering + Limit from root Sort

Moves merge-order/limit metadata from coordinator-side ExtractMergeInfo
call to the Gather stage itself. ExchangeStage payload gains Limit int.
Coordinator no longer extracts from the logical plan for the gather
step — the stage is self-describing.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 13: Regenerate parity baselines from main

**Files:**
- Modify: `benchmarks/tpch/testdata/parity/*.json` (if any remain post-revert)
- Modify: `benchmarks/tpch/parity_test.go` (if reverted)

- [ ] **Step 1: Check whether PARITY harness survived the revert**

Task 1 reverted Phase 2b, which included the PARITY harness activation. Check:

```bash
ls benchmarks/tpch/parity_test.go benchmarks/tpch/testdata/parity/ 2>&1
```

If PARITY is gone, cherry-pick it back from the Phase 2b commit `b2d205e` (or the equivalent commit SHA):

```bash
git show b2d205e --stat  # confirm it's the PARITY commit
git cherry-pick b2d205e
```

Resolve conflicts if any (unlikely since the revert touched different files).

- [ ] **Step 2: Regenerate baselines from main**

Create a main worktree, build binaries from main, capture baselines:

```bash
git worktree add /tmp/wadjet-main-parity main
cd /tmp/wadjet-main-parity
UPDATE_PARITY_BASELINES=1 go test -run TestTPCHParity ./benchmarks/tpch/ -v 2>&1 | tail -10
# Copy the generated baselines back into feat branch:
cp testdata/parity/*.json /home/dwright/Projects/caelum/benchmarks/tpch/testdata/parity/
cd /home/dwright/Projects/caelum
git worktree remove /tmp/wadjet-main-parity --force
```

Note: if PARITY test source from feat branch differs from what main has, you need to apply PARITY to main first. Simpler: build tpch-harness from feat (which has PARITY), but point it at the main wadjet binary. The baselines are just run outputs.

- [ ] **Step 3: Verify parity passes on feat with native-DAG enabled**

```bash
PARITY=1 go test -run TestTPCHParity ./benchmarks/tpch/ -v 2>&1 | tail -10
```

Expected: 22/22 PASS. If any query diverges, it means native-DAG produces different rows than main — a real correctness bug to fix before SF10.

- [ ] **Step 4: Commit**

```bash
git add benchmarks/tpch/testdata/parity/
git commit -m "test(tpch): regenerate PARITY baselines from main

Baselines captured from main's SF0.01 TPC-H output using the PARITY
harness. Feat branch (native-DAG executor) now compares against these
canonical baselines — any divergence flags a real correctness issue.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Task 14: Local harness gate — SF10 via PR A's S3 mode

**Files:** none; this is a validation task.

- [ ] **Step 1: Run harness with --source=s3 against SF10 bucket**

```bash
./tpch-harness --mode=local --source=s3 --bucket=wadjet-bench-sf10-use2 \
               --region=us-east-2 --endpoint=s3.us-east-2.amazonaws.com --ssl \
               --data-prefix=tables/ --pg-addr=:15499 \
               --wadjet-bin=./wadjet_bin --no-compare 2>&1 | tee /tmp/harness-native-dag.txt
```

Expected: all 22 queries pass; row counts match the Phase 2b-broken row counts we captured in `docs/_archive/research/sf10-phase2b-2026-04-21/main-60bb9d3.txt` (since those are from main, which is what native-DAG should match).

Specifically, look for:
- Q18: 100 rows (not 0, not timeout)
- Q21: 100 rows (not 1)
- Q20: 6194 rows (not 3780)
- Q15: 3 rows (not 1)

- [ ] **Step 2: If any query fails, debug BEFORE touching EC2**

Failures here mean the native-DAG executor has a bug. Bisect: which dispatch helper produces wrong output? Add logging, re-run locally. No EC2 spend until the harness is green.

- [ ] **Step 3: No commit — this is a gate, not a code change.**

---

## Task 15: SF10 EC2 A/B — REQUIRES USER APPROVAL

**Files:** none.

- [ ] **Step 1: STOP and request user approval**

Per `memory/feedback_no_auto_deploy.md` and the user's explicit gate in this session: every EC2 deploy must have explicit user approval.

Report to user: "Task 14 (local harness SF10) is green. Ready to deploy main baseline and feat/native-DAG branch to EC2 for T2 gate. Estimated cost: ~$1.20, wall time ~2h. Approve?"

- [ ] **Step 2: On approval, follow deploy preflight checklist**

Per `memory/feedback_deploy_preflight.md`:
1. Verify no orphans: `aws ec2 describe-instances --profile citc --region us-east-2 --filters "Name=instance-state-name,Values=running,pending" --query 'Reservations[].Instances[].InstanceId'`
2. Stage main binaries from a main worktree (see Task 13 Step 2 for the pattern), run `./deploy/benchmark/stage-binaries.sh`
3. `export AWS_PROFILE=citc; cd deploy/benchmark/terraform; tofu apply -auto-approve -var-file=sf10-distributed.tfvars -var=bin_version=<main-sha> -var=use_spot=false`
4. Active monitoring at T+60s, T+2min per `feedback_deploy_monitoring.md`
5. Pull results from S3 as soon as the result file appears
6. `tofu destroy -auto-approve -var-file=sf10-distributed.tfvars -var=bin_version=<main-sha> -var=use_spot=false`
7. Verify destroy clean: no instances in running/pending/stopping states
8. Repeat for feat branch

- [ ] **Step 3: Compare + gate**

Per-query: no query more than 10% slower on branch. No correctness regressions. Per the spec's Success Criteria:
- Q18, Q20, Q21 return the right rows
- Q12, Q17, Q03, Q21 improve vs current main (closing the Phase 5 collapse regression)
- Total SF10 time moves toward the historical 2m02s baseline

If all green, proceed. If regressions remain, STOP and investigate.

- [ ] **Step 4: Archive results**

```bash
mkdir -p docs/_archive/research/sf10-native-dag-$(date +%Y-%m-%d)
cp /tmp/sf10-main-native.txt docs/_archive/research/sf10-native-dag-$(date +%Y-%m-%d)/main.txt
cp /tmp/sf10-feat-native.txt docs/_archive/research/sf10-native-dag-$(date +%Y-%m-%d)/feat.txt
git add docs/_archive/research/
git commit -m "docs: archive SF10 native-DAG A/B results"
```

---

## Task 16: Cleanup — delete legacy fields and code paths

Only run AFTER Task 15 is green (SF10 passes). This task is irreversible.

**Files:** many — see spec "Deletions" section.

- [ ] **Step 1: Delete PreScannedInputs, BuildCachePreScans, ProbeSplitAlias, ProbeSplitFiles, PreComputedAggregates from `physical.Stage`**

Grep: `grep -rn "PreScannedInputs\|BuildCachePreScans\|ProbeSplitAlias\|ProbeSplitFiles" internal/ --include='*.go'`

Each reference: either copy the value to `Task.Inputs` during dispatch (if still needed transitionally) OR delete if unused. For the Q17 pre-compute path, `PreComputedAggregates` likely needs to keep flowing — trace and preserve only what that path needs.

- [ ] **Step 2: Delete `mergeProbePartials` + coordinator-side ExtractMergeInfo gather call**

Grep: `grep -rn "mergeProbePartials\|ExtractMergeInfo" internal/coordinator/`

The only remaining caller of `ExtractMergeInfo` after Task 12 should be the Q17 aggregate-shuffle pre-compute branch. Delete the gather-side calls.

- [ ] **Step 3: Delete the legacy four-mode switch body**

If the switch body is still present inside `executeDistributed` (kept under `UseNativeDAG=false` fallback), delete it. Also delete `UseNativeDAG` field itself since the fallback is gone.

```bash
grep -n "UseNativeDAG\|probe-split\|canProbeSplit\|shuffleApplicable" internal/coordinator/coordinator.go
```

Every hit is a candidate for deletion. The `executeDistributed` becomes:

```go
func (c *Coordinator) executeDistributed(ctx context.Context, ...) (*QueryResult, error) {
    physStages, err := c.buildPhysicalStages(...)
    if err != nil { return nil, err }
    // Q17 inline pre-compute stays here (until moved to planner rewrite in Phase 4)
    // ...
    return c.executeStageDAG(ctx, queryID, sql, physStages, c.workers.Count())
}
```

- [ ] **Step 4: Run full test suite + SF0.01**

```bash
go build ./...
go test ./...
go test -v -run TestTPCHQueries ./benchmarks/tpch/ 2>&1 | tail -5
PARITY=1 go test -run TestTPCHParity ./benchmarks/tpch/ -v 2>&1 | tail -5
```
All green.

- [ ] **Step 5: Commit (one cleanup commit per subsystem if the diff is big)**

```bash
git add -u
git commit -m "refactor: delete legacy dispatch paths and UseNativeDAG flag

Post-T2-gate cleanup. SF10 A/B confirmed native-DAG matches or beats
main on all 22 queries; legacy four-mode switch body and side-channel
fields on physical.Stage are no longer needed.

Deleted:
- physical.Stage.PreScannedInputs, BuildCachePreScans, ProbeSplitAlias,
  ProbeSplitFiles (generalized into Task.Inputs)
- mergeProbePartials (replaced by gather_receiver)
- ExtractMergeInfo coordinator-side call for gather (Gather stage now
  carries its own Ordering/Limit)
- Legacy four-mode switch body in executeDistributed
- Coordinator.UseNativeDAG flag (native-DAG is unconditional)

Q17 aggregate-shuffle pre-compute path retained — lift to logical
rewrite in Phase 4.

Phase 3 spec: docs/_archive/specs/2026-04-22-distribution-native-dag-execution-design.md"
```

---

## Self-review

**Spec coverage:**
- W3 architecture with coordinator-as-stage-dispatcher: Tasks 6-10 ✓
- Per-stage S3 materialization: Tasks 6-8 ✓
- Gather as real NATS-streaming operator: Tasks 3, 9 ✓
- Task schema extension: Task 2 ✓
- EnsureDistribution always emits Gather: Task 11 ✓
- Gather carries Ordering/Limit: Task 12 ✓
- UseNativeDAG transition flag: Task 10 ✓
- Parity baselines from main: Task 13 ✓
- Local harness gate via PR A: Task 14 ✓
- SF10 EC2 A/B with user approval: Task 15 ✓
- Cleanup / legacy deletion: Task 16 ✓
- Phase 2b revert: Task 1 ✓

**Placeholder scan:** 
- Task 3 Step 3: "If encoding a RecordBatch ... STOP and report BLOCKED" — defensible escalation, not a hidden TODO.
- Task 7 Step 1: stub `dispatchReplicateStage` that returns error; Step 2 is the real implementation via `preScanBuildTablesFromInput`. The implementer has to trace `preScanBuildTables` to write the bridge. Intentionally leaving that tracing to them — it's repo-shape-dependent.
- Task 9 Step 2: `receiveGatherStreamAsync` is stubbed with a note to refactor `receiveGatherStream` to accept a dispatch callback. Clear intent.
- Task 11 Step 3: references `tpchPlansWithEnsureDistribution` which may or may not survive the revert; the step gives both recovery paths.
- Task 12 Step 4: assumes `ExtractMergeInfo` has a Q17 call worth preserving; implementer verifies via grep.

Each unknown is flagged and bounded; no silent gaps.

**Type consistency:**
- `StageOutput{Kind, NumPartitions, Files}` consistent across Tasks 5, 6, 7, 8, 9.
- `GatherBatchMsg` fields consistent between Task 3 and Task 9.
- `Task.Inputs` shape consistent (Task 2 declares, Task 4 reads, Task 8 writes).
- `dispatchXxxStage` method signatures match their call site in Task 10 `executeStageDAG`.

**Task count:** 16 tasks for PR B + 6 tasks for PR A = 22 tasks total. Within the 25-35 target from the user's prompt.

---

## Execution handoff

Plan saved to `docs/_archive/plans/2026-04-22-native-dag-execution.md` (this file) and `docs/_archive/plans/2026-04-22-harness-s3-mode.md` (PR A).

Two execution options:
1. **Subagent-driven** (recommended) — fresh subagent per task + review checkpoints
2. **Inline** — execute in this session with batch checkpoints

PR A runs first (6 tasks, mostly mechanical). PR B runs after PR A merges (16 tasks including the revert and the SF10 A/B gate).

All deploys (Task 15) are gated on explicit user approval per `feedback_no_auto_deploy.md`.
