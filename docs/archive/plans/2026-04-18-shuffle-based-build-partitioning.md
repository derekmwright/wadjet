# Shuffle-Based Build Partitioning Implementation Plan


**Goal:** Add a shuffle-distributed execution path for queries whose largest build table exceeds 4 GB, so per-worker memory scales 1/N instead of being broadcast-duplicated. Unblocks Q03/Q05/Q07 at SF100.

**Architecture:** Add `TaskTypeShuffle` and a `Distribution` property on physical plan nodes. The coordinator routing decision picks shuffle over probe-split when the largest build's `EstimatedBytes` exceeds `shuffleBuildThreshold` (4 GB). A new partitioned shuffle sink writes 4N per-partition `.wshf` files; a new partition-shard source reads all `.wshf` files for an assigned partition. Coordinator orchestrates shuffle stages (build + probe) before dispatching the final co-located join stage. Phase 1 supports a single shuffle stage; Phase 2 (chained shuffles for Q09) is out of scope.

**Tech Stack:** Go, NATS JetStream, S3 (MinIO in tests), `wadjet` engine `exec` package, custom WSHF on-disk format.

**Spec:** `docs/archive/specs/2026-04-18-shuffle-based-build-partitioning-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `internal/distributed/messages.go` | Modify | Add `TaskTypeShuffle` constant; rely on existing `ShuffleKeys`/`NumPartitions`/`PartitionID`/`ResultFiles` fields |
| `internal/planner/physical/distribution.go` | NEW | `Distribution` type, `DistKind`, equality/compatibility helpers |
| `internal/planner/physical/plan.go` | Modify | Add `Distribution` field to `Stage`; add `Type = "shuffle"` stage variant; `InsertShuffleForLargeBuild()` planner pass |
| `internal/coordinator/coordinator.go` | Modify | New routing branch: when `largestBuildBytes(stages) > shuffleBuildThreshold`, route to shuffle path |
| `internal/coordinator/shuffle_orchestrator.go` | NEW | Dispatch shuffle stage (build side + probe side), wait for completion, build probe-stage tasks with partition assignments |
| `internal/coordinator/scheduler.go` (or `coordinator.go`) | Modify | `createTasksForStage` handles new shuffle stage type |
| `internal/worker/executor.go` | Modify | Switch case for `TaskTypeShuffle`; new `executeShuffle` function |
| `internal/worker/partitioned_shuffle_sink.go` | NEW | Sink that hash-partitions into 4N `.wshf` output files (one per partition) |
| `internal/worker/partition_shard_source.go` | NEW | Source that reads all `.wshf` files at an assigned S3 partition prefix and streams batches |
| `internal/worker/shuffle_format.go` | Modify | No format change; reuse existing chunk format |
| `benchmarks/local/broadcast_pressure_test.go` | NEW | Local memory-pressure repro under tight `GOMEMLIMIT` (Gate 0) |
| `internal/coordinator/distributed_tpch_test.go` | Modify | Add SF0.01 case forcing shuffle path via lowered `shuffleBuildThreshold` |
| `internal/planner/physical/distribution_test.go` | NEW | Unit tests for `Distribution` arithmetic |
| `internal/planner/physical/shuffle_insertion_test.go` | NEW | Unit tests for `InsertShuffleForLargeBuild()` |
| `internal/worker/partitioned_shuffle_sink_test.go` | NEW | Round-trip test: hash-partition then read back, verify rows-per-partition correct |

---

## Task 1: Add `TaskTypeShuffle` constant

**Files:**
- Modify: `internal/distributed/messages.go:10-15`

- [ ] **Step 1: Read current messages.go to confirm structure**

Run: `head -20 internal/distributed/messages.go`
Expected: Confirms only `TaskTypePipeline` exists.

- [ ] **Step 2: Add `TaskTypeShuffle` constant**

In `internal/distributed/messages.go`, replace the const block:

```go
const (
	TaskTypePipeline TaskType = "pipeline" // full query executed as standalone pipeline on one worker
	TaskTypeShuffle  TaskType = "shuffle"  // hash-partitions input rows into N output partition files
)
```

- [ ] **Step 3: Build to verify nothing breaks**

Run: `go build ./...`
Expected: PASS (no import cycles, no compile errors).

- [ ] **Step 4: Commit**

```bash
git add internal/distributed/messages.go
git commit -m "feat(distributed): add TaskTypeShuffle constant"
```

---

## Task 2: `Distribution` property type

**Files:**
- Create: `internal/planner/physical/distribution.go`
- Create: `internal/planner/physical/distribution_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/planner/physical/distribution_test.go`:

```go
package physical

import "testing"

func TestDistributionEquals(t *testing.T) {
	tests := []struct {
		name string
		a, b Distribution
		want bool
	}{
		{"both broadcast", Distribution{Kind: DistBroadcast}, Distribution{Kind: DistBroadcast}, true},
		{"singleton vs broadcast", Distribution{Kind: DistSingleton}, Distribution{Kind: DistBroadcast}, false},
		{"hash same keys same count", Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 12}, Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 12}, true},
		{"hash different keys", Distribution{Kind: DistHashPartitioned, Keys: []string{"a"}, Count: 12}, Distribution{Kind: DistHashPartitioned, Keys: []string{"b"}, Count: 12}, false},
		{"hash different count", Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 12}, Distribution{Kind: DistHashPartitioned, Keys: []string{"k"}, Count: 24}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equals(tt.b); got != tt.want {
				t.Errorf("Equals = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDistributionSatisfiesJoin(t *testing.T) {
	d := Distribution{Kind: DistHashPartitioned, Keys: []string{"orderkey"}, Count: 12}
	if !d.SatisfiesJoinKeys([]string{"orderkey"}) {
		t.Error("expected hash-on-orderkey to satisfy join on orderkey")
	}
	if d.SatisfiesJoinKeys([]string{"custkey"}) {
		t.Error("expected hash-on-orderkey to NOT satisfy join on custkey")
	}
	bcast := Distribution{Kind: DistBroadcast}
	if !bcast.SatisfiesJoinKeys([]string{"anything"}) {
		t.Error("broadcast should always satisfy join")
	}
}
```

- [ ] **Step 2: Run test — should fail to compile**

Run: `go test ./internal/planner/physical/ -run TestDistribution -v`
Expected: FAIL — `Distribution`, `DistKind`, etc. undefined.

- [ ] **Step 3: Implement distribution.go**

Create `internal/planner/physical/distribution.go`:

```go
package physical

// DistKind is the kind of partitioning a stage's output has.
type DistKind int

const (
	DistSingleton       DistKind = iota // single worker has all rows
	DistBroadcast                       // every worker has all rows
	DistHashPartitioned                 // rows partitioned by hash(Keys) % Count
)

// Distribution describes how a stage's output is partitioned across workers.
type Distribution struct {
	Kind  DistKind
	Keys  []string // for DistHashPartitioned
	Count int      // for DistHashPartitioned
}

// Equals reports whether two Distributions are identical.
func (d Distribution) Equals(other Distribution) bool {
	if d.Kind != other.Kind {
		return false
	}
	if d.Kind != DistHashPartitioned {
		return true
	}
	if d.Count != other.Count || len(d.Keys) != len(other.Keys) {
		return false
	}
	for i := range d.Keys {
		if d.Keys[i] != other.Keys[i] {
			return false
		}
	}
	return true
}

// SatisfiesJoinKeys reports whether this distribution allows a co-located
// join on the given keys without re-shuffling. Broadcast always satisfies;
// hash-partitioned satisfies iff the keys match exactly (in order).
func (d Distribution) SatisfiesJoinKeys(joinKeys []string) bool {
	switch d.Kind {
	case DistBroadcast:
		return true
	case DistHashPartitioned:
		if len(d.Keys) != len(joinKeys) {
			return false
		}
		for i := range d.Keys {
			if d.Keys[i] != joinKeys[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/planner/physical/ -run TestDistribution -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/distribution.go internal/planner/physical/distribution_test.go
git commit -m "feat(planner): add Distribution property type for shuffle planning"
```

---

## Task 3: Add `Distribution` field to `Stage`

**Files:**
- Modify: `internal/planner/physical/plan.go:35-50` (Stage struct)

- [ ] **Step 1: Add field to Stage struct**

In `internal/planner/physical/plan.go`, find the `Stage` struct (currently around line 35) and add a `Distribution` field. The minimal addition:

```go
type Stage struct {
	ID           string
	Type         string // scan, aggregate, sort, hash_join, broadcast_join, window, shuffle, pipeline
	// ... existing fields unchanged ...

	// Distribution describes how this stage's output is partitioned.
	// Default zero value is {Kind: DistSingleton} which is correct for
	// most existing stages (single-worker output). Shuffle stages set this
	// to DistHashPartitioned with Keys and Count populated. Broadcast pre-scans
	// (build cache) set Kind: DistBroadcast.
	Distribution Distribution
}
```

Update the Type comment to add `shuffle` to the list.

- [ ] **Step 2: Verify nothing breaks**

Run: `go build ./... && go test ./internal/planner/physical/ -count=1`
Expected: PASS — `Distribution` zero value is `DistSingleton` so no test changes needed.

- [ ] **Step 3: Commit**

```bash
git add internal/planner/physical/plan.go
git commit -m "feat(planner): add Distribution field to Stage"
```

---

## Task 4: Build the partitioned shuffle sink

This is the per-worker output side of a shuffle: hash-partition incoming batches into 4N output `.wshf` files (one per target partition), each written incrementally to disk to bound memory.

**Files:**
- Create: `internal/worker/partitioned_shuffle_sink.go`
- Create: `internal/worker/partitioned_shuffle_sink_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/worker/partitioned_shuffle_sink_test.go`:

```go
package worker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestPartitionedShuffleSink_RoundTrip verifies that rows hash-partitioned
// across N output files can be read back, that no row is lost, and that
// every row in partition p hashes to p.
func TestPartitionedShuffleSink_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	const numParts = 4

	schema := []parquet.Column{{Name: "k", Type: parquet.Int64Type}}
	sink := newPartitionedShuffleSink(dir, []string{"k"}, numParts, schema)
	if err := sink.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Build a batch of 1000 rows with sequential int64 keys.
	const n = 1000
	col := batch.NewInt64Vector(n)
	for i := 0; i < n; i++ {
		col.Set(i, int64(i))
	}
	b := &batch.RecordBatch{
		Schema:  schema,
		Columns: []batch.Vector{col},
	}
	if err := sink.Consume(context.Background(), b); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	paths := sink.PartitionFiles()
	if len(paths) != numParts {
		t.Fatalf("expected %d partition files, got %d", numParts, len(paths))
	}

	// Read each partition back and verify every row's hash maps back to its partition.
	totalRows := 0
	for p, path := range paths {
		if path == "" {
			continue // empty partition
		}
		rows := readWSHFInts(t, filepath.Clean(path), "k")
		for _, k := range rows {
			got := int(hashInt64(k) % uint64(numParts))
			if got != p {
				t.Errorf("row k=%d ended up in partition %d, hash maps to %d", k, p, got)
			}
		}
		totalRows += len(rows)
	}
	if totalRows != n {
		t.Errorf("total rows across partitions = %d, want %d", totalRows, n)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// readWSHFInts reads back int64 values from a single-column WSHF file.
// (Helper assumed elsewhere in worker tests; if not, declare locally.)
func readWSHFInts(t *testing.T, path, col string) []int64 {
	t.Helper()
	rdr, err := openShuffleReader(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer rdr.Close()
	var out []int64
	for {
		b, err := rdr.Next()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if b == nil {
			break
		}
		v := b.Columns[0].(*batch.Int64Vector)
		for i := 0; i < b.ActiveLen(); i++ {
			out = append(out, v.Get(i))
		}
	}
	return out
}
```

Note: `hashInt64`, `openShuffleReader`, and helper details may need small adjustments to match existing helpers in the package — Step 3 implementation will name them consistently.

- [ ] **Step 2: Run test — should fail to compile**

Run: `go test ./internal/worker/ -run TestPartitionedShuffleSink -v`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `partitioned_shuffle_sink.go`**

Create `internal/worker/partitioned_shuffle_sink.go`:

```go
package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// partitionedShuffleSink is an exec.Sink that hash-partitions incoming batches
// into N output .wshf files, one per partition. Each partition's writer flushes
// its accumulated rows once a per-partition buffer threshold is reached, so
// peak memory is bounded by (N partitions × flush threshold), independent of
// the total input size.
//
// This is the build-side and probe-side output sink for the shuffle execution
// path. The N output files are uploaded by the executor to S3 under a stable
// per-partition prefix, and downstream join tasks read all files at their
// assigned partition prefix via partitionShardSource.
type partitionedShuffleSink struct {
	spillDir   string
	keys       []string // partition key column names
	numParts   int
	schema     []parquet.Column
	flushBytes int // per-partition row buffer flush threshold

	mu      sync.Mutex
	parts   []*partitionWriter
	closed  bool
	keyIdxs []int // resolved column indices for keys (set on first Consume)
}

type partitionWriter struct {
	file    *os.File
	writer  *shuffleWriter
	rowBuf  *batch.RecordBatch // accumulator
	bufRows int
	numRows int64
}

// flushPartitionBytes is the per-partition accumulator size (in approx bytes
// of row data) at which we flush a chunk to disk. 64 KB keeps memory bounded:
// 4N partitions × 64 KB = ~768 KB per shuffle task at N=3.
const flushPartitionBytes = 64 * 1024

func newPartitionedShuffleSink(spillDir string, keys []string, numParts int, schema []parquet.Column) *partitionedShuffleSink {
	return &partitionedShuffleSink{
		spillDir:   spillDir,
		keys:       keys,
		numParts:   numParts,
		schema:     schema,
		flushBytes: flushPartitionBytes,
		parts:      make([]*partitionWriter, numParts),
	}
}

func (s *partitionedShuffleSink) Init(_ context.Context) error {
	if s.spillDir == "" {
		return fmt.Errorf("partitionedShuffleSink: spillDir empty")
	}
	if err := os.MkdirAll(s.spillDir, 0o755); err != nil {
		return fmt.Errorf("creating spill dir: %w", err)
	}
	for p := 0; p < s.numParts; p++ {
		path := filepath.Join(s.spillDir, fmt.Sprintf("part-%04d.wshf", p))
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("creating partition %d: %w", p, err)
		}
		s.parts[p] = &partitionWriter{file: f}
	}
	return nil
}

// Consume hash-partitions each row in b into its target partition, appending
// to that partition's row buffer. Buffers are flushed when they exceed
// flushBytes worth of accumulated rows.
func (s *partitionedShuffleSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.keyIdxs) == 0 {
		// Resolve key column indices from schema on first batch.
		s.keyIdxs = make([]int, len(s.keys))
		for i, k := range s.keys {
			idx := -1
			for j, col := range b.Schema {
				if col.Name == k {
					idx = j
					break
				}
			}
			if idx < 0 {
				return fmt.Errorf("partitioned shuffle: key %q not in schema", k)
			}
			s.keyIdxs[i] = idx
		}
	}

	n := b.ActiveLen()
	for i := 0; i < n; i++ {
		row := i
		if b.Sel != nil {
			row = b.Sel[i]
		}
		p := s.partitionFor(b, row)
		if err := s.appendRow(p, b, row); err != nil {
			return err
		}
	}
	return s.flushIfNeeded()
}

// partitionFor computes hash(b.Columns[keyIdxs][row]) % numParts.
func (s *partitionedShuffleSink) partitionFor(b *batch.RecordBatch, row int) int {
	h := fnv.New64a()
	var buf [8]byte
	for _, idx := range s.keyIdxs {
		// Hash the column value at row. Implementation depends on column type;
		// a small dispatch on schema type covers our supported keys
		// (Int32, Int64, String, Bytes — the join-key types in TPC-H).
		hashColValue(h, b.Columns[idx], row, buf[:])
	}
	return int(h.Sum64() % uint64(s.numParts))
}

// appendRow copies the row into partition p's row buffer.
// (Implementation copies typed column values into a per-partition RecordBatch.
// The minimal correct version allocates per-partition single-row scratch
// batches and uses batch.AppendRow; optimization can come later.)
func (s *partitionedShuffleSink) appendRow(p int, b *batch.RecordBatch, row int) error {
	pw := s.parts[p]
	if pw.rowBuf == nil {
		pw.rowBuf = batch.NewRecordBatch(b.Schema)
	}
	if err := pw.rowBuf.AppendRow(b, row); err != nil {
		return err
	}
	pw.bufRows++
	return nil
}

// flushIfNeeded writes any partition whose buffer exceeds the flush threshold.
func (s *partitionedShuffleSink) flushIfNeeded() error {
	for p, pw := range s.parts {
		if pw.rowBuf == nil {
			continue
		}
		if pw.rowBuf.EstimatedBytes() < s.flushBytes {
			continue
		}
		if err := s.flushPartition(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *partitionedShuffleSink) flushPartition(p int) error {
	pw := s.parts[p]
	if pw.rowBuf == nil || pw.bufRows == 0 {
		return nil
	}
	if pw.writer == nil {
		pw.writer = newShuffleWriter(pw.file, s.schema)
		if err := pw.writer.writeHeader(); err != nil {
			return fmt.Errorf("partition %d header: %w", p, err)
		}
	}
	if err := pw.writer.writeChunk(pw.rowBuf.Columns, pw.rowBuf.Sel, pw.bufRows); err != nil {
		return fmt.Errorf("partition %d chunk: %w", p, err)
	}
	pw.numRows += int64(pw.bufRows)
	pw.rowBuf.Reset()
	pw.bufRows = 0
	return nil
}

func (s *partitionedShuffleSink) Finalize(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p := range s.parts {
		if err := s.flushPartition(p); err != nil {
			return err
		}
	}
	for p, pw := range s.parts {
		if pw.writer == nil {
			// Empty partition — leave file at zero bytes; downstream treats as no rows.
			if err := pw.file.Sync(); err != nil {
				return err
			}
			continue
		}
		// Patch chunk count in header (mirrors shuffleStreamSink.Finalize).
		if _, err := pw.file.Seek(4, 0); err != nil {
			return fmt.Errorf("partition %d seek: %w", p, err)
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], pw.writer.numChunks)
		if _, err := pw.file.Write(buf[:]); err != nil {
			return fmt.Errorf("partition %d patch: %w", p, err)
		}
		if _, err := pw.file.Seek(0, 2); err != nil {
			return err
		}
		if err := pw.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (s *partitionedShuffleSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, pw := range s.parts {
		if pw != nil && pw.file != nil {
			_ = pw.file.Close()
		}
	}
	return nil
}

// PartitionFiles returns the local file paths for each partition (index = partition id).
// Empty string for a partition that received zero rows (and thus has no usable file).
func (s *partitionedShuffleSink) PartitionFiles() []string {
	out := make([]string, s.numParts)
	for p, pw := range s.parts {
		if pw == nil || pw.numRows == 0 {
			continue
		}
		out[p] = pw.file.Name()
	}
	return out
}

// hashColValue mixes a single column value at the given row into h.
// Supports the join-key types we encounter in TPC-H/SIEM workloads.
func hashColValue(h interface{ Write([]byte) (int, error) }, col batch.Vector, row int, scratch []byte) {
	switch v := col.(type) {
	case *batch.Int32Vector:
		binary.LittleEndian.PutUint32(scratch[:4], uint32(v.Get(row)))
		_, _ = h.Write(scratch[:4])
	case *batch.Int64Vector:
		binary.LittleEndian.PutUint64(scratch[:8], uint64(v.Get(row)))
		_, _ = h.Write(scratch[:8])
	case *batch.StringVector:
		_, _ = h.Write([]byte(v.Get(row)))
	case *batch.BytesVector:
		_, _ = h.Write(v.Get(row))
	default:
		// Unsupported key type — should be caught by planner before dispatch.
		// Hash zero so partition assignment is deterministic but skewed.
		_, _ = h.Write([]byte{0})
	}
}
```

> Note: This file references `batch.AppendRow`, `batch.RecordBatch.Reset()`, `batch.RecordBatch.EstimatedBytes()`, and `batch.NewRecordBatch`. If any of these helpers don't exist in the codebase, add minimal implementations in `internal/engine/batch/` as part of this task — do NOT skip and assume. Look at how `shuffleStreamSink` writes batches (`writeChunk(b.Columns, b.Sel, nRows)`) for the existing pattern, and follow it; the AppendRow/Reset helpers may already exist under different names.

- [ ] **Step 4: Iterate until tests pass**

Run: `go test ./internal/worker/ -run TestPartitionedShuffleSink -v`
Expected: PASS. If a helper signature mismatch — adjust to match existing batch package API.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/partitioned_shuffle_sink.go internal/worker/partitioned_shuffle_sink_test.go
git commit -m "feat(worker): partitioned shuffle sink writes N output .wshf files"
```

---

## Task 5: Build the partition shard source

The downstream side of the shuffle: a Source that reads ALL `.wshf` files at a given S3 partition prefix and streams batches.

**Files:**
- Create: `internal/worker/partition_shard_source.go`
- Modify: `internal/worker/stream_source.go` (only if a helper needs to be exported — check first)

- [ ] **Step 1: Read existing stream_source.go to see the pattern**

Run: `wc -l internal/worker/stream_source.go && head -80 internal/worker/stream_source.go`
Expected: Confirms the pattern used by `cachedFileStreamSource` (which already reads `.wshf` files from S3).

- [ ] **Step 2: Implement `partition_shard_source.go`**

Create `internal/worker/partition_shard_source.go`:

```go
package worker

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// partitionShardSource is an exec.Source that reads all .wshf files at a
// given S3 prefix (one shuffle partition's contribution from every upstream
// task) and streams batches to the downstream operator. Files are read
// sequentially; each file is fully drained before the next one starts.
//
// Used by the join executor on the probe side of a shuffled join: each
// worker is assigned a contiguous slice of partitions and instantiates one
// partitionShardSource per assigned partition.
type partitionShardSource struct {
	store    objstore.Store
	bucket   string
	prefix   string // e.g. "shuffle/<query>/<stage>/partition=07/"
	files    []string
	current  *cachedFileStreamSource // reuses existing single-file reader
	idx      int
	initOnce bool
}

func newPartitionShardSource(store objstore.Store, bucket, prefix string) *partitionShardSource {
	return &partitionShardSource{store: store, bucket: bucket, prefix: prefix}
}

func (s *partitionShardSource) Init(ctx context.Context) error {
	if s.initOnce {
		return nil
	}
	s.initOnce = true
	files, err := s.store.List(ctx, s.bucket, s.prefix)
	if err != nil {
		return fmt.Errorf("listing partition prefix %q: %w", s.prefix, err)
	}
	s.files = files
	return nil
}

func (s *partitionShardSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		if s.current == nil {
			if s.idx >= len(s.files) {
				return nil, nil // exhausted
			}
			s.current = newCachedFileStreamSource(s.store, s.bucket, s.files[s.idx])
			if err := s.current.Init(ctx); err != nil {
				return nil, err
			}
			s.idx++
		}
		b, err := s.current.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
		// Current file drained; close and advance.
		_ = s.current.Close()
		s.current = nil
	}
}

func (s *partitionShardSource) Close() error {
	if s.current != nil {
		return s.current.Close()
	}
	return nil
}

// Compile-time check.
var _ exec.Source = (*partitionShardSource)(nil)
```

> If `cachedFileStreamSource` is unexported and its constructor signature differs, adjust the constructor call. The intent is to delegate single-file reading to the existing reader.

- [ ] **Step 3: Add a basic round-trip test**

In `internal/worker/partition_shard_source_test.go`:

```go
package worker

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func TestPartitionShardSource_ReadsAllFiles(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	prefix := "shuffle/q1/s0/partition=03/"

	// Stage two .wshf files at the prefix using the partitioned sink path.
	dir := t.TempDir()
	// (Build two small .wshf files via shuffleStreamSink directly; upload to memstore.)
	// ... omitted for brevity — see partitioned_shuffle_sink_test for fixture-build ...

	src := newPartitionShardSource(store, bucket, prefix)
	if err := src.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer src.Close()

	rows := 0
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		rows += b.ActiveLen()
	}
	if rows == 0 {
		t.Fatal("expected non-zero rows from 2 staged files")
	}
}
```

> The fixture-staging code is intentionally elided here. When implementing, use `shuffleStreamSink` to write a small `.wshf` to local disk, then upload to the `MemStore` via `store.Put(...)`. Keep the test under 80 lines.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/worker/ -run TestPartitionShardSource -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/partition_shard_source.go internal/worker/partition_shard_source_test.go
git commit -m "feat(worker): partitionShardSource reads .wshf files at a shuffle partition prefix"
```

---

## Task 6: Worker executor handles `TaskTypeShuffle`

**Files:**
- Modify: `internal/worker/executor.go:167` (add switch case)
- Modify: `internal/worker/executor.go` (add `executeShuffle` function)

- [ ] **Step 1: Add the switch case**

In `internal/worker/executor.go`, find the `switch task.Type` block (around line 167) and add:

```go
switch task.Type {
case distributed.TaskTypePipeline:
    // ... existing ...
case distributed.TaskTypeShuffle:
    return e.executeShuffle(ctx, task)
default:
    return nil, fmt.Errorf("unknown task type: %s", task.Type)
}
```

- [ ] **Step 2: Implement `executeShuffle`**

Add a new function in `internal/worker/executor.go`:

```go
// executeShuffle reads input data (either source files via SQLText projection,
// or pre-staged shuffle inputs via task.Files), hash-partitions on
// task.ShuffleKeys into task.NumPartitions output .wshf files, and uploads
// each non-empty partition to S3 under:
//   <ResultBucket>/<ResultPrefix>/partition=<NNNN>/<TaskID>.wshf
//
// On success, ResultNotification.ResultFiles contains one entry per non-empty
// partition: "<ResultPrefix>/partition=<NNNN>/<TaskID>.wshf".
func (e *Executor) executeShuffle(ctx context.Context, task *distributed.Task) (*distributed.ResultNotification, error) {
	if len(task.ShuffleKeys) == 0 {
		return nil, fmt.Errorf("shuffle task missing ShuffleKeys")
	}
	if task.NumPartitions <= 0 {
		return nil, fmt.Errorf("shuffle task NumPartitions must be > 0")
	}

	// Build a Source that reads the assigned input. Two cases:
	//   1. task.SQLText set → execute SQL pipeline, collect output stream as input
	//      (used for the build-side of a shuffle: full table scan with optional projection)
	//   2. task.Files set → read those .wshf files directly
	//      (used when shuffle input is itself a previous stage's output)
	src, schema, err := e.buildShuffleInputSource(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("building shuffle input: %w", err)
	}
	defer src.Close()

	spillDir := e.spillDirFor(task.QueryID, task.ID)
	sink := newPartitionedShuffleSink(spillDir, task.ShuffleKeys, task.NumPartitions, schema)
	if err := sink.Init(ctx); err != nil {
		return nil, fmt.Errorf("sink init: %v", err)
	}
	defer sink.Close()

	// Pump batches from source through the sink.
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("reading shuffle input: %w", err)
		}
		if b == nil {
			break
		}
		if err := sink.Consume(ctx, b); err != nil {
			return nil, fmt.Errorf("partitioning batch: %w", err)
		}
	}
	if err := sink.Finalize(ctx); err != nil {
		return nil, fmt.Errorf("sink finalize: %w", err)
	}

	// Upload each non-empty partition file to S3.
	resultFiles := make([]string, 0, task.NumPartitions)
	var totalBytes int64
	for p, localPath := range sink.PartitionFiles() {
		if localPath == "" {
			continue
		}
		key := fmt.Sprintf("%s/partition=%04d/%s.wshf", task.ResultPrefix, p, task.ID)
		size, upErr := e.uploadFile(ctx, task.ResultBucket, key, localPath)
		if upErr != nil {
			return nil, fmt.Errorf("upload partition %d: %w", p, upErr)
		}
		resultFiles = append(resultFiles, key)
		totalBytes += size
		// Best-effort cleanup of local spill.
		_ = os.Remove(localPath)
	}

	return &distributed.ResultNotification{
		TaskID:      task.ID,
		QueryID:     task.QueryID,
		StageID:     task.StageID,
		WorkerID:    e.workerID,
		Success:     true,
		ResultFiles: resultFiles,
		SizeBytes:   totalBytes,
		Timestamp:   time.Now(),
	}, nil
}
```

> `buildShuffleInputSource`, `spillDirFor`, and `uploadFile` are stubs — wire them to whatever the existing executor uses for the same primitives (look at how `executePipeline` reads input + uploads results). Keep this function focused on the shuffle-specific orchestration.

- [ ] **Step 3: Add a unit test for the executor's shuffle path using MemStore**

Create `internal/worker/executor_shuffle_test.go` with one happy-path test that:
1. Stages a small input file on `MemStore`.
2. Constructs a `Task` with `Type=TaskTypeShuffle`, `Files=[that file]`, `ShuffleKeys=["k"]`, `NumPartitions=4`.
3. Calls `executor.Execute(ctx, task)`.
4. Asserts `ResultFiles` contains files and the union of all partition file row counts equals input row count.

(Mirror the existing test pattern in `executor_pipeline_test.go`.)

- [ ] **Step 4: Run tests**

Run: `go test ./internal/worker/ -run Shuffle -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/executor.go internal/worker/executor_shuffle_test.go
git commit -m "feat(worker): execute TaskTypeShuffle by hash-partitioning to N output files"
```

---

## Task 7: Local memory-pressure repro (Validation Gate 0)

A standalone test that reproduces the broadcast-duplication OOM signature locally so we can iterate without burning EC2 dollars.

**Files:**
- Create: `benchmarks/local/broadcast_pressure_test.go`

- [ ] **Step 1: Confirm `benchmarks/local/` doesn't exist yet**

Run: `ls benchmarks/`
Expected: shows `security/` and `tpch/`. We're creating a new directory.

- [ ] **Step 2: Create the test**

Create `benchmarks/local/broadcast_pressure_test.go`:

```go
//go:build pressure
// +build pressure

// Package local contains memory-pressure repros that intentionally allocate
// large amounts of memory under tight GOMEMLIMIT to validate that
// broadcast-replacement code paths actually reduce peak heap.
//
// Build tag `pressure` keeps these out of normal `go test ./...` runs; they
// are explicitly invoked via:
//
//   GOMEMLIMIT=8GiB go test -tags=pressure -v ./benchmarks/local/ -run BroadcastPressure -timeout 10m
package local

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/citc-tech/wadjet"
)

// TestBroadcastPressure_OrdersHashOOM_ReproducesSignature loads the SF100-sample
// orders table into a hash table TWICE concurrently within a single process
// under GOMEMLIMIT=8 GiB. With the legacy broadcast path, two copies of the
// orders hash should push the process near its memory limit. With the shuffled
// build path (lowered shuffleBuildThreshold), each "worker" only loads its
// shard, peak heap stays bounded.
//
// This test is the local gate before any EC2 deploy of the shuffle change.
func TestBroadcastPressure_OrdersHashOOM_ReproducesSignature(t *testing.T) {
	if testing.Short() {
		t.Skip("pressure repro skipped in -short mode")
	}

	// Use SF100-sample orders table from the public test bucket.
	// (Path/data setup adapted from existing TPC-H bench helpers.)
	tableDir := requireSF100SampleOrders(t)

	db, err := wadjet.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.AttachParquet(context.Background(), "orders", tableDir); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// Sample peak heap throughout the run.
	var peakHeapBytes atomic.Uint64
	stopSampler := make(chan struct{})
	go func() {
		var ms runtime.MemStats
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peakHeapBytes.Load() {
					peakHeapBytes.Store(ms.HeapAlloc)
				}
			}
		}
	}()

	// Run two concurrent SELECT * FROM orders queries that fully materialize
	// each row into a hash on o_orderkey — mimicking the build-side of a join.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, qerr := db.Query(context.Background(),
				`SELECT o_orderkey, o_custkey, o_orderdate FROM orders`)
			if qerr != nil {
				t.Logf("query error (expected under tight limit): %v", qerr)
			}
		}()
	}
	wg.Wait()
	close(stopSampler)

	gomemlimit := debug.SetMemoryLimit(-1) // read current
	peak := peakHeapBytes.Load()
	t.Logf("peak heap: %d MiB; GOMEMLIMIT: %d MiB", peak/(1<<20), gomemlimit/(1<<20))

	// With shuffled build: peak should stay below ~50% of GOMEMLIMIT.
	// With broadcast: peak typically pushes near the limit (or OOMs).
	// We don't fail on the threshold here — this test's value is the heap
	// number it prints, used to compare before/after a shuffle change.
}
```

- [ ] **Step 3: Run the baseline (BEFORE the shuffle path is wired)**

Run: `GOMEMLIMIT=8GiB go test -tags=pressure -v ./benchmarks/local/ -run BroadcastPressure -timeout 10m 2>&1 | tee /tmp/pressure-before.txt`
Expected: Test runs, logs peak heap. We want this to be high (close to GOMEMLIMIT).

> If the SF100 sample is too big to download here, scale to SF10 or a synthetic 5 GB table — the goal is to reproduce the *signature*, not match SF100 exactly.

- [ ] **Step 4: Commit (no shuffle wiring yet — we'll re-run after Tasks 8-12)**

```bash
git add benchmarks/local/broadcast_pressure_test.go
git commit -m "test(local): broadcast-pressure repro for shuffle validation gate"
```

---

## Task 8: Planner: identify shuffle candidate and shape the shuffled plan

**Files:**
- Modify: `internal/planner/physical/plan.go` (add `InsertShuffleForLargeBuild`)
- Create: `internal/planner/physical/shuffle_insertion_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/planner/physical/shuffle_insertion_test.go`:

```go
package physical

import "testing"

func TestInsertShuffleForLargeBuild_PicksLargestBuildAboveThreshold(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 8 << 30},          // 8 GB - above
		{ID: "scan-customer", Type: "scan", ScanAlias: "customer", EstimatedBytes: 100 << 20},     // 100 MB - below
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},      // 80 GB - probe
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "orders", JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"}, Dependencies: []string{"scan-orders", "scan-lineitem"}},
		{ID: "join-2", Type: "hash_join", BuildTableAlias: "customer", JoinLeftKeys: []string{"o_custkey"}, JoinRightKeys: []string{"c_custkey"}, Dependencies: []string{"join-1", "scan-customer"}},
	}
	cand, ok := PickShuffleCandidate(stages, 4<<30 /* threshold */)
	if !ok {
		t.Fatal("expected shuffle candidate")
	}
	if cand.BuildAlias != "orders" {
		t.Errorf("BuildAlias = %q, want orders", cand.BuildAlias)
	}
	if cand.ProbeAlias != "lineitem" {
		t.Errorf("ProbeAlias = %q, want lineitem", cand.ProbeAlias)
	}
	if len(cand.JoinKeys) != 1 || cand.JoinKeys[0] != "o_orderkey" {
		t.Errorf("JoinKeys = %v, want [o_orderkey]", cand.JoinKeys)
	}
}

func TestInsertShuffleForLargeBuild_NoCandidateBelowThreshold(t *testing.T) {
	stages := []Stage{
		{ID: "scan-orders", Type: "scan", ScanAlias: "orders", EstimatedBytes: 1 << 30},  // 1 GB - below 4 GB threshold
		{ID: "scan-lineitem", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 80 << 30},
		{ID: "join-1", Type: "hash_join", BuildTableAlias: "orders", JoinLeftKeys: []string{"l_orderkey"}, JoinRightKeys: []string{"o_orderkey"}},
	}
	if _, ok := PickShuffleCandidate(stages, 4<<30); ok {
		t.Error("expected no shuffle candidate below threshold")
	}
}
```

- [ ] **Step 2: Run test — should fail**

Run: `go test ./internal/planner/physical/ -run InsertShuffle -v`
Expected: FAIL — `PickShuffleCandidate` undefined.

- [ ] **Step 3: Implement `PickShuffleCandidate`**

Add to `internal/planner/physical/plan.go`:

```go
// ShuffleCandidate describes a join in the plan whose build side is large
// enough to warrant the shuffle execution path instead of broadcast.
type ShuffleCandidate struct {
	JoinStageID string   // the join stage to be served by shuffled inputs
	BuildAlias  string   // which scan stage produces the build side
	ProbeAlias  string   // which scan stage produces the probe side (the largest scan)
	BuildKeys   []string // build-side join keys (the JoinRightKeys of the join)
	ProbeKeys   []string // probe-side join keys (the JoinLeftKeys of the join)
	JoinKeys    []string // canonical (build-side) join key names for partitioning
	BuildBytes  int64    // EstimatedBytes of the build scan (for logging)
}

// PickShuffleCandidate finds the join in `stages` whose build-side scan has
// the largest EstimatedBytes above `thresholdBytes`. Returns ok=false if no
// build exceeds the threshold.
//
// Phase 1 only: returns a single candidate. Phase 2 will return all candidates
// for chained shuffles.
func PickShuffleCandidate(stages []Stage, thresholdBytes int64) (ShuffleCandidate, bool) {
	// Build alias → scan stage lookup.
	byAlias := map[string]Stage{}
	for _, s := range stages {
		if s.Type == "scan" && s.ScanAlias != "" {
			byAlias[s.ScanAlias] = s
		}
	}

	// Find largest probe (largest scan, period — the one CanProbeSplit would pick).
	var probeAlias string
	var probeBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > probeBytes {
			probeAlias = s.ScanAlias
			probeBytes = s.EstimatedBytes
		}
	}

	var best ShuffleCandidate
	var bestBytes int64
	for _, s := range stages {
		if s.Type != "hash_join" {
			continue
		}
		buildAlias := s.BuildTableAlias
		if buildAlias == "" || buildAlias == probeAlias {
			continue
		}
		buildScan, ok := byAlias[buildAlias]
		if !ok {
			continue
		}
		if buildScan.EstimatedBytes <= thresholdBytes {
			continue
		}
		if buildScan.EstimatedBytes > bestBytes {
			best = ShuffleCandidate{
				JoinStageID: s.ID,
				BuildAlias:  buildAlias,
				ProbeAlias:  probeAlias,
				BuildKeys:   append([]string(nil), s.JoinRightKeys...),
				ProbeKeys:   append([]string(nil), s.JoinLeftKeys...),
				JoinKeys:    append([]string(nil), s.JoinRightKeys...),
				BuildBytes:  buildScan.EstimatedBytes,
			}
			bestBytes = buildScan.EstimatedBytes
		}
	}
	return best, bestBytes > 0
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/planner/physical/ -run InsertShuffle -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/planner/physical/plan.go internal/planner/physical/shuffle_insertion_test.go
git commit -m "feat(planner): PickShuffleCandidate identifies large-build joins above threshold"
```

---

## Task 9: Coordinator orchestrator — dispatch shuffle stages then probe stage

**Files:**
- Create: `internal/coordinator/shuffle_orchestrator.go`

- [ ] **Step 1: Implement `shuffle_orchestrator.go`**

Create `internal/coordinator/shuffle_orchestrator.go`:

```go
package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// shuffleBuildThreshold gates the routing decision: if the largest build
// table's EstimatedBytes exceeds this, route to shuffle-distributed instead
// of probe-split. Declared as var so tests can lower it to exercise the path
// on small data.
var shuffleBuildThreshold int64 = 4 * 1024 * 1024 * 1024 // 4 GB

// shufflePartitionMultiplier sets partition count = workerCount × this. A
// modest multiplier reduces hot-key skew impact while keeping the file
// count bounded. With 3 workers and multiplier=4, we get 12 partitions.
const shufflePartitionMultiplier = 4

// shuffleStageTimeout is the per-shuffle-stage timeout. If exceeded, the
// query fails — there is no per-stage retry in Phase 1.
const shuffleStageTimeout = 10 * time.Minute

// orchestrateShuffleQuery runs a query through the shuffle execution path:
//
//  1. Dispatch a shuffle stage for the build side: each worker reads a slice
//     of build files, hash-partitions on the join keys, writes 4N output
//     .wshf files keyed by partition.
//  2. Dispatch a shuffle stage for the probe side: same, on the probe table.
//  3. Wait for both shuffle stages to complete.
//  4. Dispatch one final pipeline task per worker; each task is assigned a
//     contiguous slice of partitions (e.g., worker 0 → partitions [0..3] when
//     N=3 and numPartitions=12). Each task sequentially loads its assigned
//     partitions' build shards and joins against probe shards.
//  5. Coordinator merges per-worker partials as in the existing probe-split
//     path (re-aggregate / sort / limit).
func (c *Coordinator) orchestrateShuffleQuery(
	ctx context.Context,
	queryID string,
	sql string,
	cand physical.ShuffleCandidate,
	stages []physical.Stage,
	workerCount int,
) error {
	numParts := workerCount * shufflePartitionMultiplier

	buildFiles, probeFiles, err := splitScansForShuffle(stages, cand)
	if err != nil {
		return fmt.Errorf("split scans: %w", err)
	}

	resultPrefix := fmt.Sprintf("queries/%s/shuffle", queryID)

	// Stage 1+2: build-side and probe-side shuffles in parallel.
	buildPrefix := resultPrefix + "/build"
	probePrefix := resultPrefix + "/probe"

	buildTasks := buildShuffleTasks(queryID, "shuffle-build", c.resultBucket, buildPrefix,
		cand.BuildKeys, numParts, workerCount, buildFiles, cand.BuildAlias)
	probeTasks := buildShuffleTasks(queryID, "shuffle-probe", c.resultBucket, probePrefix,
		cand.ProbeKeys, numParts, workerCount, probeFiles, cand.ProbeAlias)

	// Dispatch in parallel.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, batchOfTasks := range [][]distributed.Task{buildTasks, probeTasks} {
		wg.Add(1)
		go func(tasks []distributed.Task) {
			defer wg.Done()
			if err := c.dispatchAndWait(ctx, queryID, tasks, shuffleStageTimeout); err != nil {
				errs <- err
			}
		}(batchOfTasks)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return fmt.Errorf("shuffle stage: %w", err)
		}
	}

	// Stage 3: probe pipeline tasks with partition assignments.
	probeStageTasks := buildProbePipelineTasks(queryID, sql, c.resultBucket,
		buildPrefix, probePrefix, numParts, workerCount, cand)

	if err := c.dispatchAndWait(ctx, queryID, probeStageTasks, shuffleStageTimeout); err != nil {
		return fmt.Errorf("probe stage: %w", err)
	}

	return nil
}

// splitScansForShuffle returns (buildFiles, probeFiles) by looking up the
// scan stages for the candidate's build and probe aliases.
func splitScansForShuffle(stages []physical.Stage, cand physical.ShuffleCandidate) (buildFiles, probeFiles []string, err error) {
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		switch s.ScanAlias {
		case cand.BuildAlias:
			buildFiles = s.ScanFiles
		case cand.ProbeAlias:
			probeFiles = s.ScanFiles
		}
	}
	if len(buildFiles) == 0 {
		return nil, nil, fmt.Errorf("no scan files for build alias %q", cand.BuildAlias)
	}
	if len(probeFiles) == 0 {
		return nil, nil, fmt.Errorf("no scan files for probe alias %q", cand.ProbeAlias)
	}
	return buildFiles, probeFiles, nil
}

// buildShuffleTasks slices `files` evenly into `workerCount` shuffle tasks.
// Each task is responsible for reading its slice and hash-partitioning into
// numParts output files. The task type is TaskTypeShuffle.
func buildShuffleTasks(
	queryID, stageID, bucket, prefix string,
	keys []string,
	numParts, workerCount int,
	files []string,
	tableAlias string,
) []distributed.Task {
	slices := splitFilesEvenly(files, workerCount)
	tasks := make([]distributed.Task, 0, len(slices))
	for i, slice := range slices {
		if len(slice) == 0 {
			continue
		}
		tasks = append(tasks, distributed.Task{
			ID:            fmt.Sprintf("%s-%s-%d", queryID, stageID, i),
			QueryID:       queryID,
			StageID:       stageID,
			Type:          distributed.TaskTypeShuffle,
			TableName:     tableAlias,
			Files:         slice,
			ShuffleKeys:   keys,
			NumPartitions: numParts,
			ResultBucket:  bucket,
			ResultPrefix:  prefix,
			CreatedAt:     time.Now(),
		})
	}
	return tasks
}

// buildProbePipelineTasks creates one TaskTypePipeline task per worker.
// Each task is assigned partitions [w*partsPerWorker, (w+1)*partsPerWorker).
// The pipeline task reads its assigned partitions' shuffled build + probe
// shards and executes the original query SQL with the join's inputs replaced
// by the shard sources.
//
// PartitionID is set to the FIRST partition this worker handles; partitions
// per worker are derived as [PartitionID, PartitionID + numParts/workerCount).
// (Single-int partition assignment fits the existing field; if richer
// assignment is needed, extend the Task message in a follow-up.)
func buildProbePipelineTasks(
	queryID, sql, bucket, buildPrefix, probePrefix string,
	numParts, workerCount int,
	cand physical.ShuffleCandidate,
) []distributed.Task {
	partsPerWorker := numParts / workerCount
	tasks := make([]distributed.Task, 0, workerCount)
	for w := 0; w < workerCount; w++ {
		startPart := w * partsPerWorker
		tasks = append(tasks, distributed.Task{
			ID:           fmt.Sprintf("%s-shuffle-probe-pipeline-%d", queryID, w),
			QueryID:      queryID,
			StageID:      "shuffle-pipeline",
			Type:         distributed.TaskTypePipeline,
			SQLText:      sql,
			ShuffleKeys:  cand.JoinKeys, // signals: this is a shuffled-input pipeline
			NumPartitions: numParts,
			PartitionID:  startPart,
			// Two prefixes encoded in PreScannedInputs by alias for lookup at the worker.
			PreScannedInputs: map[string][]string{
				cand.BuildAlias + "@@shuffle": {bucket + "/" + buildPrefix},
				cand.ProbeAlias + "@@shuffle": {bucket + "/" + probePrefix},
			},
			PartialAggregate: true,
			ResultBucket:     bucket,
			ResultPrefix:     fmt.Sprintf("queries/%s/results", queryID),
			CreatedAt:        time.Now(),
		})
	}
	return tasks
}

// splitFilesEvenly divides files into n contiguous slices of as-equal-as-
// possible size.
func splitFilesEvenly(files []string, n int) [][]string {
	if n <= 0 || len(files) == 0 {
		return nil
	}
	out := make([][]string, n)
	per := (len(files) + n - 1) / n
	for i := 0; i < n; i++ {
		start := i * per
		if start >= len(files) {
			break
		}
		end := start + per
		if end > len(files) {
			end = len(files)
		}
		out[i] = files[start:end]
	}
	return out
}
```

> `c.dispatchAndWait` and `c.resultBucket` may not exist as named — adapt to whatever pattern the existing coordinator uses for "publish tasks and wait for all results" (likely the same machinery used by `preScanBuildTables` for the build cache). Reuse rather than re-invent.

- [ ] **Step 2: Build to verify**

Run: `go build ./...`
Expected: PASS (modulo helper-name adjustments).

- [ ] **Step 3: Add a unit test for `splitFilesEvenly`**

Append to a small test file (or create `shuffle_orchestrator_test.go`):

```go
package coordinator

import "testing"

func TestSplitFilesEvenly(t *testing.T) {
	files := []string{"a", "b", "c", "d", "e", "f", "g"}
	out := splitFilesEvenly(files, 3)
	if len(out) != 3 {
		t.Fatalf("len=%d, want 3", len(out))
	}
	total := 0
	for _, s := range out {
		total += len(s)
	}
	if total != len(files) {
		t.Errorf("total assigned = %d, want %d", total, len(files))
	}
}
```

- [ ] **Step 4: Run**

Run: `go test ./internal/coordinator/ -run SplitFiles -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/shuffle_orchestrator.go internal/coordinator/shuffle_orchestrator_test.go
git commit -m "feat(coordinator): orchestrate two-stage shuffle then partition-assigned probe pipeline"
```

---

## Task 10: Wire the coordinator routing decision

**Files:**
- Modify: `internal/coordinator/coordinator.go:468-509` (the routing block in `ExecuteSQL`)

- [ ] **Step 1: Insert shuffle-routing branch ahead of the existing probe-split branch**

Replace the routing block currently at `coordinator.go:468-509` with:

```go
// Route all queries through pipeline execution. Three modes:
// 1. Shuffle-distributed: when the largest build table exceeds
//    shuffleBuildThreshold (4 GB), partition both sides on the join key
//    and run a co-located join — peak per-worker memory scales 1/N.
// 2. Probe-split: partition probe table files across workers; build tables
//    are broadcast (or cached) — fast for small/medium builds.
// 3. Single-worker: entire query on one worker.
var probeSplitMergeInfo *logical.MergeInfo
probeAlias, probeFiles, canProbeSplit := physical.CanProbeSplit(physStages, c.workers.Count())
mergeInfo := logical.ExtractMergeInfo(logicalPlan)

shuffleCand, shuffleApplicable := physical.PickShuffleCandidate(physStages, shuffleBuildThreshold)

switch {
case shuffleApplicable && mergeInfo != nil:
	c.logger.Info("routing to shuffle-distributed",
		"query", queryID,
		"build_alias", shuffleCand.BuildAlias,
		"build_bytes", shuffleCand.BuildBytes,
		"probe_alias", shuffleCand.ProbeAlias,
		"workers", c.workers.Count(),
		"partitions", c.workers.Count()*shufflePartitionMultiplier)
	probeSplitMergeInfo = mergeInfo
	if err := c.orchestrateShuffleQuery(ctx, queryID, sql, shuffleCand, physStages, c.workers.Count()); err != nil {
		return nil, err
	}
	// Skip the standard pipeline path below — orchestrateShuffleQuery owns
	// dispatch and result gathering.
	physStages = nil

case canProbeSplit && mergeInfo != nil:
	// ... existing probe-split branch unchanged ...

default:
	// ... existing single-worker branch unchanged ...
}
```

> The exact integration with the existing `subscribeResults` / `doneCh` / `readFinalResults` flow needs care. The cleanest pattern: have `orchestrateShuffleQuery` populate the same `queryMetas` entry as probe-split would (so `readFinalResults` picks up the right files), and return after that — letting the existing wait-and-merge code path handle the rest. **Read the full `ExecuteSQL` body before integrating** to confirm where the seam should land.

- [ ] **Step 2: Build**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 3: Run all coordinator tests**

Run: `go test ./internal/coordinator/ -count=1`
Expected: PASS — existing tests should not be affected (they don't trip the 4 GB threshold).

- [ ] **Step 4: Commit**

```bash
git add internal/coordinator/coordinator.go
git commit -m "feat(coordinator): route large-build queries through shuffle path"
```

---

## Task 11: Worker-side handling of shuffled-input pipeline tasks

The probe pipeline task carries `ShuffleKeys`, `NumPartitions`, `PartitionID`, and `PreScannedInputs[alias+"@@shuffle"] = [bucket+prefix]`. The worker needs to recognize this shape and substitute `partitionShardSource` for the build and probe scan inputs.

**Files:**
- Modify: `internal/worker/executor.go` (existing `executePipeline` or its plan-building helper)

- [ ] **Step 1: Identify the seam**

Run: `grep -n "PreScannedInputs\|ScanFileFilter" internal/worker/executor.go`
Expected: shows where the existing executor swaps in pre-scanned inputs for scan operators. We add a parallel branch for the `@@shuffle` suffix.

- [ ] **Step 2: Add shuffled-input substitution**

In the helper that builds the pipeline plan from the task, add:

```go
// If task.ShuffleKeys is set and PreScannedInputs contains @@shuffle entries,
// this is a shuffled-input pipeline. For each scan alias whose key
// "<alias>@@shuffle" appears in PreScannedInputs, replace the scan source
// with a sequential partition-shard reader covering this worker's assigned
// partitions: [PartitionID, PartitionID + NumPartitions/workerCount).
if len(task.ShuffleKeys) > 0 {
	partsPerWorker := task.NumPartitions / e.workerCount
	for alias, prefixes := range task.PreScannedInputs {
		if !strings.HasSuffix(alias, "@@shuffle") {
			continue
		}
		realAlias := strings.TrimSuffix(alias, "@@shuffle")
		bucketPrefix := prefixes[0] // "<bucket>/<prefix>"
		bucket, prefix := splitBucketPrefix(bucketPrefix)
		sources := make([]exec.Source, 0, partsPerWorker)
		for p := task.PartitionID; p < task.PartitionID+partsPerWorker; p++ {
			partPrefix := fmt.Sprintf("%s/partition=%04d/", prefix, p)
			sources = append(sources, newPartitionShardSource(e.objStore, bucket, partPrefix))
		}
		// Concatenate sources sequentially so memory holds at most one
		// partition's hash table at a time.
		plan.SubstituteScanSource(realAlias, exec.Concat(sources...))
	}
}
```

> `SubstituteScanSource` and `exec.Concat` may not exist verbatim — locate the existing primitive used to inject `PreScannedInputs` files into the plan and use it. The intent is: replace the source for `realAlias` with a sequential reader over this worker's assigned partition shards.

- [ ] **Step 3: Add a small integration test**

In `internal/worker/executor_shuffle_test.go`, add a test that:
1. Stages partitioned build and probe shards in `MemStore` under fake prefixes (use `partitionedShuffleSink` to produce them).
2. Constructs a pipeline task with `ShuffleKeys`, `NumPartitions=4`, `PartitionID=0`, `PreScannedInputs` mapping aliases to prefixes.
3. Runs the executor; asserts results match a baseline non-shuffled join over the same input rows.

- [ ] **Step 4: Run**

Run: `go test ./internal/worker/ -run Shuffle -v -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/executor.go internal/worker/executor_shuffle_test.go
git commit -m "feat(worker): substitute partition-shard sources for shuffled-input pipeline tasks"
```

---

## Task 12: SF0.01 integration test forcing the shuffle path

**Files:**
- Modify: `internal/coordinator/distributed_tpch_test.go`

- [ ] **Step 1: Add a Q03 shuffle-forced test**

Append to `distributed_tpch_test.go`:

```go
// TestDistributedTPCH_Q03_ShuffleForced runs Q03 at SF0.01 with
// shuffleBuildThreshold lowered to 1 byte, forcing the shuffle path.
// Validates correctness end-to-end: planner picks the candidate, both
// shuffle stages dispatch, partitioned shards are produced and consumed,
// and the merged result matches the expected SF0.01 Q03 output.
func TestDistributedTPCH_Q03_ShuffleForced(t *testing.T) {
	prev := shuffleBuildThreshold
	shuffleBuildThreshold = 1
	t.Cleanup(func() { shuffleBuildThreshold = prev })

	// Reuse the existing SF0.01 distributed harness setup; mirror an
	// existing Q03 test in this file. Assert row count and first-row values
	// against the same baseline.
	// ... (full test body mirrors existing TPC-H distributed test pattern) ...
}
```

> Implement by copying the existing `TestDistributedTPCH_Q03` (or equivalent) test in this file and prepending the threshold flip. The shuffle threshold flip + cleanup is the only behavioral change.

- [ ] **Step 2: Run all 22 queries with the threshold lowered**

Optionally add a parametric variant that runs all 22 queries with `shuffleBuildThreshold = 1` to confirm no correctness regression on the shuffled path. Mark as `t.Parallel()` only if existing distributed tests are parallel-safe.

- [ ] **Step 3: Run**

Run: `go test ./internal/coordinator/ -run ShuffleForced -v -timeout 5m`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/coordinator/distributed_tpch_test.go
git commit -m "test(coordinator): SF0.01 integration test forces shuffle path for correctness gate"
```

---

## Task 13: Re-run the local memory-pressure repro and confirm peak heap drop

**Files:**
- (No file changes — verification step)

- [ ] **Step 1: Run the pressure test against the new shuffle path**

Run: `GOMEMLIMIT=8GiB go test -tags=pressure -v ./benchmarks/local/ -run BroadcastPressure -timeout 10m 2>&1 | tee /tmp/pressure-after.txt`

> Note: the pressure test as written in Task 7 calls into the embedded `wadjet` API with a single-process scenario. To exercise the shuffle path locally, the test needs to either:
>   (a) run a 3-process in-process cluster (mirror the harness in `distributed_tpch_test.go`), or
>   (b) be augmented with a build-side concurrency gating that simulates the broadcast vs sharded difference.
> Pick whichever is closer to the existing local test scaffolding. The test's value is the printed peak-heap number, which we compare:

- [ ] **Step 2: Compare before/after**

Run: `diff <(grep "peak heap" /tmp/pressure-before.txt) <(grep "peak heap" /tmp/pressure-after.txt)`
Expected: "after" peak heap is substantially lower than "before" — ideally < 50% of GOMEMLIMIT.

- [ ] **Step 3: If the drop is NOT visible, STOP. Do not deploy to EC2.**

The whole point of Gate 0 is to confirm locally that the change reduces peak memory. If it doesn't, debug here — re-check the partition-shard substitution at the worker, the partitioning function, and the sequential-vs-concurrent partition processing.

- [ ] **Step 4: Commit the captured before/after numbers as part of the PR description (no file commit needed)**

---

## Task 14: SF10 EC2 distributed gate (Gate 3)

**Files:**
- (No file changes — deploy + run)

- [ ] **Step 1: Build the bench binary locally and upload to S3**

Per `feedback_benchmark_consistency.md` and `feedback_deploy_binary.md`:

```bash
GOOS=linux GOARCH=arm64 go build -o tpch-bench ./cmd/tpch-bench
aws --profile citc s3 cp tpch-bench s3://wadjet-bench-sf10-use2/bin/latest/tpch-bench
```

- [ ] **Step 2: Deploy the standard SF10 cluster**

Per `feedback_benchmark_consistency.md`: coordinator c7g.2xlarge + 3× c7g.4xlarge workers, us-east-2, bucket `wadjet-bench-sf10-use2`.

```bash
cd deploy/benchmark/terraform
tofu apply -var bin_version=latest -var data_bucket=wadjet-bench-sf10-use2
```

- [ ] **Step 3: Run all 22 queries**

```bash
ssh-into-coordinator
./tpch-bench --scale=10 --queries=all 2>&1 | tee /tmp/sf10-shuffle.txt
```

- [ ] **Step 4: Tear down regardless of outcome**

```bash
tofu destroy -auto-approve
```

- [ ] **Step 5: Validate**

- All 22 queries return >0 rows
- Q03/Q05/Q07 took the shuffle path (grep coordinator log for `routing to shuffle-distributed`)
- Wall-clock for shuffle-routed queries within 2× of the pre-change baseline

If any query regresses or returns 0 rows, do NOT proceed to SF100. Debug locally first.

---

## Task 15: SF100 final validation (Gate 4)

**Files:**
- (No file changes — deploy + run)

- [ ] **Step 1: Run Q03 in isolation first**

```bash
# After deploying the SF100 cluster (per feedback_benchmark_consistency.md)
SKIP_QUERIES=1,2,4-22 ./tpch-bench --scale=100 2>&1 | tee /tmp/sf100-q03-shuffle.txt
```

Expected: Q03 completes (was OOMing at 26 min on prior runs).

- [ ] **Step 2: If Q03 passes, run all 22**

```bash
./tpch-bench --scale=100 --queries=all 2>&1 | tee /tmp/sf100-all-shuffle.txt
```

- [ ] **Step 3: Tear down**

```bash
tofu destroy -auto-approve
```

- [ ] **Step 4: Update memory**

Save a project memory recording the SF100 shuffle results. Replace `project_sf100_q03_broadcast_duplication_2026-04-18.md` with an updated status note.

---

## Self-Review Checklist (run before declaring plan complete)

**Spec coverage:** ✓ Each spec section maps to one or more tasks above:
- Architecture (Distribution, shuffle stage, co-located join) → Tasks 2, 3, 4, 5, 6, 9, 11
- Routing decision → Task 10
- Validation gates 0–4 → Tasks 7, 12, 13, 14, 15
- Phase 2 explicitly out of scope → respected (no chained-shuffle code)

**Placeholder scan:** Several tasks contain notes like "look at how the existing executor uses X" rather than full code. These are deliberate references-to-existing-pattern, not TODOs — the implementer must read those files. Flagged here so the implementer knows to expect this.

**Type consistency:**
- `Distribution.Kind`, `DistKind`, `DistSingleton`/`DistBroadcast`/`DistHashPartitioned` — used consistently
- `ShuffleCandidate` fields (`BuildAlias`, `ProbeAlias`, `JoinKeys`, `BuildKeys`, `ProbeKeys`, `JoinStageID`, `BuildBytes`) — consistent across Tasks 8, 9, 10
- `shuffleBuildThreshold` (var) — consistent across Tasks 9, 10, 12
- `partitionedShuffleSink` / `partitionShardSource` / `executeShuffle` — consistent across Tasks 4, 5, 6, 11
- `TaskTypeShuffle` — Tasks 1, 6, 9
- `ShuffleKeys`, `NumPartitions`, `PartitionID`, `ResultFiles`, `PreScannedInputs[alias+"@@shuffle"]` — consistent

**Open implementation choices documented inline (not blockers):**
- Exact name of the source-substitution primitive (`SubstituteScanSource`) — to be matched to whatever the existing codebase uses for `PreScannedInputs`.
- Helper functions in coordinator (`dispatchAndWait`, `resultBucket`) — to be matched to existing coordinator state machine.
- Local pressure test — single-process vs in-process 3-worker cluster scaffolding choice noted in Task 13.

These are intentional — the implementer reads the surrounding code in those files and matches existing patterns rather than inventing new names.
