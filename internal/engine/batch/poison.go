package batch

import "sync/atomic"

// Poison-on-release: the adversarial half of the batch-reuse contract.
//
// A pooled batch's storage is UNDEFINED the moment Release() hands it back.
// Every operator that keeps a value past the call it arrived on is required
// to own it — Detach() (which severs the pool link and claims every column)
// or a deep copy. The contract is stated at (*RecordBatch).Detach and repeated
// at each `b.Detach() // prevent pool recycle` site in package exec.
//
// Nothing enforced it. Whether a violation produces a wrong answer depended on
// whether the next batch happened to write over the same bytes with something
// different — so the failure was data-dependent, invisible at small scale, and
// invisible to every corpus gate. (*Vector).GetValue's TypeBytes arm returns a
// slice ALIASING the column arena; MIN_BY over a BYTES column retains it; no
// gate could see it because no corpus has a top-level BYTES column.
//
// Poison mode makes the undefined behaviour DEFINED and LOUD: on Release, every
// unclaimed value arena in the batch is overwritten with a recognisable pattern
// before the batch reaches the pool. A retained alias then reads 0xA5 bytes and
// -1 scalars instead of the values it thought it held, and the query answers
// differently from the same query with poison off. Comparing the two runs is
// the gate; see TestTypeMatrixBatchReuse in package wadjet.
//
// Fairness. Poison writes exactly where a real recycle is free to write:
//
//   - Only when b.pool != nil. Detach() and DetachPool() both nil the pool, so
//     a batch whose shell anybody claimed is never poisoned.
//   - Never through a view (Base != nil). A view owns no storage; writing
//     through it would hit a base that may not be pooled at all.
//   - Only the VALUE arenas. Offsets, null bitmaps and nested shape metadata
//     are rewritten wholesale by Reset/resetVectorForReuse, so scribbling them
//     would model a recycle that cannot happen.
//
// A per-VECTOR claim (Vector.Claim) does NOT exempt a column, deliberately.
// Nothing in the pool honours it: resetVectorForReuse clears claimed and writes
// over the arena on the next Get, on the stated assumption that "a batch only
// reaches a pool when nobody claimed it". That assumption does not hold —
// partitioned aggregation's selView calls Detach (which claims every shared
// column) on a view whose underlying pooled batch sharedBatch.release() then
// returns to the pool. Exempting claimed columns would model the invariant the
// engine documents; poisoning them models what the allocator actually does,
// which is the behaviour a gate has to be able to see.
//
// Cost when off: one relaxed atomic load per batch — per 2048 rows, not per
// row — and no writes. It is a correctness instrument, not a debug build:
// keeping it in the normal build is what lets a gate flip it on around a query
// and off again, which is the only way to compare the two runs in one process.
var poisonOnRelease atomic.Bool

// poisonByte is the fill for byte arenas. 0xA5 is not valid UTF-8 on its own,
// is not 0x00, and is not a plausible ASCII payload, so a value that survives
// into a result is unmistakable in a failure message.
const poisonByte = 0xA5

// SetPoisonOnRelease turns poison-on-release on or off and returns the previous
// setting, so a caller can restore it with a defer.
//
// It is process-global and affects every pool in the process. Callers that flip
// it must not run concurrently with unrelated queries whose answers they care
// about — a gate opens it around one query at a time.
func SetPoisonOnRelease(on bool) bool { return poisonOnRelease.Swap(on) }

// PoisonOnRelease reports whether poison-on-release is armed.
func PoisonOnRelease() bool { return poisonOnRelease.Load() }

// poisonedBatches counts batches actually scribbled. A gate that compares a
// poisoned run against a clean one proves nothing if no batch was ever
// recycled during it — the same reason the shape fuzzer reports how many of
// its queries returned rows. Callers assert this moved.
var poisonedBatches atomic.Uint64

// PoisonedBatches returns the running count of batches poisoned on release.
func PoisonedBatches() uint64 { return poisonedBatches.Load() }

// poisonBatch overwrites every unclaimed column's value storage. Called from
// Release, before the batch reaches the pool.
func poisonBatch(b *RecordBatch) {
	poisonedBatches.Add(1)
	for _, col := range b.Columns {
		poisonVector(col)
	}
}

// poisonVector scribbles one vector's value arenas, recursing into nested
// children. A view (Base != nil) owns no storage — poisoning through it would
// hit whoever the base belongs to — so views are skipped, as are vectors a
// consumer has claimed.
func poisonVector(v *Vector) {
	if v == nil || v.Base != nil {
		return
	}
	// Bytes arenas are filled to CAPACITY, not length: Reset truncates Data to
	// [:0] and the next cycle appends into the same backing array, so every
	// byte up to cap is territory a recycle may overwrite — and an alias taken
	// from an earlier, longer batch points past the current length.
	if d := v.BytesData.Data; cap(d) > 0 {
		full := d[:cap(d)]
		for i := range full {
			full[i] = poisonByte
		}
	}
	// Scalar arenas are filled over their length. resetVectorForReuse does not
	// clear these (only ResetForWrite does), so stale scalars are already what
	// a recycled vector carries; poison just makes the staleness recognisable.
	for i := range v.BoolData {
		v.BoolData[i] = true
	}
	for i := range v.Int32Data {
		v.Int32Data[i] = -1
	}
	for i := range v.Int64Data {
		v.Int64Data[i] = -1
	}
	for i := range v.Float32Data {
		v.Float32Data[i] = -1
	}
	for i := range v.Float64Data {
		v.Float64Data[i] = -1
	}
	for i := range v.DecimalData.Data {
		v.DecimalData.Data[i] = Int128{Lo: ^uint64(0), Hi: -1}
	}
	poisonVector(v.Child)
	for _, ch := range v.Children {
		poisonVector(ch)
	}
}
