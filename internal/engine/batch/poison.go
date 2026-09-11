package batch

import "sync/atomic"

// Released pooled storage is undefined: retain it only through Detach (claims
// columns) or deep copy. Poison mode overwrites unclaimed value arenas with
// 0xA5/-1 before pooling; TestTypeMatrixBatchReuse compares on/off answers.
// Write only when pool != nil, never through Base views, and never into offsets,
// null bitmaps or nested shape metadata. These limits model real recycling.
// retainsClaimedStorage vetoes the WHOLE batch, both pooling and poison (#897),
// including claims made through a derived shell sharing the vectors.
// Keep runtime toggling available; disabled mode does one atomic load per batch.
// See docs/internals/batch-poison-release-ownership.md for the design.
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

// poisonBatch overwrites the batch's column value storage. Called from
// Release, before the batch reaches the pool, and only for a batch that
// carries no claim at all — Release vetoes the claimed ones before here.
func poisonBatch(b *RecordBatch) {
	poisonedBatches.Add(1)
	for _, col := range b.Columns {
		poisonVector(col)
	}
}

// poisonVector scribbles one vector's value arenas, recursing into nested
// children. A view (Base != nil) owns no storage — poisoning through it would
// hit whoever the base belongs to — so views are skipped. Claimed vectors
// never reach here: the veto is whole-batch, one step up.
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
