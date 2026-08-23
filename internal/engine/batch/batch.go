package batch

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DefaultBatchSize is the number of rows per batch (2048 for cache-friendly vectorized processing).
const DefaultBatchSize = 2048

// RecordBatch is the unit of data flowing between operators.
type RecordBatch struct {
	Columns []*Vector
	Schema  []parquet.Column
	Len     int
	Sel     []uint32 // selection vector: indices of active rows (nil = all rows active)
	pool    *BatchPool
	// retained records that a consumer claimed ownership of this batch with
	// Detach. Producers that reuse output storage across calls read it (with
	// the per-column claims Detach also sets) to decide whether last call's
	// batch may be written over. Cleared by Reset, so a recycled batch starts
	// unclaimed.
	retained bool
	// ownerID identifies which memory-tier owner currently accounts this batch's
	// bytes. 0 means unstamped (not pool-owned, e.g. the over-size escape hatch
	// or a Detach'd long-lived batch); ReservoirOwner means a BatchPool minted it
	// and its bytes are charged to the pool reservoir. Survives Reset so a reused
	// batch keeps its owner. The non-zero sentinel keeps the zero value
	// unambiguous, matching the Sel==nil / pool==nil "absent" conventions.
	ownerID uint64
	// mint is the producing free list's identity stamp — see MintStamp.
	mint MintStamp
}

// MintStamp records WHICH producer minted a batch's storage and WHICH issue of
// that storage this is. It exists so a producer that hands storage out and
// takes it back — today the scan row-group backing pool,
// docs/design/scan-output-backing-reuse.md — can recognize its own batch at
// the release edge WITHOUT keeping a reference to it.
//
// A registry of outstanding batches keyed by pointer is the obvious
// implementation and the wrong one: it is a strong reference the GC cannot
// collect and the memory ledger cannot see. A consumer with no release edge,
// or a batch the pipeline simply drops, pins whole decoded row groups (~280 MB
// each at SF100) for the producer's lifetime, and any bound on the registry's
// SIZE silently turns reuse off instead. The stamp inverts the direction: the
// batch points at nothing, the producer holds nothing, and identity survives
// in a pair of integers the batch carries.
//
// Owner is a process-unique producer id from NewMintOwner. Zero means
// unstamped — what a WSHF shuffle chunk, a row-based fallback batch or any
// batch from a different producer carries — and a release edge must treat it
// as foreign, because adopting it would create a second owner for storage
// somebody else recycles.
//
// Seq is bumped on every re-issue of the SAME storage. A release names the Seq
// it was handed, so a stale release from a previous generation (a retire that
// fired twice around a re-mint) names an older Seq and is ignored: re-admitting
// a LIVE backing to the free list would give two decoders one buffer, the one
// failure this design must not have.
//
// The stamp is written by the producer while it owns the batch exclusively and
// read at the release edge; the producer's own publication edge (the decode
// ring's channel, the dispenser's channel send) and the pool mutex order every
// access.
type MintStamp struct {
	Owner uint64
	Seq   uint64
}

// Valid reports whether the stamp names a producer.
func (m MintStamp) Valid() bool { return m.Owner != 0 }

// mintOwnerSeq allocates MintStamp.Owner values. Never reset: ids must not be
// reused while a batch stamped with an old one is still in flight.
var mintOwnerSeq atomic.Uint64

// NewMintOwner returns a process-unique producer id for MintStamp.Owner.
func NewMintOwner() uint64 { return mintOwnerSeq.Add(1) }

// Mint returns the producer stamp on this batch (zero Owner = unstamped).
func (b *RecordBatch) Mint() MintStamp { return b.mint }

// SetMint stamps the batch for its producing free list. Only the producer
// calls it, and only while it owns the batch exclusively — at mint, and again
// to clear the stamp when the storage is taken back. The stamp travels with a
// VALUE copy of the RecordBatch (e.g. `nb := *b`), so any such copy taken over
// a scan batch must zero the stamp (`nb.SetMint(batch.MintStamp{})`) or it
// would alias the parent's pool identity; today the only value copy
// (internal/engine/exec/partitioned_agg.go's selView) Detaches immediately,
// which claims the shared columns and trips the release veto regardless.
func (b *RecordBatch) SetMint(m MintStamp) { b.mint = m }

// NewRecordBatch creates a new record batch with the given schema and row count.
func NewRecordBatch(schema []parquet.Column, numRows int) *RecordBatch {
	cols := make([]*Vector, len(schema))
	for i, col := range schema {
		cols[i] = newVectorFromColumn(col, numRows)
	}
	return &RecordBatch{
		Columns: cols,
		Schema:  schema,
		Len:     numRows,
	}
}

// NewColumnVector creates a single Vector from a Column definition with
// numRows pre-allocated rows, recursively initializing nested type children —
// the per-column equivalent of NewRecordBatch for callers that materialize
// some columns of a batch while emitting others as views.
func NewColumnVector(col parquet.Column, numRows int) *Vector {
	return newVectorFromColumn(col, numRows)
}

// newVectorFromColumn creates a Vector from a Column definition, recursively
// initializing nested type children.
func newVectorFromColumn(col parquet.Column, numRows int) *Vector {
	v := NewVectorWithScale(col.Type, numRows, col.Scale)
	switch col.Type {
	case TypeVector:
		if col.Dimension > 0 {
			v.VectorDim = col.Dimension
			v.Float32Data = make([]float32, numRows*col.Dimension)
		}
	case TypeArray, TypeMap:
		if col.ElementType != nil {
			v.Child = newVectorFromColumn(*col.ElementType, 0)
		}
	case TypeRow:
		if len(col.Fields) > 0 {
			v.FieldNames = make([]string, len(col.Fields))
			v.Children = make([]*Vector, len(col.Fields))
			for i, f := range col.Fields {
				v.FieldNames[i] = f.Name
				v.Children[i] = newVectorFromColumn(f, numRows)
			}
		}
	}
	return v
}

// ActiveLen returns the number of active rows (respecting selection vector).
func (b *RecordBatch) ActiveLen() int {
	if b.Sel == nil {
		return b.Len
	}
	return len(b.Sel)
}

// ColumnByName returns the vector for the named column, or nil if not found.
func (b *RecordBatch) ColumnByName(name string) *Vector {
	for i, col := range b.Schema {
		if col.Name == name {
			return b.Columns[i]
		}
	}
	return nil
}

// ColumnIndex returns the index of the named column, or -1.
func (b *RecordBatch) ColumnIndex(name string) int {
	for i, col := range b.Schema {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// Release returns the batch to its pool if applicable.
//
// Once released, the batch's storage is undefined: the pool may hand it to
// another operator, which resets it and writes over the same arenas. Anything
// keeping a value out of the batch past this point must own it — see Detach.
// Poison mode (see poison.go) makes that undefinedness observable by
// scribbling the arenas here, which is what the batch-reuse gate compares
// against a clean run.
func (b *RecordBatch) Release() {
	if b.pool != nil {
		if poisonOnRelease.Load() {
			poisonBatch(b)
		}
		b.pool.Put(b)
	}
}

// Detach claims ownership of the batch: Release() becomes a no-op so no pool
// can recycle it, and the claim is recorded on the batch AND on every column
// vector so a producer that reuses vector backing across calls surrenders it
// (see (*Vector).Claim). Call it from anything that keeps a batch — or
// anything pointing into its column storage — past the call that handed it
// over: the hash-join build, Sort, Window, the collect sinks, the spillable
// collector, partitioned aggregation's per-partition views.
//
// The per-column claim is what makes the contract hold through a derived
// batch: ColumnPrune and the set-op emitter mint a NEW RecordBatch over the
// same *Vector pointers, so a consumer that detaches the derived batch would
// otherwise leave the producer of the original believing nobody kept it.
func (b *RecordBatch) Detach() {
	b.pool = nil
	b.retained = true
	for _, c := range b.Columns {
		c.Claim()
	}
}

// DetachPool severs only the pool link, WITHOUT claiming the batch or its
// columns. It is for the one caller whose reference is transitive rather than
// independent: the hash-join late-materialization emitter, whose output views
// read the input's vectors, so the input must not be recycled underneath them
// by a concurrently-running source — but whose views die with its own output
// batch, whose consumer's Detach (if any) propagates the claim through
// Vector.Base anyway. Anything that genuinely KEEPS a batch calls Detach.
func (b *RecordBatch) DetachPool() {
	b.pool = nil
}

// Retained reports whether a consumer claimed ownership of this batch with
// Detach.
func (b *RecordBatch) Retained() bool { return b.retained }

// MemBytes returns the in-memory byte footprint of the batch's column data,
// summing each Vector's MemBytes(). It deliberately omits operator-specific
// overhead (e.g. the HashJoin hash-index charge) — that stays at the call site
// (see hashBuildBytes in package exec). Replaces the per-type estimate that
// lived in exec.EstimateBatchBytes.
func (b *RecordBatch) MemBytes() int64 {
	var size int64
	for _, v := range b.Columns {
		size += v.MemBytes()
	}
	return size
}

// EnsureCapacity grows every column so positions [0, n) are addressable for
// in-place writes, preserving existing data, and sets Len to n. Used by
// append-style builders (e.g. the hash-join per-partition accumulator) that
// grow a batch across many source batches rather than pre-sizing to a
// worst-case capacity. See Vector.EnsureLen for per-type behavior and the
// nested-type caveat.
func (b *RecordBatch) EnsureCapacity(n int) {
	for _, col := range b.Columns {
		col.EnsureLen(n)
	}
	if n > b.Len {
		b.Len = n
	}
}

// Reset clears the batch for reuse, keeping allocated memory.
func (b *RecordBatch) Reset(numRows int) {
	b.Len = numRows
	b.Sel = nil
	b.retained = false
	// A batch entering a BatchPool cycle is no longer the scan pool's to take
	// back: clear the stamp so a late release from the previous producer finds
	// it foreign rather than handing one buffer to two owners.
	b.mint = MintStamp{}
	for _, col := range b.Columns {
		resetVectorForReuse(col, numRows)
	}
}

// resetVectorForReuse clears a vector's per-row state for pooled reuse,
// recursing into nested children. Without the recursion, a pooled ROW/ARRAY
// column kept its previous cycle's child arenas, offsets and null bits —
// the first reused row read back the prior batch's data concatenated with
// the new value, and child arenas grew monotonically per reuse cycle.
func resetVectorForReuse(col *Vector, numRows int) {
	// Views must never survive into a pooled reuse cycle; drop the
	// indirection so the batch is a plain (empty) owned batch again.
	col.Base = nil
	col.Indices = nil
	// A batch only reaches a pool when nobody claimed it (Detach severs the
	// pool link), so a recycled vector starts unclaimed again.
	col.claimed = false
	col.Len = numRows
	col.Nulls.ResetNonNull(numRows)
	switch col.Type {
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		col.BytesData.Reset()
	case TypeArray, TypeMap:
		for i := 0; i < numRows+1 && i < len(col.Offsets); i++ {
			col.Offsets[i] = 0
		}
		if col.Child != nil {
			resetVectorForReuse(col.Child, 0)
			// Child element storage is append-built; truncate the arenas so
			// CopyValueFrom/AppendFrom start from a zero-length child.
			truncateVectorStorage(col.Child)
		}
	case TypeRow:
		for _, ch := range col.Children {
			resetVectorForReuse(ch, numRows)
		}
	}
}

// truncateVectorStorage re-slices a vector's element storage to zero length
// (capacity retained for reuse). Recurses into nested children.
func truncateVectorStorage(v *Vector) {
	v.Base = nil
	v.Indices = nil
	v.Len = 0
	v.BoolData = v.BoolData[:0]
	v.Int32Data = v.Int32Data[:0]
	v.Int64Data = v.Int64Data[:0]
	v.Float32Data = v.Float32Data[:0]
	v.Float64Data = v.Float64Data[:0]
	if v.DecimalData.Data != nil {
		v.DecimalData.Data = v.DecimalData.Data[:0]
	}
	v.BytesData.Reset()
	if len(v.BytesData.Offsets) > 0 {
		v.BytesData.Offsets = v.BytesData.Offsets[:1]
		v.BytesData.Offsets[0] = 0
	}
	if v.Type == TypeArray || v.Type == TypeMap {
		if len(v.Offsets) > 0 {
			v.Offsets = v.Offsets[:1]
			v.Offsets[0] = 0
		}
		if v.Child != nil {
			truncateVectorStorage(v.Child)
		}
	}
	for _, ch := range v.Children {
		truncateVectorStorage(ch)
	}
	v.Nulls.ResetNonNull(0)
}

// FromRows creates a RecordBatch from row-oriented data.
func FromRows(schema []parquet.Column, rows []map[string]any) *RecordBatch {
	b := NewRecordBatch(schema, len(rows))
	for i, row := range rows {
		for j, col := range schema {
			val := row[col.Name]
			b.Columns[j].SetValue(i, val)
		}
	}
	return b
}

// Compact materializes the selection vector into a contiguous batch using
// the typed nested-aware value copier. Returns the batch unchanged when no
// selection vector is set.
//
// Was previously a hand-rolled per-type switch whose ROW case wrote null
// child rows via SetNull alone — never advancing a string child's offset
// slot — so every later row in that child read back as concatenated
// garbage (same bug class as the windowCopyVectorRange nullable-BYTES fix;
// regression test TestCompact_RowChildNullableString).
func (b *RecordBatch) Compact() *RecordBatch {
	if b.Sel == nil {
		return b
	}
	n := len(b.Sel)
	out := NewRecordBatch(b.Schema, n)
	for j := range b.Schema {
		src := b.Columns[j]
		dst := out.Columns[j]
		for di, si := range b.Sel {
			dst.CopyValueFrom(di, src, int(si))
		}
	}
	return out
}

// RowAt boxes a single physical row (Sel is not consulted) as a map.
// For callers that need a few rows out of a large batch — boxing the whole
// batch via ToRows for a low-selectivity pick is the documented multi-GB
// heap pattern.
func (b *RecordBatch) RowAt(i int) map[string]any {
	row := make(map[string]any, len(b.Schema))
	for j, col := range b.Schema {
		row[col.Name] = b.Columns[j].GetValue(i)
	}
	return row
}

// ToRows converts a RecordBatch to row-oriented data.
func (b *RecordBatch) ToRows() []map[string]any {
	rows := make([]map[string]any, 0, b.ActiveLen())
	if b.Sel != nil {
		for _, idx := range b.Sel {
			row := make(map[string]any, len(b.Schema))
			for j, col := range b.Schema {
				row[col.Name] = b.Columns[j].GetValue(int(idx))
			}
			rows = append(rows, row)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			row := make(map[string]any, len(b.Schema))
			for j, col := range b.Schema {
				row[col.Name] = b.Columns[j].GetValue(i)
			}
			rows = append(rows, row)
		}
	}
	return rows
}
