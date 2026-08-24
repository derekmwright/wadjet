package scan

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Merge-on-read delete markers, and the one representation every read path
// shares.
//
// A DELETE does not rewrite parquet. It records, per data file, the
// FILE-ABSOLUTE 0-based row indices of the rows it removed
// (catalog.DeleteMarker), and every scan of that file must skip them until
// compaction folds them in. "File-absolute" means counted over the file's
// row groups in order from row 0 — NOT relative to a row group, a shard, or
// whatever slice of the file a particular reader happens to be assigned.
// The single-process scanner tracks that with a per-row-group prefix sum
// (physical.rgUnit.rgRowOffset); the distributed scan source gets the same
// number from its row-group iterator (RowGroupIter.RowOffset).
//
// DeleteSet is the runtime form: the sorted row indices coalesced into
// disjoint runs, which is both the compact wire encoding (EncodeDeleteRuns)
// and a fast membership test. A map[int64]bool costs ~50 B/row and is the
// wrong shape for a DELETE that removed a contiguous range — the common
// case, since a WHERE over a clustered column marks runs, not confetti.
type DeleteSet struct {
	// starts and ends are parallel, sorted ascending, and disjoint:
	// run i covers rows [starts[i], ends[i]).
	starts []int64
	ends   []int64
	rows   int64
}

// NewDeleteSet builds a DeleteSet from unsorted, possibly duplicated
// file-absolute row indices. Negative indices are dropped — a marker that
// names one is corrupt, and skipping it can only ever fail to delete a row
// that no reader would have matched to it anyway. Returns nil for an empty
// set so every consumer's nil check is the fast path.
func NewDeleteSet(rows []int64) *DeleteSet {
	if len(rows) == 0 {
		return nil
	}
	sorted := make([]int64, 0, len(rows))
	for _, r := range rows {
		if r >= 0 {
			sorted = append(sorted, r)
		}
	}
	if len(sorted) == 0 {
		return nil
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	d := &DeleteSet{}
	start := sorted[0]
	end := sorted[0] + 1
	for _, r := range sorted[1:] {
		switch {
		case r < end: // duplicate
			continue
		case r == end: // extends the run
			end = r + 1
		default:
			d.appendRun(start, end)
			start, end = r, r+1
		}
	}
	d.appendRun(start, end)
	return d
}

func (d *DeleteSet) appendRun(start, end int64) {
	d.starts = append(d.starts, start)
	d.ends = append(d.ends, end)
	d.rows += end - start
}

// Empty reports whether the set deletes nothing. Nil-safe.
func (d *DeleteSet) Empty() bool { return d == nil || len(d.starts) == 0 }

// Rows is the number of deleted row indices the set holds. Nil-safe.
func (d *DeleteSet) Rows() int64 {
	if d == nil {
		return 0
	}
	return d.rows
}

// Runs is the number of disjoint runs. Nil-safe; used by tests and by the
// spec-size accounting.
func (d *DeleteSet) Runs() int {
	if d == nil {
		return 0
	}
	return len(d.starts)
}

// Contains reports whether the given file-absolute row index is deleted.
// Nil-safe.
func (d *DeleteSet) Contains(row int64) bool {
	if d == nil || len(d.starts) == 0 {
		return false
	}
	// Last run whose start is <= row.
	i := sort.Search(len(d.starts), func(i int) bool { return d.starts[i] > row }) - 1
	return i >= 0 && row < d.ends[i]
}

// Overlaps reports whether any deleted row falls in [offset, offset+count).
// The cheap reject that keeps a table with a handful of markers from paying
// a per-row test on every row group of every file. Nil-safe.
func (d *DeleteSet) Overlaps(offset, count int64) bool {
	if d == nil || len(d.starts) == 0 || count <= 0 {
		return false
	}
	end := offset + count
	// First run that ends after offset.
	i := sort.Search(len(d.ends), func(i int) bool { return d.ends[i] > offset })
	return i < len(d.starts) && d.starts[i] < end
}

// EncodeDeleteRuns serializes a set of file-absolute row indices as
// varint (gap, length) pairs over the coalesced runs — the form a task
// spec carries per input file (distributed.DeleteSpec.Runs).
//
// gap is the distance from the previous run's END to this run's START, so
// the first pair's gap is the first deleted row itself and every later gap
// is >= 1 (runs are coalesced, hence never adjacent). A contiguous DELETE
// of any size is 2 varints; scattered deletes cost ~2 bytes each, against
// ~8 for the same index as a JSON decimal in the manifest that produced it.
// See the spec-size table in docs/internals/native-dag-execution.md.
func EncodeDeleteRuns(rows []int64) []byte {
	return NewDeleteSet(rows).Encode()
}

// Encode is EncodeDeleteRuns for an already-built set. Nil-safe; returns
// nil for an empty set so the wire field stays absent under omitempty.
func (d *DeleteSet) Encode() []byte {
	if d.Empty() {
		return nil
	}
	buf := make([]byte, 0, 2*len(d.starts)+8)
	var prevEnd int64
	var scratch [binary.MaxVarintLen64]byte
	put := func(v int64) {
		n := binary.PutUvarint(scratch[:], uint64(v))
		buf = append(buf, scratch[:n]...)
	}
	for i := range d.starts {
		put(d.starts[i] - prevEnd)
		put(d.ends[i] - d.starts[i])
		prevEnd = d.ends[i]
	}
	return buf
}

// DecodeDeleteSet reverses Encode. A malformed payload is an error, never a
// panic and never a silently short set: a scan that cannot read its delete
// markers must fail the task, because the alternative is answering with the
// deleted rows still in it.
func DecodeDeleteSet(b []byte) (*DeleteSet, error) {
	if len(b) == 0 {
		return nil, nil
	}
	d := &DeleteSet{}
	var prevEnd int64
	for len(b) > 0 {
		gap, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, fmt.Errorf("delete markers: truncated run gap at offset %d", len(b))
		}
		b = b[n:]
		length, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, fmt.Errorf("delete markers: truncated run length at offset %d", len(b))
		}
		b = b[n:]
		if length == 0 {
			return nil, fmt.Errorf("delete markers: zero-length run")
		}
		if gap > uint64(1<<62) || length > uint64(1<<62) {
			return nil, fmt.Errorf("delete markers: run out of range (gap=%d len=%d)", gap, length)
		}
		start := prevEnd + int64(gap)
		if start < prevEnd {
			return nil, fmt.Errorf("delete markers: run start overflow")
		}
		end := start + int64(length)
		if end < start {
			return nil, fmt.Errorf("delete markers: run end overflow")
		}
		d.appendRun(start, end)
		prevEnd = end
	}
	return d, nil
}

// ApplyDeleteMarkers narrows b's selection to the rows the set does not
// delete, where rowOffset is the FILE-ABSOLUTE index of b's row 0. Returns
// false when nothing survives — the caller drops the batch rather than
// passing an empty selection downstream.
//
// An existing selection is intersected, never overwritten: a scan-level
// filter that already marked rows must not have them resurrected here (the
// same rule the single-process rgWorker follows).
func ApplyDeleteMarkers(b *batch.RecordBatch, rowOffset int64, del *DeleteSet) bool {
	if b == nil {
		return false
	}
	if del.Empty() || !del.Overlaps(rowOffset, int64(b.Len)) {
		return true
	}
	var sel []uint32
	if b.Sel != nil {
		sel = make([]uint32, 0, len(b.Sel))
		for _, i := range b.Sel {
			if !del.Contains(rowOffset + int64(i)) {
				sel = append(sel, i)
			}
		}
	} else {
		sel = make([]uint32, 0, b.Len)
		for i := 0; i < b.Len; i++ {
			if !del.Contains(rowOffset + int64(i)) {
				sel = append(sel, uint32(i))
			}
		}
	}
	if len(sel) == 0 {
		return false
	}
	if b.Sel != nil || len(sel) < b.Len {
		b.Sel = sel
	}
	return true
}
