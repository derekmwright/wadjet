package parquet

import "fmt"

// Row-group-at-a-time backing for a FileReader.
//
// A FileReader has two byte backings today: a whole-file `data []byte`
// (zero-copy page slices) and a staged `src io.ReaderAt` (one pooled ranged
// read per column chunk). This is the third: the file's bytes held one ROW
// GROUP at a time, so a caller can free a row group's bytes — and the memory
// charge that goes with them — as soon as that row group has been decoded,
// instead of holding the whole file until its last row group is done.
//
// Page access stays zero-copy: a column chunk lies entirely inside its own
// row group's byte range (ValidateChunkLayout proves at open that the chunks
// tile the data region without overlapping), so every page is still a slice
// of the caller's buffer with no copy — exactly what slice mode does, with
// the offsets biased by the buffer's file offset.
//
// The caller owns the buffers' lifetime and must not recycle a row group's
// buffer until every value decoded from it has been copied out — the same
// copy-before-release contract staged mode already carries (page_reader.go's
// Close doc, docs/design/scan-pread-reads.md).

// RowGroupBytes supplies a parquet file's bytes one row group at a time.
//
// RowGroupBytes returns the buffer holding row group rgIdx's bytes and the
// FILE offset that buffer's first byte has, or an error when those bytes are
// not available. The returned buffer must cover the whole [start, end) range
// FileReader.RowGroupByteRange reports for rgIdx; a chunk that falls outside
// it is refused rather than decoded from the wrong bytes.
//
// Implementations must be safe for concurrent use: row groups decode in
// parallel.
type RowGroupBytes interface {
	RowGroupBytes(rgIdx int) (buf []byte, base int64, err error)
}

// SetRowGroupBytes puts the reader in row-group mode, reading each column
// chunk out of the buffer src supplies for that chunk's row group. size is
// the file's byte size, which the chunk-range validation is measured against.
//
// Call once, before handing the reader to concurrent consumers — the fields
// are read without synchronization, the same contract SetCacheIdentity has.
// A reader that already has a whole-file buffer or a staged source keeps it:
// row-group mode is only taken when neither is set.
func (f *FileReader) SetRowGroupBytes(src RowGroupBytes, size int64) {
	f.rgSrc = src
	f.size = size
}

// RowGroupByteRange is the [start, end) file byte range spanned by every
// column chunk of row group rgIdx: from the first byte of its earliest chunk
// (its dictionary page when it has one) to the last byte of its latest.
// ok is false when the row group has no bytes at all — no chunk with a
// positive compressed size — which is a legal empty row group, not an error.
//
// The range is derived from the same chunk extents ValidateChunkLayout walks
// at open, so a file whose footer this reader accepted has ranges that do not
// overlap between row groups and do not reach into the footer.
func (f *FileReader) RowGroupByteRange(rgIdx int) (start, end int64, ok bool, err error) {
	rg := f.RowGroupMeta(rgIdx)
	if rg == nil {
		return 0, 0, false, fmt.Errorf("parquet: row group %d out of range (%d row groups)",
			rgIdx, f.NumRowGroups())
	}
	dataEnd := f.size
	if dataEnd <= 0 {
		dataEnd = int64(len(f.data)) // whole-file backing: the slice is the file
	}
	if dataEnd <= 0 {
		return 0, 0, false, fmt.Errorf("parquet: row group %d byte range needs the file size, which this reader does not have", rgIdx)
	}
	for j := range rg.Columns {
		e, has, cerr := chunkExtent(f.meta, rgIdx, j, dataEnd)
		if cerr != nil {
			return 0, 0, false, cerr
		}
		if !has {
			continue
		}
		if !ok || e.start < start {
			start = e.start
		}
		if !ok || e.end > end {
			end = e.end
		}
		ok = true
	}
	return start, end, ok, nil
}
