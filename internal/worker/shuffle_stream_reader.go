package worker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Sanity bounds for stream-decoded WSHF fields. The mmap reader is
// implicitly bounded by the file size already on disk; a stream decoder
// allocates BEFORE seeing the bytes, so a corrupt length prefix must not
// translate into an arbitrary allocation.
const (
	streamMaxCols     = 1 << 12
	streamMaxNameLen  = 1 << 12
	streamMaxRows     = 1 << 26
	streamMaxBytesLen = 1 << 31
)

// streamingShuffleReader decodes WSHF chunks directly from a byte stream
// (peer gRPC fetch or S3 body) as frames arrive, instead of staging the
// whole file to NVMe + mmap first (docs/design/
// exchange-streaming-consumption.md §3 D1). The wire format is unchanged:
// completed files carry the patched NumChunks in the header, each chunk is
// self-contained, and the promised count is validated as the stream drains
// (the same truncation guard as shuffleChunkReader, moved to
// end-of-stream).
//
// Each chunk's wire bytes are assembled into a reusable scratch buffer and
// decoded with the exact same readColumnData path as the mmap reader, so
// the two readers cannot diverge on payload interpretation.
//
// The reader owns the transport body (and the pooled s2/zstd reader for
// WSHC/WSHZ) once constructed: Close releases both and is idempotent.
// Batches it returns are self-contained copies, exactly like the mmap
// reader's.
type streamingShuffleReader struct {
	br        *bufio.Reader
	s2r       *s2.Reader    // non-nil for WSHC; returned to the pool on Close
	zr        *zstd.Decoder // non-nil for WSHZ; returned to the pool on Close
	body      io.Closer     // transport body; closed on Close
	schema    []parquet.Column
	numChunks uint32
	chunk     uint32
	delivered int // non-empty batches handed out (fallback skip count)
	scratch   []byte
	hdr       [8]byte
	closed    bool

	// da, when non-nil, replaces the serial Next with chunk-parallel
	// decode-ahead (shuffle_decode_ahead.go): the stream walk stays on one
	// scanner goroutine, column decode fans out to workers, delivery stays
	// strictly in order. Set once by startDecodeAhead before the first
	// Next; nil = the serial path below, byte-identical to before.
	da *shuffleDecodeAhead

	// headerEnd is the absolute file offset where chunk data begins
	// (magic + header + schema), computed arithmetically — the bufio may
	// have buffered ahead, so stream position is not usable.
	headerEnd int64

	// Extent index (docs/design/shuffle-extent-index.md). Set by
	// tryEnableExtentIndex on file-backed opens whose WIDX footer
	// validates: idx holds numChunks+1 offsets (idx[i] = chunk i's
	// row-count word, idx[numChunks] = end of the last chunk), file is the
	// fd decode workers pread extents from (ReadAt is goroutine-safe), and
	// idxOwned marks single-pass temps eligible for cursor drop-behind.
	// Nil idx = the stage walk, unchanged.
	file     *os.File
	idx      []int64
	idxOwned bool
}

// newStreamingShuffleReader parses the WSHF header from rc, which must be
// positioned immediately AFTER the outer 4-byte magic (the tiered open
// path consumes it to detect the format). codec selects the compressed
// framing (WSHC=s2, WSHZ=zstd); the compressed stream contains the
// complete inner WSHF payload including its own magic.
//
// Ownership: on success the reader owns rc. On error the caller still
// owns rc (matching the tiered open path's deferred close).
func newStreamingShuffleReader(rc io.ReadCloser, codec shuffleCodec) (*streamingShuffleReader, error) {
	r := &streamingShuffleReader{body: rc}
	var src io.Reader = rc
	if codec != codecNone {
		switch codec {
		case codecZstd:
			zr, err := acquireZstdReader(rc)
			if err != nil {
				return nil, fmt.Errorf("attaching zstd decoder to WSHZ stream: %w", err)
			}
			r.zr = zr
			src = zr
		default:
			r.s2r = acquireS2Reader(rc)
			src = r.s2r
		}
		// The compressed body re-carries the inner WSHF magic.
		var magic [4]byte
		if _, err := io.ReadFull(src, magic[:]); err != nil {
			r.releasePooled()
			return nil, fmt.Errorf("reading inner magic from compressed shuffle stream: %w", err)
		}
		if magic != shuffleMagic {
			r.releasePooled()
			return nil, fmt.Errorf("compressed shuffle stream does not contain WSHF payload (magic %q)", magic[:])
		}
	}
	r.br = bufio.NewReaderSize(src, 256<<10)

	if err := r.readHeader(); err != nil {
		r.releasePooled()
		return nil, err
	}
	return r, nil
}

func (r *streamingShuffleReader) readHeader() error {
	if _, err := io.ReadFull(r.br, r.hdr[:6]); err != nil {
		return fmt.Errorf("reading shuffle stream header: %w", err)
	}
	r.numChunks = binary.LittleEndian.Uint32(r.hdr[:4])
	numCols := int(binary.LittleEndian.Uint16(r.hdr[4:6]))
	if numCols > streamMaxCols {
		return fmt.Errorf("shuffle stream header: implausible column count %d", numCols)
	}
	r.headerEnd = 4 + 6 // outer magic (consumed by the open path) + this header
	r.schema = make([]parquet.Column, numCols)
	for i := 0; i < numCols; i++ {
		if _, err := io.ReadFull(r.br, r.hdr[:2]); err != nil {
			return fmt.Errorf("truncated schema at column %d: %w", i, err)
		}
		nameLen := int(binary.LittleEndian.Uint16(r.hdr[:2]))
		if nameLen > streamMaxNameLen {
			return fmt.Errorf("shuffle stream schema: implausible name length %d at column %d", nameLen, i)
		}
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r.br, name); err != nil {
			return fmt.Errorf("truncated schema name at column %d: %w", i, err)
		}
		r.schema[i].Name = string(name)
		if _, err := io.ReadFull(r.br, r.hdr[:1]); err != nil {
			return fmt.Errorf("truncated schema type at column %d: %w", i, err)
		}
		r.schema[i].Type = parquet.TypeID(r.hdr[0])
		r.schema[i].Nullable = true
		r.headerEnd += int64(2 + nameLen + 1)
		if r.schema[i].Type == parquet.TypeDecimal {
			if _, err := io.ReadFull(r.br, r.hdr[:2]); err != nil {
				return fmt.Errorf("truncated decimal schema at column %d: %w", i, err)
			}
			r.schema[i].Scale = int(r.hdr[0])
			r.schema[i].Precision = int(r.hdr[1])
			r.headerEnd += 2
		}
	}
	return nil
}

// tryEnableExtentIndex offers a file-backed reader its fd for WIDX
// index-staged decode (docs/design/shuffle-extent-index.md). owned marks a
// single-pass temp this source will unlink (cursor drop-behind applies).
// Must run after the header parse and before startDecodeAhead; a missing
// or invalid footer leaves the reader on the stage walk unchanged.
func (r *streamingShuffleReader) tryEnableExtentIndex(f *os.File, size int64, owned bool) {
	if offs := parseShuffleExtentIndex(f, size, r.numChunks, r.headerEnd); offs != nil {
		r.file, r.idx, r.idxOwned = f, offs, owned
	}
}

// Next returns the next RecordBatch, or (nil, nil) once all promised
// chunks have been decoded. A short stream inside the promised count
// surfaces the same truncation error class as the mmap reader — silent
// EOF would drop rows on the floor (the SF100 Q05 zero-rows incident).
func (r *streamingShuffleReader) Next() (*batch.RecordBatch, error) {
	if r.da != nil {
		return r.da.next()
	}
	for r.chunk < r.numChunks {
		if _, err := io.ReadFull(r.br, r.hdr[:4]); err != nil {
			return nil, fmt.Errorf("shuffle stream truncated: chunk %d/%d row-count: %w", r.chunk, r.numChunks, err)
		}
		numRows := int(binary.LittleEndian.Uint32(r.hdr[:4]))
		r.chunk++
		if numRows == 0 {
			continue
		}
		if numRows > streamMaxRows {
			return nil, fmt.Errorf("shuffle stream chunk %d: implausible row count %d", r.chunk-1, numRows)
		}

		chunkBytes, err := r.readChunkBytes(numRows)
		if err != nil {
			return nil, fmt.Errorf("shuffle stream truncated: chunk %d/%d: %w", r.chunk-1, r.numChunks, err)
		}

		b, err := decodeShuffleChunk(r.schema, numRows, chunkBytes, r.chunk-1)
		if err != nil {
			return nil, err
		}
		r.delivered++
		return b, nil
	}
	return nil, nil
}

// decodeShuffleChunk materializes one staged chunk's column segments into a
// fresh RecordBatch. Shared verbatim by the serial path and the decode-ahead
// workers so the two cannot diverge on payload interpretation; chunkIdx is
// for error text only.
func decodeShuffleChunk(schema []parquet.Column, numRows int, chunkBytes []byte, chunkIdx uint32) (*batch.RecordBatch, error) {
	b := batch.NewRecordBatch(schema, numRows)
	pos := 0
	for ci := range schema {
		var err error
		pos, err = readColumnData(chunkBytes, pos, b.Columns[ci], numRows, schema[ci].Type)
		if err != nil {
			return nil, fmt.Errorf("reading column %d (%s) chunk %d: %w", ci, schema[ci].Name, chunkIdx, err)
		}
	}
	return b, nil
}

// readChunkBytes reads one chunk's column segments (everything after the
// row-count word) into the reusable scratch buffer, walking the same
// wire layout readColumnData expects. Length prefixes for fixed-width
// types are cross-checked against numRows — a stream decoder must not
// trust a length it is about to allocate/skip by.
func (r *streamingShuffleReader) readChunkBytes(numRows int) ([]byte, error) {
	buf, err := r.readChunkBytesInto(r.scratch[:0], numRows)
	if err != nil {
		return nil, err
	}
	r.scratch = buf[:0]
	return buf, nil
}

// readChunkBytesInto is readChunkBytes over a caller-supplied buffer — the
// decode-ahead scanner stages each chunk into a pooled private buffer so
// workers can decode chunk N while the scanner walks N+1. Single caller at
// a time (the stream is serial); only the buffer's ownership differs.
func (r *streamingShuffleReader) readChunkBytesInto(buf []byte, numRows int) ([]byte, error) {
	for ci := range r.schema {
		// Null bitmap: word count + words.
		w, err := r.appendN(buf, 4)
		if err != nil {
			return nil, fmt.Errorf("column %d bitmap header: %w", ci, err)
		}
		buf = w
		bitmapWords := int(binary.LittleEndian.Uint32(buf[len(buf)-4:]))
		maxWords := (numRows+63)/64 + 1
		if bitmapWords < 0 || bitmapWords > maxWords {
			return nil, fmt.Errorf("column %d: implausible bitmap words %d for %d rows", ci, bitmapWords, numRows)
		}
		if buf, err = r.appendN(buf, bitmapWords*8); err != nil {
			return nil, fmt.Errorf("column %d bitmap: %w", ci, err)
		}

		// Typed payload: length prefix + payload (+ offsets for bytes).
		if buf, err = r.appendN(buf, 4); err != nil {
			return nil, fmt.Errorf("column %d data header: %w", ci, err)
		}
		dataLen := int(binary.LittleEndian.Uint32(buf[len(buf)-4:]))
		// -1 = variable: dataLen is the concatenated payload length.
		want, err := fixedShuffleTypeLen(r.schema[ci].Type, numRows)
		if err != nil {
			return nil, fmt.Errorf("column %d: %w", ci, err)
		}
		if want >= 0 {
			if dataLen != want {
				return nil, fmt.Errorf("column %d (%v): data length %d != expected %d for %d rows",
					ci, r.schema[ci].Type, dataLen, want, numRows)
			}
			if buf, err = r.appendN(buf, dataLen); err != nil {
				return nil, fmt.Errorf("column %d data: %w", ci, err)
			}
		} else {
			if dataLen < 0 || dataLen > streamMaxBytesLen {
				return nil, fmt.Errorf("column %d: implausible bytes payload length %d", ci, dataLen)
			}
			if buf, err = r.appendN(buf, dataLen); err != nil {
				return nil, fmt.Errorf("column %d bytes data: %w", ci, err)
			}
			if buf, err = r.appendN(buf, numRows*4); err != nil {
				return nil, fmt.Errorf("column %d offsets: %w", ci, err)
			}
		}
	}
	return buf, nil
}

// appendN appends exactly n bytes from the stream to buf, growing in
// bounded steps so a corrupt length prefix cannot allocate gigabytes
// ahead of bytes that will never arrive — the allocation never outruns
// delivered data by more than one step.
func (r *streamingShuffleReader) appendN(buf []byte, n int) ([]byte, error) {
	const step = 4 << 20
	for n > 0 {
		take := n
		if take > step {
			take = step
		}
		off := len(buf)
		if cap(buf) < off+take {
			newCap := 2 * cap(buf)
			if newCap < off+take {
				newCap = off + take
			}
			grown := make([]byte, off, newCap)
			copy(grown, buf)
			buf = grown
		}
		buf = buf[:off+take]
		if _, err := io.ReadFull(r.br, buf[off:]); err != nil {
			return nil, err
		}
		n -= take
	}
	return buf, nil
}

// Delivered reports how many non-empty batches Next has returned — the
// exact skip count a fallback re-read of the same (immutable, completed)
// file must discard to avoid double delivery.
func (r *streamingShuffleReader) Delivered() int { return r.delivered }

func (r *streamingShuffleReader) releasePooled() {
	if r.s2r != nil {
		releaseS2Reader(r.s2r)
		r.s2r = nil
	}
	if r.zr != nil {
		releaseZstdReader(r.zr)
		r.zr = nil
	}
}

// Close releases the pooled codec reader and closes the transport body.
// Idempotent. In decode-ahead mode the body closes FIRST (unblocking a
// scanner parked in a read syscall), then the scanner and workers are
// joined before the pooled codec reader is released — the scanner reads
// through it, so the return-to-pool must not race a live read.
func (r *streamingShuffleReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.da != nil {
		var err error
		if r.body != nil {
			err = r.body.Close()
			r.body = nil
		}
		r.da.stop()
		r.releasePooled()
		return err
	}
	r.releasePooled()
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}
