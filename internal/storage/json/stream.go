package json

import (
	"fmt"
	"io"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Streaming limits. The window holds at most one refill chunk plus the
// largest in-flight object; maxObjectBytes is a corruption backstop, not a
// tuning knob — a single 64 MB JSON object means the input isn't the
// row-oriented data this reader exists for.
const (
	streamChunkBytes = 256 << 10
	maxSampleBytes   = 8 << 20
	maxObjectBytes   = 64 << 20
)

// StreamReader is the incremental counterpart of ColumnarReader: it parses
// JSON (top-level array of objects, or JSONL/concatenated objects) from an
// io.Reader one batch at a time, holding only a bounded byte window instead
// of the whole file plus a full columnar copy (issue #130 — read_json
// materialized ~2-3× the input in heap).
//
// Schema semantics match the eager reader exactly: inferred from the first
// defaultSampleSize complete objects (the eager path samples the same
// prefix), capped at maxSampleBytes of buffered input. Values are parsed by
// the same scanObjectInto byte scanner, so output batches are identical to
// NewColumnarReader's for the same input.
type StreamReader struct {
	r      io.Reader
	schema []parquet.Column
	colIdx map[string]int
	seen   []bool

	buf    []byte // window; buf[start:filled] is unconsumed input
	start  int
	filled int
	eof    bool

	isArray     bool
	openSkipped bool // leading '[' consumed
	done        bool

	chunkSize int // test hook; defaults to streamChunkBytes
}

// NewStreamReader buffers just enough input to infer the schema, then
// parses lazily. The caller retains ownership of r (close it after the
// reader is exhausted or abandoned).
func NewStreamReader(r io.Reader) (*StreamReader, error) {
	return newStreamReaderSized(r, streamChunkBytes)
}

func newStreamReaderSized(r io.Reader, chunkSize int) (*StreamReader, error) {
	sr := &StreamReader{r: r, chunkSize: chunkSize}

	// Fill until the window covers defaultSampleSize complete objects, EOF,
	// or the sample cap. Schema inference sees the same prefix the eager
	// reader sampled.
	for {
		sr.skipLeadingSpace()
		end, count := sr.completeValuesEnd()
		if count >= defaultSampleSize || sr.eof || sr.filled-sr.start >= maxSampleBytes {
			_ = end
			break
		}
		if err := sr.refill(); err != nil {
			return nil, err
		}
	}
	sr.skipLeadingSpace()
	if sr.start >= sr.filled {
		sr.done = true // empty input → zero-column reader, like the eager path
		return sr, nil
	}

	sr.isArray = sr.buf[sr.start] == '['
	prefixEnd, nObjs := sr.completeValuesEnd()
	if nObjs == 0 {
		// No complete object buffered ("[]", or a truncated head): give
		// inference the whole window, exactly what the eager reader saw —
		// its token loop degrades gracefully at the cut.
		prefixEnd = sr.filled
	}
	schema, err := inferSchemaTokens(sr.buf[sr.start:prefixEnd], sr.isArray, defaultSampleSize)
	if err != nil {
		return nil, fmt.Errorf("schema inference: %w", err)
	}
	if len(schema) == 0 {
		sr.done = true
		return sr, nil
	}
	sr.schema = schema
	sr.colIdx = make(map[string]int, len(schema))
	for i, col := range schema {
		sr.colIdx[col.Name] = i
	}
	sr.seen = make([]bool, len(schema))
	return sr, nil
}

// Schema returns the inferred schema (nil for empty input).
func (sr *StreamReader) Schema() []parquet.Column { return sr.schema }

// Next parses and returns the next batch, or nil when exhausted.
func (sr *StreamReader) Next() (*batch.RecordBatch, error) {
	if sr.done {
		return nil, nil
	}
	rb := batch.NewRecordBatch(sr.schema, defaultBatchSize)
	row := 0
	for row < defaultBatchSize {
		objStart, objEnd, err := sr.nextObjectSpan()
		if err != nil {
			return nil, err
		}
		if objStart < 0 {
			sr.done = true
			break
		}
		sc := &jsonScanner{data: sr.buf[:objEnd], pos: objStart}
		if err := scanObjectInto(sc, rb, row, sr.schema, sr.colIdx, sr.seen); err != nil {
			return nil, fmt.Errorf("row %d: %w", row, err)
		}
		sr.start = objEnd
		row++
	}
	if row == 0 {
		return nil, nil
	}
	if row < defaultBatchSize {
		rb.Len = row
		for _, col := range rb.Columns {
			col.Len = row
		}
	}
	return rb, nil
}

// nextObjectSpan positions the window on the next complete top-level
// object, refilling as needed, and returns its [start, end) offsets in
// sr.buf. Returns objStart=-1 at clean end of input (']', non-object
// content, or EOF — same termination rules as parseColumnarDirect).
func (sr *StreamReader) nextObjectSpan() (int, int, error) {
	for {
		// Skip inter-object separators.
		i := sr.start
		for i < sr.filled {
			c := sr.buf[i]
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',' {
				i++
				continue
			}
			if c == '[' && sr.isArray && !sr.openSkipped {
				sr.openSkipped = true
				i++
				continue
			}
			break
		}
		sr.start = i
		if i >= sr.filled {
			if sr.eof {
				return -1, 0, nil
			}
			if err := sr.refill(); err != nil {
				return -1, 0, err
			}
			continue
		}
		if sr.buf[i] != '{' {
			// ']' (array close), or anything the eager parser stops at.
			return -1, 0, nil
		}
		end, ok := completeObjectEnd(sr.buf[i:sr.filled])
		if ok {
			return i, i + end, nil
		}
		if sr.eof {
			return -1, 0, fmt.Errorf("truncated JSON object at end of input")
		}
		if sr.filled-i > maxObjectBytes {
			return -1, 0, fmt.Errorf("JSON object exceeds %d bytes", maxObjectBytes)
		}
		if err := sr.refill(); err != nil {
			return -1, 0, err
		}
	}
}

// refill compacts the window and reads one more chunk.
func (sr *StreamReader) refill() error {
	if sr.start > 0 {
		copy(sr.buf, sr.buf[sr.start:sr.filled])
		sr.filled -= sr.start
		sr.start = 0
	}
	if sr.filled+sr.chunkSize > len(sr.buf) {
		grown := make([]byte, sr.filled+sr.chunkSize)
		copy(grown, sr.buf[:sr.filled])
		sr.buf = grown
	}
	n, err := io.ReadFull(sr.r, sr.buf[sr.filled:sr.filled+sr.chunkSize])
	sr.filled += n
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		sr.eof = true
		return nil
	}
	return err
}

// skipLeadingSpace advances start past whitespace.
func (sr *StreamReader) skipLeadingSpace() {
	for sr.start < sr.filled {
		switch sr.buf[sr.start] {
		case ' ', '\t', '\n', '\r':
			sr.start++
		default:
			return
		}
	}
}

// completeValuesEnd scans buf[start:filled] and returns the offset (in buf
// coordinates) just past the last complete top-level object, plus the count
// of complete objects. Understands strings/escapes and nesting; leading
// '[' and separators belong to the prefix.
func (sr *StreamReader) completeValuesEnd() (int, int) {
	end := sr.start
	count := 0
	i := sr.start
	openSkipped := sr.openSkipped
	for i < sr.filled {
		c := sr.buf[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',' {
			i++
			continue
		}
		if c == '[' && !openSkipped {
			openSkipped = true
			i++
			continue
		}
		if c != '{' {
			break
		}
		objEnd, ok := completeObjectEnd(sr.buf[i:sr.filled])
		if !ok {
			break
		}
		i += objEnd
		end = i
		count++
	}
	return end, count
}

// completeObjectEnd returns the length of the complete JSON object starting
// at data[0] (which must be '{'), or ok=false if the object is not yet
// fully buffered. String/escape aware.
func completeObjectEnd(data []byte) (int, bool) {
	depth := 0
	inStr := false
	esc := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if inStr {
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
	}
	return 0, false
}
