// SPDX-License-Identifier: MIT

package json

import (
	"fmt"
	"io"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/fileinput"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
// JSON (a top-level array of objects, or JSONL/concatenated objects) one
// batch at a time, holding only a bounded byte window instead of the whole
// file plus a full columnar copy (issue #130 — read_json materialized ~2-3×
// the input in heap).
//
// Its input is a SEQUENCE of files (fileinput): read_json over a glob hands
// it the matched files in name order, and each is its own JSON DOCUMENT —
// its own `[`…`]` or run of objects, its own row numbers — with at most one
// open at a time. Through v0.24.0 the files' bytes were concatenated and the
// first file's `]` ended the whole input, so a glob of array files read the
// first file only (#1262). A document that is not one — content after the
// closing `]`, a top-level value that is not an object, an array with no
// `]` — is refused with 22P02, naming the file and row, rather than ending
// the input early as if it were the end of the file.
//
// Schema semantics match the eager reader's: inferred from the first
// defaultSampleSize complete objects of the SEQUENCE (crossing into later
// files when the first is short), except that the sample stops at
// maxSampleBytes — then it is the complete objects that fit, and `sampled`
// records how many, since every row past it is checked against the schema
// (22P02 on a mismatch, 22003 out of range, 22007 for a timestamp). The
// sampled objects are copied out of their files, so a sample that crosses
// files holds at most maxSampleBytes and still only one open file.
// Values are parsed by the same scanObjectInto byte scanner, so output
// batches are identical to NewColumnarReader's whenever the samples agree.
type StreamReader struct {
	inputs  []fileinput.Input
	nextIn  int
	cur     io.ReadCloser
	curName string
	named   bool

	schema []parquet.Column
	colIdx map[string]int
	seen   []bool

	// The window over the CURRENT file: buf[start:filled] is unconsumed.
	buf    []byte
	start  int
	filled int
	eof    bool

	docStarted  bool // the file's first non-space byte has been seen
	isArray     bool // …and it was '['
	arrayClosed bool // the array's ']' has been consumed
	fileRow     int  // 1-based row, within the current file, of the last object
	rows        int  // rows scanned from the whole sequence
	sampled     int  // leading rows the schema was inferred from
	done        bool

	// The sample: the leading objects, copied out of their files.
	sampleBuf  []byte
	sample     []sampledObject
	nextSample int
	sampleErr  error // what ended the sample early, reported in turn

	chunkSize int // test hook; defaults to streamChunkBytes
}

// sampledObject is one sampled object's bytes in sampleBuf and where it came
// from.
type sampledObject struct {
	start, end int
	file       string
	fileRow    int
}

// NewStreamReader reads one input. The caller retains ownership of r.
func NewStreamReader(r io.Reader) (*StreamReader, error) {
	return newStreamReaderSized(fileinput.Reader(r), streamChunkBytes)
}

// NewFilesReader reads a sequence of files, each its own JSON document.
// Close releases the file it holds.
func NewFilesReader(inputs []fileinput.Input) (*StreamReader, error) {
	return newStreamReaderSized(inputs, streamChunkBytes)
}

func newStreamReaderSized(inputs []fileinput.Input, chunkSize int) (*StreamReader, error) {
	sr := &StreamReader{inputs: inputs, chunkSize: chunkSize, named: len(inputs) > 1}
	for _, in := range inputs {
		sr.named = sr.named || in.Name != ""
	}

	// The sample: up to defaultSampleSize complete objects that fit in
	// maxSampleBytes, across files. A malformed input met here is reported
	// after the rows before it, as it would be past the sample.
	for len(sr.sample) < defaultSampleSize {
		objStart, objEnd, err := sr.nextObject()
		if err != nil {
			sr.sampleErr = err
			break
		}
		if objStart < 0 {
			break
		}
		obj := sr.buf[objStart:objEnd]
		if len(sr.sampleBuf)+len(obj) > maxSampleBytes && len(sr.sample) > 0 {
			break // the object stays in the window, the first row past the sample
		}
		sr.fileRow++
		s := len(sr.sampleBuf)
		sr.sampleBuf = append(sr.sampleBuf, obj...)
		sr.sample = append(sr.sample, sampledObject{start: s, end: len(sr.sampleBuf), file: sr.curName, fileRow: sr.fileRow})
		sr.sampleBuf = append(sr.sampleBuf, '\n')
		sr.start = objEnd
		if len(sr.sampleBuf) >= maxSampleBytes {
			break
		}
	}
	sr.sampled = len(sr.sample)
	if sr.sampled == 0 {
		if sr.sampleErr != nil {
			sr.Close()
			return nil, sr.sampleErr
		}
		sr.done = true // empty input → zero-column reader, like the eager path
		return sr, nil
	}
	schema, err := inferSchemaTokens(sr.sampleBuf, false, defaultSampleSize)
	if err != nil {
		sr.Close()
		return nil, sqlerr.New("22P02", "schema inference: %v", err)
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

// Close releases the file the reader holds open, if any.
func (sr *StreamReader) Close() error {
	sr.closeCurrent()
	sr.done = true
	return nil
}

func (sr *StreamReader) closeCurrent() {
	if sr.cur != nil {
		sr.cur.Close()
	}
	sr.cur = nil
}

// Next parses and returns the next batch, or nil when exhausted.
func (sr *StreamReader) Next() (*batch.RecordBatch, error) {
	if sr.done && sr.nextSample >= len(sr.sample) {
		return nil, nil
	}
	if sr.schema == nil {
		return nil, nil
	}
	rb := batch.NewRecordBatch(sr.schema, defaultBatchSize)
	row := 0
	for row < defaultBatchSize {
		var sc *jsonScanner
		var file string
		var fileRow int
		if sr.nextSample < len(sr.sample) {
			o := sr.sample[sr.nextSample]
			sr.nextSample++
			sc = &jsonScanner{data: sr.sampleBuf[:o.end], pos: o.start}
			file, fileRow = o.file, o.fileRow
		} else {
			if sr.sampleErr != nil {
				err := sr.sampleErr
				sr.sampleErr, sr.done = nil, true
				return nil, err
			}
			if sr.done {
				break
			}
			objStart, objEnd, err := sr.nextObject()
			if err != nil {
				sr.done = true
				return nil, err
			}
			if objStart < 0 {
				sr.done = true
				break
			}
			sr.fileRow++
			sc = &jsonScanner{data: sr.buf[:objEnd], pos: objStart}
			file, fileRow = sr.curName, sr.fileRow
			sr.start = objEnd
		}
		sr.rows++
		sc.fileRow, sc.sampled = sr.rows, sr.sampled
		sc.fileRowBase = sr.rows - fileRow
		if sr.named {
			sc.file = file
		}
		if err := scanObjectInto(sc, rb, row, sr.schema, sr.colIdx, sr.seen); err != nil {
			if sqlerr.StateOf(err) != "" {
				return nil, err // already names its row
			}
			return nil, sr.syntaxError(file, fileRow, err)
		}
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

// syntaxError is 22P02 for an object this reader cannot parse — what
// PostgreSQL's json input function raises for the same text.
func (sr *StreamReader) syntaxError(file string, fileRow int, err error) error {
	where := fmt.Sprintf("row %d", fileRow)
	if sr.named && file != "" {
		where = file + " " + where
	}
	return sqlerr.New("22P02", "%s: invalid JSON object: %v", where, err)
}

// nextObject positions the window on the next complete top-level object of
// the sequence, opening the next file when one ends, and returns its
// [start, end) offsets in sr.buf; objStart=-1 after the last file.
func (sr *StreamReader) nextObject() (int, int, error) {
	for {
		if sr.cur == nil {
			if sr.nextIn >= len(sr.inputs) {
				return -1, 0, nil
			}
			in := sr.inputs[sr.nextIn]
			sr.nextIn++
			rc, err := in.Open()
			if err != nil {
				return -1, 0, err
			}
			sr.cur, sr.curName = rc, in.Name
			sr.start, sr.filled, sr.eof = 0, 0, false
			sr.docStarted, sr.isArray, sr.arrayClosed, sr.fileRow = false, false, false, 0
		}
		s, e, err := sr.nextObjectSpan()
		if err != nil {
			return -1, 0, sr.inFile(err)
		}
		if s >= 0 {
			return s, e, nil
		}
		sr.closeCurrent()
	}
}

// inFile names the current file in an error about it, across a glob.
func (sr *StreamReader) inFile(err error) error {
	if !sr.named || sr.curName == "" {
		return err
	}
	return fmt.Errorf("%s: %w", sr.curName, err)
}

// nextObjectSpan positions the window on the next complete top-level
// object of the CURRENT file, refilling as needed, and returns its
// [start, end) offsets in sr.buf; objStart=-1 at the end of the file's
// document. The document is one JSON array of objects, or objects one
// after another; separators between them are whitespace and commas.
func (sr *StreamReader) nextObjectSpan() (int, int, error) {
	for {
		i := sr.start
		for i < sr.filled {
			c := sr.buf[i]
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' || (c == ',' && !sr.arrayClosed) {
				i++
				continue
			}
			break
		}
		sr.start = i
		if i >= sr.filled {
			if !sr.eof {
				if err := sr.refill(); err != nil {
					return -1, 0, err
				}
				continue
			}
			if sr.isArray && !sr.arrayClosed {
				return -1, 0, sqlerr.New("22P02", "row %d: the JSON array has no closing \"]\"", sr.fileRow+1)
			}
			return -1, 0, nil
		}
		c := sr.buf[i]
		if sr.arrayClosed {
			return -1, 0, sqlerr.New("22P02", "unexpected content %q after the JSON array's closing \"]\"", preview(sr.buf[i:sr.filled]))
		}
		if !sr.docStarted {
			sr.docStarted = true
			if c == '[' {
				sr.isArray = true
				sr.start = i + 1
				continue
			}
		}
		if c == ']' && sr.isArray {
			sr.arrayClosed = true
			sr.start = i + 1
			continue
		}
		if c != '{' {
			return -1, 0, sqlerr.New("22P02", "row %d: %q is not a JSON object", sr.fileRow+1, preview(sr.buf[i:sr.filled]))
		}
		end, ok := completeObjectEnd(sr.buf[i:sr.filled])
		if ok {
			return i, i + end, nil
		}
		if sr.eof {
			return -1, 0, sqlerr.New("22P02", "row %d: truncated JSON object at end of input", sr.fileRow+1)
		}
		if sr.filled-i > maxObjectBytes {
			return -1, 0, sqlerr.New("54000", "row %d: JSON object exceeds %d bytes", sr.fileRow+1, maxObjectBytes)
		}
		if err := sr.refill(); err != nil {
			return -1, 0, err
		}
	}
}

// preview is the start of unexpected content, for a message.
func preview(b []byte) string {
	if len(b) > 20 {
		b = b[:20]
	}
	return string(b)
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
	n, err := io.ReadFull(sr.cur, sr.buf[sr.filled:sr.filled+sr.chunkSize])
	sr.filled += n
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		sr.eof = true
		return nil
	}
	return err
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
