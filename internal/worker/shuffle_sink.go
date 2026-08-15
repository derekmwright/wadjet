package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/diskio"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// shuffleStreamSink is an exec.Sink that writes consumed batches to a local
// spill file in WSHF format as they arrive, instead of accumulating them in
// memory like CollectSink does. After Finalize, the caller uploads the spill
// file to S3 and obtains its path via FilePath().
//
// This is the build-cache pre-scan path. Without it, a worker tasked with
// "SELECT * FROM partsupp" at SF100 would accumulate ~12GB of partsupp rows
// in CollectSink, then serialize them again into a 12GB byte buffer, then
// compress that — peaking at ~24GB+ which OOM-kills 32GB workers. With this
// sink each batch is written to disk and released back to the pool before
// the next batch arrives, so memory stays bounded by one batch.
type shuffleStreamSink struct {
	spillDir string

	mu      sync.Mutex
	file    *os.File
	bufFile *bufio.Writer // wraps file so the small per-row/per-column
	// Writes from shuffleWriter coalesce into one
	// syscall per buffer worth (see partitioned
	// shuffle sink for the profile that drove this).
	writer  *shuffleWriter
	schema  []parquet.Column
	numRows int64
	closed  bool
}

func newShuffleStreamSink(spillDir string) *shuffleStreamSink {
	return &shuffleStreamSink{spillDir: spillDir}
}

// Init creates the spill file. The header is deferred to the first Consume
// because the schema isn't known until then.
func (s *shuffleStreamSink) Init(_ context.Context) error {
	if s.spillDir == "" {
		return fmt.Errorf("shuffleStreamSink: spillDir is empty")
	}
	if err := os.MkdirAll(s.spillDir, 0o755); err != nil {
		return fmt.Errorf("creating spill dir: %w", err)
	}
	f, err := os.CreateTemp(s.spillDir, "build-cache-*.wshf")
	if err != nil {
		return fmt.Errorf("opening spill file: %w", err)
	}
	s.file = f
	return nil
}

// Consume writes the batch as a WSHF chunk to the spill file. Safe for
// concurrent calls from parallel pipeline workers — serialized via mu.
func (s *shuffleStreamSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return fmt.Errorf("shuffleStreamSink: not initialized")
	}

	if s.writer == nil {
		s.schema = b.Schema
		wf, _ := diskio.NewWriter(s.file, diskio.KeepResident)
		s.bufFile = bufio.NewWriterSize(wf, 256*1024)
		s.writer = newShuffleWriter(s.bufFile, s.schema)
		if err := s.writer.writeHeader(); err != nil {
			return fmt.Errorf("writing shuffle header: %w", err)
		}
	}

	nRows := b.ActiveLen()
	if nRows == 0 {
		return nil
	}

	if err := s.writer.writeChunk(b.Columns, b.Sel, nRows); err != nil {
		return fmt.Errorf("writing shuffle chunk: %w", err)
	}
	s.numRows += int64(nRows)
	return nil
}

// Finalize patches the chunk count in the header and flushes the file.
// The file remains open so the caller can upload it; Close releases the fd.
func (s *shuffleStreamSink) Finalize(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}
	if s.writer == nil {
		// No batches consumed — leave the file empty so upstream sees zero rows.
		return syncStageFile(s.file)
	}

	// Extent-index footer appends through the same buffered stream, so it
	// must precede the flush (the NumChunks patch below overwrites in
	// place and never appends).
	if err := s.writer.writeFooter(); err != nil {
		return fmt.Errorf("writing shuffle extent footer: %w", err)
	}
	// Flush the bufio.Writer before seeking the underlying file: a Seek
	// bypasses the buffer, so any buffered bytes would otherwise land at
	// the wrong offset.
	if s.bufFile != nil {
		if err := s.bufFile.Flush(); err != nil {
			return fmt.Errorf("flushing shuffle stream buffer: %w", err)
		}
	}

	// Patch chunk count in the header. Layout (see shuffle_format.go):
	//   offset 0..4 = magic "WSHF"
	//   offset 4..8 = numChunks (uint32 LE) — placeholder written by writeHeader
	if _, err := s.file.Seek(4, 0); err != nil {
		return fmt.Errorf("seeking to patch chunk count: %w", err)
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], s.writer.numChunks)
	if _, err := s.file.Write(buf[:]); err != nil {
		return fmt.Errorf("patching chunk count: %w", err)
	}
	if _, err := s.file.Seek(0, 2); err != nil { // back to end
		return fmt.Errorf("seeking back to end: %w", err)
	}
	return syncStageFile(s.file)
}

// Close releases the file handle. The spill file itself is left on disk for
// the caller to upload + remove.
func (s *shuffleStreamSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.file == nil {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

// FilePath returns the path to the spill file. Empty string if Init failed.
func (s *shuffleStreamSink) FilePath() string {
	if s.file == nil {
		return ""
	}
	return s.file.Name()
}

// NumRows returns the total number of rows written across all chunks.
func (s *shuffleStreamSink) NumRows() int64 { return s.numRows }
