package worker

import (
	"bytes"
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// cachedFileStreamSource lazily reads pre-scanned build-cache files one at a
// time, yielding batches from each file before moving to the next. This avoids
// materializing all files into memory upfront — the hash join's grace spill
// handles memory pressure naturally as batches flow through.
//
// WSHF files are streamed one chunk at a time via shuffleChunkReader, so only
// one RecordBatch worth of data is allocated at a time per file. Parquet files
// are buffered per row-group (already lazy by design).
type cachedFileStreamSource struct {
	executor *Executor
	bucket   string
	files    []string

	fileIdx int

	// Active WSHF chunk reader for the current file (nil if current file is Parquet).
	chunkReader *shuffleChunkReader

	// Buffered batches for the current Parquet file.
	batches  []*batch.RecordBatch
	batchIdx int
}

func newCachedFileStreamSource(executor *Executor, bucket string, files []string) *cachedFileStreamSource {
	return &cachedFileStreamSource{
		executor: executor,
		bucket:   bucket,
		files:    files,
	}
}

func (s *cachedFileStreamSource) Init(_ context.Context) error { return nil }

func (s *cachedFileStreamSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		// Stream from active WSHF chunk reader.
		if s.chunkReader != nil {
			b, err := s.chunkReader.Next()
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			s.chunkReader = nil // file exhausted — move to next
		}

		// Yield buffered Parquet batches.
		if s.batchIdx < len(s.batches) {
			b := s.batches[s.batchIdx]
			s.batches[s.batchIdx] = nil // release for GC
			s.batchIdx++
			return b, nil
		}

		// All files exhausted.
		if s.fileIdx >= len(s.files) {
			return nil, nil
		}

		// Open next file and set up the appropriate reader.
		if err := s.openNextFile(ctx); err != nil {
			return nil, err
		}
	}
}

func (s *cachedFileStreamSource) Close() error {
	s.chunkReader = nil
	for i := s.batchIdx; i < len(s.batches); i++ {
		s.batches[i] = nil
	}
	s.batches = nil
	return nil
}

// openNextFile downloads and opens the next file, setting either chunkReader
// (for WSHF) or batches (for Parquet). Advances fileIdx.
func (s *cachedFileStreamSource) openNextFile(ctx context.Context) error {
	filePath := s.files[s.fileIdx]
	s.fileIdx++

	data, err := s.executor.getFileData(ctx, s.bucket, filePath)
	if err != nil {
		return fmt.Errorf("streaming file %s: %w", filePath, err)
	}

	data, err = DecompressShuffleData(data)
	if err != nil {
		return fmt.Errorf("decompressing file %s: %w", filePath, err)
	}

	if isShuffleFormat(data) {
		r, err := newShuffleChunkReader(data)
		if err != nil {
			return fmt.Errorf("opening shuffle file %s: %w", filePath, err)
		}
		s.chunkReader = r
		return nil
	}

	// Parquet: buffer row-group batches (already lazy by row group in the reader).
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("opening parquet file %s: %w", filePath, err)
	}
	batches, err := scan.ReadFileBatches(reader, reader.Schema().Columns, nil)
	if err != nil {
		return fmt.Errorf("reading parquet file %s: %w", filePath, err)
	}
	s.batches = batches
	s.batchIdx = 0
	return nil
}
