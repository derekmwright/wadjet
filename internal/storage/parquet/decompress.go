package parquet

import (
	"bytes"
	"fmt"
	"io"
	"runtime"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Decompress decompresses data using the specified codec.
// The uncompressedSize hint is used to pre-allocate the output buffer.
func Decompress(codec CompressionCodec, compressed []byte, uncompressedSize int) ([]byte, error) {
	switch codec {
	case CodecNone:
		return compressed, nil
	case CodecSnappy:
		return decompressSnappy(compressed, uncompressedSize)
	case CodecGzip:
		return decompressGzip(compressed, uncompressedSize)
	case CodecZstd:
		return decompressZstd(compressed, uncompressedSize)
	case CodecLZ4, CodecLZ4Raw:
		return decompressLZ4(compressed, uncompressedSize)
	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}

func decompressSnappy(data []byte, size int) ([]byte, error) {
	dst := make([]byte, 0, size)
	dst, err := snappy.Decode(dst, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress: %w", err)
	}
	return dst, nil
}

// Shared zstd decoder — thread-safe, reusable. Concurrency bounds how many
// DecodeAll calls run in parallel; callers beyond the bound queue on the
// decoder's internal state channel. Scan fans out one goroutine per column
// (ReadRowGroupNative), so this must scale with cores: the original
// concurrency-1 setting serialized every page decompression in the process
// and left 16-vCPU workers ~91% idle on zstd-compressed data (SF100
// profiling run 20260711-225605, ~16.7k goroutine-seconds queued per
// worker; docs/design/scan-decompress-parallelism.md). Capped at 16 so
// larger hosts don't multiply per-state memory unbounded; lowmem keeps
// each state's buffers small for the short-page DecodeAll pattern.
var zstdDecoder *zstd.Decoder

// zstdDecoderConcurrency is how many DecodeAll calls may run in parallel.
// Must scale with cores — pinning this back to 1 recreates the SF100
// scan-serialization bottleneck.
func zstdDecoderConcurrency() int {
	return min(runtime.GOMAXPROCS(0), 16)
}

func init() {
	var err error
	zstdDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(zstdDecoderConcurrency()),
		zstd.WithDecoderLowmem(true))
	if err != nil {
		panic("creating zstd decoder: " + err.Error())
	}
}

func decompressZstd(data []byte, size int) ([]byte, error) {
	dst := getZstdBuf(size)
	dst, err := zstdDecoder.DecodeAll(data, dst)
	if err != nil {
		putZstdBuf(dst)
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return dst, nil
}

// ReleaseDecompressed returns the slice returned by Decompress to its pool,
// if it came from one. Safe to call with any slice — non-pooled buffers are
// dropped to GC. Only the zstd path is currently pooled; other codecs are
// no-ops.
func ReleaseDecompressed(codec CompressionCodec, buf []byte) {
	if codec == CodecZstd {
		putZstdBuf(buf)
	}
}

func decompressGzip(data []byte, size int) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip init: %w", err)
	}
	defer r.Close()
	dst, err := io.ReadAll(io.LimitReader(r, int64(size)+1))
	if err != nil {
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}
	return dst, nil
}

func decompressLZ4(data []byte, size int) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	dst, err := io.ReadAll(io.LimitReader(r, int64(size)+1))
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}
	return dst, nil
}
