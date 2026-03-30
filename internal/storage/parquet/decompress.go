package parquet

import (
	"bytes"
	"fmt"
	"io"

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

// Shared zstd decoder — thread-safe, reusable.
var zstdDecoder *zstd.Decoder

func init() {
	var err error
	zstdDecoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		panic("creating zstd decoder: " + err.Error())
	}
}

func decompressZstd(data []byte, size int) ([]byte, error) {
	dst := make([]byte, 0, size)
	dst, err := zstdDecoder.DecodeAll(data, dst)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return dst, nil
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
