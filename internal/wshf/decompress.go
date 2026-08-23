package wshf

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Decompress unwraps a WSHC (s2) or WSHZ (zstd) envelope back to raw WSHF.
// Plain WSHF — or anything that is not a shuffle payload at all, e.g. a
// parquet result file — is returned unchanged, so callers can sniff once
// and branch after.
//
// Both envelopes, not just WSHC: WSHZ is what an S3 stage upload carries
// under WADJET_EXCHANGE_ZSTD=1, and a reader that knows only WSHC hands
// the compressed bytes on to a parquet decoder and fails with a parquet
// error on a perfectly good shuffle file.
//
// The worker's own DecompressShuffleData is the pooled, streaming variant
// of this for its hot file paths; this is the whole-payload form for
// callers that already hold the bytes.
func Decompress(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return data, nil
	}
	codec, ok := CodecForMagic([4]byte{data[0], data[1], data[2], data[3]})
	if !ok || codec == CodecNone {
		return data, nil
	}
	var buf bytes.Buffer
	buf.Grow(len(data) * 2)
	if err := DecompressStream(bytes.NewReader(data[4:]), &buf, codec); err != nil {
		return nil, fmt.Errorf("decompressing shuffle data: %w", err)
	}
	return buf.Bytes(), nil
}

// DecompressStream copies the compressed body that follows a WSHC/WSHZ
// magic from src to dst. codec names the envelope (the caller sniffed the
// magic); the WSHF magic itself is inside the compressed body, so dst
// receives a complete WSHF payload.
func DecompressStream(src io.Reader, dst io.Writer, codec Codec) error {
	switch codec {
	case CodecZstd:
		zr, err := zstd.NewReader(src)
		if err != nil {
			return fmt.Errorf("attaching zstd decoder: %w", err)
		}
		defer zr.Close()
		if _, err := zr.WriteTo(dst); err != nil {
			return fmt.Errorf("decompressing zstd shuffle stream: %w", err)
		}
	default:
		if _, err := io.Copy(dst, s2.NewReader(src)); err != nil {
			return fmt.Errorf("decompressing s2 shuffle stream: %w", err)
		}
	}
	return nil
}
