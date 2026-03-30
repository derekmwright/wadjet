package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Parquet file layout:
//   [magic: "PAR1" (4 bytes)]
//   [row group data...]
//   [footer: Thrift-encoded FileMetaData]
//   [footer length: 4 bytes LE uint32]
//   [magic: "PAR1" (4 bytes)]
//
// Total trailer = footer + 4 (length) + 4 (magic) = footer + 8

var parquetMagic = [4]byte{'P', 'A', 'R', '1'}

const (
	// footerMaxSize is the maximum footer size we'll read. Parquet files
	// with footers larger than 64MB are pathological — the footer contains
	// only metadata (schema, row group offsets, statistics), not data.
	footerMaxSize = 64 << 20 // 64 MB

	// trailerSize is the fixed-size trailer: 4 bytes footer length + 4 bytes magic.
	trailerSize = 8

	// minFileSize is the minimum valid Parquet file: header magic + trailer.
	minFileSize = 4 + trailerSize
)

// ReadFileMetaData reads the FileMetaData from a Parquet file.
// It reads the trailing 8 bytes to get the footer length, then reads
// and decodes the Thrift-encoded footer.
func ReadFileMetaData(r io.ReaderAt, fileSize int64) (*FileMetaData, error) {
	if fileSize < minFileSize {
		return nil, fmt.Errorf("parquet: file too small (%d bytes, minimum %d)", fileSize, minFileSize)
	}

	// Read trailer: [footer_length: 4 bytes LE][magic: 4 bytes]
	trailer := make([]byte, trailerSize)
	if _, err := r.ReadAt(trailer, fileSize-trailerSize); err != nil {
		return nil, fmt.Errorf("parquet: reading trailer: %w", err)
	}

	// Verify trailing magic.
	if [4]byte(trailer[4:8]) != parquetMagic {
		return nil, fmt.Errorf("parquet: invalid magic %q (expected %q)", trailer[4:8], parquetMagic[:])
	}

	footerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if footerLen <= 0 {
		return nil, fmt.Errorf("parquet: invalid footer length %d", footerLen)
	}
	if footerLen > footerMaxSize {
		return nil, fmt.Errorf("parquet: footer too large (%d bytes, max %d)", footerLen, footerMaxSize)
	}
	if footerLen+trailerSize > fileSize-4 { // -4 for header magic
		return nil, fmt.Errorf("parquet: footer length %d exceeds file size %d", footerLen, fileSize)
	}

	// Read the footer bytes.
	footerOffset := fileSize - trailerSize - footerLen
	footer := make([]byte, footerLen)
	if _, err := r.ReadAt(footer, footerOffset); err != nil {
		return nil, fmt.Errorf("parquet: reading footer: %w", err)
	}

	// Decode Thrift-encoded FileMetaData.
	md, err := DecodeFileMetaData(footer)
	if err != nil {
		return nil, fmt.Errorf("parquet: decoding footer: %w", err)
	}
	return md, nil
}

// ValidateHeader checks that the file starts with the Parquet magic bytes.
func ValidateHeader(r io.ReaderAt) error {
	header := make([]byte, 4)
	if _, err := r.ReadAt(header, 0); err != nil {
		return fmt.Errorf("parquet: reading header: %w", err)
	}
	if [4]byte(header) != parquetMagic {
		return fmt.Errorf("parquet: invalid header magic %q (expected %q)", header, parquetMagic[:])
	}
	return nil
}
