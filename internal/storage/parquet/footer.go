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

// readAtFull is a tolerant ReadAt that treats (n == len(p), err == io.EOF)
// as a successful read instead of a failure. Per the io.ReaderAt contract:
//
//	If ReadAt is reading from an input source with a known end, ReadAt may
//	return either err == EOF or err == nil after reading the final bytes.
//
// minio-go's *minio.Object.ReadAt happily returns (n, io.EOF) when you read
// the very last bytes of an S3 object — for example the parquet trailer at
// (size-8). The previous code in ReadFileMetaData / ValidateHeader treated
// any non-nil error as fatal and broke schema autodetection against the
// SF100 bucket. The full-file read path in OpenFileReader already tolerated
// EOF; the trailer/footer/header readers were just inconsistent.
func readAtFull(r io.ReaderAt, p []byte, off int64) error {
	n, err := r.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return err
	}
	if n != len(p) {
		return fmt.Errorf("short read: got %d bytes, want %d", n, len(p))
	}
	return nil
}

// ReadFileMetaData reads the FileMetaData from a Parquet file.
// It reads the trailing 8 bytes to get the footer length, then reads
// and decodes the Thrift-encoded footer.
func ReadFileMetaData(r io.ReaderAt, fileSize int64) (*FileMetaData, error) {
	if fileSize < minFileSize {
		return nil, fmt.Errorf("parquet: file too small (%d bytes, minimum %d)", fileSize, minFileSize)
	}

	// Read trailer: [footer_length: 4 bytes LE][magic: 4 bytes]
	trailer := make([]byte, trailerSize)
	if err := readAtFull(r, trailer, fileSize-trailerSize); err != nil {
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
	if err := readAtFull(r, footer, footerOffset); err != nil {
		return nil, fmt.Errorf("parquet: reading footer: %w", err)
	}

	// Decode Thrift-encoded FileMetaData.
	md, err := DecodeFileMetaData(footer)
	if err != nil {
		return nil, fmt.Errorf("parquet: decoding footer: %w", err)
	}
	if err := ValidateFileMetaData(md); err != nil {
		return nil, fmt.Errorf("parquet: %w", err)
	}
	return md, nil
}

// ValidateFileMetaData holds a decoded footer to the claims it makes about
// itself, before any of its numbers is used to size something.
//
// A row group's num_rows is the size EVERY destination vector in a scan is
// allocated for (batch.NewRecordBatch(schema, numRows)), and it is a signed
// 64-bit thrift field nothing had checked. The whole-file mutation fuzz
// reached a negative one in seconds: "makeslice: len out of range", raised
// while building the batch, before a single page was read.
//
// The upper bound is the file's OWN total. Parquet requires
// FileMetaData.num_rows to be the sum of the row groups', so a group
// claiming more than the file does is already self-contradictory — and
// nothing legitimate is refused by taking the file at its word here, while a
// single flipped byte in one row group's varint no longer asks the allocator
// for terabytes. A zero total is left alone: writers that emit an empty file
// leave it at zero, and there are no rows to size anything from anyway.
func ValidateFileMetaData(md *FileMetaData) error {
	if md == nil {
		return fmt.Errorf("footer decoded to nothing")
	}
	if md.NumRows < 0 {
		return fmt.Errorf("footer declares %d rows", md.NumRows)
	}
	for i := range md.RowGroups {
		rg := &md.RowGroups[i]
		if rg.NumRows < 0 {
			return fmt.Errorf("row group %d declares %d rows", i, rg.NumRows)
		}
		if md.NumRows > 0 && rg.NumRows > md.NumRows {
			return fmt.Errorf("row group %d declares %d rows but the file declares %d in total",
				i, rg.NumRows, md.NumRows)
		}
	}
	return nil
}

// ValidateHeader checks that the file starts with the Parquet magic bytes.
func ValidateHeader(r io.ReaderAt) error {
	header := make([]byte, 4)
	if err := readAtFull(r, header, 0); err != nil {
		return fmt.Errorf("parquet: reading header: %w", err)
	}
	if [4]byte(header) != parquetMagic {
		return fmt.Errorf("parquet: invalid header magic %q (expected %q)", header, parquetMagic[:])
	}
	return nil
}
