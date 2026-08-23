package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
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

	// MaxRowsPerRowGroup bounds a row group's declared row count.
	//
	// A row group's num_rows is the length EVERY destination vector in a
	// scan is allocated for, so it is the one footer field that turns
	// directly into an allocation before any page is read. Writers do not
	// approach this figure: pyarrow's max_rows_per_group defaults to 2^20,
	// parquet-mr and Spark size row groups by BYTES (128 MiB), which at one
	// byte per row is 2^27 rows for a single-column table and far fewer for
	// anything real, and wadjet's own default is 128 * 1024. 2^26 is 64x
	// pyarrow's default and above anything the corpus contains, while
	// keeping the worst single allocation a corrupt footer can provoke to
	// tens of millions of entries rather than the terabyte a flipped varint
	// used to ask for.
	MaxRowsPerRowGroup = 1 << 26

	// maxRowsPerFileByte bounds a row count against the size of the file
	// that carries it. See rowCeiling.
	maxRowsPerFileByte = 1 << 12
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
	if err := ValidateFileMetaData(md, fileSize); err != nil {
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
// Refusing negatives was not enough, and neither was holding each row group
// to the FILE's total: the file's total is a varint out of the same footer.
// num_rows = 2^40 on a two-row file reached makeslice with 128 GiB and died
// as "fatal error: runtime: out of memory" — unrecoverable, so in a worker
// process it is the worker; 2^30 was accepted outright and decoded after
// allocating gibibytes. Three bounds close that, in the order they cost:
//
//  1. Nothing is negative.
//  2. Every row group is within MaxRowsPerRowGroup, and within what the
//     file's own BYTES can carry (rowCeiling). Both are policy ceilings,
//     documented at their constants.
//  3. The file's total is exactly the sum of its row groups'. The format
//     requires that, every writer in the corpus honours it (244 files,
//     wadjet's own and pyarrow's, checked), and it is the only check here
//     that is exact rather than generous — which makes it the one that
//     catches a single flipped varint wherever it landed.
func ValidateFileMetaData(md *FileMetaData, fileSize int64) error {
	if md == nil {
		return fmt.Errorf("footer decoded to nothing")
	}
	if md.NumRows < 0 {
		return fmt.Errorf("footer declares %d rows", md.NumRows)
	}
	ceiling := rowCeiling(fileSize)
	if md.NumRows > ceiling {
		return fmt.Errorf("footer declares %d rows, more than the %d a %d-byte file can carry",
			md.NumRows, ceiling, fileSize)
	}
	var sum int64
	for i := range md.RowGroups {
		rg := &md.RowGroups[i]
		if rg.NumRows < 0 {
			return fmt.Errorf("row group %d declares %d rows", i, rg.NumRows)
		}
		if rg.NumRows > MaxRowsPerRowGroup {
			return fmt.Errorf("row group %d declares %d rows, past the %d one row group may hold",
				i, rg.NumRows, int64(MaxRowsPerRowGroup))
		}
		if rg.NumRows > ceiling {
			return fmt.Errorf("row group %d declares %d rows, more than the %d a %d-byte file can carry",
				i, rg.NumRows, ceiling, fileSize)
		}
		sum += rg.NumRows
	}
	if sum != md.NumRows {
		return fmt.Errorf("footer declares %d rows but its %d row groups sum to %d",
			md.NumRows, len(md.RowGroups), sum)
	}
	return nil
}

// rowCeiling is the most rows a file of this many bytes can be carrying.
//
// There is no bound here that is both tight and sound: RLE encodes a run of
// arbitrarily many equal values in a handful of bytes, so in principle a few
// hundred bytes can declare billions of rows. What CAN be bounded is what a
// writer actually emits, and that was measured rather than guessed. The
// densest shapes pyarrow will produce -- one all-null or one constant column,
// zstd, dictionary on, pages as large as it allows -- land at 210 to 464 rows
// per byte across 1e6, 1e7 and 1e8 rows; the densest file in this repo's own
// corpus is 0.42 rows per byte. 2^12 leaves an order of magnitude of headroom
// over the densest thing a real writer produced, and still refuses 2^30 rows
// out of a kilobyte.
//
// This is a policy ceiling of the same kind as footerMaxSize and
// maxPageBodyBytes, and it is stated as one: a file past it is refused by
// name, not silently truncated.
func rowCeiling(fileSize int64) int64 {
	if fileSize <= 0 {
		return 0
	}
	if fileSize > math.MaxInt64/maxRowsPerFileByte {
		return math.MaxInt64
	}
	return fileSize * maxRowsPerFileByte
}

// CheckRowGroupRowCount is the allocation-site half of the row-count bound.
//
// ValidateFileMetaData is the enforcement point and runs on every open, but
// the number reaches an allocator through several doors -- the native
// columnar reader's batch, the row reader's per-column []any, the staged
// reader -- and a footer that arrived some other way (a cache, a test, a
// future call site) must not be able to walk through one of them. One
// comparison in front of each allocation is cheaper than trusting that the
// only path here is the validated one.
func CheckRowGroupRowCount(rgIdx int, numRows int64) error {
	if numRows < 0 {
		return fmt.Errorf("row group %d declares %d rows", rgIdx, numRows)
	}
	if numRows > MaxRowsPerRowGroup {
		return fmt.Errorf("row group %d declares %d rows, past the %d one row group may hold",
			rgIdx, numRows, int64(MaxRowsPerRowGroup))
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
