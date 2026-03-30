package parquet

import (
	"testing"
)

// FuzzDecodeFileMetaData tests that the Thrift decoder handles arbitrary input
// without panicking. This is critical because metadata is read from untrusted
// Parquet files that may be corrupted or maliciously crafted.
func FuzzDecodeFileMetaData(f *testing.F) {
	// Seed with a minimal valid Thrift-encoded FileMetaData.
	// version=1, schema=[], num_rows=0, row_groups=[]
	f.Add([]byte{
		0x15, 0x02, // field 1 (version): i32 zigzag = 1
		0x19, 0x00, // field 2 (schema): empty list
		0x16, 0x00, // field 3 (num_rows): i64 zigzag = 0
		0x19, 0x00, // field 4 (row_groups): empty list
		0x00,       // stop byte
	})

	// Seed with truncated data.
	f.Add([]byte{0x15, 0x02})

	// Seed with zero bytes.
	f.Add([]byte{0x00})

	// Seed with empty input.
	f.Add([]byte{})

	// Seed with large varint (many continuation bytes).
	f.Add([]byte{0x15, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic on any input. Errors are expected and fine.
		_, _ = DecodeFileMetaData(data)
	})
}

// FuzzDecodePageHeader tests that page header decoding handles arbitrary input
// without panicking.
func FuzzDecodePageHeader(f *testing.F) {
	// Seed with a minimal data page v1 header.
	f.Add([]byte{
		0x15, 0x00, // field 1 (type): i32 = 0 (DATA_PAGE)
		0x15, 0x14, // field 2 (uncompressed_page_size): i32 = 10
		0x15, 0x14, // field 3 (compressed_page_size): i32 = 10
		0x00,       // stop byte
	})

	f.Add([]byte{0x00})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodePageHeader(data)
	})
}

// FuzzThriftReadVarint tests varint decoding with arbitrary input.
func FuzzThriftReadVarint(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x80, 0x01})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		d := &thriftDecoder{buf: data}
		_, _ = d.readVarint()
	})
}
