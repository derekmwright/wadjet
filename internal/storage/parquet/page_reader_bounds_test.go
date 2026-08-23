package parquet

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// Every offset and length a page reader navigates by comes out of the
// FOOTER or a PAGE HEADER, and both are the file's own claims about bytes
// the file does not have to actually contain. None of them were checked:
// DataPageOffset became a slice index directly (`r.data[r.off:]` at
// r.off = -9025 in five of the six whole-file fuzz crashers),
// CompressedPageSize became a slice length, and UncompressedPageSize became
// an allocation size.

// mutateChunkMeta rewrites one column chunk's metadata in a real file's
// footer and re-encodes it. The data pages are untouched; only the claims
// about where they are change.
func mutateChunkMeta(t *testing.T, raw []byte, edit func(*ColumnMetaData)) []byte {
	t.Helper()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	start := len(raw) - 8 - footerLen
	md, err := DecodeFileMetaData(raw[start : start+footerLen])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	if len(md.RowGroups) == 0 || len(md.RowGroups[0].Columns) == 0 {
		t.Fatal("the fixture has no column chunks")
	}
	edit(md.RowGroups[0].Columns[0].MetaData)
	footer := EncodeFileMetaData(md)
	out := make([]byte, 0, start+len(footer)+8)
	out = append(out, raw[:start]...)
	out = append(out, footer...)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(footer)))
	out = append(out, l[:]...)
	return append(out, "PAR1"...)
}

func boundsFixture(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, Schema{Columns: []Column{{Name: "c", Type: TypeInt64}}}, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rows := make([]map[string]any, 64)
	for i := range rows {
		rows[i] = map[string]any{"c": int64(i)}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

// readChunkPages drives a chunk to exhaustion the way every caller does, and
// reports the first error. A panic here is the failure the test exists for,
// and no recover is installed: the panic these offsets produced was raised
// inside the scan errgroup, where a recover in this package would not have
// caught it either.
func readChunkPages(t *testing.T, raw []byte) error {
	t.Helper()
	fr, err := OpenFileReaderFromBytes(raw)
	if err != nil {
		return err
	}
	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		return nil
	}
	defer pr.Close()
	if _, err := pr.NextDictionary(); err != nil {
		return err
	}
	for {
		page, err := pr.NextPage()
		if err != nil {
			return err
		}
		if page == nil {
			return nil
		}
		page.Release()
	}
}

func TestFooterOffsetsAreValidatedAgainstTheFile(t *testing.T) {
	good := boundsFixture(t)
	if err := readChunkPages(t, good); err != nil {
		t.Fatalf("the fixture does not read: %v", err)
	}

	cases := []struct {
		name string
		edit func(*ColumnMetaData)
		want string
	}{
		{"negative data page offset", func(cm *ColumnMetaData) { cm.DataPageOffset = -9025 }, "data_page_offset"},
		{"small negative data page offset", func(cm *ColumnMetaData) { cm.DataPageOffset = -29 }, "data_page_offset"},
		{"data page offset past EOF", func(cm *ColumnMetaData) { cm.DataPageOffset = 1 << 40 }, "past the"},
		{"negative total compressed size", func(cm *ColumnMetaData) {
			cm.TotalCompressedSize = -1 << 20
		}, "total_compressed_size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := readChunkPages(t, mutateChunkMeta(t, good, tc.edit))
			if err == nil {
				t.Fatal("the chunk read without error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// A TotalCompressedSize that runs past the data region used to be
	// CLAMPED, on the belief that writers round the figure up. They do not
	// (see ValidateChunkLayout), and clamping let an overstated chunk read
	// its neighbour's bytes as its own values. It is refused now.
	t.Run("oversized total compressed size is refused", func(t *testing.T) {
		raw := mutateChunkMeta(t, good, func(cm *ColumnMetaData) { cm.TotalCompressedSize += 1 << 20 })
		err := readChunkPages(t, raw)
		if err == nil {
			t.Fatal("the chunk read without error")
		}
		if !strings.Contains(err.Error(), "past the") {
			t.Errorf("error %q does not say the chunk runs past the file", err)
		}
	})

	// One byte over is still over. This is the shape that matters: a small
	// overstatement stays inside the file and reaches into the NEXT chunk.
	t.Run("total compressed size overstated by one byte", func(t *testing.T) {
		raw := mutateChunkMeta(t, good, func(cm *ColumnMetaData) { cm.TotalCompressedSize++ })
		if err := readChunkPages(t, raw); err == nil {
			t.Fatal("a one-byte overstatement was accepted")
		}
	})
}

// TestChunkRangeRefusesUnbackedClaims tests the guard directly, because our
// own thrift encoder drops a non-positive dictionary_page_offset and cannot
// produce the file that carries one. A file written elsewhere can.
func TestChunkRangeRefusesUnbackedClaims(t *testing.T) {
	cases := []struct {
		name string
		cm   ColumnMetaData
		size int64
		want string
	}{
		{"negative dictionary page offset",
			ColumnMetaData{DataPageOffset: 4, DictionaryPageOffset: -1, TotalCompressedSize: 16}, 1024,
			"dictionary_page_offset"},
		{"negative data page offset",
			ColumnMetaData{DataPageOffset: -9025, TotalCompressedSize: 16}, 1024, "data_page_offset"},
		{"start past EOF",
			ColumnMetaData{DataPageOffset: 4096, TotalCompressedSize: 16}, 1024, "past the end"},
		{"negative compressed size",
			ColumnMetaData{DataPageOffset: 4, TotalCompressedSize: -16}, 1024, "total_compressed_size"},
		{"negative file size",
			ColumnMetaData{DataPageOffset: 0, TotalCompressedSize: 0}, -1, "file size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := chunkRange(&tc.cm, tc.size); err == nil {
				t.Fatal("accepted")
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// The benign shape still resolves: a dictionary page before the data
	// page moves the start back.
	start, end, err := chunkRange(&ColumnMetaData{
		DataPageOffset: 100, DictionaryPageOffset: 40, TotalCompressedSize: 60}, 1024)
	if err != nil || start != 40 || end != 100 {
		t.Errorf("dictionary-first chunk = (%d, %d, %v), want (40, 100, nil)", start, end, err)
	}
	// A chunk that ends exactly at the last byte of the file is not
	// overstated, and must not be refused along with the ones that are.
	start, end, err = chunkRange(&ColumnMetaData{DataPageOffset: 900, TotalCompressedSize: 124}, 1024)
	if err != nil || start != 900 || end != 1024 {
		t.Errorf("chunk ending at EOF = (%d, %d, %v), want (900, 1024, nil)", start, end, err)
	}
	// And one that runs past it is refused rather than clamped.
	if _, _, err := chunkRange(&ColumnMetaData{DataPageOffset: 900, TotalCompressedSize: 1 << 20}, 1024); err == nil {
		t.Error("an oversized chunk was clamped instead of refused")
	}
	// total_compressed_size large enough to overflow the addition.
	if _, _, err := chunkRange(&ColumnMetaData{
		DataPageOffset: 900, TotalCompressedSize: 1<<63 - 1}, 1024); err == nil {
		t.Error("an overflowing total_compressed_size was accepted")
	}
}

// TestPageHeaderSizesAreValidatedAgainstTheChunk covers the other half: the
// page header's own two size fields. CompressedPageSize was sliced with
// (`r.data[r.off : r.off+size]`) and UncompressedPageSize was handed to the
// decompressors, which pre-allocate from it.
func TestPageHeaderSizesAreValidatedAgainstTheChunk(t *testing.T) {
	body := make([]byte, 64*8)
	for i := 0; i < 64; i++ {
		binary.LittleEndian.PutUint64(body[i*8:], uint64(i))
	}
	build := func(compressed, uncompressed int32) []byte {
		header := EncodePageHeader(&PageHeader{
			Type:                 PageDataV1,
			UncompressedPageSize: uncompressed,
			CompressedPageSize:   compressed,
			DataPageHeader: &DataPageHeader{
				NumValues:               64,
				Encoding:                EncodingPlain,
				DefinitionLevelEncoding: EncodingRLE,
				RepetitionLevelEncoding: EncodingRLE,
			},
		})
		const dataStart = 4
		chunkBytes := int64(len(header) + len(body))
		int64Phys := PhysicalInt64
		md := &FileMetaData{
			Version: 1,
			Schema: []SchemaElement{
				{Name: "hand_built", NumChildren: 1},
				{Name: "c", Type: &int64Phys, RepetitionType: FieldRequired},
			},
			NumRows: 64,
			RowGroups: []RowGroup{{
				NumRows:       64,
				TotalByteSize: chunkBytes,
				Columns: []ColumnChunk{{
					FileOffset: dataStart,
					MetaData: &ColumnMetaData{
						Type: PhysicalInt64, Encodings: []Encoding{EncodingPlain},
						PathInSchema: []string{"c"}, Codec: CodecNone, NumValues: 64,
						TotalUncompressedSize: chunkBytes, TotalCompressedSize: chunkBytes,
						DataPageOffset: dataStart,
					},
				}},
			}},
			CreatedBy: "wadjet page-bounds test",
		}
		out := append([]byte("PAR1"), header...)
		out = append(out, body...)
		footer := EncodeFileMetaData(md)
		out = append(out, footer...)
		var l [4]byte
		binary.LittleEndian.PutUint32(l[:], uint32(len(footer)))
		out = append(out, l[:]...)
		return append(out, "PAR1"...)
	}

	if err := readChunkPages(t, build(int32(len(body)), int32(len(body)))); err != nil {
		t.Fatalf("the honest file does not read: %v", err)
	}
	for _, tc := range []struct {
		name                     string
		compressed, uncompressed int32
		want                     string
	}{
		{"compressed size overruns the chunk", 1 << 20, int32(len(body)), "body but the chunk ends"},
		{"negative compressed size", -1, int32(len(body)), "body but the chunk ends"},
		{"uncompressed size claims 8 GiB", int32(len(body)), 1<<31 - 1, "uncompressed size"},
		{"negative uncompressed size", int32(len(body)), -1, "uncompressed size"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := readChunkPages(t, build(tc.compressed, tc.uncompressed))
			if err == nil {
				t.Fatal("the page read without error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}
