package parquet

import (
	"fmt"
	"reflect"
	"testing"
)

// sliceRowGroups is a RowGroupBytes that hands out sub-slices of a whole-file
// buffer — the shape the scan builds one row group at a time, without the
// streaming. copyOut makes each row group its OWN buffer, so a decode that
// read outside its row group reads different bytes rather than the right ones
// by accident.
type sliceRowGroups struct {
	fr      *FileReader
	data    []byte
	copyOut bool
	handed  map[int]int // rgIdx -> times asked
}

func newSliceRowGroups(t *testing.T, data []byte, copyOut bool) (*FileReader, *sliceRowGroups) {
	t.Helper()
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Re-open metadata-only so the reader has no whole-file backing of its
	// own: row-group mode must be the only source of bytes.
	meta, err := OpenFileReaderMetadata(newBytesReaderAt(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	src := &sliceRowGroups{fr: fr, data: data, copyOut: copyOut, handed: map[int]int{}}
	meta.SetRowGroupBytes(src, int64(len(data)))
	return meta, src
}

func (s *sliceRowGroups) RowGroupBytes(rgIdx int) ([]byte, int64, error) {
	start, end, ok, err := s.fr.RowGroupByteRange(rgIdx)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, fmt.Errorf("row group %d has no bytes", rgIdx)
	}
	s.handed[rgIdx]++
	buf := s.data[start:end]
	if s.copyOut {
		cp := make([]byte, end-start)
		copy(cp, buf)
		buf = cp
	}
	return buf, start, nil
}

// TestRowGroupByteRangesTileTheDataRegion: the ranges row-group mode reads by
// must cover every column chunk of their row group, ascend, and never overlap
// another row group's. If they did not, a row group's buffer would be missing
// bytes its own decode needs, or would be freed while another row group still
// needed it.
func TestRowGroupByteRangesTileTheDataRegion(t *testing.T) {
	for _, comp := range []Compression{CompressionNone, CompressionSnappy, CompressionZstd} {
		t.Run(fmt.Sprint(comp), func(t *testing.T) {
			data := writeStagedTestFile(t, comp, 256, 1000)
			fr, err := OpenFileReaderFromBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			if fr.NumRowGroups() < 2 {
				t.Fatalf("fixture produced %d row groups, want >= 2", fr.NumRowGroups())
			}
			var prevEnd int64
			for rg := 0; rg < fr.NumRowGroups(); rg++ {
				start, end, ok, err := fr.RowGroupByteRange(rg)
				if err != nil || !ok {
					t.Fatalf("row group %d: ok=%v err=%v", rg, ok, err)
				}
				if start < prevEnd {
					t.Fatalf("row group %d starts at %d, inside row group %d which ends at %d",
						rg, start, rg-1, prevEnd)
				}
				if end <= start {
					t.Fatalf("row group %d spans [%d, %d)", rg, start, end)
				}
				// Every chunk of this row group lies inside the range.
				md := fr.RowGroupMeta(rg)
				for j := range md.Columns {
					cm := md.Columns[j].MetaData
					if cm == nil || cm.TotalCompressedSize <= 0 {
						continue
					}
					cs, ce, cerr := chunkRange(cm, int64(len(data)))
					if cerr != nil {
						t.Fatal(cerr)
					}
					if cs < start || ce > end {
						t.Fatalf("row group %d column %d spans [%d, %d), outside the row group's [%d, %d)",
							rg, j, cs, ce, start, end)
					}
				}
				prevEnd = end
			}
		})
	}
}

// TestRowGroupModeDecodesWhatTheWholeFileDecodes is the parity gate: the same
// file read one row group at a time must produce the same values as the
// whole-file reader, for every codec, with each row group in a buffer of its
// own so a read outside the row group cannot land on the right bytes by
// accident.
func TestRowGroupModeDecodesWhatTheWholeFileDecodes(t *testing.T) {
	codecs := []struct {
		name string
		comp Compression
	}{
		{"none", CompressionNone}, // the aliasing codec: pages point into the buffer
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
		{"gzip", CompressionGzip},
		{"lz4", CompressionLZ4},
	}
	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			data := writeStagedTestFile(t, c.comp, 256, 1000)
			ref, err := NewReaderFromBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			if ref.NumRowGroups() < 2 {
				t.Fatalf("fixture produced %d row groups, want >= 2", ref.NumRowGroups())
			}
			want, err := ref.ReadRows(nil)
			if err != nil {
				t.Fatal(err)
			}

			fr, src := newSliceRowGroups(t, data, true)
			got := make([]map[string]any, 0, len(want))
			for rg := 0; rg < fr.NumRowGroups(); rg++ {
				rows, err := readRowGroupThroughReader(t, fr, rg)
				if err != nil {
					t.Fatalf("row group %d: %v", rg, err)
				}
				got = append(got, rows...)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("row-group-mode rows differ from the whole-file reader")
			}
			for rg := 0; rg < fr.NumRowGroups(); rg++ {
				if src.handed[rg] == 0 {
					t.Fatalf("row group %d was never served from row-group mode — the test proved nothing", rg)
				}
			}
		})
	}
}

// readRowGroupThroughReader reads one row group through a FileReader's own
// page path, which is what row-group mode changes.
func readRowGroupThroughReader(t *testing.T, fr *FileReader, rg int) ([]map[string]any, error) {
	t.Helper()
	r := &Reader{fr: fr, schema: fr.Schema()}
	return r.ReadRowGroup(rg, nil)
}

// TestRowGroupModeRefusesBytesThatAreNotTheRowGroups: a source that hands back
// a buffer the chunk does not fall inside must produce an ERROR, never values.
// Decoding a chunk from the wrong offset returns a neighbour's bytes as this
// column's values with err == nil, which is the silent-wrong-answer shape the
// chunk-range validation exists for.
func TestRowGroupModeRefusesBytesThatAreNotTheRowGroups(t *testing.T) {
	data := writeStagedTestFile(t, CompressionNone, 256, 1000)
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		src  RowGroupBytes
	}{
		{"buffer too short", rowGroupBytesFunc(func(rg int) ([]byte, int64, error) {
			start, end, _, err := fr.RowGroupByteRange(rg)
			if err != nil {
				return nil, 0, err
			}
			return data[start : end-1], start, nil // one byte short
		})},
		{"wrong base offset", rowGroupBytesFunc(func(rg int) ([]byte, int64, error) {
			start, end, _, err := fr.RowGroupByteRange(rg)
			if err != nil {
				return nil, 0, err
			}
			return data[start:end], start + 1, nil // claims to start one byte later
		})},
		{"another row group's bytes", rowGroupBytesFunc(func(rg int) ([]byte, int64, error) {
			other := (rg + 1) % fr.NumRowGroups()
			start, end, _, err := fr.RowGroupByteRange(other)
			if err != nil {
				return nil, 0, err
			}
			return data[start:end], start, nil
		})},
		{"source failure", rowGroupBytesFunc(func(rg int) ([]byte, int64, error) {
			return nil, 0, fmt.Errorf("no bytes for row group %d", rg)
		})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta, err := OpenFileReaderMetadata(newBytesReaderAt(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			meta.SetRowGroupBytes(tc.src, int64(len(data)))
			if _, err := readRowGroupThroughReader(t, meta, 0); err == nil {
				t.Fatal("decoded a row group from bytes that are not its own, and returned no error")
			}
		})
	}
}

type rowGroupBytesFunc func(rgIdx int) ([]byte, int64, error)

func (f rowGroupBytesFunc) RowGroupBytes(rgIdx int) ([]byte, int64, error) { return f(rgIdx) }

// TestRowGroupModeYieldsToAWholeFileBacking: a reader that already has bytes
// keeps them. Row-group mode is only ever the backing when there is no other,
// so attaching a source to a loaded reader cannot silently redirect its reads.
func TestRowGroupModeYieldsToAWholeFileBacking(t *testing.T) {
	data := writeStagedTestFile(t, CompressionNone, 256, 500)
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	fr.SetRowGroupBytes(rowGroupBytesFunc(func(int) ([]byte, int64, error) {
		return nil, 0, fmt.Errorf("must not be consulted")
	}), int64(len(data)))
	if _, err := readRowGroupThroughReader(t, fr, 0); err != nil {
		t.Fatalf("whole-file reader consulted the row-group source: %v", err)
	}
}
