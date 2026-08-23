package scan

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The (catalog, file) type matrix ran one shape: a PLAIN, single-page,
// three-row file through ReadRowGroupNative. Three of the decoder's four
// surfaces were therefore untested by it.
//
//   - DICTIONARY pages take a different route in
//     (resolveNativeDictionaryScratch switches on the FILE's physical type
//     and gathers, and only then hands the result to the same copy paths).
//   - The SELECTION decoder (sel_decode.go) and the LENGTHS-only decoder
//     (lengths_decode.go) are separate walks over the same pages, and
//     ReadRowGroupNativeSel discards a selection vector unless it covers less
//     than a quarter of the row group — which a three-row fixture can never
//     satisfy, so no sel arm was reachable from the old fixture at all.
//
// This drives every cell through all three read paths, over both a PLAIN and
// a dictionary-encoded file. The property is the same and still total: each
// cell either decodes, or comes back as an error naming the column. Not a
// panic, and not a disagreement between the three paths about which.

// matrixRows is enough rows that ReadRowGroupNativeSel keeps the selection
// vector (it drops one covering a quarter of the row group or more).
const matrixRows = 40

func matrixSel() []uint32 {
	sel := make([]uint32, 0, matrixRows/8)
	for i := 0; i < matrixRows; i += 8 {
		sel = append(sel, uint32(i))
	}
	return sel
}

// writeMatrixFile writes a single-column file with one uncompressed page per
// chunk: 40 rows, every fifth one NULL, all present values identical (which
// is what lets dictEncodeOneColumnFile rewrite it with a one-entry
// dictionary).
func writeMatrixFile(t testing.TB, id pqt.TypeID) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{colFor(id)}}
	cfg := pqt.DefaultWriterConfig()
	cfg.Compression = pqt.CompressionNone
	cfg.PageBufferSize = 0 // one page per chunk: no splitting
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("%s writer: %v", id, err)
	}
	rows := make([]map[string]any, matrixRows)
	for i := range rows {
		v := sampleFor(id)
		if i%5 == 0 {
			v = nil
		}
		rows[i] = map[string]any{"c": v}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("%s write: %v", id, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("%s close: %v", id, err)
	}
	return buf.Bytes()
}

// dictEncodeOneColumnFile rewrites a PLAIN single-page file as a
// dictionary-encoded one, keeping the footer's SCHEMA untouched so the file
// recovers the same type it did before — only the page layout changes.
//
// The wadjet writer never emits a dictionary page, so there is no other way
// to get a dictionary-encoded file of every type: the pyarrow fixtures in
// testdata cover two of them. The transformation is mechanical because the
// dictionary page body IS the PLAIN value bytes: take the page's values as
// written, keep the first one as a one-entry dictionary, and replace the
// value section with an index stream that points every row at it.
func dictEncodeOneColumnFile(t testing.TB, raw []byte) []byte {
	t.Helper()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	footerStart := len(raw) - 8 - footerLen
	md, err := pqt.DecodeFileMetaData(raw[footerStart : footerStart+footerLen])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	if len(md.RowGroups) != 1 || len(md.RowGroups[0].Columns) != 1 {
		t.Fatalf("fixture has %d row groups", len(md.RowGroups))
	}
	cm := md.RowGroups[0].Columns[0].MetaData

	// The chunk's single data page: header, then [4-byte def-level length]
	// [def levels RLE][PLAIN values].
	ph, hdrLen, err := pqt.DecodePageHeader(raw[cm.DataPageOffset:])
	if err != nil {
		t.Fatalf("decoding the page header: %v", err)
	}
	if ph.DataPageHeader == nil {
		t.Fatalf("the fixture's first page is not a v1 data page")
	}
	bodyStart := int(cm.DataPageOffset) + hdrLen
	body := raw[bodyStart : bodyStart+int(ph.CompressedPageSize)]
	defLen := int(binary.LittleEndian.Uint32(body[:4]))
	levels := body[:4+defLen]
	values := body[4+defLen:]

	// One dictionary entry: the first value, in the width its physical type
	// gives it.
	var entryLen int
	switch cm.Type {
	case pqt.PhysicalInt32, pqt.PhysicalFloat:
		entryLen = 4
	case pqt.PhysicalInt64, pqt.PhysicalDouble:
		entryLen = 8
	case pqt.PhysicalFixedLenByteArray:
		entryLen = len(values) / nonNullCount(int(ph.DataPageHeader.NumValues))
	case pqt.PhysicalByteArray:
		entryLen = 4 + int(binary.LittleEndian.Uint32(values[:4]))
	default:
		t.Fatalf("no dictionary encoding for physical type %v", cm.Type)
	}
	dictBody := values[:entryLen]

	// The index stream: a one-byte bit width, then an RLE run of index 0,
	// one per non-null value.
	idx := []byte{1}
	idx = binary.AppendUvarint(idx, uint64(nonNullCount(int(ph.DataPageHeader.NumValues)))<<1)
	idx = append(idx, 0)

	dataBody := append(append([]byte{}, levels...), idx...)

	dictHeader := pqt.EncodePageHeader(&pqt.PageHeader{
		Type:                 pqt.PageDictionary,
		UncompressedPageSize: int32(len(dictBody)),
		CompressedPageSize:   int32(len(dictBody)),
		DictionaryPageHeader: &pqt.DictionaryPageHeader{NumValues: 1, Encoding: pqt.EncodingPlain},
	})
	dataHeader := pqt.EncodePageHeader(&pqt.PageHeader{
		Type:                 pqt.PageDataV1,
		UncompressedPageSize: int32(len(dataBody)),
		CompressedPageSize:   int32(len(dataBody)),
		DataPageHeader: &pqt.DataPageHeader{
			NumValues:               ph.DataPageHeader.NumValues,
			Encoding:                pqt.EncodingRLEDictionary,
			DefinitionLevelEncoding: pqt.EncodingRLE,
			RepetitionLevelEncoding: pqt.EncodingRLE,
		},
	})

	out := append([]byte{}, "PAR1"...)
	dictOff := int64(len(out))
	out = append(out, dictHeader...)
	out = append(out, dictBody...)
	dataOff := int64(len(out))
	out = append(out, dataHeader...)
	out = append(out, dataBody...)

	cm.DictionaryPageOffset = dictOff
	cm.DataPageOffset = dataOff
	cm.TotalCompressedSize = int64(len(out)) - dictOff
	cm.TotalUncompressedSize = cm.TotalCompressedSize
	cm.Codec = pqt.CodecNone
	cm.Encodings = []pqt.Encoding{pqt.EncodingPlain, pqt.EncodingRLEDictionary}
	md.RowGroups[0].Columns[0].FileOffset = dictOff
	md.RowGroups[0].TotalByteSize = cm.TotalCompressedSize

	return withFooter(out, pqt.EncodeFileMetaData(md))
}

// nonNullCount is the fixture's own null pattern: every fifth row.
func nonNullCount(rows int) int { return rows - (rows+4)/5 }

// TestNativeTypePairMatrixAcrossEncodingsAndPaths is the extension: the same
// 361 cells, but through the selection and lengths-only decoders as well as
// the full one, over a dictionary-encoded file as well as a PLAIN one.
func TestNativeTypePairMatrixAcrossEncodingsAndPaths(t *testing.T) {
	type fixture struct {
		fr  *pqt.FileReader
		rec pqt.TypeID
	}
	plain := make(map[pqt.TypeID]fixture, len(flatTypes))
	dict := make(map[pqt.TypeID]fixture, len(flatTypes))
	for _, ft := range flatTypes {
		raw := writeMatrixFile(t, ft)
		fr, err := pqt.OpenFileReaderFromBytes(raw)
		if err != nil {
			t.Fatalf("%s: %v", ft, err)
		}
		plain[ft] = fixture{fr, pqt.TypeIDFromSchemaNode(fr.Leaves()[0])}

		// BOOLEAN is left out of the dictionary arm on purpose: the format
		// does not dictionary-encode it and no writer produces one.
		if ft == pqt.TypeBool {
			continue
		}
		dfr, err := pqt.OpenFileReaderFromBytes(dictEncodeOneColumnFile(t, raw))
		if err != nil {
			t.Fatalf("%s dictionary fixture: %v", ft, err)
		}
		dict[ft] = fixture{dfr, pqt.TypeIDFromSchemaNode(dfr.Leaves()[0])}
	}

	sel := matrixSel()
	for _, enc := range []struct {
		name  string
		files map[pqt.TypeID]fixture
	}{{"plain", plain}, {"dict", dict}} {
		for _, ft := range flatTypes {
			f, ok := enc.files[ft]
			if !ok {
				continue
			}
			for _, ct := range flatTypes {
				name := enc.name + "/" + ft.String() + "_file/" + ct.String() + "_catalog"
				t.Run(name, func(t *testing.T) {
					want := pqt.StorageClassOf(f.rec) == pqt.StorageClassOf(ct) ||
						pqt.CoercibleTo(f.rec, ct)
					schema := []pqt.Column{colFor(ct)}
					paths := []struct {
						what string
						run  func() (*batch.RecordBatch, error)
					}{
						{"full", func() (*batch.RecordBatch, error) {
							return ReadRowGroupNative(f.fr, 0, schema, nil)
						}},
						{"sel", func() (*batch.RecordBatch, error) {
							return ReadRowGroupNativeSel(f.fr, 0, schema, nil, sel)
						}},
						{"shaped", func() (*batch.RecordBatch, error) {
							return ReadRowGroupNativeShaped(f.fr, 0, schema, nil, sel,
								map[string]bool{"c": true})
						}},
					}
					for _, p := range paths {
						b, err := p.run()
						switch {
						case want && err != nil:
							t.Fatalf("%s: file %s (recovered %s) as catalog %s: %v",
								p.what, ft, f.rec, ct, err)
						case want && b == nil:
							t.Fatalf("%s: file %s as catalog %s: no batch and no error", p.what, ft, ct)
						case !want && err == nil:
							t.Fatalf("%s: file %s (recovered %s) decoded as catalog %s with no error",
								p.what, ft, f.rec, ct)
						}
						if err == nil {
							touchEveryValue(b.Columns[0], b.Len)
						}
					}
				})
			}
		}
	}
}

// touchEveryValue reads every position, which is what turns a bad copy into
// an observable failure rather than a quiet one. A shape-only column has no
// value bytes by construction and answers GetValue with a panic on purpose,
// so its lengths are what there is to read.
func touchEveryValue(v *batch.Vector, n int) {
	if v.BytesData.ShapeOnly {
		for i := 0; i < n; i++ {
			_ = v.BytesData.LengthAt(i)
		}
		return
	}
	for i := 0; i < n; i++ {
		_ = v.GetValue(i)
	}
}

// TestKnownPanicPairsUnderDictAndSel pins the cells the bucketed
// storageClass got wrong, in the shapes the original matrix could not
// reach: a dictionary-encoded page, and the selection and lengths-only
// decoders.
func TestKnownPanicPairsUnderDictAndSel(t *testing.T) {
	pairs := []struct{ file, catalog pqt.TypeID }{
		{pqt.TypeDecimal, pqt.TypeString},
		{pqt.TypeString, pqt.TypeDecimal},
		{pqt.TypeString, pqt.TypeVector},
		{pqt.TypeVector, pqt.TypeString},
		{pqt.TypeDecimal, pqt.TypeVector},
		{pqt.TypeVector, pqt.TypeDecimal},
		{pqt.TypeBytes, pqt.TypeDecimal},
		{pqt.TypeBytes, pqt.TypeVector},
		{pqt.TypeIPv6, pqt.TypeDecimal},
		{pqt.TypeIPv6, pqt.TypeVector},
		{pqt.TypeCIDR, pqt.TypeDecimal},
		{pqt.TypeCIDR, pqt.TypeVector},
		{pqt.TypeUUID, pqt.TypeDecimal},
		{pqt.TypeUUID, pqt.TypeVector},
		{pqt.TypeDecimal, pqt.TypeBytes},
		{pqt.TypeVector, pqt.TypeBytes},
	}
	sel := matrixSel()
	for _, tc := range pairs {
		raw := writeMatrixFile(t, tc.file)
		for _, enc := range []struct {
			name string
			raw  []byte
		}{{"plain", raw}, {"dict", dictEncodeOneColumnFile(t, raw)}} {
			t.Run(enc.name+"/"+tc.file.String()+"_as_"+tc.catalog.String(), func(t *testing.T) {
				fr, err := pqt.OpenFileReaderFromBytes(enc.raw)
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				schema := []pqt.Column{colFor(tc.catalog)}
				for what, run := range map[string]func() (*batch.RecordBatch, error){
					"full": func() (*batch.RecordBatch, error) { return ReadRowGroupNative(fr, 0, schema, nil) },
					"sel":  func() (*batch.RecordBatch, error) { return ReadRowGroupNativeSel(fr, 0, schema, nil, sel) },
					"shaped": func() (*batch.RecordBatch, error) {
						return ReadRowGroupNativeShaped(fr, 0, schema, nil, sel, map[string]bool{"c": true})
					},
				} {
					b, err := run()
					if err == nil {
						t.Errorf("%s: decoded a %s column as %s: %v", what, tc.file, tc.catalog, b)
					} else if !contains(err.Error(), tc.catalog.String()) {
						t.Errorf("%s: error %q does not name the declared type", what, err)
					}
					if b != nil {
						t.Errorf("%s: a batch came back alongside the error", what)
					}
				}
			})
		}
	}
}

// TestDictMatrixFixtureIsReallyDictionaryEncoded is the precondition the
// dictionary arm rests on. Without it the arm could be silently re-running
// the PLAIN one: a fixture whose pages did not come out dictionary-encoded
// would still decode, and every cell would still pass.
func TestDictMatrixFixtureIsReallyDictionaryEncoded(t *testing.T) {
	for _, ft := range flatTypes {
		if ft == pqt.TypeBool {
			continue
		}
		t.Run(ft.String(), func(t *testing.T) {
			raw := dictEncodeOneColumnFile(t, writeMatrixFile(t, ft))
			fr, err := pqt.OpenFileReaderFromBytes(raw)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			pr := fr.ColumnPages(0, 0)
			if pr == nil {
				t.Fatal("no page reader")
			}
			defer pr.Close()
			dict, pure, err := pr.DictionaryIfPure()
			if err != nil {
				t.Fatalf("walking the chunk's page headers: %v", err)
			}
			if !pure || dict == nil {
				t.Fatalf("the fixture's pages are not pure dictionary (pure=%v dict=%v)", pure, dict)
			}
			if dict.NumValues != 1 {
				t.Errorf("dictionary holds %d entries, want 1", dict.NumValues)
			}

			// And it reads back as the value that was written, on all three
			// paths — a dictionary that resolves to the wrong entry would
			// otherwise pass the matrix, which only asks for decode-or-error.
			schema := []pqt.Column{colFor(ft)}
			plainFR, err := pqt.OpenFileReaderFromBytes(writeMatrixFile(t, ft))
			if err != nil {
				t.Fatalf("open plain: %v", err)
			}
			wantBatch, err := ReadRowGroupNative(plainFR, 0, schema, nil)
			if err != nil {
				t.Fatalf("plain read: %v", err)
			}
			gotBatch, err := ReadRowGroupNative(fr, 0, schema, nil)
			if err != nil {
				t.Fatalf("dict read: %v", err)
			}
			if gotBatch.Len != wantBatch.Len {
				t.Fatalf("dict read %d rows, plain read %d", gotBatch.Len, wantBatch.Len)
			}
			for i := 0; i < wantBatch.Len; i++ {
				wn := wantBatch.Columns[0].Nulls.IsNull(i)
				gn := gotBatch.Columns[0].Nulls.IsNull(i)
				if wn != gn {
					t.Fatalf("row %d: dict IS NULL = %v, plain = %v", i, gn, wn)
				}
				if wn {
					continue
				}
				if got, want := gotBatch.Columns[0].GetValue(i), wantBatch.Columns[0].GetValue(i); !valuesEqual(got, want) {
					t.Fatalf("row %d: dict %v, plain %v", i, got, want)
				}
			}
		})
	}
}

func valuesEqual(a, b any) bool {
	ab, aok := a.([]byte)
	bb, bok := b.([]byte)
	if aok && bok {
		return bytes.Equal(ab, bb)
	}
	af, aok := a.([]float32)
	bf, bok := b.([]float32)
	if aok && bok {
		if len(af) != len(bf) {
			return false
		}
		for i := range af {
			if af[i] != bf[i] {
				return false
			}
		}
		return true
	}
	return a == b
}
