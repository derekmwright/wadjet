package scan

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A dictionary page that declares (and holds) ZERO entries, with a data page
// that still carries indices. Every native gather computed its bound as
// len(dictOffsets)-1, which is -1 for an empty dictionary, and then tested it
// with `uint(idx) >= uint(numVals)` — uint(-1) is MaxUint, so the guard was
// vacuous and the gather indexed a nil offsets slice. The panic was raised
// inside a per-column errgroup goroutine, where no caller can recover it.
//
// The row path refused the same bytes by name, which is what makes this an
// ADR-0018 §3 defect and not merely a crash: one file, two readers, two
// answers.
func TestEmptyDictionaryWithLiveIndicesErrorsNotPanics(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked instead of returning an error: %v", r)
		}
	}()
	dict := &pqt.DictionaryData{NumValues: 0, Data: pqt.ByteArrayValues(nil, nil)}
	page := pqt.PlainInt32Values([]int32{0, 0, 0})
	v, err := resolveNativeDictionaryScratch(nil, dict, page, pqt.TypeString)
	if err == nil {
		t.Fatalf("an empty dictionary with 3 live indices was accepted: %v", v)
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error %q does not name the out-of-range index", err)
	}
}

// The well-formed shape #432 fixed — an empty dictionary page for a row group
// whose values are all NULL, so the data page carries no indices at all — must
// still decode. The clamp must refuse indices, not dictionaries.
func TestEmptyDictionaryWithNoIndicesIsFine(t *testing.T) {
	dict := &pqt.DictionaryData{NumValues: 0, Data: pqt.ByteArrayValues(nil, nil)}
	page := pqt.PlainInt32Values(nil)
	if _, err := resolveNativeDictionaryScratch(nil, dict, page, pqt.TypeString); err != nil {
		t.Fatalf("an empty dictionary with no indices was refused: %v", err)
	}
}

// The same shape as a whole FILE, through every read path there is. Each one
// must refuse it, and none may fault: this is the file a single flipped byte
// in a pyarrow-written column chunk produces.
func TestEmptyDictionaryPageIsRefusedOnEveryPath(t *testing.T) {
	raw := emptyDictionaryFile(t, dictEncodeOneColumnFile(t, writeMatrixFile(t, pqt.TypeString)))
	col := colFor(pqt.TypeString)
	sel := matrixSel()

	fr, err := pqt.OpenFileReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("opening the file: %v", err)
	}
	for _, p := range []struct {
		what string
		run  func() (*batch.RecordBatch, error)
	}{
		{"full", func() (*batch.RecordBatch, error) {
			return ReadRowGroupNative(fr, 0, []pqt.Column{col}, nil)
		}},
		{"sel", func() (*batch.RecordBatch, error) {
			return ReadRowGroupNativeSel(fr, 0, []pqt.Column{col}, nil, sel)
		}},
		{"shaped", func() (*batch.RecordBatch, error) {
			return ReadRowGroupNativeShaped(fr, 0, []pqt.Column{col}, nil, sel,
				map[string]bool{col.Name: true})
		}},
	} {
		b, err := p.run()
		if err == nil {
			t.Errorf("%s path accepted an empty dictionary with live indices: %v", p.what, b)
			continue
		}
		if !strings.Contains(err.Error(), "out of range") {
			t.Errorf("%s path error %q does not name the out-of-range index", p.what, err)
		}
	}

	r, err := pqt.NewReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("opening the file for the row path: %v", err)
	}
	if rows, err := r.ReadRowsAs([]pqt.Column{col}, nil); err == nil {
		t.Errorf("the row path accepted an empty dictionary with live indices: %d rows", len(rows))
	}
}

// emptyDictionaryFile rewrites an already dictionary-encoded single-column
// file so its dictionary page declares and holds nothing, leaving the data
// page's index stream exactly as it was. The result is a file whose indices
// all point past the end of a dictionary that is not there.
//
// It composes with dictEncodeOneColumnFile rather than building a footer from
// scratch: the schema, the levels and the index stream all stay byte-identical
// to a file the reader accepts, so the only thing under test is the empty
// dictionary.
func emptyDictionaryFile(t testing.TB, raw []byte) []byte {
	t.Helper()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	footerStart := len(raw) - 8 - footerLen
	md, err := pqt.DecodeFileMetaData(raw[footerStart : footerStart+footerLen])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	cm := md.RowGroups[0].Columns[0].MetaData
	if cm.DictionaryPageOffset == 0 {
		t.Fatalf("the input file has no dictionary page")
	}

	dph, dataHdrLen, err := pqt.DecodePageHeader(raw[cm.DataPageOffset:])
	if err != nil {
		t.Fatalf("decoding the data page header: %v", err)
	}
	dataHeader := raw[cm.DataPageOffset : int(cm.DataPageOffset)+dataHdrLen]
	dataBody := raw[int(cm.DataPageOffset)+dataHdrLen : int(cm.DataPageOffset)+dataHdrLen+int(dph.CompressedPageSize)]

	emptyDictHeader := pqt.EncodePageHeader(&pqt.PageHeader{
		Type:                 pqt.PageDictionary,
		UncompressedPageSize: 0,
		CompressedPageSize:   0,
		DictionaryPageHeader: &pqt.DictionaryPageHeader{NumValues: 0, Encoding: pqt.EncodingPlain},
	})

	out := append([]byte{}, "PAR1"...)
	dictOff := int64(len(out))
	out = append(out, emptyDictHeader...)
	dataOff := int64(len(out))
	out = append(out, dataHeader...)
	out = append(out, dataBody...)

	cm.DictionaryPageOffset = dictOff
	cm.DataPageOffset = dataOff
	cm.TotalCompressedSize = int64(len(out)) - dictOff
	cm.TotalUncompressedSize = cm.TotalCompressedSize
	md.RowGroups[0].Columns[0].FileOffset = dictOff
	md.RowGroups[0].TotalByteSize = cm.TotalCompressedSize

	return withFooter(out, pqt.EncodeFileMetaData(md))
}
