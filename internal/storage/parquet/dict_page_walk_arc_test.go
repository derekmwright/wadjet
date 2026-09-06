package parquet

import (
	"bytes"
	"testing"
)

// #924 regression: a later data page relabeled DICTIONARY_PAGE is skipped after
// its rows were charged, so the chunk reconciles while a whole page's values
// vanish. FAILS on 868e307b (300 rows, row128=136, tail NULL, nil error);
// refused after.
func TestLaterDataPageRelabeledAsDictionaryIsRefused(t *testing.T) {
	rows := make([]map[string]any, 300)
	for i := range rows {
		rows[i] = map[string]any{"x": int64(i)}
	}
	var b bytes.Buffer
	w, err := NewWriter(&b, Schema{Columns: []Column{{Name: "x", Type: TypeInt64}}},
		WriterConfig{Compression: CompressionNone, PageBufferSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	raw := b.Bytes()
	md, err := ReadFileMetaData(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	_, frames := chunkFrames(t, md, raw, 0, "x")
	if len(frames) < 2 {
		t.Fatal("need multiple pages")
	}
	f := frames[1]
	ph, n, err := DecodePageHeader(raw[f.at:])
	if err != nil {
		t.Fatal(err)
	}
	ph.Type = PageDictionary
	hdr := EncodePageHeader(ph)
	if len(hdr) != n {
		t.Fatal("header size changed")
	}
	copy(raw[f.at:], hdr)
	r, err := NewReaderFromBytes(raw)
	if err != nil {
		return
	}
	got, err := r.ReadRows(nil)
	if err == nil {
		t.Fatalf("misclassified page read successfully: rows=%d row128=%v last=%v", len(got), got[128], got[len(got)-1])
	}
}
