package parquet

import (
	"bytes"
	"testing"
)

// #923 regression: a v1 page whose definition-level length prefix is zeroed
// drops its levels and shifts the level bytes into the value section. FAILS on
// 868e307b (returns [721156,1441792] with nil error); refused after.
func TestV1MissingDefinitionLevelsAreRefused(t *testing.T) {
	rows := []map[string]any{{"x": int64(11)}, {"x": int64(22)}}
	var b bytes.Buffer
	w, err := NewWriter(&b, Schema{Columns: []Column{{Name: "x", Type: TypeInt64, Nullable: true}}},
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
	if len(frames) != 1 {
		t.Fatalf("pages=%d", len(frames))
	}
	f := frames[0]
	ph, _, err := DecodePageHeader(raw[f.at:])
	if err != nil {
		t.Fatal(err)
	}
	if ph.Type != PageDataV1 || ph.DataPageHeader == nil {
		t.Fatalf("fixture page=%v", ph.Type)
	}
	bodyAt := f.at + f.hdrLen
	copy(raw[bodyAt:bodyAt+4], []byte{0, 0, 0, 0})
	r, err := NewReaderFromBytes(raw)
	if err != nil {
		return
	}
	got, err := r.ReadRows(nil)
	if err == nil {
		t.Fatalf("zero-length definition section read successfully: %v", got)
	}
}
