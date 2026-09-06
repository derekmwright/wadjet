package parquet

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// #927 regression: swapping two same-type column-chunk metadata entries in the
// footer silently swaps their values because the reader binds chunks to schema
// leaves by slice position. FAILS on 868e307b (returns {a:22,b:11}, nil error);
// refused at open by full-path binding after.
func TestSwappedColumnMetadataIsRefused(t *testing.T) {
	s := Schema{Columns: []Column{{Name: "a", Type: TypeInt64}, {Name: "b", Type: TypeInt64}}}
	var b bytes.Buffer
	w, err := NewWriter(&b, s, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteRows([]map[string]any{{"a": int64(11), "b": int64(22)}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	raw := b.Bytes()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	start := len(raw) - 8 - footerLen
	md, err := DecodeFileMetaData(raw[start : start+footerLen])
	if err != nil {
		t.Fatal(err)
	}
	md.RowGroups[0].Columns[0], md.RowGroups[0].Columns[1] = md.RowGroups[0].Columns[1], md.RowGroups[0].Columns[0]
	footer := EncodeFileMetaData(md)
	mutated := append([]byte(nil), raw[:start]...)
	mutated = append(mutated, footer...)
	mutated = binary.LittleEndian.AppendUint32(mutated, uint32(len(footer)))
	mutated = append(mutated, "PAR1"...)
	r, err := NewReaderFromBytes(mutated)
	if err != nil {
		return
	}
	got, err := r.ReadRows(nil)
	if err == nil {
		t.Fatalf("swapped column metadata read successfully: %v", got)
	}
}
