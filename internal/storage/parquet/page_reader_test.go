package parquet

import (
	"bytes"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
)

// TestPageReaderRoundTrip writes data with parquet-go and reads it back
// with our custom page reader, verifying bit-exact value matching.
func TestPageReaderRoundTrip(t *testing.T) {
	type Record struct {
		ID   int64   `parquet:"id"`
		Name string  `parquet:"name"`
		Val  float64 `parquet:"val"`
	}

	rows := []Record{
		{1, "alice", 1.1},
		{2, "bob", 2.2},
		{3, "charlie", 3.3},
		{4, "delta", 4.4},
		{5, "echo", 5.5},
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fileData := buf.Bytes()
	r := bytes.NewReader(fileData)

	// Read footer with our decoder.
	md, err := ReadFileMetaData(r, int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}
	if len(md.RowGroups) == 0 {
		t.Fatal("no row groups")
	}

	rg := md.RowGroups[0]

	// Find the "id" column (INT64).
	var idCol *ColumnMetaData
	for i := range rg.Columns {
		cc := &rg.Columns[i]
		if cc.MetaData != nil && len(cc.MetaData.PathInSchema) > 0 &&
			cc.MetaData.PathInSchema[len(cc.MetaData.PathInSchema)-1] == "id" {
			idCol = cc.MetaData
			break
		}
	}
	if idCol == nil {
		t.Fatal("id column not found")
	}

	// Read pages from the id column.
	pr := NewColumnPageReader(fileData, idCol, 0, 0)
	defer pr.Close()

	// Check for dictionary page first.
	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var allValues []int64
	for {
		page, err := pr.NextPage()
		if err != nil {
			t.Fatal(err)
		}
		if page == nil {
			break
		}

		if dict != nil {
			// Dictionary-encoded: resolve indices.
			indices := page.Data.Int32()
			dictValues := dict.Data.Int64()
			for _, idx := range indices {
				allValues = append(allValues, dictValues[idx])
			}
		} else {
			// Plain-encoded.
			vals := page.Data.Int64()
			allValues = append(allValues, vals...)
		}
	}

	wantIDs := []int64{1, 2, 3, 4, 5}
	if len(allValues) != len(wantIDs) {
		t.Fatalf("got %d values, want %d", len(allValues), len(wantIDs))
	}
	for i, want := range wantIDs {
		if allValues[i] != want {
			t.Errorf("id[%d] = %d, want %d", i, allValues[i], want)
		}
	}
}

// TestPageReaderString tests reading string (BYTE_ARRAY) columns.
func TestPageReaderString(t *testing.T) {
	type Record struct {
		Name string `parquet:"name"`
	}

	rows := []Record{
		{"alice"}, {"bob"}, {"charlie"}, {"delta"}, {"echo"},
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fileData := buf.Bytes()
	r := bytes.NewReader(fileData)

	md, err := ReadFileMetaData(r, int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}

	rg := md.RowGroups[0]
	var nameCol *ColumnMetaData
	for i := range rg.Columns {
		cc := &rg.Columns[i]
		if cc.MetaData != nil && len(cc.MetaData.PathInSchema) > 0 &&
			cc.MetaData.PathInSchema[len(cc.MetaData.PathInSchema)-1] == "name" {
			nameCol = cc.MetaData
			break
		}
	}
	if nameCol == nil {
		t.Fatal("name column not found")
	}

	pr := NewColumnPageReader(fileData, nameCol, 0, 0)
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var allStrings []string
	for {
		page, err := pr.NextPage()
		if err != nil {
			t.Fatal(err)
		}
		if page == nil {
			break
		}

		if dict != nil {
			indices := page.Data.Int32()
			dictData, dictOffsets := dict.Data.ByteArray()
			for _, idx := range indices {
				s := string(dictData[dictOffsets[idx]:dictOffsets[idx+1]])
				allStrings = append(allStrings, s)
			}
		} else {
			data, offsets := page.Data.ByteArray()
			for i := 0; i < page.Data.Count(); i++ {
				allStrings = append(allStrings, string(data[offsets[i]:offsets[i+1]]))
			}
		}
	}

	wantNames := []string{"alice", "bob", "charlie", "delta", "echo"}
	if len(allStrings) != len(wantNames) {
		t.Fatalf("got %d strings, want %d", len(allStrings), len(wantNames))
	}
	for i, want := range wantNames {
		if allStrings[i] != want {
			t.Errorf("name[%d] = %q, want %q", i, allStrings[i], want)
		}
	}
}

// TestPageReaderFloat64 tests reading float64 (DOUBLE) columns.
func TestPageReaderFloat64(t *testing.T) {
	type Record struct {
		Val float64 `parquet:"val"`
	}

	rows := []Record{{1.1}, {2.2}, {3.3}}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fileData := buf.Bytes()
	r := bytes.NewReader(fileData)

	md, err := ReadFileMetaData(r, int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}

	rg := md.RowGroups[0]
	cm := rg.Columns[0].MetaData

	pr := NewColumnPageReader(fileData, cm, 0, 0)
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var allVals []float64
	for {
		page, err := pr.NextPage()
		if err != nil {
			t.Fatal(err)
		}
		if page == nil {
			break
		}
		if dict != nil {
			indices := page.Data.Int32()
			dictVals := dict.Data.Double()
			for _, idx := range indices {
				allVals = append(allVals, dictVals[idx])
			}
		} else {
			allVals = append(allVals, page.Data.Double()...)
		}
	}

	wantVals := []float64{1.1, 2.2, 3.3}
	if len(allVals) != len(wantVals) {
		t.Fatalf("got %d vals, want %d", len(allVals), len(wantVals))
	}
	for i, want := range wantVals {
		if allVals[i] != want {
			t.Errorf("val[%d] = %v, want %v", i, allVals[i], want)
		}
	}
}

func TestBitsRequired(t *testing.T) {
	tests := []struct {
		v    int
		want int
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{7, 3},
		{8, 4},
		{255, 8},
		{256, 9},
	}
	for _, tt := range tests {
		if got := bitsRequired(tt.v); got != tt.want {
			t.Errorf("bitsRequired(%d) = %d, want %d", tt.v, got, tt.want)
		}
	}
}
