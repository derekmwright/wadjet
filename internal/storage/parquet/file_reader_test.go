package parquet

import (
	"bytes"
	"math"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
)

// TestFileReaderRoundTrip writes a Parquet file with parquet-go and reads it
// back through FileReader, verifying metadata and all column values match.
func TestFileReaderRoundTrip(t *testing.T) {
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

	fr, err := OpenFileReaderFromBytes(fileData)
	if err != nil {
		t.Fatal(err)
	}

	// Verify basic metadata.
	if fr.NumRows() != 5 {
		t.Errorf("NumRows = %d, want 5", fr.NumRows())
	}
	if fr.NumRowGroups() != 1 {
		t.Errorf("NumRowGroups = %d, want 1", fr.NumRowGroups())
	}

	// Verify row group metadata.
	rg := fr.RowGroupMeta(0)
	if rg == nil {
		t.Fatal("RowGroupMeta(0) returned nil")
	}
	if rg.NumRows != 5 {
		t.Errorf("RG[0].NumRows = %d, want 5", rg.NumRows)
	}
	if len(rg.Columns) != 3 {
		t.Fatalf("RG[0] columns = %d, want 3", len(rg.Columns))
	}

	// Read all values from the "id" column.
	idVals := readInt64Column(t, fr, 0, 0)
	wantIDs := []int64{1, 2, 3, 4, 5}
	if len(idVals) != len(wantIDs) {
		t.Fatalf("id column: got %d values, want %d", len(idVals), len(wantIDs))
	}
	for i, want := range wantIDs {
		if idVals[i] != want {
			t.Errorf("id[%d] = %d, want %d", i, idVals[i], want)
		}
	}

	// Read all values from the "name" column.
	nameVals := readStringColumn(t, fr, 0, 1)
	wantNames := []string{"alice", "bob", "charlie", "delta", "echo"}
	if len(nameVals) != len(wantNames) {
		t.Fatalf("name column: got %d values, want %d", len(nameVals), len(wantNames))
	}
	for i, want := range wantNames {
		if nameVals[i] != want {
			t.Errorf("name[%d] = %q, want %q", i, nameVals[i], want)
		}
	}

	// Read all values from the "val" column.
	valVals := readFloat64Column(t, fr, 0, 2)
	wantVals := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
	if len(valVals) != len(wantVals) {
		t.Fatalf("val column: got %d values, want %d", len(valVals), len(wantVals))
	}
	for i, want := range wantVals {
		if valVals[i] != want {
			t.Errorf("val[%d] = %v, want %v", i, valVals[i], want)
		}
	}
}

// TestFileReaderSchema verifies our schema conversion matches parquet-go's view.
func TestFileReaderSchema(t *testing.T) {
	type Record struct {
		ID   int64   `parquet:"id"`
		Name string  `parquet:"name"`
		Val  float64 `parquet:"val"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write([]Record{{1, "alice", 1.1}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	fileData := buf.Bytes()

	fr, err := OpenFileReaderFromBytes(fileData)
	if err != nil {
		t.Fatal(err)
	}

	schema := fr.Schema()
	if len(schema.Columns) != 3 {
		t.Fatalf("schema has %d columns, want 3", len(schema.Columns))
	}

	// Verify column names.
	wantNames := []string{"id", "name", "val"}
	for i, want := range wantNames {
		if schema.Columns[i].Name != want {
			t.Errorf("schema.Columns[%d].Name = %q, want %q", i, schema.Columns[i].Name, want)
		}
	}

	// Verify column types.
	wantTypes := []TypeID{TypeInt64, TypeString, TypeFloat64}
	for i, want := range wantTypes {
		if schema.Columns[i].Type != want {
			t.Errorf("schema.Columns[%d].Type = %v, want %v", i, schema.Columns[i].Type, want)
		}
	}

	// Compare against parquet-go's schema interpretation.
	pqFile, err := goparquet.OpenFile(bytes.NewReader(fileData), int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}
	pqCols := pqFile.Schema().Columns()
	if len(schema.Columns) != len(pqCols) {
		t.Errorf("column count: ours=%d, parquet-go=%d", len(schema.Columns), len(pqCols))
	}
	for i, pqPath := range pqCols {
		pqName := pqPath[len(pqPath)-1]
		if schema.Columns[i].Name != pqName {
			t.Errorf("column[%d] name: ours=%q, parquet-go=%q", i, schema.Columns[i].Name, pqName)
		}
	}

	// Verify leaves match.
	leaves := fr.Leaves()
	if len(leaves) != 3 {
		t.Fatalf("leaves = %d, want 3", len(leaves))
	}
	for i, leaf := range leaves {
		if leaf.Name != wantNames[i] {
			t.Errorf("leaf[%d].Name = %q, want %q", i, leaf.Name, wantNames[i])
		}
	}
}

// TestFileReaderMultiRowGroup tests reading files with multiple row groups.
func TestFileReaderMultiRowGroup(t *testing.T) {
	type Record struct {
		ID  int64  `parquet:"id"`
		Tag string `parquet:"tag"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf,
		goparquet.MaxRowsPerRowGroup(3),
	)

	rows := []Record{
		{1, "a"}, {2, "b"}, {3, "c"},
		{4, "d"}, {5, "e"},
	}
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	fileData := buf.Bytes()

	fr, err := OpenFileReaderFromBytes(fileData)
	if err != nil {
		t.Fatal(err)
	}

	if fr.NumRows() != 5 {
		t.Errorf("NumRows = %d, want 5", fr.NumRows())
	}
	if fr.NumRowGroups() < 2 {
		t.Fatalf("NumRowGroups = %d, want >= 2", fr.NumRowGroups())
	}

	// Verify total rows across row groups.
	var totalRows int64
	for i := 0; i < fr.NumRowGroups(); i++ {
		totalRows += fr.RowGroupNumRows(i)
	}
	if totalRows != 5 {
		t.Errorf("sum of RG rows = %d, want 5", totalRows)
	}

	// Read all IDs across all row groups.
	var allIDs []int64
	for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
		ids := readInt64Column(t, fr, rgIdx, 0)
		allIDs = append(allIDs, ids...)
	}
	wantIDs := []int64{1, 2, 3, 4, 5}
	if len(allIDs) != len(wantIDs) {
		t.Fatalf("got %d IDs, want %d", len(allIDs), len(wantIDs))
	}
	for i, want := range wantIDs {
		if allIDs[i] != want {
			t.Errorf("id[%d] = %d, want %d", i, allIDs[i], want)
		}
	}
}

// TestFileReaderRowGroupStats verifies statistics extraction from row group metadata.
func TestFileReaderRowGroupStats(t *testing.T) {
	type Record struct {
		ID  int64   `parquet:"id"`
		Val float64 `parquet:"val"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	rows := []Record{
		{10, 1.5},
		{20, 2.5},
		{30, 3.5},
	}
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	fileData := buf.Bytes()

	fr, err := OpenFileReaderFromBytes(fileData)
	if err != nil {
		t.Fatal(err)
	}

	stats := fr.RowGroupStats(0)
	if stats.NumRows != 3 {
		t.Errorf("stats.NumRows = %d, want 3", stats.NumRows)
	}

	// Check id column stats.
	idStats, ok := stats.Columns["id"]
	if !ok {
		t.Fatal("no stats for 'id' column")
	}
	if !idStats.HasStats {
		t.Fatal("id column: HasStats = false")
	}
	if idMin, ok := idStats.MinValue.(int64); ok {
		if idMin != 10 {
			t.Errorf("id min = %d, want 10", idMin)
		}
	} else {
		t.Logf("id min type = %T (may not have statistics)", idStats.MinValue)
	}
	if idMax, ok := idStats.MaxValue.(int64); ok {
		if idMax != 30 {
			t.Errorf("id max = %d, want 30", idMax)
		}
	}

	// Check val column stats.
	valStats, ok := stats.Columns["val"]
	if !ok {
		t.Fatal("no stats for 'val' column")
	}
	if !valStats.HasStats {
		t.Fatal("val column: HasStats = false")
	}
	if valMin, ok := valStats.MinValue.(float64); ok {
		if valMin != 1.5 {
			t.Errorf("val min = %v, want 1.5", valMin)
		}
	}
	if valMax, ok := valStats.MaxValue.(float64); ok {
		if valMax != 3.5 {
			t.Errorf("val max = %v, want 3.5", valMax)
		}
	}

	// Out-of-range row group should return empty stats.
	empty := fr.RowGroupStats(99)
	if empty.NumRows != 0 {
		t.Errorf("stats for invalid RG: NumRows = %d", empty.NumRows)
	}
}

// TestFileReaderFromReaderAt tests the io.ReaderAt path.
func TestFileReaderFromReaderAt(t *testing.T) {
	type Record struct {
		X int32 `parquet:"x"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write([]Record{{42}, {99}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	fileData := buf.Bytes()

	fr, err := OpenFileReader(bytes.NewReader(fileData), int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}

	if fr.NumRows() != 2 {
		t.Errorf("NumRows = %d, want 2", fr.NumRows())
	}

	// Read the x column.
	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		t.Fatal("ColumnPages returned nil")
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var allVals []int32
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
			dictVals := dict.Data.Int32()
			for _, idx := range indices {
				allVals = append(allVals, dictVals[idx])
			}
		} else {
			allVals = append(allVals, page.Data.Int32()...)
		}
	}

	want := []int32{42, 99}
	if len(allVals) != len(want) {
		t.Fatalf("got %d values, want %d", len(allVals), len(want))
	}
	for i, w := range want {
		if allVals[i] != w {
			t.Errorf("x[%d] = %d, want %d", i, allVals[i], w)
		}
	}
}

// TestFileReaderColumnPagesEdgeCases tests edge cases for ColumnPages.
func TestFileReaderColumnPagesEdgeCases(t *testing.T) {
	type Record struct {
		ID int64 `parquet:"id"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write([]Record{{1}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	// Invalid row group index.
	if pr := fr.ColumnPages(-1, 0); pr != nil {
		t.Error("ColumnPages(-1, 0) should return nil")
	}
	if pr := fr.ColumnPages(99, 0); pr != nil {
		t.Error("ColumnPages(99, 0) should return nil")
	}

	// Invalid column index.
	if pr := fr.ColumnPages(0, -1); pr != nil {
		t.Error("ColumnPages(0, -1) should return nil")
	}
	if pr := fr.ColumnPages(0, 99); pr != nil {
		t.Error("ColumnPages(0, 99) should return nil")
	}

	// RowGroupMeta out of range.
	if rg := fr.RowGroupMeta(-1); rg != nil {
		t.Error("RowGroupMeta(-1) should return nil")
	}
	if rg := fr.RowGroupMeta(99); rg != nil {
		t.Error("RowGroupMeta(99) should return nil")
	}

	// RowGroupNumRows out of range.
	if n := fr.RowGroupNumRows(-1); n != 0 {
		t.Errorf("RowGroupNumRows(-1) = %d, want 0", n)
	}
}

// TestFileReaderBoolColumn tests reading BOOLEAN columns.
func TestFileReaderBoolColumn(t *testing.T) {
	type Record struct {
		Flag bool `parquet:"flag"`
	}

	rows := []Record{{true}, {false}, {true}, {true}, {false}}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		t.Fatal("ColumnPages returned nil")
	}
	defer pr.Close()

	_, _ = pr.NextDictionary()

	var allVals []bool
	for {
		page, err := pr.NextPage()
		if err != nil {
			t.Fatal(err)
		}
		if page == nil {
			break
		}
		// Boolean() returns packed bit bytes; unpack manually.
		boolBytes := page.Data.Boolean()
		for j := 0; j < page.Data.Count(); j++ {
			allVals = append(allVals, boolBytes[j/8]&(1<<uint(j%8)) != 0)
		}
	}

	want := []bool{true, false, true, true, false}
	if len(allVals) != len(want) {
		t.Fatalf("got %d bools, want %d", len(allVals), len(want))
	}
	for i, w := range want {
		if allVals[i] != w {
			t.Errorf("flag[%d] = %v, want %v", i, allVals[i], w)
		}
	}
}

// TestFileReaderFloat32Column tests reading FLOAT columns.
func TestFileReaderFloat32Column(t *testing.T) {
	type Record struct {
		Score float32 `parquet:"score"`
	}

	rows := []Record{{1.5}, {2.5}, {3.5}}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		t.Fatal("ColumnPages returned nil")
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var allVals []float32
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
			dictVals := dict.Data.Float()
			for _, idx := range indices {
				allVals = append(allVals, dictVals[idx])
			}
		} else {
			allVals = append(allVals, page.Data.Float()...)
		}
	}

	want := []float32{1.5, 2.5, 3.5}
	if len(allVals) != len(want) {
		t.Fatalf("got %d values, want %d", len(allVals), len(want))
	}
	for i, w := range want {
		if allVals[i] != w {
			t.Errorf("score[%d] = %v, want %v", i, allVals[i], w)
		}
	}
}

// TestFileReaderStatsToNative verifies the statsToNative conversion for all types.
func TestFileReaderStatsToNative(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		physType PhysicalType
		want     any
	}{
		{
			name:     "bool_true",
			data:     []byte{1},
			physType: PhysicalBoolean,
			want:     true,
		},
		{
			name:     "bool_false",
			data:     []byte{0},
			physType: PhysicalBoolean,
			want:     false,
		},
		{
			name:     "int32",
			data:     []byte{0x2a, 0x00, 0x00, 0x00}, // 42
			physType: PhysicalInt32,
			want:     int64(42),
		},
		{
			name:     "int64",
			data:     []byte{0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 100
			physType: PhysicalInt64,
			want:     int64(100),
		},
		{
			name:     "float",
			data:     func() []byte { b := make([]byte, 4); v := math.Float32bits(3.14); b[0] = byte(v); b[1] = byte(v >> 8); b[2] = byte(v >> 16); b[3] = byte(v >> 24); return b }(),
			physType: PhysicalFloat,
			want:     float64(float32(3.14)),
		},
		{
			name:     "double",
			data:     func() []byte { b := make([]byte, 8); v := math.Float64bits(2.718); b[0] = byte(v); b[1] = byte(v >> 8); b[2] = byte(v >> 16); b[3] = byte(v >> 24); b[4] = byte(v >> 32); b[5] = byte(v >> 40); b[6] = byte(v >> 48); b[7] = byte(v >> 56); return b }(),
			physType: PhysicalDouble,
			want:     2.718,
		},
		{
			name:     "byte_array",
			data:     []byte("hello"),
			physType: PhysicalByteArray,
			want:     "hello",
		},
		{
			name:     "fixed_len_byte_array",
			data:     []byte{0xDE, 0xAD},
			physType: PhysicalFixedLenByteArray,
			want:     string([]byte{0xDE, 0xAD}),
		},
		{
			name:     "bool_empty",
			data:     []byte{},
			physType: PhysicalBoolean,
			want:     nil,
		},
		{
			name:     "int32_too_short",
			data:     []byte{1, 2},
			physType: PhysicalInt32,
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := statsToNative(tt.data, tt.physType)
			if got != tt.want {
				t.Errorf("statsToNative(%v, %v) = %v (%T), want %v (%T)",
					tt.data, tt.physType, got, got, tt.want, tt.want)
			}
		})
	}
}

// TestFileReaderSchemaTypes verifies type mapping for common Parquet logical/physical types.
func TestFileReaderSchemaTypes(t *testing.T) {
	type Record struct {
		B    bool    `parquet:"b"`
		I32  int32   `parquet:"i32"`
		I64  int64   `parquet:"i64"`
		F32  float32 `parquet:"f32"`
		F64  float64 `parquet:"f64"`
		Str  string  `parquet:"str"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write([]Record{{true, 1, 2, 3.0, 4.0, "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	schema := fr.Schema()
	wantTypes := map[string]TypeID{
		"b":   TypeBool,
		"i32": TypeInt32,
		"i64": TypeInt64,
		"f32": TypeFloat32,
		"f64": TypeFloat64,
		"str": TypeString,
	}

	for _, col := range schema.Columns {
		want, ok := wantTypes[col.Name]
		if !ok {
			t.Errorf("unexpected column %q", col.Name)
			continue
		}
		if col.Type != want {
			t.Errorf("column %q: type = %v, want %v", col.Name, col.Type, want)
		}
	}
}

// readInt64Column reads all int64 values from a column, handling dictionary encoding.
func readInt64Column(t *testing.T, fr *FileReader, rgIdx, colIdx int) []int64 {
	t.Helper()
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		t.Fatalf("ColumnPages(%d, %d) returned nil", rgIdx, colIdx)
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var vals []int64
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
			dictVals := dict.Data.Int64()
			for _, idx := range indices {
				vals = append(vals, dictVals[idx])
			}
		} else {
			vals = append(vals, page.Data.Int64()...)
		}
	}
	return vals
}

// readStringColumn reads all string values from a column, handling dictionary encoding.
func readStringColumn(t *testing.T, fr *FileReader, rgIdx, colIdx int) []string {
	t.Helper()
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		t.Fatalf("ColumnPages(%d, %d) returned nil", rgIdx, colIdx)
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var vals []string
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
				vals = append(vals, string(dictData[dictOffsets[idx]:dictOffsets[idx+1]]))
			}
		} else {
			data, offsets := page.Data.ByteArray()
			for i := 0; i < page.Data.Count(); i++ {
				vals = append(vals, string(data[offsets[i]:offsets[i+1]]))
			}
		}
	}
	return vals
}

// readFloat64Column reads all float64 values from a column, handling dictionary encoding.
func readFloat64Column(t *testing.T, fr *FileReader, rgIdx, colIdx int) []float64 {
	t.Helper()
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		t.Fatalf("ColumnPages(%d, %d) returned nil", rgIdx, colIdx)
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		t.Fatal(err)
	}

	var vals []float64
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
				vals = append(vals, dictVals[idx])
			}
		} else {
			vals = append(vals, page.Data.Double()...)
		}
	}
	return vals
}
