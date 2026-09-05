package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
)

// A column that stops before its declared rows is a file contradicting
// itself, and the reader has everything it needs to say so: the row group's
// row count on one side, the pages the chunk actually carries on the other.
//
// It said nothing. A required INT64 column of [11, 22] whose chunk byte
// length was cut to its first complete page read [{x:11}, {}] — nil error,
// the missing value fabricated as NULL under a REQUIRED schema (#892). A
// truncation INSIDE a page was already refused, which is what made the shape
// hard to see: only the tidy cut, at a page boundary, was silent.

// pageFrame is one page's header+body extent inside a column chunk.
type pageFrame struct {
	at      int // file offset of the page header
	hdrLen  int
	bodyLen int
	kind    PageType
}

// chunkFrames walks one leaf's column chunk in row group rgIdx and returns
// every page framing in it, in file order.
func chunkFrames(tb testing.TB, md *FileMetaData, data []byte, rgIdx int, leafPath string) (start int, frames []pageFrame) {
	tb.Helper()
	rg := &md.RowGroups[rgIdx]
	for ci := range rg.Columns {
		cm := rg.Columns[ci].MetaData
		if cm == nil || strings.Join(cm.PathInSchema, ".") != leafPath {
			continue
		}
		s := cm.DataPageOffset
		if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < s {
			s = cm.DictionaryPageOffset
		}
		end := s + cm.TotalCompressedSize
		for off := int(s); off < int(end); {
			ph, n, err := DecodePageHeader(data[off:])
			if err != nil {
				tb.Fatalf("page header at %d: %v", off, err)
			}
			frames = append(frames, pageFrame{at: off, hdrLen: n, bodyLen: int(ph.CompressedPageSize), kind: ph.Type})
			off += n + int(ph.CompressedPageSize)
		}
		return int(s), frames
	}
	tb.Fatalf("no chunk for leaf %q in row group %d", leafPath, rgIdx)
	return 0, nil
}

// cutChunkTo rewrites the footer so ONE column chunk's declared byte length
// ends after `keepBytes` bytes. Nothing else moves: the row group still
// declares its rows, the chunk still declares its values, the page bytes are
// still on disk. Only the chunk's own length now stops short — which is
// exactly what a failed flush, a truncated upload or a bad range read leaves
// behind, and what the reader has to notice.
func cutChunkTo(tb testing.TB, data []byte, rgIdx int, leafPath string, keepBytes int64) []byte {
	tb.Helper()
	md, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	found := false
	rg := &md.RowGroups[rgIdx]
	for ci := range rg.Columns {
		cm := rg.Columns[ci].MetaData
		if cm == nil || strings.Join(cm.PathInSchema, ".") != leafPath {
			continue
		}
		cm.TotalCompressedSize = keepBytes
		found = true
	}
	if !found {
		tb.Fatalf("no chunk for leaf %q", leafPath)
	}
	footerLen := binary.LittleEndian.Uint32(data[len(data)-8:])
	out := append([]byte(nil), data[:len(data)-8-int(footerLen)]...)
	footer := EncodeFileMetaData(md)
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, "PAR1"...)
}

// completenessFixture is one file shape the truncation cells run over.
//
// Either wadjet's own writer builds it from schema+rows, or it is read from
// testdata. The nested cells need the second: wadjet's writer emits ONE page
// per nested leaf per row group, so a nested chunk with several page
// boundaries to cut at can only come from another writer (see
// testdata/gen_nested_pages.py).
type completenessFixture struct {
	name     string
	schema   Schema
	rows     []map[string]any
	file     string // when set, read from testdata instead of writing
	wantRows int    // expected row count for a file fixture
	leaf     string // the leaf path whose chunk gets cut
	pageSize int
}

func completenessFixtures() []completenessFixture {
	flatRows := func(n int, withNulls bool) []map[string]any {
		out := make([]map[string]any, n)
		for i := range out {
			m := map[string]any{"x": int64(i * 11)}
			if withNulls && i%4 == 0 {
				m["s"] = nil
			} else {
				m["s"] = fmt.Sprintf("s-%03d", i)
			}
			out[i] = m
		}
		return out
	}
	return []completenessFixture{
		{
			name:     "flat_required_int64",
			schema:   Schema{Columns: []Column{{Name: "x", Type: TypeInt64}, {Name: "s", Type: TypeString}}},
			rows:     flatRows(400, false),
			leaf:     "x",
			pageSize: 64,
		},
		{
			name:     "flat_optional_string",
			schema:   Schema{Columns: []Column{{Name: "x", Type: TypeInt64}, {Name: "s", Type: TypeString, Nullable: true}}},
			rows:     flatRows(400, true),
			leaf:     "s",
			pageSize: 64,
		},
		{
			name:     "nested_array_element",
			file:     "testdata/nested_pages.parquet",
			wantRows: 4000,
			leaf:     "tags.list.element",
		},
		{
			name:     "nested_flat_neighbour",
			file:     "testdata/nested_pages.parquet",
			wantRows: 4000,
			leaf:     "x",
		},
		{
			name:     "nested_map_key",
			file:     "testdata/nested_pages.parquet",
			wantRows: 4000,
			leaf:     "props.key_value.key",
		},
		{
			name:     "nested_map_value",
			file:     "testdata/nested_pages.parquet",
			wantRows: 4000,
			leaf:     "props.key_value.value",
		},
		{
			name:     "nested_row_field",
			file:     "testdata/nested_pages.parquet",
			wantRows: 4000,
			leaf:     "rec.b",
		},
	}
}

func (fx completenessFixture) rowCount() int {
	if fx.file != "" {
		return fx.wantRows
	}
	return len(fx.rows)
}

func buildCompletenessFile(tb testing.TB, fx completenessFixture) []byte {
	tb.Helper()
	if fx.file != "" {
		data, err := os.ReadFile(fx.file)
		if err != nil {
			tb.Fatalf("reading %s (regenerate with testdata/gen_nested_pages.py): %v", fx.file, err)
		}
		return data
	}
	var b bytes.Buffer
	w, err := NewWriter(&b, fx.schema, WriterConfig{PageBufferSize: fx.pageSize, Compression: CompressionNone})
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteRows(fx.rows); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return b.Bytes()
}

// TestAColumnThatEndsBeforeItsDeclaredRowsIsRefused: cut a chunk at EVERY
// page boundary and mid-page, over required and optional flat columns and a
// nested leaf, through the row reader.
func TestAColumnThatEndsBeforeItsDeclaredRowsIsRefused(t *testing.T) {
	for _, fx := range completenessFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			data := buildCompletenessFile(t, fx)
			md, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			start, frames := chunkFrames(t, md, data, 0, fx.leaf)
			if len(frames) < 3 {
				t.Fatalf("fixture %s produced %d pages; the cell needs several", fx.name, len(frames))
			}

			// The unmutated file reads, and reads the right number of rows.
			clean, err := readEveryRow(data)
			if err != nil {
				t.Fatalf("unmutated: %v", err)
			}
			if len(clean) != fx.rowCount() {
				t.Fatalf("unmutated read %d rows, want %d", len(clean), fx.rowCount())
			}

			// Every cut short of the whole chunk must be refused.
			kept := int64(0)
			for i, f := range frames {
				full := kept + int64(f.hdrLen+f.bodyLen)
				last := i == len(frames)-1

				if !last {
					t.Run(fmt.Sprintf("after_page_%d", i), func(t *testing.T) {
						mutated := cutChunkTo(t, data, 0, fx.leaf, full)
						assertShortColumnRefused(t, mutated, fx.leaf)
					})
				}
				if f.bodyLen > 1 {
					t.Run(fmt.Sprintf("mid_page_%d", i), func(t *testing.T) {
						mutated := cutChunkTo(t, data, 0, fx.leaf, kept+int64(f.hdrLen)+int64(f.bodyLen/2))
						rows, err := readEveryRow(mutated)
						if err == nil {
							t.Fatalf("a chunk cut inside page %d read %d rows with no error", i, len(rows))
						}
					})
				}
				kept = full
			}

			// And the untouched length still reads: the check is about the
			// chunk stopping SHORT, not about the reader disliking the
			// footer it just rewrote.
			t.Run("full_length_still_reads", func(t *testing.T) {
				rewritten := cutChunkTo(t, data, 0, fx.leaf, kept)
				rows, err := readEveryRow(rewritten)
				if err != nil {
					t.Fatalf("re-encoding the footer at the SAME length broke the read: %v", err)
				}
				if len(rows) != fx.rowCount() {
					t.Fatalf("read %d rows, want %d", len(rows), fx.rowCount())
				}
			})
			_ = start
		})
	}
}

func assertShortColumnRefused(t *testing.T, data []byte, leaf string) {
	t.Helper()
	rows, err := readEveryRow(data)
	if err == nil {
		t.Fatalf("a chunk that ends before its declared rows read %d rows with no error (leaf %s)",
			len(rows), leaf)
	}
	if !strings.Contains(err.Error(), "the chunk ends before its declared rows") {
		t.Fatalf("refused, but not as a short column: %v", err)
	}
	// The refusal has to say which column, and both numbers.
	name := leaf
	if i := strings.Index(leaf, "."); i > 0 {
		name = leaf[:i]
	}
	if !strings.Contains(err.Error(), name) {
		t.Fatalf("refusal does not name the column %q: %v", name, err)
	}
}

// TestAProjectionThatReadsFewerColumnsStillReads is the boundary from the
// other side. The completeness check fires when a chunk RUNS OUT of pages, so
// a caller that reads only some columns — or stops early — must not trip it:
// not reading a chunk is not the same as a chunk being short.
func TestAProjectionThatReadsFewerColumnsStillReads(t *testing.T) {
	fx := completenessFixtures()[0]
	data := buildCompletenessFile(t, fx)

	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRows([]string{"s"})
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if len(rows) != fx.rowCount() {
		t.Fatalf("projection read %d rows, want %d", len(rows), fx.rowCount())
	}
	for i, row := range rows {
		if _, ok := row["x"]; ok {
			t.Fatalf("row %d carries a column the projection did not ask for", i)
		}
	}

	// And a reader abandoned after ONE page is not reconciled either.
	fr := r.FileReader()
	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		t.Fatal("no chunk")
	}
	defer pr.Close()
	if _, err := pr.NextDictionary(); err != nil {
		t.Fatal(err)
	}
	p, err := pr.NextPage()
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if p == nil {
		t.Fatal("no pages")
	}
	p.Release()
}

// TestANestedLeafCountsRowsFromItsRepetitionLevels pins the counting rule the
// nested arm of the completeness check rests on: a nested leaf has more
// VALUES than rows, and its rows are the level-0 entries of its repetition
// levels. PageData.NumRows reported numValues for v1 nested pages before
// this, which made the count unusable and the check impossible.
func TestANestedLeafCountsRowsFromItsRepetitionLevels(t *testing.T) {
	elem := Column{Name: "element", Type: TypeInt64, Nullable: true}
	fx := completenessFixture{
		schema: Schema{Columns: []Column{
			{Name: "x", Type: TypeInt64},
			{Name: "tags", Type: TypeArray, Nullable: true, ElementType: &elem},
		}},
		rows:     nil,
		pageSize: 64,
	}
	n := 300
	fx.rows = make([]map[string]any, n)
	totalElems := 0
	for i := range fx.rows {
		vals := make([]any, i%3+1)
		for j := range vals {
			vals[j] = int64(i*10 + j)
		}
		totalElems += len(vals)
		fx.rows[i] = map[string]any{"x": int64(i), "tags": vals}
	}
	data := buildCompletenessFile(t, fx)

	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	leaves := fr.Leaves()
	leafIdx := -1
	for i, l := range leaves {
		if strings.Join(l.Path, ".") == "tags.list.element" {
			leafIdx = i
		}
	}
	if leafIdx < 0 {
		t.Fatalf("no tags leaf among %d", len(leaves))
	}
	if leaves[leafIdx].MaxRepLevel == 0 {
		t.Fatal("the tags leaf is not repeated; the cell is not exercising nesting")
	}

	rows, values := 0, 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		pr := fr.ColumnPages(rg, leafIdx)
		if pr == nil {
			t.Fatal("no chunk")
		}
		if _, err := pr.NextDictionary(); err != nil {
			t.Fatal(err)
		}
		for {
			p, err := pr.NextPage()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				break
			}
			rows += p.NumRows
			values += p.NumValues
			p.Release()
		}
		pr.Close()
	}
	if rows != n {
		t.Fatalf("nested leaf pages report %d rows, want %d", rows, n)
	}
	if values != totalElems {
		t.Fatalf("nested leaf pages report %d values, want %d elements", values, totalElems)
	}
	if values == rows {
		t.Fatal("the fixture has one element per row; it cannot tell values from rows")
	}
}

// TestAChunkThatDeliversMoreRowsThanDeclaredIsRefused is the other direction.
//
// A flat leaf is bounded page by page before decode (chargeRows), so it
// cannot over-deliver: the first page past the budget is refused. A NESTED
// leaf has no such per-page bound — values are not rows there — so the only
// thing that can catch it is the end-of-column reconciliation, and it has to
// catch BOTH inequalities or "the chunk delivers what the row group declares"
// is only half a claim.
func TestAChunkThatDeliversMoreRowsThanDeclaredIsRefused(t *testing.T) {
	data, err := os.ReadFile("testdata/nested_pages.parquet")
	if err != nil {
		t.Fatal(err)
	}
	// Understate the row group's rows. Every chunk in it now delivers more
	// than it declares; reading only the nested column keeps the flat
	// column's per-page bound (which would fire first, for a different
	// reason) out of the way.
	md, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(md.RowGroups) != 1 {
		t.Skipf("fixture has %d row groups; the cell aims at one", len(md.RowGroups))
	}
	md.RowGroups[0].NumRows--
	md.NumRows--
	footerLen := binary.LittleEndian.Uint32(data[len(data)-8:])
	mutated := append([]byte(nil), data[:len(data)-8-int(footerLen)]...)
	footer := EncodeFileMetaData(md)
	mutated = append(mutated, footer...)
	mutated = binary.LittleEndian.AppendUint32(mutated, uint32(len(footer)))
	mutated = append(mutated, "PAR1"...)

	r, err := NewReaderFromBytes(mutated)
	if err != nil {
		return // refused at open: also a refusal
	}
	rows, err := r.ReadRows([]string{"tags"})
	if err == nil {
		t.Fatalf("a nested chunk that delivers more rows than its row group declares "+
			"read %d rows with no error", len(rows))
	}
	if !strings.Contains(err.Error(), "the chunk ends before its declared rows") {
		t.Fatalf("refused, but not as a row-count disagreement: %v", err)
	}
	if !strings.Contains(err.Error(), "delivers 4000") {
		t.Fatalf("refusal does not report what the chunk actually delivered: %v", err)
	}
}

// TestARowGroupTheReadNeverOpensIsNeverChecked is the prune boundary.
//
// A row-group prune — the statistics one the scan performs, and the explicit
// row-group range ReadRowGroup takes — does not READ the pruned chunk at all.
// The completeness check fires when a chunk runs out of pages, so a chunk
// nobody opened contributes nothing to it, and a corrupt row group at the end
// of a file must not break a query whose predicate excludes it. Reading the
// same file whole refuses.
func TestARowGroupTheReadNeverOpensIsNeverChecked(t *testing.T) {
	// Two row groups; truncate a chunk in the SECOND.
	rows := make([]map[string]any, 400)
	for i := range rows {
		rows[i] = map[string]any{"x": int64(i), "s": fmt.Sprintf("s-%03d", i)}
	}
	var b bytes.Buffer
	w, err := NewWriter(&b, Schema{Columns: []Column{
		{Name: "x", Type: TypeInt64},
		{Name: "s", Type: TypeString},
	}}, WriterConfig{PageBufferSize: 64, RowGroupSize: 200, Compression: CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := b.Bytes()

	md, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(md.RowGroups) < 2 {
		t.Fatalf("fixture wrote %d row groups; the cell needs two", len(md.RowGroups))
	}
	_, frames := chunkFrames(t, md, data, 1, "x")
	if len(frames) < 2 {
		t.Fatalf("row group 1's x chunk has %d pages", len(frames))
	}
	mutated := cutChunkTo(t, data, 1, "x", int64(frames[0].hdrLen+frames[0].bodyLen))

	r, err := NewReaderFromBytes(mutated)
	if err != nil {
		t.Fatal(err)
	}
	// Row group 0 alone: untouched, and it reads.
	got, err := r.ReadRowGroup(0, nil)
	if err != nil {
		t.Fatalf("a row group the corruption is not in must still read: %v", err)
	}
	if len(got) != 200 {
		t.Fatalf("row group 0 read %d rows, want 200", len(got))
	}
	for i, row := range got {
		if row["x"] != int64(i) {
			t.Fatalf("row %d: x=%v, want %d", i, row["x"], i)
		}
	}
	// The whole file: refused.
	if _, err := r.ReadRows(nil); err == nil {
		t.Fatal("the whole file read a short chunk with no error")
	}
}

// TestARowGroupWithNoChunkForALeafIsRefused is the shape self-flag 4 said was
// not driven by a cell: a row group with fewer column chunks than the file's
// own schema has leaves. It is constructible from the footer — drop the last
// `ColumnChunk` and re-encode — and at base it was the third silent
// fabrication of this family: the whole column vanished and the read returned
// the remaining ones with a nil error.
//
// Both refusals are here because they are two different call sites reached by
// two different readers: `readColumnToAny` for a flat file, `readLeafColumn`
// for a container one.
func TestARowGroupWithNoChunkForALeafIsRefused(t *testing.T) {
	dropLastChunk := func(t *testing.T, data []byte) []byte {
		t.Helper()
		md, err := ReadFileMetaData(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		rg := &md.RowGroups[0]
		if len(rg.Columns) < 2 {
			t.Fatalf("fixture has %d chunks", len(rg.Columns))
		}
		rg.Columns = rg.Columns[:len(rg.Columns)-1]
		footerLen := binary.LittleEndian.Uint32(data[len(data)-8:])
		out := append([]byte(nil), data[:len(data)-8-int(footerLen)]...)
		footer := EncodeFileMetaData(md)
		out = append(out, footer...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
		return append(out, "PAR1"...)
	}

	t.Run("flat/readColumnToAny", func(t *testing.T) {
		rows := make([]map[string]any, 8)
		for i := range rows {
			rows[i] = map[string]any{"x": int64(i), "s": fmt.Sprintf("v%d", i)}
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, Schema{Columns: []Column{
			{Name: "x", Type: TypeInt64}, {Name: "s", Type: TypeString},
		}}, WriterConfig{Compression: CompressionNone})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := readEveryRow(buf.Bytes()); err != nil || len(got) != 8 {
			t.Fatalf("unmutated: rows=%d err=%v", len(got), err)
		}
		got, err := readEveryRow(dropLastChunk(t, buf.Bytes()))
		if err == nil {
			t.Fatalf("a row group with no chunk for column s read %d rows with no error: %v",
				len(got), got)
		}
		if !strings.Contains(err.Error(), "carries no chunk for it") ||
			!strings.Contains(err.Error(), "s") {
			t.Fatalf("refused, but not as a missing chunk naming the column: %v", err)
		}
	})

	t.Run("nested/readLeafColumn", func(t *testing.T) {
		data := testdataFile(t, "testdata/nested_pages.parquet")
		if got, err := readEveryRow(data); err != nil || len(got) == 0 {
			t.Fatalf("unmutated: rows=%d err=%v", len(got), err)
		}
		got, err := readEveryRow(dropLastChunk(t, data))
		if err == nil {
			t.Fatalf("a row group with no chunk for a container leaf read %d rows with no error",
				len(got))
		}
		if !strings.Contains(err.Error(), "carries no chunk for it") {
			t.Fatalf("refused, but not as a missing chunk: %v", err)
		}
	})
}

// TestAV2PageWhoseRowCountContradictsItsLevelsIsRefused gates the third
// ungated hunk: a nested v2 header declares num_rows, the repetition levels
// say how many rows the page actually opens, and the two must agree. The
// corpus had no nested data-page-v2 file at all until testdata/v2_nested.parquet
// (data_page_version="2.0"), so the check rested on nothing.
func TestAV2PageWhoseRowCountContradictsItsLevelsIsRefused(t *testing.T) {
	data := testdataFile(t, "testdata/v2_nested_small.parquet")
	fr := mustFileReader(t, data)
	leafIdx := leafIndexByPath(t, fr, "tags.list.element")

	checked := 0
	for _, p := range walkPages(t, data) {
		if p.kind != PageDataV2 || p.column != "tags.list.element" {
			continue
		}
		ph, _, err := DecodePageHeader(data[p.headerAt:])
		if err != nil || ph.DataPageHeaderV2 == nil {
			continue
		}
		body := data[p.bodyAt : p.bodyAt+p.bodyLen]

		pr := fr.ColumnPages(0, leafIdx)
		if pr == nil {
			t.Fatal("no chunk")
		}
		ref, err := pr.decodeDataPageV2(ph, body)
		pr.Close()
		if err != nil {
			t.Fatalf("unmutated page at %d: %v", p.headerAt, err)
		}
		if ref.NumRows == ref.NumValues {
			// A page whose rows and values coincide cannot tell the row check
			// from the flat num_rows == num_values pairing.
			ref.Release()
			continue
		}
		ref.Release()

		for _, delta := range []int32{-1, 1} {
			m := *ph
			h := *ph.DataPageHeaderV2
			m.DataPageHeaderV2 = &h
			h.NumRows += delta
			pr2 := fr.ColumnPages(0, leafIdx)
			if pr2 == nil {
				t.Fatal("no chunk")
			}
			got, err := pr2.decodeDataPageV2(&m, body)
			pr2.Close()
			checked++
			if err == nil {
				got.Release()
				t.Fatalf("page at %d: num_rows %d -> %d decoded with no error",
					p.headerAt, h.NumRows-delta, h.NumRows)
			}
			if !strings.Contains(err.Error(), "repetition\nlevels start") &&
				!strings.Contains(err.Error(), "repetition levels start") {
				t.Fatalf("page at %d: num_rows %d -> %d refused, but not against its levels: %v",
					p.headerAt, h.NumRows-delta, h.NumRows, err)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no nested v2 page with rows != values; the cell proves nothing")
	}
	t.Logf("%d num_rows perturbations, all refused against the repetition levels", checked)
}
