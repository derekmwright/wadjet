package compaction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The compaction gate.
//
// Compaction is the one background job that READS a table and REPLACES it
// with what it read. Every read→write asymmetry in the parquet package is
// therefore a data-loss bug there, and the inputs are gone before anyone can
// compare. Two shipped that way and neither made a sound:
//
//   - #428: ReadRowGroup had no nested dispatch, so every ARRAY/ROW/MAP
//     column merged to NULL for every row.
//   - #429: the writer read a DECIMAL integer box as the whole number, so
//     every pass multiplied the column by 10^scale.
//
// Neither could be caught by a corpus built on Int32/Float64/String, which
// is what every other differential gate in this repo is built on (see
// internal/oracle/typematrix). This gate is that corpus, driven through the
// compactor: all 22 types plus the shapes the matrix does not carry — the
// two DECIMAL precisions either side of the 64-bit box boundary and three
// containers nested inside containers — over three generations.
//
// It runs in the default `go test ./internal/...`, so CI's unit step carries
// it. Keep it that way: the cost is a few hundred milliseconds and the class
// of defect it covers is silent data destruction.

const (
	// Rows per input file. Small enough to stay fast, large enough that the
	// row-group size below splits each input into several groups — the
	// per-row-group read is the path compaction takes, and #428 lived in it.
	gateRowsPerFile = 400
	gateFiles       = 3
	gateRowGroup    = 137
	gateGenerations = 3
)

// --- fixture ---------------------------------------------------------------

func gateFlatSchema() parquet.Schema {
	s := typematrix.Schema()
	// The type matrix carries one DECIMAL, at (18,4). The two that matter
	// for read→write sit either side of the box boundary: (9,2) is the
	// scale-2 case a compaction pass multiplied by 100, and (38,10) is the
	// precision at which the reader switches to the 128-bit box.
	s.Columns = append(s.Columns,
		parquet.Column{Name: "c_dec9", Type: parquet.TypeDecimal, Nullable: true, Precision: 9, Scale: 2},
		parquet.Column{Name: "c_dec38", Type: parquet.TypeDecimal, Nullable: true, Precision: 38, Scale: 10},
	)
	return s
}

func gateNestedSchema() parquet.Schema {
	i64 := func(name string) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeInt64, Nullable: true}
	}
	arr := func(name string, elem parquet.Column) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeArray, Nullable: true, ElementType: &elem}
	}
	mp := func(name string, val parquet.Column) parquet.Column {
		val.Name = "value"
		return parquet.Column{Name: name, Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString}, val,
			}}}
	}
	row := func(name string, fields ...parquet.Column) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeRow, Nullable: true, Fields: fields}
	}
	s := typematrix.NestedSchema()
	// Containers inside containers: the matrix's four are one level deep,
	// and #409's assembler bug lived below that.
	s.Columns = append(s.Columns,
		arr("c_arr_arr", arr("element", i64("element"))),
		mp("c_map_arr", arr("", i64("element"))),
		row("c_row_map", i64("a"), mp("m", i64(""))),
	)
	return s
}

// gateNull spells the matrix's own "NULL every stride-th row" rule.
func gateNull(i, stride int, v any) any {
	if i%stride == stride-1 {
		return nil
	}
	return v
}

func gateFlatData(offset, n int) []map[string]any {
	rows := typematrix.Data(n)
	for i, r := range rows {
		gi := offset + i
		r["id"] = int64(gi)
		// A REAL box: scaled on the way in, and the value that a pass used
		// to multiply by 100.
		r["c_dec9"] = gateNull(i, 11, float64(gi%9_000_000)/100)
		// An INTEGER box at a precision the reader hands back as
		// Decimal128: already unscaled, and stored verbatim.
		r["c_dec38"] = gateNull(i, 7, int64(gi)*1_000_000_003)
		switch i {
		case 0:
			r["c_dec9"], r["c_dec38"] = 0.0, int64(0)
		case 1:
			r["c_dec9"], r["c_dec38"] = -9999999.99, int64(-9223372036854775807)
		case 2:
			r["c_dec9"], r["c_dec38"] = 9999999.99, int64(9223372036854775807)
		}
	}
	return rows
}

func gateNestedData(offset, n int) []map[string]any {
	rows := typematrix.NestedData(n)
	for i, r := range rows {
		gi := offset + i
		r["id"] = int64(gi)
		// NULL, empty, a NULL one level in, and a populated value in every
		// position, cycling so each lands at interior and boundary rows.
		switch i % 5 {
		case 0:
			r["c_arr_arr"] = []any{[]any{int64(gi), int64(2)}, []any{}}
			r["c_map_arr"] = map[string]any{"k": []any{int64(gi)}, "e": []any{}}
			r["c_row_map"] = map[string]any{"a": int64(gi), "m": map[string]any{"k": int64(gi)}}
		case 1:
			r["c_arr_arr"] = []any{}
			r["c_map_arr"] = map[string]any{}
			r["c_row_map"] = map[string]any{"a": int64(gi), "m": map[string]any{}}
		case 2:
			r["c_arr_arr"] = []any{nil}
			r["c_map_arr"] = map[string]any{"k": nil}
			r["c_row_map"] = map[string]any{"m": map[string]any{"k": nil}}
		case 3:
			r["c_arr_arr"] = nil
			r["c_map_arr"] = nil
			r["c_row_map"] = nil
		case 4:
			r["c_arr_arr"] = []any{[]any{nil, int64(gi)}}
			r["c_map_arr"] = map[string]any{"a": []any{nil}, "b": []any{int64(gi)}}
			r["c_row_map"] = map[string]any{"a": nil, "m": map[string]any{"x": nil}}
		}
	}
	return rows
}

// --- harness ---------------------------------------------------------------

type gateTable struct {
	name   string
	schema parquet.Schema
	rows   []map[string]any // the LOGICAL rows, as ingest handed them over
	cat    *catalog.Catalog
	store  objstore.Store
	// generation counts the resplits, so each one's file paths are fresh.
	generation int
}

func gateWriteFile(t *testing.T, store objstore.Store, path string, schema parquet.Schema, rows []map[string]any) int64 {
	t.Helper()
	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.RowGroupSize = gateRowGroup
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	size := int64(buf.Len())
	if _, err := store.Put(context.Background(), "test-bucket", path,
		bytes.NewReader(buf.Bytes()), size, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	return size
}

func gateIngest(t *testing.T, name string, schema parquet.Schema, data func(offset, n int) []map[string]any) *gateTable {
	t.Helper()
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatal(err)
	}
	var all []map[string]any
	for f := 0; f < gateFiles; f++ {
		rows := data(f*gateRowsPerFile, gateRowsPerFile)
		all = append(all, rows...)
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, f)
		size := gateWriteFile(t, store, path, schema, rows)
		if err := cat.AddFiles(ctx, name, nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: int64(len(rows)), CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}
	return &gateTable{name: name, schema: schema, rows: all, cat: cat, store: store}
}

func (g *gateTable) files(t *testing.T) [][]byte {
	t.Helper()
	ctx := context.Background()
	manifest, err := g.cat.GetManifest(ctx, g.name)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			paths = append(paths, f.Path)
		}
	}
	sort.Strings(paths)
	out := make([][]byte, 0, len(paths))
	for _, p := range paths {
		rc, _, err := g.store.Get(ctx, "test-bucket", p)
		if err != nil {
			t.Fatalf("get %s: %v", p, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, data)
	}
	return out
}

// rowPathRows reads every file through the ROW path, one row group at a time
// — exactly what the compactor itself does.
func (g *gateTable) rowPathRows(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, data := range g.files(t) {
		r, err := parquet.NewReaderFromBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		for rg := 0; rg < r.NumRowGroups(); rg++ {
			rows, err := r.ReadRowGroupAs(rg, g.schema.Columns, g.schema.ColumnNames())
			if err != nil {
				t.Fatalf("ReadRowGroupAs(%d): %v", rg, err)
			}
			out = append(out, rows...)
		}
	}
	sortByID(out)
	return out
}

// nativeRows reads every file through the NATIVE COLUMNAR path. It is the
// path production queries take, and it decodes the same bytes with different
// code — ADR-0018 §3 is that the two must agree, which is only a property if
// both are exercised.
//
// It is only defined for the flat table: ReadRowGroupNative refuses an
// ARRAY/ROW/MAP column by design (callers fall back to the row reader), so
// the nested table has one read path and is checked on that one.
func (g *gateTable) nativeRows(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, data := range g.files(t) {
		r, err := parquet.NewReaderFromBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		for rg := 0; rg < r.NumRowGroups(); rg++ {
			b, err := scan.ReadRowGroupNative(r.FileReader(), rg, g.schema.Columns, nil)
			if err != nil {
				t.Fatalf("ReadRowGroupNative(%d): %v", rg, err)
			}
			out = append(out, batchToRows(b)...)
		}
	}
	sortByID(out)
	return out
}

func batchToRows(b *batch.RecordBatch) []map[string]any {
	out := make([]map[string]any, b.Len)
	for i := 0; i < b.Len; i++ {
		row := make(map[string]any, len(b.Columns))
		for c, vec := range b.Columns {
			if vec == nil || vec.Nulls.IsNullFast(i) {
				continue
			}
			row[b.Schema[c].Name] = vec.GetValue(i)
		}
		out[i] = row
	}
	return out
}

func sortByID(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		a, _ := rows[i]["id"].(int64)
		b, _ := rows[j]["id"].(int64)
		return a < b
	})
}

func (g *gateTable) compact(t *testing.T) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MinFiles = 2
	if _, err := New(g.cat, nil, cfg).CompactTable(context.Background(), g.name); err != nil {
		t.Fatalf("CompactTable(%s): %v", g.name, err)
	}
	if n := len(g.files(t)); n != 1 {
		t.Fatalf("after compaction the table has %d files, want 1 — the pass did not run", n)
	}
}

// resplit is the next generation's INGEST: it reads the table's single
// compacted file back and lays it down as several files again, which is what
// gives the compactor something to merge. A one-file partition is not a
// compaction candidate (shouldCompact), and lowering MinFiles to 1 does not
// help — CompactTable's pass loop would then never reach "no partition needs
// compaction" and would rewrite the same file forever.
func (g *gateTable) resplit(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	rows := g.rowPathRows(t)
	manifest, err := g.cat.GetManifest(ctx, g.name)
	if err != nil {
		t.Fatal(err)
	}
	var old []string
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			old = append(old, f.Path)
		}
	}
	per := (len(rows) + gateFiles - 1) / gateFiles
	var entries []catalog.FileEntry
	for i := 0; i < gateFiles; i++ {
		lo, hi := i*per, min((i+1)*per, len(rows))
		if lo >= hi {
			break
		}
		path := fmt.Sprintf("tables/%s/split_%d_%04d.parquet", g.name, g.generation, i)
		size := gateWriteFile(t, g.store, path, g.schema, rows[lo:hi])
		entries = append(entries, catalog.FileEntry{
			Path: path, SizeBytes: size, NumRows: int64(hi - lo), CreatedAt: time.Now().UTC(),
		})
	}
	g.generation++
	if err := g.cat.RemoveFiles(ctx, g.name, old); err != nil {
		t.Fatal(err)
	}
	if err := g.cat.AddFiles(ctx, g.name, nil, "", entries); err != nil {
		t.Fatal(err)
	}
}

func gateDiff(t *testing.T, what string, got, want []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d", what, len(got), len(want))
	}
	bad := 0
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			bad++
			if bad <= 5 {
				for _, k := range gateKeys(got[i], want[i]) {
					gv, gok := got[i][k]
					wv, wok := want[i][k]
					if gok != wok || !reflect.DeepEqual(gv, wv) {
						t.Errorf("%s row %d column %q:\n   got %#v (present=%v)\n  want %#v (present=%v)",
							what, i, k, gv, gok, wv, wok)
					}
				}
			}
		}
	}
	if bad > 5 {
		t.Errorf("%s: %d rows differ in total", what, bad)
	}
}

func gateKeys(ms ...map[string]any) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range ms {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

// --- the gate --------------------------------------------------------------

func TestCompactionIsIdempotentOverTheTypeMatrix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema parquet.Schema
		data   func(offset, n int) []map[string]any
		native bool // the native columnar path is defined for this table
	}{
		{"flat", gateFlatSchema(), gateFlatData, true},
		{"nested", gateNestedSchema(), gateNestedData, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gateIngest(t, "typemx_"+tc.name, tc.schema, tc.data)

			// Generation 0: what the reader says the freshly ingested table
			// holds. Every later generation is compared against this.
			gen0Row := g.rowPathRows(t)
			// The native path is compared against ITSELF across
			// generations, not against the row path: the two paths box a
			// value differently on purpose (GetValue renders an IPv4 as
			// text where the row reader hands back the int64 it stores),
			// and it is ADR-0013's two-path VALUE agreement — not the
			// boxing — that other gates cover.
			var gen0Native []map[string]any
			if tc.native {
				gen0Native = g.nativeRows(t)
			}
			gateAssertNullPositions(t, g.rows, gen0Row)
			gateAssertSchema(t, tc.schema, g.files(t)[0])
			gen0Files := g.files(t)

			for gen := 1; gen <= gateGenerations; gen++ {
				if gen > 1 {
					g.resplit(t)
				}
				g.compact(t)
				what := fmt.Sprintf("generation %d", gen)

				rows := g.rowPathRows(t)
				gateDiff(t, what+" (row path)", rows, gen0Row)
				if tc.native {
					gateDiff(t, what+" (native scan)", g.nativeRows(t), gen0Native)
				}
				gateAssertNullPositions(t, g.rows, rows)
				for _, data := range g.files(t) {
					gateAssertSchema(t, tc.schema, data)
				}
			}

			gatePyArrowCrossCheck(t, tc.schema, gen0Files, g.files(t))
		})
	}
}

// (2) The row count and the NULL positions the caller handed in are the ones
// the file holds: a NULL top-level value is an ABSENT key on the way back,
// and a present one is present. This is the only assertion tied to the
// LOGICAL input rather than to a previous read, so it is what stops a
// uniformly-wrong read from agreeing with itself.
func gateAssertNullPositions(t *testing.T, want, got []map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d", len(got), len(want))
	}
	byID := make(map[int64]map[string]any, len(got))
	for _, r := range got {
		id, _ := r["id"].(int64)
		byID[id] = r
	}
	for _, w := range want {
		id, _ := w["id"].(int64)
		g, ok := byID[id]
		if !ok {
			t.Fatalf("row id=%d is missing", id)
		}
		for k, v := range w {
			_, present := g[k]
			if (v == nil) == present {
				t.Errorf("row id=%d column %q: input %v, read back present=%v", id, k, v, present)
			}
		}
	}
}

// (3) The schema survives: names, types, and the three parameters a file can
// lose without losing a value — DECIMAL precision and scale, VECTOR
// dimension. A scale that does not survive is a value that reads at the
// wrong magnitude.
func gateAssertSchema(t *testing.T, want parquet.Schema, data []byte) {
	t.Helper()
	r, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Schema().Columns
	if len(got) != len(want.Columns) {
		t.Fatalf("schema has %d columns, want %d", len(got), len(want.Columns))
	}
	for i, w := range want.Columns {
		g := got[i]
		if g.Name != w.Name || g.Type != w.Type {
			t.Errorf("column %d: %s %v, want %s %v", i, g.Name, g.Type, w.Name, w.Type)
			continue
		}
		if w.Type == parquet.TypeDecimal && (g.Precision != w.Precision || g.Scale != w.Scale) {
			t.Errorf("column %s: DECIMAL(%d,%d), want (%d,%d)", w.Name, g.Precision, g.Scale, w.Precision, w.Scale)
		}
		if w.Type == parquet.TypeVector && g.Dimension != w.Dimension {
			t.Errorf("column %s: VECTOR(%d), want VECTOR(%d)", w.Name, g.Dimension, w.Dimension)
		}
	}
}

// (4) PyArrow is the third party: it reads the FINAL compacted file and has
// to see what it saw in the pre-compaction files. Both sides go through the
// same renderer, so a difference is a difference in the DATA and not in how
// either side spells a type.
func gatePyArrowCrossCheck(t *testing.T, schema parquet.Schema, before, after [][]byte) {
	t.Helper()
	if exec.Command("python3", "-c", "import pyarrow").Run() != nil {
		t.Log("python3 with pyarrow is not importable here — skipping the PyArrow cross-check")
		return
	}
	got := gatePyArrowDump(t, after)
	want := gatePyArrowDump(t, before)
	if len(got) != len(want) {
		t.Fatalf("pyarrow read %d rows after compaction, want %d", len(got), len(want))
	}
	for i := range want {
		g, _ := json.Marshal(got[i])
		w, _ := json.Marshal(want[i])
		if !bytes.Equal(g, w) {
			t.Fatalf("pyarrow row %d differs after compaction:\n  after  %s\n  before %s", i, g, w)
		}
	}
}

func gatePyArrowDump(t *testing.T, files [][]byte) []map[string]any {
	t.Helper()
	dir := t.TempDir()
	args := []string{"-c", pyArrowGateScript}
	for i, data := range files {
		p := filepath.Join(dir, fmt.Sprintf("f%03d.parquet", i))
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		args = append(args, p)
	}
	out, err := exec.Command("python3", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("pyarrow read failed: %v\n%s", err, out)
	}
	var rows []map[string]any
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("decoding pyarrow output: %v\n%s", err, out)
	}
	return rows
}

// pyArrowGateScript renders every row of every file it is given as JSON,
// sorted by id, with binary as hex and DECIMAL as its decimal string so the
// comparison is about the value and not about Python's boxing.
const pyArrowGateScript = `
import datetime, json, sys, pyarrow as pa, pyarrow.parquet as pq

def conv(ty, v):
    if v is None:
        return None
    if pa.types.is_map(ty):
        return {str(k): conv(ty.item_type, x) for k, x in v}
    if pa.types.is_list(ty) or pa.types.is_large_list(ty):
        return [conv(ty.value_type, e) for e in v]
    if pa.types.is_struct(ty):
        return {ty.field(i).name: conv(ty.field(i).type, v[ty.field(i).name])
                for i in range(ty.num_fields)}
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if pa.types.is_decimal(ty):
        return str(v)
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
        return str(v)
    return v

rows = []
for path in sys.argv[1:]:
    t = pq.read_table(path)
    for i in range(t.num_rows):
        row = {}
        for name in t.column_names:
            row[name] = conv(t.schema.field(name).type, t.column(name)[i].as_py())
        rows.append(row)
rows.sort(key=lambda r: r["id"])
json.dump(rows, sys.stdout)
`

// --- the ANALYZE sibling ---------------------------------------------------

// analyzeUnsupported is the explicit list of types ANALYZE collects NO
// sketch for. It is asserted in both directions, so adding a type to the
// exclusion — or letting a type fall out of coverage by accident — fails
// here rather than showing up as a planner that silently stopped having
// statistics for a column.
//
// The three containers are excluded because a container has no single scalar
// value to hash: IsHLLSupportedType says so, and the sampler follows.
var analyzeUnsupported = map[parquet.TypeID]bool{
	parquet.TypeArray: true,
	parquet.TypeRow:   true,
	parquet.TypeMap:   true,
}

func TestAnalyzeCoversEveryTypeMatrixColumn(t *testing.T) {
	// One table with all 22 types: ANALYZE reads through the row path, which
	// handles flat and nested columns alike, so the split the query gates
	// need does not apply here.
	schema := gateFlatSchema()
	schema.Columns = append(schema.Columns, gateNestedSchema().Columns[2:]...)
	data := func(offset, n int) []map[string]any {
		flat := gateFlatData(offset, n)
		nested := gateNestedData(offset, n)
		for i := range flat {
			for k, v := range nested[i] {
				if k == "id" || k == "g" {
					continue
				}
				flat[i][k] = v
			}
		}
		return flat
	}
	g := gateIngest(t, "typemx_analyze", schema, data)

	ctx := context.Background()
	if _, err := g.cat.AnalyzeTable(ctx, g.name); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}
	stats, err := g.cat.AggregateColumnStats(ctx, g.name)
	if err != nil {
		t.Fatalf("AggregateColumnStats: %v", err)
	}

	// Every type in the matrix is represented in this table, or the gate is
	// not covering what it claims to.
	present := map[parquet.TypeID]bool{}
	for _, c := range schema.Columns {
		present[c.Type] = true
	}
	for _, c := range typematrix.Columns() {
		if !present[c.Type] {
			t.Errorf("type %v is in the matrix but not in this table's schema", c.Type)
		}
	}

	var missingSketch, unexpectedSketch []string
	for _, c := range schema.Columns {
		if c.Name == "id" {
			continue
		}
		cs, ok := stats[c.Name]
		if analyzeUnsupported[c.Type] {
			if ok && cs.NDV > 0 {
				unexpectedSketch = append(unexpectedSketch,
					fmt.Sprintf("%s (%v) NDV=%d", c.Name, c.Type, cs.NDV))
			}
			continue
		}
		if !ok || cs.NDV <= 0 {
			missingSketch = append(missingSketch, fmt.Sprintf("%s (%v)", c.Name, c.Type))
			continue
		}
		if cs.TotalRows != int64(len(g.rows)) {
			t.Errorf("column %s: TotalRows = %d, want %d", c.Name, cs.TotalRows, len(g.rows))
		}
	}
	if len(missingSketch) > 0 {
		t.Errorf("ANALYZE produced no sketch for: %v\n"+
			"either the type lost coverage or it belongs in analyzeUnsupported — say which, deliberately",
			missingSketch)
	}
	if len(unexpectedSketch) > 0 {
		t.Errorf("ANALYZE sketched types listed as unsupported: %v\n"+
			"drop them from analyzeUnsupported", unexpectedSketch)
	}

	// A histogram needs a reservoir sample AND an ordering the builder
	// understands, so its coverage is narrower than the sketch's. Pinned the
	// same way and in both directions.
	var missingHistogram, unexpectedHistogram []string
	for _, c := range schema.Columns {
		cs, ok := stats[c.Name]
		has := ok && cs.Histogram != nil
		want := !analyzeUnsupported[c.Type] && !analyzeNoHistogram(c)
		switch {
		case want && !has:
			missingHistogram = append(missingHistogram, fmt.Sprintf("%s (%v)", c.Name, c.Type))
		case !want && has:
			unexpectedHistogram = append(unexpectedHistogram, fmt.Sprintf("%s (%v)", c.Name, c.Type))
		}
	}
	if len(missingHistogram) > 0 {
		t.Errorf("ANALYZE built no histogram for: %v\n"+
			"either the type lost coverage or it belongs in analyzeNoHistogram", missingHistogram)
	}
	if len(unexpectedHistogram) > 0 {
		t.Errorf("ANALYZE built a histogram for types listed as not getting one: %v\n"+
			"drop them from analyzeNoHistogram — the planner can use it", unexpectedHistogram)
	}
}

// analyzeNoHistogram names the scalar columns that get an NDV sketch but no
// equi-depth histogram, each for a reason rather than by accident:
//
//   - BOOL has two values; a histogram over it carries nothing an NDV and a
//     null count do not.
//   - VECTOR has no total order to bucket by.
//
// Every DECIMAL gets one, the wide ones included: the sampler and the
// histogram builder both take the 128-bit box.
func analyzeNoHistogram(c parquet.Column) bool {
	switch c.Type {
	case parquet.TypeBool, parquet.TypeVector:
		return true
	}
	return false
}
