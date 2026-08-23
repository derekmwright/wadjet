package compaction

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Compaction is read → write over the table's whole schema, so anything the
// pair is not the identity for, compaction DESTROYS — and it replaces the
// inputs with the result, so there is nothing to compare against afterwards.
//
// These are end-to-end over the compactor itself: build a table, snapshot
// what the reader says it holds, compact, and demand the same answer back
// through both row-path entry points.
//
//   - #428: ReadRowGroup dropped every ARRAY/ROW/MAP column, so merging a
//     nested table wrote those columns as NULL for every row.
//   - #429: the writer read a DECIMAL integer box as the whole number and
//     multiplied it by 10^scale, so every pass multiplied the column.
//
// Both were silent.

// readTableRows reads every file the manifest lists, through the row-path
// entry the caller names, and returns the rows sorted by id.
func readTableRows(t *testing.T, cat *catalog.Catalog, store objstore.Store, table string,
	schema parquet.Schema, perRowGroup bool) []map[string]any {
	t.Helper()
	ctx := context.Background()
	manifest, err := cat.GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			rc, _, err := store.Get(ctx, "test-bucket", f.Path)
			if err != nil {
				t.Fatalf("get %s: %v", f.Path, err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			r, err := parquet.NewReaderFromBytes(data)
			if err != nil {
				t.Fatalf("reader %s: %v", f.Path, err)
			}
			if perRowGroup {
				for rg := 0; rg < r.NumRowGroups(); rg++ {
					rows, err := r.ReadRowGroupAs(rg, schema.Columns, schema.ColumnNames())
					if err != nil {
						t.Fatalf("ReadRowGroupAs(%s, %d): %v", f.Path, rg, err)
					}
					out = append(out, rows...)
				}
			} else {
				rows, err := r.ReadRowsAs(schema.Columns, nil)
				if err != nil {
					t.Fatalf("ReadRowsAs(%s): %v", f.Path, err)
				}
				out = append(out, rows...)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return idOf(out[i]) < idOf(out[j]) })
	return out
}

func idOf(row map[string]any) int64 {
	if v, ok := row["id"].(int64); ok {
		return v
	}
	return -1
}

func assertSameRows(t *testing.T, got, want []map[string]any, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d", what, len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Errorf("%s row %d:\n   got %#v\n  want %#v", what, i, got[i], want[i])
		}
	}
}

// buildAndCompact creates the table, writes n files of the given rows (with
// ids offset per file), snapshots the pre-compaction content and compacts.
func buildAndCompact(t *testing.T, table string, schema parquet.Schema,
	rowsFor func(file int) []map[string]any, files int) (*catalog.Catalog, objstore.Store, []map[string]any) {
	t.Helper()
	cat, store := setupTestCatalog(t)
	ctx := context.Background()
	if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatal(err)
	}
	var nrows int
	for i := 0; i < files; i++ {
		rows := rowsFor(i)
		nrows = len(rows)
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
		size := writeTestFile(t, store, "test-bucket", path, schema, rows)
		if err := cat.AddFiles(ctx, table, nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: int64(len(rows)), CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}
	before := readTableRows(t, cat, store, table, schema, false)
	if len(before) != files*nrows {
		t.Fatalf("pre-compaction read %d rows, want %d", len(before), files*nrows)
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 2
	if _, err := New(cat, nil, cfg).CompactTable(ctx, table); err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	return cat, store, before
}

func nestedTestSchema() parquet.Schema {
	i64 := func(name string) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeInt64, Nullable: true}
	}
	str := func(name string) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true}
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
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		arr("tags", str("element")),
		arr("nest", arr("element", i64("element"))),
		row("info", i64("a"), row("s", i64("b"))),
		mp("props", i64("")),
		mp("plists", arr("", i64("element"))),
	}}
}

func TestCompactTable_NestedColumnsSurvive(t *testing.T) {
	schema := nestedTestSchema()
	rowsFor := func(file int) []map[string]any {
		base := int64(file * 10)
		return []map[string]any{
			{ // ordinary values, containers inside containers
				"id":     base + 0,
				"tags":   []any{"a", "b"},
				"nest":   []any{[]any{int64(1), int64(2)}, []any{int64(3)}},
				"info":   map[string]any{"a": int64(5), "s": map[string]any{"b": int64(9)}},
				"props":  map[string]any{"p": int64(1), "q": int64(2)},
				"plists": map[string]any{"k": []any{int64(7), int64(8)}},
			},
			{ // empty containers — not the same value as NULL
				"id":     base + 1,
				"tags":   []any{},
				"nest":   []any{},
				"info":   map[string]any{"a": int64(6), "s": map[string]any{}},
				"props":  map[string]any{},
				"plists": map[string]any{"k": []any{}},
			},
			{ // NULL one level in
				"id":     base + 2,
				"tags":   []any{nil, "c"},
				"nest":   []any{nil, []any{nil, int64(5)}},
				"info":   map[string]any{},
				"props":  map[string]any{"p": nil},
				"plists": map[string]any{"k": nil},
			},
			{ // every container NULL
				"id": base + 3,
			},
		}
	}
	cat, store, before := buildAndCompact(t, "events", schema, rowsFor, 4)

	// Every nested column used to come back absent here: the merged file
	// held NULL for all of them, and the inputs were already gone.
	after := readTableRows(t, cat, store, "events", schema, false)
	assertSameRows(t, after, before, "after compaction (ReadRows)")
	assertSameRows(t, readTableRows(t, cat, store, "events", schema, true), before,
		"after compaction (ReadRowGroup)")

	// And the nested keys are actually there, so a test that compared two
	// equally-empty reads could not pass.
	for _, k := range []string{"tags", "nest", "info", "props", "plists"} {
		if _, ok := after[0][k]; !ok {
			t.Errorf("column %q is absent from the compacted row", k)
		}
	}
}

func TestCompactTable_DecimalIsNotMultiplied(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "narrow", Type: parquet.TypeDecimal, Nullable: true, Precision: 9, Scale: 2},
		{Name: "mid", Type: parquet.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
		// The reviewer's "compaction of DECIMAL(38,·) fails loudly" case:
		// the reader boxes it as Decimal128 and the old writer multiplied
		// that by 10^10 until it no longer fit 64 bits.
		{Name: "wide", Type: parquet.TypeDecimal, Nullable: true, Precision: 38, Scale: 10},
	}}
	rowsFor := func(file int) []map[string]any {
		base := int64(file * 10)
		return []map[string]any{
			{"id": base + 0, "narrow": 3.25, "mid": 3.25, "wide": 3.25},
			{"id": base + 1, "narrow": -1.5, "mid": -1.5, "wide": -1.5},
			{"id": base + 2, "narrow": 0.0, "mid": 0.0, "wide": 0.0},
			{"id": base + 3, "narrow": 9999999.99, "mid": 99999999999999.9999, "wide": 922337203.6854775807},
			{"id": base + 4},
		}
	}
	cat, store, before := buildAndCompact(t, "ledger", schema, rowsFor, 3)
	after := readTableRows(t, cat, store, "ledger", schema, false)
	assertSameRows(t, after, before, "after one compaction")

	// 3.25 at scale 2 is the unscaled 325, and it stays 325.
	if got := after[0]["narrow"]; got != int64(325) {
		t.Errorf("narrow = %#v, want int64(325) — compaction rescaled the column", got)
	}

	// A second pass: one generation cannot see a per-pass multiplier.
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("tables/ledger/extra_%04d.parquet", i)
		size := writeTestFile(t, store, "test-bucket", path, schema, rowsFor(100+i))
		if err := cat.AddFiles(ctx, "ledger", nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 5, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := DefaultConfig()
	cfg.MinFiles = 2
	if _, err := New(cat, nil, cfg).CompactTable(ctx, "ledger"); err != nil {
		t.Fatalf("second CompactTable: %v", err)
	}
	twice := readTableRows(t, cat, store, "ledger", schema, false)
	assertSameRows(t, twice[:len(before)], before, "after two compactions")
}

// The v0.18.0 writer refuses a value it cannot represent, which is right —
// but compaction must never hand it one. Reading without the TABLE's schema
// gave an IPv6 back as a Go string of sixteen raw bytes, which the writer
// then put through net.ParseIP and refused: compaction could not read back
// its own output.
func TestCompactTable_BinaryAndNetworkTypes(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "raw", Type: parquet.TypeBytes, Nullable: true},
		{Name: "v6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "m", Type: parquet.TypeMAC, Nullable: true},
		{Name: "v4", Type: parquet.TypeIPv4, Nullable: true},
	}}
	rowsFor := func(file int) []map[string]any {
		base := int64(file * 10)
		return []map[string]any{
			{"id": base + 0, "raw": []byte{1, 2, 3}, "v6": "2001:db8::1",
				"u": "550e8400-e29b-41d4-a716-446655440000", "m": "00:11:22:33:44:55", "v4": "10.0.0.1"},
			// The empty literal is an absence and reads back as NULL; an
			// empty BYTES value is the empty value it is.
			{"id": base + 1, "raw": []byte{}, "v6": "", "u": "", "m": "", "v4": ""},
			{"id": base + 2},
		}
	}
	cat, store, before := buildAndCompact(t, "sessions", schema, rowsFor, 3)
	assertSameRows(t, readTableRows(t, cat, store, "sessions", schema, false), before,
		"after compaction (ReadRows)")
	assertSameRows(t, readTableRows(t, cat, store, "sessions", schema, true), before,
		"after compaction (ReadRowGroup)")

	if got, ok := before[1]["v6"]; ok {
		t.Errorf("the empty IPv6 literal came back as %#v, want an absence", got)
	}
	if got, ok := before[1]["raw"].([]byte); !ok || len(got) != 0 {
		t.Errorf("the empty BYTES value came back as %#v, want a zero-length value", before[1]["raw"])
	}
	if got := before[0]["v6"]; !bytes.Equal(got.([]byte), []byte{
		0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
	}) {
		t.Errorf("v6 = %#v", got)
	}
}

// The catalog is the authority on the types a parquet file cannot describe,
// and compaction has to read through it. This is the shape where it decides
// the outcome: a file whose own schema calls the column STRING, in a table
// whose schema calls it IPv6 — a file from another writer, or from before
// the column was typed.
//
// Read as STRING, an IPv6 value comes back as a Go string of sixteen raw
// bytes, which the writer puts through net.ParseIP and REFUSES by name. The
// compactor logs a failed merge as a warning and moves on, so the visible
// symptom is not an error: it is a partition that never gets compacted
// again. Hence the file-count assertion.
func TestCompactTable_FileSchemaDisagreesWithTheCatalog(t *testing.T) {
	ctx := context.Background()
	cat, store := setupTestCatalog(t)

	tableSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v6", Type: parquet.TypeIPv6, Nullable: true},
	}}
	// What the FILES say about themselves.
	fileSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v6", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "foreign", tableSchema, nil); err != nil {
		t.Fatal(err)
	}
	addr := net.ParseIP("2001:db8::1").To16()
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("tables/foreign/chunk_%04d.parquet", i)
		size := writeTestFile(t, store, "test-bucket", path, fileSchema, []map[string]any{
			{"id": int64(i*2 + 0), "v6": string(addr)},
			{"id": int64(i*2 + 1)},
		})
		if err := cat.AddFiles(ctx, "foreign", nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 2
	if _, err := New(cat, nil, cfg).CompactTable(ctx, "foreign"); err != nil {
		t.Fatalf("CompactTable: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, "foreign")
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, part := range manifest.Partitions {
		files += len(part.Files)
	}
	if files != 1 {
		t.Fatalf("the table still has %d files — the merge was refused and swallowed as a warning", files)
	}

	after := readTableRows(t, cat, store, "foreign", tableSchema, false)
	if len(after) != 6 {
		t.Fatalf("compaction produced %d rows, want 6", len(after))
	}
	if got, ok := after[0]["v6"].([]byte); !ok || !bytes.Equal(got, addr) {
		t.Errorf("v6 = %#v, want the sixteen bytes of 2001:db8::1", after[0]["v6"])
	}
}
