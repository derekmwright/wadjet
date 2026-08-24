package pgwire

// Regression coverage for #471: formatPgValue rendered a ROW's NULL field as
// Go's "<nil>" in map-range-random field order, where PostgreSQL renders
// composites as "(a,,c)" — an empty slot for NULL — in the DECLARED field
// order. This file drives the fix through the FULL pgwire pipeline — a real
// table, a real SQL query, a real client — because the unit-level coverage
// in unit_test.go (TestFormatPgValueTypedComposite/Map) exercises
// formatPgValueTyped directly with a hand-built *parquet.Column; what it
// cannot show is that sendDataRow ever RESOLVES one for an ordinary query
// against a stored table (nestedColumnSchemas' catalog lookup, wired
// through sendResultRows in server.go/coord_query.go).

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// peopleSchema declares a ROW column whose field order is deliberately NOT
// alphabetical (city, zip, state — sorted would be city, state, zip), so a
// rendering that fell back to sorted keys would be caught by the same test
// that catches map-range-random order.
func peopleSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "addr", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "city", Type: parquet.TypeString, Nullable: true},
			{Name: "zip", Type: parquet.TypeInt32, Nullable: true},
			{Name: "state", Type: parquet.TypeString, Nullable: true},
		}},
		{Name: "tags", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		// A MAP column's own Fields is unused — its per-entry structure
		// lives under ElementType, a TypeRow column carrying the two entry
		// fields (formatPgMap's doc explains why; the shape matches every
		// other MAP schema in this codebase, e.g.
		// wadjet/container_order_test.go's coSchema).
		{Name: "scores", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt32, Nullable: true},
			}}},
	}}
}

func setupPeopleDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, srv := setupRealDB(t)

	schema := peopleSchema()
	if err := db.CreateTable(ctx, "people", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{
			"id":     int32(1),
			"addr":   map[string]any{"city": "Reston", "zip": int32(20190), "state": "VA"},
			"tags":   []any{"a", "has,comma", nil},
			"scores": map[string]any{"math": int32(90), "art": int32(75)},
		},
		{
			"id":     int32(2),
			"addr":   nil,
			"tags":   nil,
			"scores": nil,
		},
		{
			"id":     int32(3),
			"addr":   map[string]any{"city": "Fairfax", "zip": int32(22030), "state": nil},
			"tags":   []any{},
			"scores": map[string]any{},
		},
		{
			"id":     int32(4),
			"addr":   nil,
			"tags":   nil,
			"scores": map[string]any{"gym": nil},
		},
	}
	ing := db.NewIngester("people", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

// TestIssue471RowRendersInDeclaredFieldOrder is the core #471 regression: a
// ROW value's field order on the wire must match the DECLARED schema order
// (city, zip, state), never Go's randomized map iteration — and a NULL
// field is an empty slot, "(Fairfax,22030,)", not the literal text
// "<nil>" the old fmt.Sprintf("%v", ...) fallback produced.
func TestIssue471RowRendersInDeclaredFieldOrder(t *testing.T) {
	srv := setupPeopleDB(t)
	db := openPQ(t, srv.Addr())

	tests := []struct {
		id   int
		want string
	}{
		{1, "(Reston,20190,VA)"},
		{3, "(Fairfax,22030,)"},
	}
	for _, tt := range tests {
		var addr string
		if err := db.QueryRow("SELECT addr FROM people WHERE id = $1", tt.id).Scan(&addr); err != nil {
			t.Fatalf("id %d: query: %v", tt.id, err)
		}
		if addr != tt.want {
			t.Errorf("id %d: addr = %q, want %q", tt.id, addr, tt.want)
		}
	}
}

// TestIssue471RowNullIsNull covers the column-level NULL (the whole ROW is
// absent), which is a SQL NULL on the wire — distinct from a NULL FIELD
// inside a present ROW, which TestIssue471RowRendersInDeclaredFieldOrder
// covers via id 3's empty "state" slot.
func TestIssue471RowNullIsNull(t *testing.T) {
	srv := setupPeopleDB(t)
	db := openPQ(t, srv.Addr())

	var addr *string
	if err := db.QueryRow("SELECT addr FROM people WHERE id = 2").Scan(&addr); err != nil {
		t.Fatalf("query: %v", err)
	}
	if addr != nil {
		t.Errorf("addr = %q, want SQL NULL", *addr)
	}
}

// TestIssue471ArrayRendersPgBraceForm covers the ARRAY defects #471 called
// out: PostgreSQL's brace-and-comma text form (not the old
// "[a, b, c]" square-bracket rendering), a NULL element as the bare keyword,
// and quoting for an element containing the array delimiter.
func TestIssue471ArrayRendersPgBraceForm(t *testing.T) {
	srv := setupPeopleDB(t)
	db := openPQ(t, srv.Addr())

	tests := []struct {
		id   int
		want string
	}{
		{1, `{a,"has,comma",NULL}`},
		{3, "{}"},
	}
	for _, tt := range tests {
		var tags string
		if err := db.QueryRow("SELECT tags FROM people WHERE id = $1", tt.id).Scan(&tags); err != nil {
			t.Fatalf("id %d: query: %v", tt.id, err)
		}
		if tags != tt.want {
			t.Errorf("id %d: tags = %q, want %q", tt.id, tags, tt.want)
		}
	}
}

// TestIssue471MapRendersSortedKeyOrderNoNilText covers the MAP defects
// #471 called out separately from ROW/ARRAY: a deterministic order (the
// entries arrive pre-sorted by key — mapEntryRows, internal/engine/batch —
// which this must render in, not re-range) and a NULL value as the
// unquoted NULL keyword rather than Go's "<nil>".
func TestIssue471MapRendersSortedKeyOrderNoNilText(t *testing.T) {
	srv := setupPeopleDB(t)
	db := openPQ(t, srv.Addr())

	tests := []struct {
		id   int
		want string
	}{
		// Insertion order was math, art; sorted-key order is art, math.
		{1, "{art: 75, math: 90}"},
		{3, "{}"},
		{4, "{gym: NULL}"},
	}
	for _, tt := range tests {
		var scores string
		if err := db.QueryRow("SELECT scores FROM people WHERE id = $1", tt.id).Scan(&scores); err != nil {
			t.Fatalf("id %d: query: %v", tt.id, err)
		}
		if scores != tt.want {
			t.Errorf("id %d: scores = %q, want %q", tt.id, scores, tt.want)
		}
	}
}

// TestIssue471NestedColumnSchemasResolvesDeclaredOrder pins
// nestedColumnSchemas (paraminfer.go) directly: the catalog lookup that
// recovers a ROW/ARRAY/MAP column's declared structure for the legacy
// (non-coord) query path — what the end-to-end tests above exercise
// indirectly through sendDataRow.
func TestIssue471NestedColumnSchemasResolvesDeclaredOrder(t *testing.T) {
	db, _ := setupRealDB(t)
	ctx := context.Background()
	schema := peopleSchema()
	if err := db.CreateTable(ctx, "people", schema, nil); err != nil {
		t.Fatal(err)
	}

	c := &pgConn{db: db}
	metas := []wadjet.ColumnMeta{
		{Name: "id", TypeID: parquet.TypeInt32},
		{Name: "addr", TypeID: parquet.TypeRow},
		{Name: "tags", TypeID: parquet.TypeArray},
	}
	got := c.nestedColumnSchemas("SELECT id, addr, tags FROM people", metas)
	if got == nil {
		t.Fatal("nestedColumnSchemas returned nil, want a resolved schema")
	}
	addrCol, ok := got.byName["addr"]
	if !ok {
		t.Fatal(`nestedColumnSchemas did not resolve "addr"`)
	}
	if len(addrCol.Fields) != 3 || addrCol.Fields[0].Name != "city" ||
		addrCol.Fields[1].Name != "zip" || addrCol.Fields[2].Name != "state" {
		t.Errorf("addr.Fields = %+v, want [city zip state] in that order", addrCol.Fields)
	}
	tagsCol, ok := got.byName["tags"]
	if !ok {
		t.Fatal(`nestedColumnSchemas did not resolve "tags"`)
	}
	if tagsCol.ElementType == nil || tagsCol.ElementType.Type != parquet.TypeString {
		t.Errorf("tags.ElementType = %+v, want a STRING element", tagsCol.ElementType)
	}
}

// TestIssue471NestedColumnSchemasSkipsOrdinaryQueries checks the cost guard:
// no catalog round trip (nil result, not just an empty map) when nothing in
// the result is a nested type — the ordinary query.
func TestIssue471NestedColumnSchemasSkipsOrdinaryQueries(t *testing.T) {
	db, _ := setupRealDB(t)
	c := &pgConn{db: db}
	metas := []wadjet.ColumnMeta{
		{Name: "id", TypeID: parquet.TypeInt32},
		{Name: "name", TypeID: parquet.TypeString},
	}
	if got := c.nestedColumnSchemas("SELECT id, name FROM users", metas); got != nil {
		t.Errorf("nestedColumnSchemas = %v, want nil for an all-scalar result", got)
	}
}

// TestIssue471NestedColumnSchemasDropsConflicts covers the same conflict
// rule columnParamOIDs applies (paraminfer.go): a column name two tables
// carry at a DIFFERENT top-level type must not resolve to either one — a
// wrong confident structure would silently drop fields formatPgComposite
// cannot find under it, worse than the order-agnostic fallback.
func TestIssue471NestedColumnSchemasDropsConflicts(t *testing.T) {
	db, _ := setupRealDB(t)
	ctx := context.Background()

	rowSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "info", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString},
		}},
	}}
	if err := db.CreateTable(ctx, "rowtab", rowSchema, nil); err != nil {
		t.Fatal(err)
	}
	arrSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "info", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString}},
	}}
	if err := db.CreateTable(ctx, "arrtab", arrSchema, nil); err != nil {
		t.Fatal(err)
	}

	c := &pgConn{db: db}
	metas := []wadjet.ColumnMeta{
		{Name: "id", TypeID: parquet.TypeInt32},
		{Name: "info", TypeID: parquet.TypeRow},
	}
	got := c.nestedColumnSchemas("SELECT id, info FROM rowtab, arrtab", metas)
	if got == nil {
		// "id" resolves cleanly (both tables agree on int32), so the
		// conflict dropping "info" alone must not empty the whole result.
		t.Fatal("nestedColumnSchemas returned nil, want a schema with the conflict-free \"id\" column")
	}
	if _, ok := got.byName["info"]; ok {
		t.Errorf(`nestedColumnSchemas resolved "info" despite a ROW/ARRAY conflict across tables: %+v`, got.byName["info"])
	}
}
