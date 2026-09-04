package parquet

import (
	"bytes"
	"testing"
)

// dfsCatalogSchema is what the TABLE declares; dfsFileSchema is what a file
// on disk declares. The two disagree about the SCALE only — one relation, two
// halves of one declaration (#707).
func dfsCatalogSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "a", Type: TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}}
}

func dfsFileSchema(scale int) Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "a", Type: TypeDecimal, Precision: 15, Scale: scale, Nullable: true},
	}}
}

// dfsWriteFile writes one row holding `unscaled` under the given declaration.
func dfsWriteFile(t *testing.T, s Schema, id, unscaled int64, withNull bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, s, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rows := []map[string]any{{"id": id, "a": Decimal128{Lo: uint64(unscaled)}}}
	if withNull {
		rows = append(rows, map[string]any{"id": id + 1, "a": nil})
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

// TestSchemaAsAdoptsTheCatalogsDecimalDeclaration is the retypeFromCatalog half
// of #707: the read schema a caller gets back must be the CATALOG's (p, s), or
// the stage that writes a .wshf header from it declares the file's scale and
// two files of one table describe two relations (ADR-0010).
func TestSchemaAsAdoptsTheCatalogsDecimalDeclaration(t *testing.T) {
	data := dfsWriteFile(t, dfsFileSchema(4), 1, 127500, false)
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	cols, err := r.SchemaAs(dfsCatalogSchema().Columns)
	if err != nil {
		t.Fatalf("SchemaAs: %v", err)
	}
	var got Column
	for _, c := range cols {
		if c.Name == "a" {
			got = c
		}
	}
	if got.Type != TypeDecimal || got.Precision != 15 || got.Scale != 2 {
		t.Errorf("SchemaAs gave DECIMAL(%d,%d), want the CATALOG's (15,2) — a file's own "+
			"declaration is input, not fact (#707, ADR-0018)", got.Precision, got.Scale)
	}
	// Without a catalog the file still describes itself: reading a file on its
	// own terms is unchanged, which is the boundary from the other side.
	own := r.Schema()
	for _, c := range own.Columns {
		if c.Name == "a" && c.Scale != 4 {
			t.Errorf("the file's OWN schema reports scale %d, want 4", c.Scale)
		}
	}
}

// TestRowReaderMovesADecimalToTheCatalogsScale is the DECODE half: the values
// the row path hands back must mean what the catalog says they mean.
//
// The carriers are chosen so the two rules are distinguishable in both
// directions: 127500 at scale 4 is 12.75, and reading it under scale 2 without
// moving it is 1275.00.
func TestRowReaderMovesADecimalToTheCatalogsScale(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fileScale int
		unscaled  int64
		want      string // the catalog-scale carrier, rendered
	}{
		{"file finer than the catalog", 4, 127500, "1275"},
		{"file coarser than the catalog", 0, 12, "1200"},
		{"file agrees", 2, 1275, "1275"},
		{"finer, rounds half away from zero", 4, 127550, "1276"},
		{"finer, rounds down", 4, 127549, "1275"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := dfsWriteFile(t, dfsFileSchema(tc.fileScale), 1, tc.unscaled, true)
			r, err := NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			rows, err := r.ReadRowsAs(dfsCatalogSchema().Columns, nil)
			if err != nil {
				t.Fatalf("ReadRowsAs: %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("%d rows, want 2", len(rows))
			}
			d, ok := decBoxCarrier(rows[0]["a"])
			if !ok {
				t.Fatalf("row 0 column a is %#v, want a DECIMAL box", rows[0]["a"])
			}
			if d.String() != tc.want {
				t.Errorf("carrier %s at the catalog's scale 2, want %s (the file declares "+
					"scale %d and holds %d) — #707", d.String(), tc.want, tc.fileScale, tc.unscaled)
			}
			// The NULL row stays NULL: a rescale must not invent a value.
			if rows[1]["a"] != nil {
				t.Errorf("the NULL row came back as %#v", rows[1]["a"])
			}
		})
	}
}

// TestRowReaderRefusesADecimalWithNoCarrierAtTheCatalogsScale: loud beats
// plausible. A file whose value has no representation at the declared scale is
// PostgreSQL's 22003, never a wrapped or truncated number.
func TestRowReaderRefusesADecimalWithNoCarrierAtTheCatalogsScale(t *testing.T) {
	// A DECIMAL(9,2) catalog column over a file that declares (18,6) and holds
	// 123456789012.345678 — 12345678901234.57 at scale 2, far past 10^7.
	cat := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "a", Type: TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	file := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "a", Type: TypeDecimal, Precision: 18, Scale: 6, Nullable: true},
	}}
	data := dfsWriteFile(t, file, 1, 123456789012345678, false)
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if _, err := r.ReadRowsAs(cat.Columns, nil); err == nil {
		t.Fatal("a value with no carrier at the catalog's scale was read as a number")
	} else if s := errState(err); s != "22003" {
		t.Errorf("SQLSTATE %s, want 22003: %v", s, err)
	}
}

func errState(err error) string {
	var c interface{ SQLState() string }
	for e := err; e != nil; {
		if v, ok := e.(interface{ SQLState() string }); ok {
			c = v
			break
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			break
		}
		e = u.Unwrap()
	}
	if c == nil {
		return ""
	}
	return c.SQLState()
}

// TestReconcileRowGroupStatsMovesTheBounds: the footer's DECIMAL bounds are
// carriers at the FILE's scale, and the predicate arrives at the catalog's, so
// leaving them alone prunes away a row group that holds a matching row (#707).
func TestReconcileRowGroupStatsMovesTheBounds(t *testing.T) {
	data := dfsWriteFile(t, dfsFileSchema(4), 1, 127500, false)
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	raw := fr.RowGroupStats(0)
	if got, ok := raw.Columns["a"].MinValue.(int64); !ok || got != 127500 {
		t.Fatalf("the file's own bound is %#v, want the carrier 127500", raw.Columns["a"].MinValue)
	}
	out := ReconcileRowGroupStats(fr, dfsCatalogSchema().Columns, raw)
	if got, ok := out.Columns["a"].MinValue.(int64); !ok || got != 1275 {
		t.Errorf("reconciled MIN = %#v, want 1275 (the same number at the catalog's scale)",
			out.Columns["a"].MinValue)
	}
	if got, ok := out.Columns["a"].MaxValue.(int64); !ok || got != 1275 {
		t.Errorf("reconciled MAX = %#v, want 1275", out.Columns["a"].MaxValue)
	}
	// The input is not mutated: two readers share one stats object.
	if got, _ := raw.Columns["a"].MinValue.(int64); got != 127500 {
		t.Errorf("ReconcileRowGroupStats mutated its input (MIN is now %d)", got)
	}
	// A file that AGREES is returned untouched, allocation included — the
	// boundary from the other side.
	agreeing := dfsWriteFile(t, dfsFileSchema(2), 1, 1275, false)
	fr2, err := OpenFileReaderFromBytes(agreeing)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	in := fr2.RowGroupStats(0)
	same := ReconcileRowGroupStats(fr2, dfsCatalogSchema().Columns, in)
	if len(same.Columns) != len(in.Columns) {
		t.Fatalf("column count moved")
	}
	if got, _ := same.Columns["a"].MinValue.(int64); got != 1275 {
		t.Errorf("an agreeing file's bound moved to %d", got)
	}
}

// decBoxCarrier reads either of the row path's DECIMAL boxes.
// decodeDecimalValues chooses between them by the column's declared
// precision: an int64 to 18 digits, a Decimal128 beyond (#419).
func decBoxCarrier(v any) (Decimal128, bool) {
	switch tv := v.(type) {
	case int64:
		return Decimal128From(tv), true
	case Decimal128:
		return tv, true
	}
	return Decimal128{}, false
}

// #707 ONE LEVEL DOWN, at the unit: a DECIMAL leaf inside ROW / ARRAY / MAP,
// and inside a container nested in a container.
//
// Round 0's review found the reconciliation stopped at the top level, so a
// nested leaf declared at scale 4 under a catalog scale of 2 answered 127500
// where the flat column beside it answered 1275. The declaration is half of
// every DECIMAL value at every depth, so the rule cannot be a top-level one.
func TestNestedDecimalLeavesMoveToTheCatalogsScale(t *testing.T) {
	dec := func(scale int, name string) Column {
		return Column{Name: name, Type: TypeDecimal, Precision: 15, Scale: scale, Nullable: true}
	}
	schemaAt := func(scale int) Schema {
		elem := dec(scale, "element")
		mval := dec(scale, "value")
		innerElem := dec(scale, "element")
		return Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			dec(scale, "flat"),
			{Name: "rw", Type: TypeRow, Nullable: true, Fields: []Column{dec(scale, "v")}},
			{Name: "ar", Type: TypeArray, Nullable: true, ElementType: &elem},
			{Name: "mp", Type: TypeMap, Nullable: true, ElementType: &Column{
				Name: "entry", Type: TypeRow, Fields: []Column{
					{Name: "key", Type: TypeString}, mval,
				}}},
			{Name: "deep", Type: TypeRow, Nullable: true, Fields: []Column{
				{Name: "l", Type: TypeArray, Nullable: true, ElementType: &innerElem},
			}},
		}}
	}
	box := Decimal128{Lo: 127500}
	var buf bytes.Buffer
	w, err := NewWriter(&buf, schemaAt(4), DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{
		"id": int64(1), "flat": box,
		"rw":   map[string]any{"v": box},
		"ar":   []any{box},
		"mp":   map[string]any{"k": box},
		"deep": map[string]any{"l": []any{box}},
	}, {
		"id": int64(2), "flat": nil,
		"rw": map[string]any{"v": nil}, "ar": []any{}, "mp": map[string]any{},
		"deep": nil,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRowsAs(schemaAt(2).Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowsAs: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	at := func(path string, v any) {
		t.Helper()
		d, ok := decBoxCarrier(v)
		if !ok {
			t.Fatalf("%s is %#v, want a DECIMAL box", path, v)
		}
		if d.String() != "1275" {
			t.Errorf("%s carrier = %s, want 1275 — the file declares this leaf at "+
				"scale 4 and the catalog at scale 2; both mean 12.75 (#707 one level down)",
				path, d.String())
		}
	}
	at("flat", rows[0]["flat"])
	at("rw.v", ndsField(rows[0]["rw"], "v"))
	at("ar[0]", ndsElem(rows[0]["ar"], 0))
	at("mp[k]", ndsKey(rows[0]["mp"], "k"))
	at("deep.l[0]", ndsElem(ndsField(rows[0]["deep"], "l"), 0))
	if rows[1]["flat"] != nil {
		t.Errorf("the NULL flat cell came back as %#v", rows[1]["flat"])
	}
	if v := ndsField(rows[1]["rw"], "v"); v != nil {
		t.Errorf("the NULL nested cell came back as %#v", v)
	}
}
