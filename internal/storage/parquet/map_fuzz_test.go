package parquet

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"
)

// FuzzMapRoundTrip drives the MAP writer and reader against each other over
// generated shapes. Every defect #393 closed was a level bookkeeping error —
// a value written at a level the footer does not describe, an entry counted
// that carries no value, an empty map indistinguishable from a null one —
// and all of them were invisible to a test that only checked a row count.
// The property here is the one that matters: what comes back is what went
// in, for every combination of nullability, value type and entry shape, and
// no input reaches a panic.
//
// The corpus seeds the six shapes that broke: a single entry, several
// entries, NULL, empty, an entry whose VALUE is null, and a zero-length key.
//
// valueType selects the MAP's VALUE column, and it now runs past the four
// leaf types it started with into the three CONTAINER shapes #409 was about:
// a value that is itself an ARRAY, a ROW or a MAP. Those are the shapes the
// old fixed-depth leaf lookup could not resolve at all — a MAP of ARRAY or of
// ROW read back as an absent column, with no error — and they are also where
// the writer's repetition-level arithmetic is load-bearing, because a
// container inside the FIRST entry of a map is the one place where the level
// to stamp and the level's own depth differ.
func FuzzMapRoundTrip(f *testing.F) {
	// shape is a bitmask over per-row shapes; nul/val select the schema.
	for _, shape := range []uint32{
		0x00000000, // all rows empty-ish
		0x01234567,
		0x89abcdef,
		0xffffffff,
		0x5a5a5a5a,
		0xdeadbeef,
	} {
		for valueType := uint8(0); valueType < uint8(len(mapValueShapes)); valueType++ {
			f.Add(shape, valueType, true)
			f.Add(shape, valueType, false)
		}
	}

	f.Fuzz(func(t *testing.T, shape uint32, valueType uint8, mapNullable bool) {
		sel := int(valueType) % len(mapValueShapes)
		schema := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "m", Type: TypeMap, Nullable: mapNullable, ElementType: &Column{
				Name: "entry", Type: TypeRow, Fields: []Column{
					{Name: "key", Type: TypeString},
					mapValueShapes[sel].col(),
				},
			}},
		}}

		value := func(n int) any { return mapValueShapes[sel].value(n) }

		// Eight rows, each shaped by four bits of the seed.
		rows := make([]map[string]any, 0, 8)
		for i := 0; i < 8; i++ {
			bits := (shape >> (uint(i) * 4)) & 0xf
			row := map[string]any{"id": int64(i)}
			switch {
			case bits == 0:
				row["m"] = map[string]any{} // empty
			case bits == 1 && mapNullable:
				row["m"] = nil // NULL — only legal in a nullable column
			case bits == 1:
				row["m"] = map[string]any{"": value(0)} // zero-length key
			default:
				m := make(map[string]any, bits)
				for k := 0; k < int(bits); k++ {
					key := fmt.Sprintf("k%d", k)
					if k == int(bits)-1 && bits%3 == 0 {
						m[key] = nil // entry present, value NULL
						continue
					}
					m[key] = value(i*16 + k)
				}
				row["m"] = m
			}
			rows = append(rows, row)
		}

		var buf bytes.Buffer
		pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		raw := buf.Bytes()
		r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
		got, err := r.ReadRows(nil)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(got) != len(rows) {
			t.Fatalf("read %d rows, wrote %d", len(got), len(rows))
		}
		for i, want := range rows {
			if id, _ := got[i]["id"].(int64); id != want["id"].(int64) {
				t.Fatalf("row %d: id = %v, want %v", i, got[i]["id"], want["id"])
			}
			if want["m"] == nil {
				if v, ok := got[i]["m"]; ok && v != nil {
					t.Fatalf("row %d: NULL map read back as %#v", i, v)
				}
				continue
			}
			wantMap := want["m"].(map[string]any)
			gotMap, ok := got[i]["m"].(map[string]any)
			if !ok {
				t.Fatalf("row %d: map read back as %#v (%T), want %#v", i, got[i]["m"], got[i]["m"], wantMap)
			}
			if len(gotMap) != len(wantMap) {
				t.Fatalf("row %d: %d entries read back, want %d (%#v vs %#v)",
					i, len(gotMap), len(wantMap), gotMap, wantMap)
			}
			for k, wv := range wantMap {
				gv, present := gotMap[k]
				if !present {
					t.Fatalf("row %d: key %q missing from %#v", i, k, gotMap)
				}
				if wv == nil {
					if gv != nil {
						t.Fatalf("row %d key %q: NULL value read back as %#v", i, k, gv)
					}
					continue
				}
				if f, isFloat := wv.(float64); isFloat {
					gf, ok := gv.(float64)
					if !ok || math.Abs(gf-f) > 0 {
						t.Fatalf("row %d key %q: %#v, want %#v", i, k, gv, wv)
					}
					continue
				}
				// A container value compares structurally: DeepEqual over
				// []any / map[string]any IS the round-trip property.
				if !reflect.DeepEqual(gv, wv) {
					t.Fatalf("row %d key %q: %#v (%T), want %#v (%T)", i, k, gv, gv, wv, wv)
				}
			}
		}
	})
}

// mapValueShapes is the MAP value column the fuzzer varies over: four leaf
// types and three containers. Each entry pairs the schema Column with a
// generator whose output round-trips EXACTLY, so the comparison can be
// structural equality rather than a tolerance.
var mapValueShapes = []struct {
	col   func() Column
	value func(n int) any
}{
	{func() Column { return Column{Name: "value", Type: TypeInt64, Nullable: true} },
		func(n int) any { return int64(n) }},
	{func() Column { return Column{Name: "value", Type: TypeString, Nullable: true} },
		func(n int) any { return fmt.Sprintf("v%d", n) }},
	{func() Column { return Column{Name: "value", Type: TypeFloat64, Nullable: true} },
		func(n int) any { return float64(n) / 4 }},
	{func() Column { return Column{Name: "value", Type: TypeBool, Nullable: true} },
		func(n int) any { return n%2 == 0 }},

	// MAP(STRING, ARRAY(INT64)) — one of the two shapes that read back as an
	// absent column. Length cycles through 0 (an empty array is not a NULL
	// one), 1 and 3, and the middle element of a 3 is NULL.
	{func() Column {
		return Column{Name: "value", Type: TypeArray, Nullable: true,
			ElementType: &Column{Name: "element", Type: TypeInt64, Nullable: true}}
	}, func(n int) any {
		switch n % 3 {
		case 0:
			return []any{}
		case 1:
			return []any{int64(n)}
		default:
			return []any{int64(n), nil, int64(n + 1)}
		}
	}},

	// MAP(STRING, ROW(x INT64, y STRING)) — the other. A NULL field is
	// omitted from the assembled struct, so the generator keeps both fields
	// present and the comparison stays exact.
	{func() Column {
		return Column{Name: "value", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "x", Type: TypeInt64, Nullable: true},
			{Name: "y", Type: TypeString, Nullable: true},
		}}
	}, func(n int) any {
		return map[string]any{"x": int64(n), "y": fmt.Sprintf("y%d", n)}
	}},

	// MAP(STRING, MAP(STRING, INT64)) — two repeated groups deep, which is
	// where the writer's repetition depth and the level it stamps part ways.
	{func() Column {
		return Column{Name: "value", Type: TypeMap, Nullable: true, ElementType: &Column{
			Name: "entry", Type: TypeRow, Fields: []Column{
				{Name: "key", Type: TypeString},
				{Name: "value", Type: TypeInt64, Nullable: true},
			},
		}}
	}, func(n int) any {
		switch n % 3 {
		case 0:
			return map[string]any{}
		case 1:
			return map[string]any{"i": int64(n)}
		default:
			return map[string]any{"i": int64(n), "j": nil, "k": int64(n + 1)}
		}
	}},
}
