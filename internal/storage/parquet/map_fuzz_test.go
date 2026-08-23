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
		for valueType := uint8(0); valueType < 4; valueType++ {
			f.Add(shape, valueType, true)
			f.Add(shape, valueType, false)
		}
	}

	f.Fuzz(func(t *testing.T, shape uint32, valueType uint8, mapNullable bool) {
		typ := []TypeID{TypeInt64, TypeString, TypeFloat64, TypeBool}[int(valueType)%4]
		schema := mapSchemaWithValue(typ, mapNullable, true)

		value := func(n int) any {
			switch typ {
			case TypeString:
				return fmt.Sprintf("v%d", n)
			case TypeFloat64:
				return float64(n) / 4
			case TypeBool:
				return n%2 == 0
			default:
				return int64(n)
			}
		}

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
				if !reflect.DeepEqual(gv, wv) {
					t.Fatalf("row %d key %q: %#v (%T), want %#v (%T)", i, k, gv, gv, wv, wv)
				}
			}
		}
	})
}
