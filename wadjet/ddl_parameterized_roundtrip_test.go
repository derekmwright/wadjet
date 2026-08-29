package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A table DECLARED THROUGH SQL DDL must be writable and readable for every
// parameterized type, with the parameters the declaration carried.
//
// The declaration is the whole point. `CREATE TABLE t (v VECTOR(384))` created
// a column with `Dimension: 0` — every INSERT into it failed at flush with
// "a VECTOR needs a positive Dimension, got 0", an internal error with no
// SQLSTATE — and ARRAY, ROW and MAP lost their element, field and key/value
// declarations the same way (#675). Every gate that exercised these types
// built its schema PROGRAMMATICALLY (the type-matrix fixture, the nested DML
// tests, the compaction matrix), so all of them were blind to the door.
//
// The VALUES are written programmatically, because the INSERT VALUES grammar
// takes one literal token per value and has no composite literal for a
// container. What is under test here is the DDL door, not that grammar.
func TestSQLDeclaredParameterizedTypesRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		decl  string
		value any
		check func(t *testing.T, got any)
	}{
		{
			name:  "VECTOR(4)",
			decl:  "VECTOR(4)",
			value: []float32{1.5, -2, 0.25, 4},
			check: func(t *testing.T, got any) {
				v, ok := got.([]float32)
				if !ok {
					t.Fatalf("boxed as %T (%v), want []float32", got, got)
				}
				want := []float32{1.5, -2, 0.25, 4}
				if len(v) != len(want) {
					t.Fatalf("dimension %d, want %d", len(v), len(want))
				}
				for i := range want {
					if v[i] != want[i] {
						t.Errorf("element %d = %v, want %v", i, v[i], want[i])
					}
				}
			},
		},
		{
			name:  "ARRAY(STRING)",
			decl:  "ARRAY(STRING)",
			value: []any{"a", "b"},
			check: func(t *testing.T, got any) {
				v, ok := got.([]any)
				if !ok || len(v) != 2 || fmt.Sprint(v[0]) != "a" || fmt.Sprint(v[1]) != "b" {
					t.Fatalf("read back %#v, want [a b]", got)
				}
			},
		},
		{
			// The element's (p, s), which is the silent half: an element
			// declaration resolved to Precision 0 would round 12.34 to 12.
			name:  "ARRAY(DECIMAL(9,2))",
			decl:  "ARRAY(DECIMAL(9,2))",
			value: []any{"12.34", "-0.05"},
			check: func(t *testing.T, got any) {
				v, ok := got.([]any)
				if !ok || len(v) != 2 {
					t.Fatalf("read back %#v, want two elements", got)
				}
				if fmt.Sprint(v[0]) != "12.34" || fmt.Sprint(v[1]) != "-0.05" {
					t.Errorf("read back [%v %v], want [12.34 -0.05] — the element's scale was lost",
						v[0], v[1])
				}
			},
		},
		{
			name:  "ROW(a INT64, d DECIMAL(9,2))",
			decl:  "ROW(a INT64, d DECIMAL(9,2))",
			value: map[string]any{"a": int64(7), "d": "12.34"},
			check: func(t *testing.T, got any) {
				m, ok := got.(map[string]any)
				if !ok {
					t.Fatalf("boxed as %T (%v), want map", got, got)
				}
				if fmt.Sprint(m["a"]) != "7" {
					t.Errorf("field a = %v, want 7", m["a"])
				}
				if fmt.Sprint(m["d"]) != "12.34" {
					t.Errorf("field d = %v, want 12.34 — the field's scale was lost", m["d"])
				}
			},
		},
		{
			name:  "MAP(STRING, DECIMAL(9,2))",
			decl:  "MAP(STRING, DECIMAL(9,2))",
			value: map[string]any{"k": "12.34"},
			check: func(t *testing.T, got any) {
				// A MAP reads back as its list of key/value entry rows on this
				// path, which is the pre-existing boxing and not what this
				// test is about; the VALUE's scale is.
				if v := mapEntryValue(t, got, "k"); fmt.Sprint(v) != "12.34" {
					t.Errorf("value at k = %v, want 12.34 — the MAP value's scale was lost", v)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			// The DDL door under test.
			if _, err := db.Query(ctx, "CREATE TABLE p (id INT64, c "+tc.decl+")"); err != nil {
				t.Fatalf("CREATE TABLE p (id INT64, c %s): %v", tc.decl, err)
			}
			meta, err := db.Catalog().GetTable(ctx, "p")
			if err != nil {
				t.Fatal(err)
			}

			ing := db.NewIngester("p", meta.Schema, nil, ingest.DefaultConfig())
			if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "c": tc.value}}); err != nil {
				t.Fatalf("Ingest into a %s column declared through SQL: %v", tc.decl, err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				// This is the shape #675 produced: an internal error at flush,
				// with no SQLSTATE, for a table SQL DDL happily created.
				t.Fatalf("FlushAll for a %s column declared through SQL: %v", tc.decl, err)
			}

			q, err := db.Query(ctx, "SELECT id, c FROM p")
			if err != nil {
				t.Fatalf("SELECT: %v", err)
			}
			if len(q.Rows) != 1 {
				t.Fatalf("%d rows, want 1", len(q.Rows))
			}
			tc.check(t, q.Rows[0]["c"])
		})
	}
}

// mapEntryValue reads one key's value out of whichever shape a MAP column
// comes back in — a Go map, or the list of {key, value} entry rows the
// columnar path boxes.
func mapEntryValue(t *testing.T, got any, key string) any {
	t.Helper()
	switch m := got.(type) {
	case map[string]any:
		return m[key]
	case []any:
		for _, e := range m {
			entry, ok := e.(map[string]any)
			if ok && fmt.Sprint(entry["key"]) == key {
				return entry["value"]
			}
		}
		t.Fatalf("MAP %v holds no entry for key %q", got, key)
	default:
		t.Fatalf("MAP boxed as %T (%v)", got, got)
	}
	return nil
}
