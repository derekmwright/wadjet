// This file holds expr map fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- MAP function implementations ---

// toMap extracts key-value pairs from a MAP value.
// MAPs are stored as []any where each element is map[string]any{"key":k, "value":v}
// or as map[string]any directly.
// elementAtExpr evaluates element_at(x, k) / the x[k] subscript, choosing MAP
// key lookup vs. ARRAY positional index from the COMPILED type of x rather than
// the runtime value's shape. A MAP column materializes as an
// ARRAY(ROW("key","value")) []any and map_entries() returns that identical
// shape, so a value-only heuristic misroutes one of them; the static type does
// not (#607).
type elementAtExpr struct {
	arg0, arg1 Expr
	// resolved publishes isMap and decimalKey, decided on the first Eval
	// because a ColRef needs the batch to know its column's declared type.
	// Set last under mu.
	resolved atomic.Bool
	mu       sync.Mutex
	isMap    bool
	// decimalKey is true for a MAP whose KEY is declared DECIMAL. Such a key
	// is stored at the key column's scale and boxes as that text, so
	// `element_at(mk, 12.75)` looked up "12.75" against a key rendered
	// "12.7500" at (18,4) and matched nothing on every row (#669 item 2).
	decimalKey bool
}

func (e *elementAtExpr) Eval(b *batch.RecordBatch, row int) any {
	container := e.arg0.Eval(b, row)
	key := e.arg1.Eval(b, row)
	if container == nil || key == nil {
		return nil
	}
	if !e.resolved.Load() {
		e.resolveDispatch(b)
	}
	if e.isMap {
		return mapElementAt(container, key, e.decimalKey)
	}
	return fnElementAt([]any{container, key})
}

func (e *elementAtExpr) resolveDispatch(b *batch.RecordBatch) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resolved.Load() {
		return
	}
	e.isMap = staticallyMap(e.arg0, b)
	if k := containerKeyVector(containerVector(e.arg0, b)); k != nil {
		e.decimalKey = k.Type == batch.TypeDecimal
	}
	e.resolved.Store(true)
}

// staticallyMap reports whether e is known from its COMPILED/declared type to
// evaluate to a MAP — never from a runtime value's shape, which a MAP shares
// with an ARRAY of {key,value} rows. b supplies a ColRef its column's declared
// type. A FuncCall is a MAP when its registered return type fixes it to one
// (map_from_entries is RetMap; map_entries is RetArray, so map_entries(m)[i]
// correctly indexes). Anything else is treated as an ARRAY.
func staticallyMap(e Expr, b *batch.RecordBatch) bool {
	if a, ok := e.(*FuncCall); ok {
		t, c := DefaultRegistry.ReturnType(a.Name).Resolve(len(a.Args), nil)
		if c != Undecided && t.ID == batch.TypeMap {
			return true
		}
	}
	// Any container expression whose declared shape this batch can resolve —
	// a column, a nested element_at, a COALESCE/CASE/GREATEST over them
	// (#635). A producer that declares no container shape stays an ARRAY
	// here, which is what it was before.
	v := containerVector(e, b)
	return v != nil && v.Type == batch.TypeMap
}

// mapElementAt looks key up in a MAP value in either materialized shape: a Go
// map (map_from_entries, constructed literals) or the ARRAY(ROW("key","value"))
// entry rows a MAP column produces through batch.Vector.GetValue. Keys compare
// by string form, covering the MAP's string and integer key types. A duplicate
// key returns the LAST match, matching toMap (the shape map_keys/map_values and
// a MAP GROUP BY key go through, which last-write-wins into a Go map). An
// absent key returns NULL.
// decimalKey spells both sides through batch.CanonicalDecimalText, the
// minimal-scale form AppendDecimalKey already uses for a stored DECIMAL, so
// `element_at(mk, 12.75)` finds the key a DECIMAL(18,4) column renders
// "12.7500" — two spellings of one number are one key (ADR-0012 item 8). It is
// set only when the map's KEY column is declared DECIMAL: a STRING-keyed map
// must keep matching by its bytes, where "1.50" and "1.5" are two keys.
func mapElementAt(m, key any, decimalKey bool) any {
	norm := func(s string) string {
		if !decimalKey {
			return s
		}
		if c, ok := batch.CanonicalDecimalText(s); ok {
			return c
		}
		return s
	}
	want := norm(fmt.Sprint(key))
	if gm, ok := m.(map[string]any); ok {
		if !decimalKey {
			return gm[want]
		}
		for k, v := range gm {
			if norm(k) == want {
				return v
			}
		}
		return nil
	}
	entries, ok := toSlice(m)
	if !ok {
		return nil
	}
	var val any
	found := false
	for _, entry := range entries {
		row, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if norm(fmt.Sprint(row["key"])) == want {
			val, found = row["value"], true
		}
	}
	if !found {
		return nil
	}
	return val
}

func toMap(v any) (map[string]any, bool) {
	switch tv := v.(type) {
	case map[string]any:
		return tv, true
	case []any:
		// ARRAY(ROW("key","value")) representation
		m := make(map[string]any, len(tv))
		for _, entry := range tv {
			if row, ok := entry.(map[string]any); ok {
				key := fmt.Sprint(row["key"])
				m[key] = row["value"]
			}
		}
		return m, true
	case []map[string]any:
		m := make(map[string]any, len(tv))
		for _, row := range tv {
			key := fmt.Sprint(row["key"])
			m[key] = row["value"]
		}
		return m, true
	default:
		return nil, false
	}
}

// map_keys(map) — returns the keys as an array
func fnMapKeys(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	keys := make([]any, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// map_values(map) — returns the values as an array
func fnMapValues(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	vals := make([]any, 0, len(m))
	for _, v := range m {
		vals = append(vals, v)
	}
	return vals
}

// map_entries(map) — returns array of ROW(key, value) entries
func fnMapEntries(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	entries := make([]any, 0, len(m))
	for k, v := range m {
		entries = append(entries, map[string]any{"key": k, "value": v})
	}
	return entries
}

// map_from_entries(array_of_rows) — constructs a map from ROW(key, value) entries
func fnMapFromEntries(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	m := make(map[string]any, len(arr))
	for _, entry := range arr {
		if row, ok := entry.(map[string]any); ok {
			key := fmt.Sprint(row["key"])
			m[key] = row["value"]
		}
	}
	return m
}

// array_max(array) — returns the maximum element
func fnArrayMax(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok || len(arr) == 0 {
		return nil
	}
	max := arr[0]
	for _, elem := range arr[1:] {
		if elem == nil {
			continue
		}
		if max == nil || fmt.Sprint(elem) > fmt.Sprint(max) {
			max = elem
		}
	}
	return max
}
