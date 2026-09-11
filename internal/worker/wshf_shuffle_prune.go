package worker

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// wshfShufflePruneKeep intersects Task.Columns with the first decoded WSHF
// schema, unlike parquet's read-time projectColumns. Always keep shuffle keys.
// Match qualifier-tolerantly in both directions and add schema-resolution
// matches for folded plan names (#731); expressions may only over-keep.
// Missing declared union members are expected. Return nil for no-op or total
// mismatch, preserving full width rather than dropping everything.
// The camel-case invariance corpus does not isolate this site because scan
// projection reaches its columns first; passing it alone is insufficient proof.
// See docs/internals/worker-wshf-shuffle-projection.md for the design.
func wshfShufflePruneKeep(schema []parquet.Column, declared, keys []string) []string {
	if len(declared) == 0 {
		return nil
	}
	want := make(map[string]bool, (len(declared)+len(keys))*2)
	add := func(name string) {
		want[name] = true
		if base, ok := qualifiedBase(name); ok {
			want[base] = true
		}
	}
	for _, c := range declared {
		add(c)
	}
	for _, k := range keys {
		add(k)
	}
	kept := make([]bool, len(schema))
	for i, col := range schema {
		if want[col.Name] {
			kept[i] = true
			continue
		}
		if base, ok := qualifiedBase(col.Name); ok && want[base] {
			kept[i] = true
		}
	}
	for name := range want {
		if i := batch.ResolveSchemaIndex(schema, name); i >= 0 {
			kept[i] = true
		}
	}
	keep := make([]string, 0, len(schema))
	for i, col := range schema {
		if kept[i] {
			keep = append(keep, col.Name)
		}
	}
	if len(keep) == 0 || len(keep) == len(schema) {
		return nil
	}
	return keep
}

// qualifiedBase returns the column name with its "alias." qualifier
// stripped, or ok=false when the name isn't qualifier-shaped. Names
// containing parens or spaces are expression outputs (e.g.
// "sum(x * 0.5)") whose dots are not qualifiers.
func qualifiedBase(name string) (string, bool) {
	if strings.ContainsAny(name, "( ") {
		return "", false
	}
	i := strings.LastIndexByte(name, '.')
	if i < 0 || i == len(name)-1 {
		return "", false
	}
	return name[i+1:], true
}
