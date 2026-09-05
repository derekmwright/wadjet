package worker

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// wshfShufflePruneKeep resolves the post-decode keep set for a shuffle task
// reading WSHF stage outputs. Parquet inputs narrow at read time via
// projectColumns; WSHF payloads arrive full-width, so the declared
// projection (Task.Columns, set by dispatchShuffleStage from the
// repartition stage's plan-time Columns) is applied here against the first
// decoded batch's schema.
//
// Intersection semantics: declared unions include sibling-table names by
// planner convention ("the reader ignores columns that don't exist"), so a
// declared name absent from the schema is expected, and a schema column is
// dropped only when nothing declared matches it. Matching is
// qualifier-tolerant in both directions ("n1.n_name" matches a declared
// "n_name" and vice versa) — self-join outputs carry alias-qualified
// duplicates the plan may reference unqualified. Mis-parsing an
// expression-shaped name can only over-keep, never over-drop.
//
// Matching also RESOLVES rather than only byte-comparing. Task.Columns is a
// plan-side list, so a name in it can still be a column REFERENCE in the
// lexer's folded spelling (#731) while the decoded batch carries the
// catalog's own — `RegionID`, not `regionid`. Byte-exact, a mixed-case schema
// (`RegionID` beside an already-folded `tier`) is the dangerous case: the
// declared names that happen to match keep their columns and the rest are
// DROPPED, which is a narrower shuffle payload than the consumer's plan
// expects rather than the "over-declared, harmlessly ignored" case the
// intersection is written for. A TOTAL miss already fell back to full width;
// the partial miss did not. The resolve pass is ADDITIVE, so it can only keep
// a column the byte-exact pass dropped.
//
// The camel-case invariance battery does not yet distinguish this site:
// measured with every other fix in place and this one reverted, it owns 0 of
// its cells — the scan-side projection guard
// (cachedFileStreamSource.projectColumns) reaches the same columns first on
// every shape it has. The pass is the hazard closed, not a cell recovered.
//
// Shuffle keys are always kept (the partitioning sink hashes them).
// Returns nil when pruning would be a no-op (nothing dropped, or nothing
// would survive — the latter indicates a declaration/schema mismatch where
// full width is the safe fallback).
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
