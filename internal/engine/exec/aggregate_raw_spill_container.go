package exec

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The legacy raw-row aggregate spill (memory.SpillManager.SpillRows) writes
// one boxed value per column and has typed arms only for bool/int/float/
// string. A container box — []any (ARRAY, and a MAP as its list of entry
// ROWs), map[string]any (ROW) or []float32 (VECTOR) — falls to its default
// arm, which renders it with fmt.Sprintf and stores the DISPLAY text. On the
// way back that text is a string, and batch.FromRows refuses to write a
// string into a container vector (#361's silent-write guard), so a GROUP BY
// over a container column plus any non-simple aggregate — the shapes
// canUseExternalMerge returns false for — failed outright the moment the
// spill buffer flushed to disk (#611).
//
// #566/ADR-0023 already gave the PARTIAL-STATE drain a lossless container
// VALUE codec (appendContainerKeyValue / decodeContainerKeyValue); this is
// its sibling site. Rather than teach the memory layer about containers (it
// imports neither batch nor this package), the raw-row path encodes a
// container box to that codec's bytes BEFORE handing rows to SpillRows and
// decodes it back AFTER ReadSpilledRows, so the box the row carried is
// reconstructed EXACTLY — the same producer, one definition of a container
// value. The encoded bytes ride as a string through SpillRows' existing
// length-prefixed string tag, which round-trips arbitrary bytes; the emit
// then reconstructs the value through the identical batch.FromRows the
// un-spilled buffered drain uses, so spilled equals in-memory by
// construction, exactly as ADR-0023 requires.

// isContainerColumn reports whether a column's declared type boxes as one of
// the container shapes the raw-row spill cannot store directly.
func isContainerColumn(c *parquet.Column) bool {
	switch c.Type {
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
		return true
	}
	return false
}

// containerColNames returns the names of schema's container columns, or nil
// when there are none — the common case, for which the encode/decode passes
// below are a single length check and return.
func containerColNames(schema []parquet.Column) []string {
	var names []string
	for i := range schema {
		if isContainerColumn(&schema[i]) {
			names = append(names, schema[i].Name)
		}
	}
	return names
}

// encodeContainerColsForSpill rewrites, in place, every non-nil value in a
// container column of schema to its lossless codec bytes (as a string). The
// caller passes rows it is about to discard (the flushed spill buffer), so an
// in-place rewrite is safe; a NULL stays nil and rides SpillRows' null tag.
func encodeContainerColsForSpill(rows []map[string]any, schema []parquet.Column) {
	cols := containerColNames(schema)
	if len(cols) == 0 {
		return
	}
	for _, row := range rows {
		for _, name := range cols {
			v, ok := row[name]
			if !ok || v == nil {
				continue
			}
			row[name] = string(appendContainerKeyValue(nil, v, 0))
		}
	}
}

// decodeContainerColsFromSpill reverses encodeContainerColsForSpill: every
// non-nil string in a container column is decoded back to the box GetValue
// produced, in place. A nil (a NULL member) is left untouched; a non-string
// value in a container column is left untouched as well, so a schema that a
// caller did not encode through the pass above is a no-op rather than an
// error.
func decodeContainerColsFromSpill(rows []map[string]any, schema []parquet.Column) error {
	cols := containerColNames(schema)
	if len(cols) == 0 {
		return nil
	}
	for _, row := range rows {
		for _, name := range cols {
			v, ok := row[name]
			if !ok || v == nil {
				continue
			}
			s, ok := v.(string)
			if !ok {
				continue
			}
			box, err := decodeContainerKeyValue([]byte(s))
			if err != nil {
				return fmt.Errorf("decoding spilled container column %q: %w", name, err)
			}
			row[name] = box
		}
	}
	return nil
}
