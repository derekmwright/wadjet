package exec

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Raw-row aggregate spill must losslessly encode ARRAY/MAP/ROW/VECTOR boxes
// before SpillRows and decode after ReadSpilledRows (#611, #361).
// Reuse appendContainerKeyValue/decodeContainerKeyValue from partial-state
// spill (#566; ADR-0023), not display formatting or a memory-layer container codec.
// Store arbitrary codec bytes as strings through SpillRows' length-prefixed
// string tag; emit through the same batch.FromRows as the unspilled path,
// reconstructing exactly the original value (ADR-0023).
// See docs/internals/raw-row-spill-container-values.md for the design.

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
