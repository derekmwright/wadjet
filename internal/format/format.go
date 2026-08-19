// Package format provides output formatting for query results.
package format

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Format identifies an output format.
type Format int

const (
	Table Format = iota
	JSON
	CSV
)

// ParseFormat parses a format name string.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "table":
		return Table, nil
	case "json":
		return JSON, nil
	case "csv":
		return CSV, nil
	default:
		return 0, fmt.Errorf("unknown format %q (supported: table, json, csv)", s)
	}
}

// Write formats columns and rows to the writer in the given format, with no
// column type information. Values render as the engine boxes them, so a
// TIMESTAMP column prints as its raw epoch integer; prefer WriteTyped
// wherever the result's column metadata is on hand.
func Write(w io.Writer, f Format, columns []string, rows []map[string]any) error {
	return WriteTyped(w, f, columns, nil, rows)
}

// WriteTyped is Write with the declared type of each column, positionally
// aligned with columns (a short or nil slice just means "unknown" for the
// remainder).
//
// The engine boxes a TIMESTAMP as epoch milliseconds because every compute
// path that shares that boxing reads it as a number; only a renderer holding
// the column's declared type can turn it back into an instant, so the type
// has to reach this far (#321).
func WriteTyped(w io.Writer, f Format, columns []string, types []parquet.TypeID, rows []map[string]any) error {
	if rendered := renderTemporal(columns, types, rows); rendered != nil {
		rows = rendered
	}
	switch f {
	case Table:
		return writeTable(w, columns, rows)
	case JSON:
		return writeJSON(w, columns, rows)
	case CSV:
		return writeCSV(w, columns, rows)
	default:
		return writeTable(w, columns, rows)
	}
}

// renderTemporal returns a copy of rows with every TIMESTAMP column replaced
// by its rendered form, or nil when there is nothing to render.
//
// It copies rather than mutating in place because the caller's rows may be
// the query result the program goes on to use for something other than
// display; formatting is this package's business, not a side effect it
// should impose on the result. Only the maps are rebuilt, and only when a
// timestamp column is actually present.
func renderTemporal(columns []string, types []parquet.TypeID, rows []map[string]any) []map[string]any {
	var tsCols []string
	for i, col := range columns {
		if i < len(types) && types[i] == parquet.TypeTimestamp {
			tsCols = append(tsCols, col)
		}
	}
	if len(tsCols) == 0 {
		return nil
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		cp := make(map[string]any, len(row))
		for k, v := range row {
			cp[k] = v
		}
		for _, col := range tsCols {
			if ms, ok := cp[col].(int64); ok {
				cp[col] = batch.FormatTimestamp(ms)
			}
		}
		out[i] = cp
	}
	return out
}

const maxColWidth = 40

func writeTable(w io.Writer, columns []string, rows []map[string]any) error {
	if len(columns) == 0 {
		fmt.Fprintln(w, "(0 rows)")
		return nil
	}

	// Compute column widths
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}
	for _, row := range rows {
		for i, col := range columns {
			val := formatValue(row[col])
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}
	// Cap widths
	for i := range widths {
		if widths[i] > maxColWidth {
			widths[i] = maxColWidth
		}
	}

	// Header
	writeSep(w, widths)
	writeRow(w, columns, widths)
	writeSep(w, widths)

	// Data
	for _, row := range rows {
		vals := make([]string, len(columns))
		for i, col := range columns {
			vals[i] = formatValue(row[col])
		}
		writeRow(w, vals, widths)
	}

	writeSep(w, widths)
	fmt.Fprintf(w, "(%d rows)\n", len(rows))
	return nil
}

func writeSep(w io.Writer, widths []int) {
	fmt.Fprint(w, "+")
	for _, width := range widths {
		fmt.Fprint(w, strings.Repeat("-", width+2))
		fmt.Fprint(w, "+")
	}
	fmt.Fprintln(w)
}

func writeRow(w io.Writer, vals []string, widths []int) {
	fmt.Fprint(w, "|")
	for i, val := range vals {
		display := val
		if len(display) > widths[i] {
			display = display[:widths[i]-3] + "..."
		}
		fmt.Fprintf(w, " %-*s |", widths[i], display)
	}
	fmt.Fprintln(w)
}

func writeJSON(w io.Writer, _ []string, rows []map[string]any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if len(rows) == 0 {
		return enc.Encode([]any{})
	}
	return enc.Encode(rows)
}

func writeCSV(w io.Writer, columns []string, rows []map[string]any) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	if err := cw.Write(columns); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, len(columns))
		for i, col := range columns {
			record[i] = formatValue(row[col])
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}
