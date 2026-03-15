// Package format provides output formatting for query results.
package format

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
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

// Write formats columns and rows to the writer in the given format.
func Write(w io.Writer, f Format, columns []string, rows []map[string]any) error {
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
