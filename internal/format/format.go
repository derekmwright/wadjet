// SPDX-License-Identifier: MIT

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
func Write(w io.Writer, f Format, columns []string, rows [][]any) error {
	return WriteTyped(w, f, columns, nil, rows)
}

// WriteTyped is Write with the declared type of each column, positionally
// aligned with columns (a short or nil slice just means "unknown" for the
// remainder).
//
// A ROW IS VALUES BY POSITION, one cell per entry of columns, and every
// format here renders it that way. It used to be a map keyed by column NAME,
// which cannot represent a result carrying two output columns of one name —
// `SELECT abs(a), abs(b)` is two columns called `abs`, and a star over a
// `JOIN … USING` whose arms share a tail name publishes that name twice. The
// map held the LAST of them, so the table and CSV forms printed one value
// under both headings and JSON emitted a single key and dropped the other
// column outright, while psql against the same server showed both (#1218).
// wadjet.QueryResult.Cells is the positional form a caller reads.
//
// The engine boxes a TIMESTAMP as epoch milliseconds because every compute
// path that shares that boxing reads it as a number; only a renderer holding
// the column's declared type can turn it back into an instant, so the type
// has to reach this far (#321).
func WriteTyped(w io.Writer, f Format, columns []string, types []parquet.TypeID, rows [][]any) error {
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
// should impose on the result. Only the row slices are rebuilt, and only when
// a timestamp column is actually present.
func renderTemporal(columns []string, types []parquet.TypeID, rows [][]any) [][]any {
	var tsCols []int
	for i := range columns {
		if i < len(types) && types[i] == parquet.TypeTimestamp {
			tsCols = append(tsCols, i)
		}
	}
	if len(tsCols) == 0 {
		return nil
	}
	out := make([][]any, len(rows))
	for i, row := range rows {
		cp := make([]any, len(row))
		copy(cp, row)
		for _, j := range tsCols {
			if j >= len(cp) {
				continue
			}
			if ms, ok := cp[j].(int64); ok {
				cp[j] = batch.FormatTimestamp(ms)
			}
		}
		out[i] = cp
	}
	return out
}

// cell reads column i of a row, and answers nil — which renders as NULL —
// for a row shorter than the column list. A short row is the engine failing
// to fill a declared column, not a reason to panic in the renderer.
func cell(row []any, i int) any {
	if i >= len(row) {
		return nil
	}
	return row[i]
}

const maxColWidth = 40

func writeTable(w io.Writer, columns []string, rows [][]any) error {
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
		for i := range columns {
			val := formatValue(cell(row, i))
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
		for i := range columns {
			vals[i] = formatValue(cell(row, i))
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

// writeJSON emits one object per row with a key per COLUMN, in column order,
// duplicates included.
//
// The object is assembled by hand rather than handed to encoding/json,
// because a Go map cannot hold two entries of one key and this result can
// carry two columns of one name. Duplicate keys in a JSON object are legal
// (RFC 8259 §4 calls them merely "unique-SHOULD"), and PostgreSQL — the
// authority on what a client of this engine expects — emits them: over
// `SELECT 1 AS u, 2 AS u`, `row_to_json` answers `{"u":1,"u":2}` and
// `json_agg` answers `[{"u":1,"u":2}]`. Dropping one of them would be the
// renderer deciding a column the query asked for does not exist (#1218).
func writeJSON(w io.Writer, columns []string, rows [][]any) error {
	if len(rows) == 0 {
		_, err := io.WriteString(w, "[]\n")
		return err
	}
	var sb strings.Builder
	sb.WriteString("[\n")
	for i, row := range rows {
		sb.WriteString("  {\n")
		for j, col := range columns {
			key, err := json.Marshal(col)
			if err != nil {
				return fmt.Errorf("encoding column name %q: %w", col, err)
			}
			// Indented under the key the way encoding/json would have
			// indented it, so a container value still reads as one.
			val, err := json.MarshalIndent(cell(row, j), "    ", "  ")
			if err != nil {
				return fmt.Errorf("encoding column %q: %w", col, err)
			}
			sb.WriteString("    ")
			sb.Write(key)
			sb.WriteString(": ")
			sb.Write(val)
			if j < len(columns)-1 {
				sb.WriteString(",")
			}
			sb.WriteString("\n")
		}
		sb.WriteString("  }")
		if i < len(rows)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("]\n")
	_, err := io.WriteString(w, sb.String())
	return err
}

func writeCSV(w io.Writer, columns []string, rows [][]any) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	if err := cw.Write(columns); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, len(columns))
		for i := range columns {
			record[i] = formatValue(cell(row, i))
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
