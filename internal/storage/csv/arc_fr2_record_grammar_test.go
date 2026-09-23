// SPDX-License-Identifier: MIT

package csv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// pgNull marks an expected NULL in a grammar cell's rows.
const pgNull = "\x00NULL"

// TestArcFR2CSVRecordGrammarIsPostgreSQLs holds read_csv's record grammar to
// PostgreSQL 17.11's `COPY t(a text, b text) FROM '<file>' WITH (FORMAT csv,
// HEADER true)` — every expectation below is what that statement answered
// for the same bytes (tooling evidence pg_csv_grammar_17.11.txt and
// pg_csv_grammar_extra_17.11.txt): the documented accepted forms (quoted and
// unquoted empty fields, whitespace, quotes opening mid-field, doubled
// quotes, line breaks inside quotes, LF/CR/CRLF line endings) and the
// documented rejected ones, every one of which is 22P04 bad_copy_file_format
// (an unterminated quote, a mixed line ending, a record whose field count is
// not the relation's, a blank line in a two-column file).
//
// The one deliberate difference is `\.`: PostgreSQL 17 ends the data at a
// line holding it and reads NO later row; here it is data, as PostgreSQL 18
// reads it from a file — so the two-column file's `\.` line is a record with
// one field, which is 22P04 "missing data".
func TestArcFR2CSVRecordGrammarIsPostgreSQLs(t *testing.T) {
	cells := []struct {
		name string
		in   string
		rows [][]string // nil with code set: refused
		code string
	}{
		{"unquoted_empty", "a,b\n1,\n", [][]string{{"1", pgNull}}, ""},
		{"quoted_empty", "a,b\n1,\"\"\n", [][]string{{"1", ""}}, ""},
		{"quoted_empty_first", "a,b\n\"\",x\n", [][]string{{"", "x"}}, ""},
		{"two_quoted_empty", "a,b\n\"\",\"\"\n", [][]string{{"", ""}}, ""},
		{"space_unquoted", "a,b\n1, \n", [][]string{{"1", " "}}, ""},
		{"tab_unquoted", "a,b\n1,\t\n", [][]string{{"1", "\t"}}, ""},
		{"spaces_around", "a,b\n1, x \n", [][]string{{"1", " x "}}, ""},
		{"spaces_in_quotes", "a,b\n1,\" x \"\n", [][]string{{"1", " x "}}, ""},
		{"quoted_space", "a,b\n1,\" \"\n", [][]string{{"1", " "}}, ""},
		{"space_then_quoted_empty", "a,b\n1, \"\"\n", [][]string{{"1", " "}}, ""},
		{"space_before_quote", "a,b\n1, \"x\"\n", [][]string{{"1", " x"}}, ""},
		{"space_after_quote", "a,b\n1,\"x\" \n", [][]string{{"1", "x "}}, ""},
		{"text_after_quote", "a,b\n1,\"x\"y\n", [][]string{{"1", "xy"}}, ""},
		{"quoted_space_quoted", "a,b\n1,\"x\" \"y\"\n", [][]string{{"1", "x y"}}, ""},
		{"quote_opens_mid_field", "a,b\n1,x\"y\"z\n", [][]string{{"1", "xyz"}}, ""},
		{"doubled_quote", "a,b\n1,\"x\"\"y\"\n", [][]string{{"1", "x\"y"}}, ""},
		{"delim_in_quotes", "a,b\n1,\"x,y\"\n", [][]string{{"1", "x,y"}}, ""},
		{"newline_in_quotes", "a,b\n1,\"x\ny\"\n", [][]string{{"1", "x\ny"}}, ""},
		{"crlf_in_quotes", "a,b\n1,\"x\r\ny\"\n", [][]string{{"1", "x\r\ny"}}, ""},
		{"crlf_lines", "a,b\r\n1,x\r\n2,y\r\n", [][]string{{"1", "x"}, {"2", "y"}}, ""},
		{"cr_lines", "a,b\r1,x\r2,y\r", [][]string{{"1", "x"}, {"2", "y"}}, ""},
		{"no_final_newline", "a,b\n1,x", [][]string{{"1", "x"}}, ""},
		{"quoted_header", "\"a\",\"b\"\n1,x\n", [][]string{{"1", "x"}}, ""},
		{"bom", "\ufeffa,b\n1,x\n", [][]string{{"1", "x"}}, ""},
		{"backslash_n_is_text", "a,b\n1,\\N\n", [][]string{{"1", "\\N"}}, ""},
		{"null_word_is_text", "a,b\n1,NULL\n", [][]string{{"1", "NULL"}}, ""},
		{"long_field", "a,b\n1," + strings.Repeat("z", 100000) + "\n", [][]string{{"1", strings.Repeat("z", 100000)}}, ""},
		{"header_only", "a,b\n", [][]string{}, ""},

		{"unterminated", "a,b\n1,\"x\n2,y\n", nil, "22P04"},
		{"unterminated_last", "a,b\n1,\"x", nil, "22P04"},
		{"quote_only_field", "a,b\n1,\"\n", nil, "22P04"},
		{"bare_quote_mid", "a,b\n1,x\"y\n", nil, "22P04"},
		{"backslash_is_not_an_escape", "a,b\n1,\"x\\\"y\"\n", nil, "22P04"},
		{"quote_in_quoted_then_delim", "a,b\n\"1\"\",x\n", nil, "22P04"},
		{"extra_column", "a,b\n1,x,z\n", nil, "22P04"},
		{"trailing_delim", "a,b\n1,x,\n", nil, "22P04"},
		{"missing_column", "a,b\n1\n", nil, "22P04"},
		{"blank_line", "a,b\n1,x\n\n2,y\n", nil, "22P04"},
		{"trailing_blank_line", "a,b\n1,x\n\n", nil, "22P04"},
		{"only_blank_line", "a,b\n\n", nil, "22P04"},
		{"cr_in_unquoted_lf", "a,b\n1,x\ry\n", nil, "22P04"},
		{"mixed_lf_then_crlf", "a,b\n1,x\r\n2,y\n", nil, "22P04"},
		{"mixed_crlf_then_lf", "a,b\r\n1,x\n2,y\r\n", nil, "22P04"},
		{"quoted_eod_marker", "a,b\n1,x\n\"\\.\"\n", nil, "22P04"},
		// PostgreSQL 17 answers {1,x} (end of data); PostgreSQL 18 and this
		// reader read `\.` as data, a one-field record in a two-column file.
		{"eod_marker_is_data", "a,b\n1,x\n\\.\n2,y\n", nil, "22P04"},

		{"one_col_blank_line", "a\n1\n\n2\n", [][]string{{"1"}, {pgNull}, {"2"}}, ""},
		{"one_col_quoted_empty", "a\n\"\"\n", [][]string{{""}}, ""},
	}
	for _, c := range cells {
		for _, path := range []string{"stream", "eager"} {
			t.Run(c.name+"/"+path, func(t *testing.T) {
				var got [][]string
				var err error
				if path == "stream" {
					got, err = readAllCSV(func() (*Reader, error) { return NewStreamReader(strings.NewReader(c.in), DefaultConfig()) })
				} else {
					if strings.TrimSpace(c.in) != c.in {
						t.Skip("NewReader trims its input")
					}
					got, err = readAllCSV(func() (*Reader, error) { return NewReader([]byte(c.in), DefaultConfig()) })
				}
				if c.code != "" {
					if err == nil {
						t.Fatalf("answered %q; PostgreSQL refuses with %s", got, c.code)
					}
					if st := sqlerr.StateOf(err); st != c.code {
						t.Fatalf("SQLSTATE %q (%v), want %s", st, err, c.code)
					}
					return
				}
				if err != nil {
					t.Fatalf("refused (%v); PostgreSQL answers %q", err, c.rows)
				}
				if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", c.rows) {
					t.Fatalf("rows %q, PostgreSQL %q", got, c.rows)
				}
			})
		}
	}
}

// TestArcFR2AMalformedCSVIsRefusedWhereverItIs is #1248: an unterminated
// quote inside the 100-row sample ended the sample as if it were the end of
// the file (COUNT answered the rows before it), and past the sample the
// error carried no SQLSTATE. Both are 22P04 now, naming the line.
func TestArcFR2AMalformedCSVIsRefusedWhereverItIs(t *testing.T) {
	for _, at := range []int{1, 2, 50, 100, 101, 110, 2100} {
		t.Run(fmt.Sprintf("row_%d", at), func(t *testing.T) {
			var b strings.Builder
			b.WriteString("a,b\n")
			for i := 1; i <= at+20; i++ {
				if i == at {
					fmt.Fprintf(&b, "%d,\"unterminated\n", i)
					continue
				}
				fmt.Fprintf(&b, "%d,x\n", i)
			}
			got, err := readAllCSV(func() (*Reader, error) { return NewStreamReader(strings.NewReader(b.String()), DefaultConfig()) })
			if err == nil {
				t.Fatalf("answered %d rows; PostgreSQL refuses with 22P04", len(got))
			}
			if st := sqlerr.StateOf(err); st != "22P04" {
				t.Fatalf("SQLSTATE %q (%v), want 22P04", st, err)
			}
			if want := fmt.Sprintf("line %d", at+1); !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not name %s", err, want)
			}
		})
	}
}

// readAllCSV drains a reader into rows of text, NULL as pgNull.
func readAllCSV(open func() (*Reader, error)) ([][]string, error) {
	r, err := open()
	if err != nil {
		return nil, err
	}
	if c, ok := any(r).(interface{ Close() error }); ok {
		defer c.Close()
	}
	rows := [][]string{}
	for {
		b, err := r.Next()
		if err != nil {
			return rows, err
		}
		if b == nil {
			return rows, nil
		}
		for i := 0; i < b.Len; i++ {
			row := make([]string, len(b.Columns))
			for j, col := range b.Columns {
				if col.Nulls.IsNull(i) {
					row[j] = pgNull
					continue
				}
				row[j] = fmt.Sprint(col.GetValue(i))
			}
			rows = append(rows, row)
		}
	}
}
