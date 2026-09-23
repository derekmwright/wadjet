// SPDX-License-Identifier: MIT

package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// fr2Format writes one file of a reader's format holding the rows given
// (each row one value of column a, or a string to be written verbatim as
// the value's JSON / CSV spelling); empty writes the format's EMPTY file.
type fr2Format struct {
	name, fn, ext, col string
	args               string // named arguments after the path
	write              func(t *testing.T, path string, rows []any)
	empty              func(t *testing.T, path string)
}

func fr2Formats() []fr2Format {
	writeFile := func(t *testing.T, path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	csvBody := func(header bool, rows []any) string {
		var b strings.Builder
		if header {
			b.WriteString("a\n")
		}
		for _, v := range rows {
			fmt.Fprintf(&b, "%v\n", v)
		}
		return b.String()
	}
	jsonValue := func(v any) string {
		if s, ok := v.(string); ok {
			return fmt.Sprintf("%q", s)
		}
		return fmt.Sprint(v)
	}
	writeParquet := func(t *testing.T, path string, cols []parquet.Column, rows []map[string]any) {
		t.Helper()
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: cols}, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) > 0 {
			if err := w.WriteRows(rows); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		writeFile(t, path, buf.String())
	}
	return []fr2Format{
		{name: "csv_header", fn: "read_csv", ext: "csv", col: "a",
			write: func(t *testing.T, p string, rows []any) { writeFile(t, p, csvBody(true, rows)) },
			empty: func(t *testing.T, p string) { writeFile(t, p, "") }},
		{name: "csv_no_header", fn: "read_csv", ext: "csv", col: "col0", args: ", header=false",
			write: func(t *testing.T, p string, rows []any) { writeFile(t, p, csvBody(false, rows)) },
			empty: func(t *testing.T, p string) { writeFile(t, p, "") }},
		{name: "ndjson", fn: "read_json", ext: "json", col: "a",
			write: func(t *testing.T, p string, rows []any) {
				var b strings.Builder
				for _, v := range rows {
					fmt.Fprintf(&b, "{\"a\":%s}\n", jsonValue(v))
				}
				writeFile(t, p, b.String())
			},
			empty: func(t *testing.T, p string) { writeFile(t, p, "") }},
		{name: "json_array", fn: "read_json", ext: "json", col: "a",
			write: func(t *testing.T, p string, rows []any) {
				parts := make([]string, len(rows))
				for i, v := range rows {
					parts[i] = fmt.Sprintf("{\"a\":%s}", jsonValue(v))
				}
				writeFile(t, p, "["+strings.Join(parts, ",\n")+"]")
			},
			empty: func(t *testing.T, p string) { writeFile(t, p, "[]") }},
		{name: "parquet", fn: "read_parquet", ext: "parquet", col: "a",
			write: func(t *testing.T, p string, rows []any) {
				col := parquet.Column{Name: "a", Type: parquet.TypeInt64}
				maps := make([]map[string]any, len(rows))
				for i, v := range rows {
					switch v := v.(type) {
					case int:
						maps[i] = map[string]any{"a": int64(v)}
					case float64:
						col.Type = parquet.TypeFloat64
						maps[i] = map[string]any{"a": v}
					default:
						col = parquet.Column{Name: "b", Type: parquet.TypeString}
						maps[i] = map[string]any{"b": fmt.Sprint(v)}
					}
				}
				writeParquet(t, p, []parquet.Column{col}, maps)
			},
			// A Parquet file with no rows still carries its footer.
			empty: func(t *testing.T, p string) {
				writeParquet(t, p, []parquet.Column{{Name: "a", Type: parquet.TypeInt64}}, nil)
			}},
	}
}

func fr2Ints(from, to int) []any {
	out := make([]any, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// TestArcFR2AGlobIsASequenceOfFiles is the seam enumerated once: every
// reader format × every glob shape × both schema paths. A glob is a SEQUENCE
// of files, each decoded by its format's own reader — a CSV header per file,
// a JSON document per file, a Parquet footer per file — with ONE schema
// across them: the types come from the first 100 rows of the sequence (the
// Parquet columns from the first file's footer), and a later file that
// disagrees is refused naming that file.
//
// Through v0.24.0 a glob was the matched files' BYTES concatenated: a glob
// of JSON array files stopped at the first file's `]` (#1262, rows silently
// dropped) and a Parquet glob of two files was `invalid magic` (#1240). The
// expectations are DuckDB 1.5.5's for the same files (the glob oracle,
// tooling duckdb_glob_1.5.5.txt), except where DuckDB silently casts a later
// Parquet file's column to the first file's type (a 1.5 read as 2): that is
// 42804 here, loud rather than plausible.
func TestArcFR2AGlobIsASequenceOfFiles(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type file struct {
		rows  []any // nil: the format's empty file
		empty bool
	}
	past := fr2Ints(1, 150) // a first file whose rows fill the sample
	shapes := []struct {
		name  string
		files []file
		n     int64
		sum   string
		code  string // a refusal: its SQLSTATE
		names string // …and the file (by index) and row it names
		only  string // a format this shape is for, when not every
		skip  string // a format this shape is not for
	}{
		{name: "one_file", files: []file{{rows: []any{1, 2}}}, n: 2, sum: "3"},
		{name: "glob_of_2", files: []file{{rows: []any{1, 2}}, {rows: []any{3}}}, n: 3, sum: "6"},
		{name: "glob_of_100", files: func() []file {
			fs := make([]file, 100)
			for i := range fs {
				fs[i] = file{rows: []any{i + 1, 1000 + i + 1}} // the sample ends in file 50
			}
			return fs
		}(), n: 200, sum: "110100"},
		{name: "glob_empty_first_and_middle", files: []file{{empty: true}, {rows: []any{1, 2}}, {empty: true}, {rows: []any{3}}}, n: 3, sum: "6"},
		{name: "glob_empty_last", files: []file{{rows: []any{1, 2}}, {rows: []any{3}}, {empty: true}}, n: 3, sum: "6"},
		// The sample crosses into the second file, and the types are the
		// sample's: 1 and 1.5 are double precision (DuckDB: DOUBLE).
		{name: "glob_sample_crosses_files", files: []file{{rows: []any{1}}, {rows: []any{1.5}}}, n: 2, sum: "2.5", skip: "parquet"},
		// A later file that disagrees with the sample's types, past it.
		{name: "glob_later_file_disagrees", files: []file{{rows: past}, {rows: []any{"x"}}}, code: "22P02", names: "p001.|row 1 ", skip: "parquet"},
		// Parquet: the later file lacks the first file's column / declares
		// it at another type.
		{name: "glob_later_file_lacks_the_column", files: []file{{rows: []any{1, 2}}, {rows: []any{"x"}}}, code: "42703", names: "p001", only: "parquet"},
		{name: "glob_later_file_other_type", files: []file{{rows: []any{1, 2}}, {rows: []any{1.5}}}, code: "42804", names: "p001", only: "parquet"},
	}
	for _, path := range []string{"plan_time_schema", "first_batch"} {
		t.Run(path, func(t *testing.T) {
			if path == "first_batch" {
				t.Setenv("WADJET_TEST_NO_READER_SCHEMA", "1")
			}
			for _, f := range fr2Formats() {
				for _, sh := range shapes {
					if (sh.only != "" && sh.only != f.name) || sh.skip == f.name {
						continue
					}
					t.Run(f.name+"/"+sh.name, func(t *testing.T) {
						dir := t.TempDir()
						for i, fl := range sh.files {
							p := filepath.Join(dir, fmt.Sprintf("p%03d.%s", i, f.ext))
							if fl.empty {
								f.empty(t, p)
								continue
							}
							f.write(t, p, fl.rows)
						}
						target := filepath.Join(dir, "*."+f.ext)
						if len(sh.files) == 1 {
							target = filepath.Join(dir, "p000."+f.ext)
						}
						sql := fmt.Sprintf("SELECT COUNT(*) AS n, SUM(%s) AS s FROM %s('%s'%s)", f.col, f.fn, target, f.args)
						res, err := db.Query(ctx, sql)
						if sh.code != "" {
							if err == nil {
								t.Fatalf("answered %v; want %s naming %s", res.Rows, sh.code, sh.names)
							}
							if st := sqlerr.StateOf(err); st != sh.code {
								t.Fatalf("SQLSTATE %q (%v), want %s", st, err, sh.code)
							}
							for _, part := range strings.Split(sh.names, "|") {
								if !strings.Contains(err.Error(), part) {
									t.Fatalf("%v does not name %q", err, part)
								}
							}
							return
						}
						if err != nil {
							t.Fatalf("refused: %v; want n=%d s=%s", err, sh.n, sh.sum)
						}
						got := fmt.Sprint(res.Rows[0]["n"], " ", res.Rows[0]["s"])
						if want := fmt.Sprint(sh.n, " ", sh.sum); got != want {
							t.Fatalf("n s = %s, want %s", got, want)
						}
					})
				}
			}
		})
	}
}

// TestArcFR2EachFileIsItsOwnDocument holds the per-file decoding to what a
// concatenation got wrong: a file's framing state ends with the file. A
// quote left open at the end of a CSV file is that file's 22P04 (the
// concatenation carried it into the next file's bytes and read them as one
// field), each CSV file fixes its own line ending, and a JSON array with no
// closing `]` is that file's 22P02 (the concatenation read the next file's
// objects into it).
func TestArcFR2EachFileIsItsOwnDocument(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, c := range []struct {
		name, fn, ext string
		files         []string
		want, code    string
		names         string
	}{
		{name: "csv_quote_open_at_end_of_file", fn: "read_csv", ext: "csv",
			files: []string{"a,b\n1,\"x\n", "a,b\n2,y\"\n3,z\n"}, code: "22P04", names: "p000."},
		{name: "csv_line_endings_differ_per_file", fn: "read_csv", ext: "csv",
			files: []string{"a,b\r\n1,x\r\n2,y\r\n", "a,b\n3,z\n"}, want: "3 6"},
		{name: "csv_no_trailing_newline", fn: "read_csv", ext: "csv",
			files: []string{"a,b\n1,x\n2,y", "a,b\n3,z"}, want: "3 6"},
		{name: "json_array_without_its_close", fn: "read_json", ext: "json",
			files: []string{"[{\"a\":1},{\"a\":2}", "[{\"a\":3}]"}, code: "22P02", names: "p000."},
		{name: "json_array_then_ndjson", fn: "read_json", ext: "json",
			files: []string{"[{\"a\":1},{\"a\":2}]", "{\"a\":3}\n{\"a\":4}\n"}, want: "4 10"},
		{name: "json_content_after_the_array", fn: "read_json", ext: "json",
			files: []string{"[{\"a\":1}] {\"a\":2}", "[{\"a\":3}]"}, code: "22P02", names: "p000."},
		{name: "json_a_value_that_is_not_an_object", fn: "read_json", ext: "json",
			files: []string{"{\"a\":1}\n7\n{\"a\":2}\n", "{\"a\":3}\n"}, code: "22P02", names: "p000."},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for i, body := range c.files {
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("p%03d.%s", i, c.ext)), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			res, err := db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) AS n, SUM(a) AS s FROM %s('%s')", c.fn, filepath.Join(dir, "*."+c.ext)))
			if c.code != "" {
				if err == nil {
					t.Fatalf("answered %v; want %s naming %s", res.Rows, c.code, c.names)
				}
				if st := sqlerr.StateOf(err); st != c.code || !strings.Contains(err.Error(), c.names) {
					t.Fatalf("SQLSTATE %q (%v), want %s naming %s", st, err, c.code, c.names)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v; want %s", err, c.want)
			}
			if got := fmt.Sprint(res.Rows[0]["n"], " ", res.Rows[0]["s"]); got != c.want {
				t.Fatalf("n s = %s, want %s", got, c.want)
			}
		})
	}
}

// TestArcFR2AMalformedCSVIsRefusedNamingTheInput is #1248 and #1259 through
// the query door: an unterminated quote inside the 100-row sample answered
// COUNT 1 with no error, and past it the error carried no SQLSTATE; a quoted
// empty field read NULL. PostgreSQL 17.11's COPY raises 22P04 for the first
// and reads the empty string for the second.
func TestArcFR2AMalformedCSVIsRefusedNamingTheInput(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	var past strings.Builder
	past.WriteString("a,b\n")
	for i := 1; i <= 300; i++ {
		if i == 200 {
			past.WriteString("200,\"unterminated\n")
			continue
		}
		fmt.Fprintf(&past, "%d,x\n", i)
	}
	for _, c := range []struct{ name, path string }{
		{"inside_the_sample", write("in.csv", "a,b\n1,x\n2,\"unterminated\n3,z\n")},
		{"past_the_sample", write("past.csv", past.String())},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) AS n FROM read_csv('%s')", c.path))
			if err == nil {
				t.Fatalf("answered %v; PostgreSQL's COPY raises 22P04", res.Rows)
			}
			if st := sqlerr.StateOf(err); st != "22P04" || !strings.Contains(err.Error(), c.path) ||
				!strings.Contains(err.Error(), "unterminated CSV quoted field") {
				t.Fatalf("%v (SQLSTATE %q), want 22P04 naming %s", err, st, c.path)
			}
		})
	}
	t.Run("quoted_empty_is_the_empty_string", func(t *testing.T) {
		p := write("q.csv", "id,b\n1,\"x\"\n2,\"\"\n3,\n")
		res, err := db.Query(ctx, fmt.Sprintf("SELECT id, b IS NULL AS n, length(b) AS l FROM read_csv('%s') ORDER BY id", p))
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for i := range res.Rows {
			got = append(got, fmt.Sprint(res.Cells(i)))
		}
		if g := strings.Join(got, " "); g != "[1 false 1] [2 false 0] [3 true <nil>]" {
			t.Fatalf("rows %s; PostgreSQL answers [1 false 1] [2 false 0] [3 true <nil>]", g)
		}
	})
}
