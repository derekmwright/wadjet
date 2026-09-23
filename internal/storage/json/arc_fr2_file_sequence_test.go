// SPDX-License-Identifier: MIT

package json

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/fileinput"
)

// fr2Inputs is a sequence of named in-memory files.
func fr2Inputs(bodies ...string) []fileinput.Input {
	inputs := make([]fileinput.Input, len(bodies))
	for i, body := range bodies {
		inputs[i] = fileinput.Input{Name: fmt.Sprintf("f%d.json", i+1), Open: func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(body)), nil
		}}
	}
	return inputs
}

// TestArcFR2EveryFileIsItsOwnJSONDocument: read_json over several files reads
// each as its own document. Through v0.24.0 the files' bytes were one stream
// and the first `]` ended it — a glob of array files read the first file
// (#1262) — and content that was not an object ended the input as if it were
// the end of the file. The sample crosses files, and a file that is not a
// document is 22P02 naming it and the row.
func TestArcFR2EveryFileIsItsOwnJSONDocument(t *testing.T) {
	var hundred []string
	for i := 1; i <= 100; i++ {
		hundred = append(hundred, fmt.Sprintf("[{\"a\":%d},{\"a\":%d}]", i, 1000+i))
	}
	for _, c := range []struct {
		name  string
		files []string
		want  string // "n sum" of column a
		code  string
		names string
	}{
		{"two_arrays", []string{`[{"a":1},{"a":2}]`, `[{"a":3}]`}, "3 6", "", ""},
		{"array_then_ndjson", []string{`[{"a":1},{"a":2}]`, "{\"a\":3}\n{\"a\":4}"}, "4 10", "", ""},
		{"ndjson_then_array", []string{"{\"a\":1}\n", `[{"a":2}]`}, "2 3", "", ""},
		{"empty_array_first", []string{`[]`, `[{"a":2}]`}, "1 2", "", ""},
		{"empty_file_middle", []string{`[{"a":1}]`, ``, "  \n", `[{"a":2}]`}, "2 3", "", ""},
		{"hundred_arrays", hundred, "200 110100", "", ""},
		{"sample_crosses_files", []string{`[{"a":1}]`, `[{"a":1.5}]`}, "2 2.5", "", ""},
		{"array_without_its_close", []string{`[{"a":1},{"a":2}`, `[{"a":3}]`}, "", "22P02", "f1.json"},
		{"content_after_the_array", []string{`[{"a":1}] x`, `[{"a":3}]`}, "", "22P02", "f1.json"},
		{"second_array_in_a_file", []string{`[{"a":1}][{"a":2}]`}, "", "22P02", "f1.json"},
		{"a_scalar_row", []string{"{\"a\":1}\n7\n"}, "", "22P02", "f1.json"},
		{"an_array_of_scalars", []string{`[1,2]`}, "", "22P02", "f1.json"},
		{"a_close_without_an_open", []string{"{\"a\":1}\n]"}, "", "22P02", "f1.json"},
		{"truncated_object_second_file", []string{`[{"a":1}]`, `[{"a":2},{"a":`}, "", "22P02", "f2.json"},
		{"later_file_disagrees_past_the_sample", []string{strings.Repeat("{\"a\":1}\n", 150), `[{"a":"x"}]`}, "", "22P02", "f2.json row 1 "},
	} {
		t.Run(c.name, func(t *testing.T) {
			sr, err := NewFilesReader(fr2Inputs(c.files...))
			var n int
			var sum float64
			if err == nil {
				defer sr.Close()
				for {
					b, nerr := sr.Next()
					if nerr != nil {
						err = nerr
						break
					}
					if b == nil {
						break
					}
					for _, r := range b.ToRows() {
						n++
						switch v := r["a"].(type) {
						case int64:
							sum += float64(v)
						case float64:
							sum += v
						}
					}
				}
			}
			if c.code != "" {
				if err == nil {
					t.Fatalf("answered n=%d; want %s naming %s", n, c.code, c.names)
				}
				if st := sqlerr.StateOf(err); st != c.code || !strings.Contains(err.Error(), c.names) {
					t.Fatalf("%v (SQLSTATE %q), want %s naming %q", err, st, c.code, c.names)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if got := fmt.Sprint(n, " ", sum); got != c.want {
				t.Fatalf("n sum = %s, want %s", got, c.want)
			}
		})
	}
}
