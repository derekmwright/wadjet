// SPDX-License-Identifier: MIT

package json

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/fileinput"
)

// rdDrain reads every batch and answers the rows read and the first error.
func rdDrain(r rpReader) (int, error) {
	n := 0
	for {
		b, err := r.Next()
		if err != nil {
			return n, err
		}
		if b == nil {
			return n, nil
		}
		n += b.Len
	}
}

// rdObjects writes n objects; row `at` gets the extra fields of `extra`
// (raw JSON members, e.g. `,"k":7`).
func rdObjects(n, at int, extra string) []byte {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		e := ""
		if i == at {
			e = extra
		}
		fmt.Fprintf(&b, "{\"a\":%d,\"m\":{\"x\":%d}%s}\n", i, i, e)
	}
	return []byte(b.String())
}

// TestArcRDKeyPastTheSampleOnEveryPath (#1242): a key first seen past the
// sample, and a field of a nested object first seen past it, on EVERY JSON
// read path. At 9420d256 each path skipped the value and the row read
// without it; now a non-NULL value under either refuses — 22P04 naming the
// key, 22P02 naming the column and the field — and a JSON null under either
// is read (it is the NULL the relation already answers for it).
func TestArcRDKeyPastTheSampleOnEveryPath(t *testing.T) {
	for _, path := range rpPaths {
		for _, at := range []int{101, 2049, 2200} {
			for _, tc := range []struct {
				name, extra, state, want string
			}{
				{"key_number", `,"k":7`, "22P04", fmt.Sprintf(`row %d: key "k" is not a column of the relation (the columns were inferred from the file's first 100 rows); sample_size = -1 infers it from every row`, at)},
				{"key_string", `,"k":""`, "22P04", `key "k" is not a column`},
				{"key_object", `,"k":{"z":null}`, "22P04", `key "k" is not a column`},
				{"key_array", `,"k":[]`, "22P04", `key "k" is not a column`},
				{"key_false", `,"k":false`, "22P04", `key "k" is not a column`},
				{"key_null", `,"k":null`, "", ""},
				{"two_keys_null", `,"m2":null,"z":null`, "", ""},
			} {
				t.Run(fmt.Sprintf("%s/%d/%s", path, at, tc.name), func(t *testing.T) {
					r, err := rpOpen(t, path, rdObjects(at+5, at, tc.extra))
					n := 0
					if err == nil {
						n, err = rdDrain(r)
					}
					if tc.state == "" {
						if err != nil || n != at+5 {
							t.Fatalf("want %d rows, got %d %v", at+5, n, err)
						}
						return
					}
					if sqlerr.StateOf(err) != tc.state || !strings.Contains(err.Error(), tc.want) {
						t.Fatalf("want %s %q, got %v", tc.state, tc.want, err)
					}
				})
			}
		}
	}
	// A nested object's field first seen past the sample.
	for _, path := range rpPaths {
		if strings.HasPrefix(path, "eager") || path == "coercion" {
			// The eager readers type a nested object as text (its JSON), and
			// a text column holds any object.
			continue
		}
		t.Run(path+"/nested_field", func(t *testing.T) {
			var b strings.Builder
			for i := 1; i <= 150; i++ {
				m := fmt.Sprintf(`{"x":%d}`, i)
				switch i {
				case 120:
					m = `{"x":1,"y":null}` // a null field is read
				case 130:
					m = `{"x":1,"y":{"deep":2}}`
				}
				fmt.Fprintf(&b, "{\"a\":%d,\"m\":%s}\n", i, m)
			}
			r, err := rpOpen(t, path, []byte(b.String()))
			if err == nil {
				_, err = rdDrain(r)
			}
			want := `row 130 column "m" field "y": value {"deep":2} is under a field the column's type (record) does not have (the column's type was inferred from the file's first 100 rows); sample_size = -1 infers it from every row`
			if sqlerr.StateOf(err) != "22P02" || !strings.Contains(err.Error(), want) {
				t.Fatalf("want 22P02 %q, got %v", want, err)
			}
		})
	}
}

// TestArcRDByteCappedSampleKey: the stream reader's sample stops at 8 MiB,
// so with large objects it holds fewer than 100 rows; a key first seen just
// past THAT sample is refused and the message gives the real count.
func TestArcRDByteCappedSampleKey(t *testing.T) {
	pad := strings.Repeat("p", 200<<10)
	var b strings.Builder
	for i := 1; i <= 60; i++ {
		k := ""
		if i == 55 {
			k = `,"k":1`
		}
		fmt.Fprintf(&b, "{\"a\":%d,\"pad\":\"%s\"%s}\n", i, pad, k)
	}
	sr, err := NewStreamReader(strings.NewReader(b.String()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = rdDrain(sr)
	if sqlerr.StateOf(err) != "22P04" || !strings.Contains(err.Error(), `row 55: key "k"`) || !strings.Contains(err.Error(), "first 40 rows") {
		t.Fatalf("got %v", err)
	}
}

func rdFileInputs(t *testing.T, files map[string]string) []fileinput.Input {
	t.Helper()
	dir := t.TempDir()
	var inputs []fileinput.Input
	for _, name := range []string{"f1.json", "f2.json"} {
		body, ok := files[name]
		if !ok {
			continue
		}
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, fileinput.Input{Name: name, Open: func() (io.ReadCloser, error) { return os.Open(p) }})
	}
	return inputs
}

// TestArcRDWholeInputInference: NewFilesReaderSampled(WholeInput) infers
// from EVERY object — the 0.75 at row 2 200 makes the column double
// precision, the key at 2 200 a column, a nested field at 2 200 a field —
// keeps no object in memory between the passes, and reads the input again.
// A positive count samples that many rows.
func TestArcRDWholeInputInference(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 2200; i++ {
		switch i {
		case 2200:
			fmt.Fprintf(&b, "{\"a\":0.75,\"m\":{\"x\":1,\"y\":\"late\"},\"k\":true}\n")
		default:
			fmt.Fprintf(&b, "{\"a\":%d,\"m\":{\"x\":%d}}\n", i, i)
		}
	}
	inputs := rdFileInputs(t, map[string]string{"f1.json": b.String()})
	sr, err := NewFilesReaderSampled(inputs, WholeInput)
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprint(sr.Schema())
	for _, want := range []string{"{a FLOAT64 ", "{k BOOL ", "{y STRING "} {
		if !strings.Contains(got, want) {
			t.Errorf("schema %s: missing %s", got, want)
		}
	}
	if len(sr.sample) != 0 || len(sr.sampleBuf) != 0 {
		t.Errorf("a whole-input reader buffered %d objects", len(sr.sample))
	}
	n, err := rdDrain(sr)
	if err != nil || n != 2200 {
		t.Fatalf("read %d rows, %v", n, err)
	}
	for _, tc := range []struct {
		size  int
		state string
	}{{2199, "22P02"}, {2200, ""}, {100, "22P02"}, {0, "22P02"}} {
		sr, err := NewFilesReaderSampled(inputs, tc.size)
		if err == nil {
			_, err = rdDrain(sr)
		}
		if sqlerr.StateOf(err) != tc.state {
			t.Errorf("sample %d: want %q, got %v", tc.size, tc.state, err)
		}
	}
	// Across two files, a malformed object is reported where it is, after
	// the rows before it, as the sampled reader reports it.
	inputs = rdFileInputs(t, map[string]string{"f1.json": "{\"a\":1}\n", "f2.json": "{\"a\":2}\n{\"a\":\n"})
	sr, err = NewFilesReaderSampled(inputs, WholeInput)
	if err != nil {
		t.Fatal(err)
	}
	n, err = rdDrain(sr)
	if sqlerr.StateOf(err) != "22P02" || !strings.Contains(err.Error(), "f2.json") {
		t.Fatalf("got %d rows, %v", n, err)
	}
}

// TestArcRDPlannedSchemaChecksEveryRow: the execution of a whole-input read
// takes the plan's schema (NewFilesReaderWithSchema) and does not infer
// again, so it checks EVERY row: an input that changed after the plan read
// it is refused, and the message says the type came from every row.
func TestArcRDPlannedSchemaChecksEveryRow(t *testing.T) {
	inputs := rdFileInputs(t, map[string]string{"f1.json": "{\"a\":1}\n{\"a\":2}\n"})
	planned, err := NewFilesReaderSampled(inputs, WholeInput)
	if err != nil {
		t.Fatal(err)
	}
	schema := planned.Schema()
	inputs = rdFileInputs(t, map[string]string{"f1.json": "{\"a\":1}\n{\"a\":\"changed\"}\n{\"a\":3,\"new\":1}\n"})
	sr := NewFilesReaderWithSchema(inputs, schema)
	_, err = rdDrain(sr)
	want := `row 2 column "a": value "changed" (text) is not of type bigint (the column's type was inferred from every row of the input)`
	if sqlerr.StateOf(err) != "22P02" || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "sample_size") {
		t.Fatalf("want %q, got %v", want, err)
	}
	inputs = rdFileInputs(t, map[string]string{"f1.json": "{\"a\":1}\n{\"a\":3,\"new\":1}\n"})
	_, err = rdDrain(NewFilesReaderWithSchema(inputs, schema))
	if sqlerr.StateOf(err) != "22P04" || !strings.Contains(err.Error(), `row 2: key "new" is not a column of the relation (the columns were inferred from every row of the input)`) {
		t.Fatalf("got %v", err)
	}
}
