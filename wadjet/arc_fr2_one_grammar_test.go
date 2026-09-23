// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestArcFR2TheSampleReadsEveryValueItTyped holds the readers' inference to
// the one-grammar rule: the type the 100-row sample infers READS every value
// the sample holds, as the input spells it — and a value past the sample that
// the type does not read is refused, never converted.
//
// Through v0.24.0 two inferences broke it inside the sample, silently:
// a sample mixing booleans and integers inferred bigint and read the
// boolean as NULL (read_csv) or 1 (read_json) (#1260) — PostgreSQL's COPY
// refuses 'true' for a bigint, so no number type is the answer and the
// column is text — and a nested ARRAY/ROW column took its element and field
// types from its FIRST occurrence, so `[1.5]` after `[1]` read `[1]` and a
// field first seen in a later object was dropped (#1261).
func TestArcFR2TheSampleReadsEveryValueItTyped(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lines := func(n int, row func(i int) string) string {
		var b strings.Builder
		for i := 1; i <= n; i++ {
			b.WriteString(row(i) + "\n")
		}
		return b.String()
	}
	at := func(k int, special, plain string) func(i int) string {
		return func(i int) string {
			if i == k {
				return fmt.Sprintf(special, i)
			}
			return fmt.Sprintf(plain, i, i)
		}
	}
	for _, c := range []struct {
		name, fn, ext, body, sel string
		want                     string // the rows as fmt prints them, or a SQLSTATE
	}{
		// #1260, inside the sample: the column is text and row 30 reads true.
		{"csv_bool_among_ints", "read_csv", "csv", "id,a\n" + lines(120, at(30, "%d,true", "%d,%d")),
			"SELECT id, a FROM %s WHERE id IN (29,30,31) ORDER BY id", "[29 29] [30 true] [31 31]"},
		{"json_bool_among_ints", "read_json", "json", lines(120, at(30, `{"id":%d,"a":true}`, `{"id":%d,"a":%d}`)),
			"SELECT id, a FROM %s WHERE id IN (29,30,31) ORDER BY id", "[29 29] [30 true] [31 31]"},
		{"csv_bool_among_floats", "read_csv", "csv", "id,a\n" + lines(120, at(30, "%d,false", "%d,%d.5")),
			"SELECT id, a FROM %s WHERE id IN (29,30) ORDER BY id", "[29 29.5] [30 false]"},
		{"json_bool_among_floats", "read_json", "json", lines(120, at(30, `{"id":%d,"a":false}`, `{"id":%d,"a":%d.5}`)),
			"SELECT id, a FROM %s WHERE id IN (29,30) ORDER BY id", "[29 29.5] [30 false]"},
		// …and the text column's type is what the relation declares.
		{"csv_bool_among_ints_is_text", "read_csv", "csv", "id,a\n" + lines(120, at(30, "%d,true", "%d,%d")),
			"SELECT a FROM %s WHERE id = 1", "type:STRING"},
		// Past the sample the rule is RP's: an all-integer sample is bigint,
		// and a boolean after it is refused, as COPY refuses it.
		{"csv_bool_past_an_int_sample", "read_csv", "csv", "id,a\n" + lines(150, at(140, "%d,true", "%d,%d")),
			"SELECT COUNT(a) FROM %s", "22P02"},
		{"json_bool_past_an_int_sample", "read_json", "json", lines(150, at(140, `{"id":%d,"a":true}`, `{"id":%d,"a":%d}`)),
			"SELECT COUNT(a) FROM %s", "22P02"},
		// #1261: the element and field types are every sampled occurrence's.
		{"json_array_element_widens", "read_json", "json", lines(120, at(5, `{"id":%d,"arr":[1.5]}`, `{"id":%d,"arr":[%d]}`)),
			"SELECT id, arr FROM %s WHERE id IN (1,5) ORDER BY id", "[1 [1]] [5 [1.5]]"},
		{"json_nested_array_element_widens", "read_json", "json", lines(120, at(5, `{"id":%d,"arr":[[1.5]]}`, `{"id":%d,"arr":[[%d]]}`)),
			"SELECT id, arr FROM %s WHERE id IN (1,5) ORDER BY id", "[1 [[1]]] [5 [[1.5]]]"},
		{"json_row_field_widens", "read_json", "json", lines(120, at(5, `{"id":%d,"r":{"x":1.5}}`, `{"id":%d,"r":{"x":%d}}`)),
			"SELECT id, r.x FROM %s WHERE id IN (1,5) ORDER BY id", "[1 1] [5 1.5]"},
		{"json_row_field_first_seen_later", "read_json", "json", lines(120, at(5, `{"id":%d,"r":{"x":5,"y":"late"}}`, `{"id":%d,"r":{"x":%d}}`)),
			"SELECT id, r.y FROM %s WHERE id IN (1,5) ORDER BY id", "[1 <nil>] [5 late]"},
		{"json_array_of_rows_field_first_seen_later", "read_json", "json", lines(120, at(5, `{"id":%d,"arr":[{"x":5,"y":"late"}]}`, `{"id":%d,"arr":[{"x":%d}]}`)),
			"SELECT id, arr FROM %s WHERE id = 5", "[5 [map[x:5 y:late]]]"},
		{"json_empty_array_then_ints", "read_json", "json", lines(120, at(1, `{"id":%d,"arr":[]}`, `{"id":%d,"arr":[%d]}`)),
			"SELECT id, arr FROM %s WHERE id IN (1,2) ORDER BY id", "[1 []] [2 [2]]"},
		{"json_row_field_boolean", "read_json", "json", lines(120, at(0, "", `{"id":%d,"r":{"b":true,"n":%d}}`)),
			"SELECT r.b FROM %s WHERE id = 1", "type:BOOL"},
		{"json_array_beside_object_is_text", "read_json", "json", lines(120, at(5, `{"id":%d,"v":{"k":1}}`, `{"id":%d,"v":[%d]}`)),
			"SELECT id, v FROM %s WHERE id IN (1,5) ORDER BY id", `[1 [1]] [5 {"k":1}]`},
		// Past the sample a nested value the element type does not read is
		// refused (RP), never truncated.
		{"json_array_element_past_the_sample", "read_json", "json", lines(150, at(140, `{"id":%d,"arr":[1.5]}`, `{"id":%d,"arr":[%d]}`)),
			"SELECT COUNT(arr) FROM %s", "22P02"},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "f."+c.ext)
			if err := os.WriteFile(path, []byte(c.body), 0o600); err != nil {
				t.Fatal(err)
			}
			res, err := db.Query(ctx, fmt.Sprintf(c.sel, fmt.Sprintf("%s('%s')", c.fn, path)))
			if len(c.want) == 5 && !strings.ContainsAny(c.want, "[ ") {
				if err == nil {
					t.Fatalf("answered %v; want %s", res.Rows, c.want)
				}
				if st := sqlerr.StateOf(err); st != c.want {
					t.Fatalf("SQLSTATE %q (%v), want %s", st, err, c.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if typ, ok := strings.CutPrefix(c.want, "type:"); ok {
				if got := res.OutputSchema[len(res.OutputSchema)-1].Type.String(); got != typ {
					t.Fatalf("declared %s, want %s", got, typ)
				}
				return
			}
			var got []string
			for i := range res.Rows {
				got = append(got, fmt.Sprint(res.Cells(i)))
			}
			if g := strings.Join(got, " "); g != c.want {
				t.Fatalf("rows %s, want %s", g, c.want)
			}
		})
	}
}
