// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC RD (#1242) — A FILE VALUE PAST THE READERS' INFERENCE SAMPLE IS LOUD
// OR WIDENED, NEVER A SILENT NULL OR A SILENT DROP. ADR-0039 §3.
//
// The position: by default read_csv / read_json type each column from the
// input's first 100 rows, and a later row the relation cannot carry is
// refused naming the reader, the file, the row, the column and both types —
// a value of another type (arc RP), a KEY the sample never saw (22P04), a
// FIELD of a nested object the sample never saw (22P02). `sample_size = -1`
// infers from every row instead, so the same files answer with the column
// widened (int → double precision → text, as inside the sample) and the key
// a column. `sample_size = N` samples N rows.
//
// The coverage table: every cell is a file of R rows (R = 101, 2 049 — the
// first row of the second batch — and 2 200) whose rows 1..R-1 are the
// sample's shape and whose row R is the cell's value, × csv / json, × the
// consumers SUM, COUNT(*), COUNT(col), the projection of row R, WHERE on the
// column, CREATE TABLE AS (what is STORED, read back) and INSERT … SELECT
// into a declared table, × the default and `sample_size = -1`.
//
// At 9420d256 the value cells were already loud (arc RP); the KEY cells read
// the row without the key (SELECT * and the CTAS stored {id, a} for a row
// holding k = 7), the NESTED-FIELD cells stored m = {x} for an object
// holding y = 2, and every `sample_size` cell was unparsed (`sample_size=-1`
// read the sign as the value and the 1 as a positional argument).
func TestArcRDReaderPastSampleCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := t.TempDir()
	tables := 0

	for _, c := range rdCells() {
		for _, kind := range []string{"csv", "json"} {
			if (kind == "csv" && c.jsonOnly) || (kind == "json" && c.csvOnly) {
				continue
			}
			for _, R := range []int{101, 2049, 2200} {
				path := rdWriteCell(t, dir, c, kind, R)
				S := (R - 1) * R / 2 // the sum of the sample-shaped rows' a
				for _, whole := range []bool{false, true} {
					opt, mode := "", "default"
					if whole {
						opt, mode = ", sample_size = -1", "whole"
					}
					src := fmt.Sprintf("read_%s('%s'%s)", kind, path, opt)
					exp := c.expect(kind, R, S, whole)
					if exp.proj == "" {
						exp.proj = "a"
					}
					name := fmt.Sprintf("%s/%s/%d/%s", c.name, kind, R, mode)
					t.Run(name, func(t *testing.T) {
						tables++
						ctas := fmt.Sprintf("rd_ctas_%d", tables)
						ins := fmt.Sprintf("rd_ins_%d", tables)
						if _, err := db.Query(ctx, "CREATE TABLE "+ins+" (id BIGINT, a BIGINT)"); err != nil {
							t.Fatal(err)
						}
						consumers := []struct{ label, sql string }{
							{"sum", "SELECT SUM(a) FROM " + src},
							{"count", "SELECT COUNT(*) FROM " + src},
							{"count_a", "SELECT COUNT(a) FROM " + src},
							{"row", "SELECT " + exp.proj + " FROM " + src + fmt.Sprintf(" WHERE id = %d", R)},
							{"where", "SELECT COUNT(*) FROM " + src + " WHERE a > 0"},
							{"ctas", "CREATE TABLE " + ctas + " AS SELECT * FROM " + src},
							{"stored", "SELECT " + exp.proj + " FROM " + ctas + fmt.Sprintf(" WHERE id = %d", R)},
							{"insert", "INSERT INTO " + ins + " SELECT id, a FROM " + src},
							{"inserted", "SELECT COUNT(*), SUM(a) FROM " + ins},
						}
						for _, q := range consumers {
							res, err := db.Query(ctx, q.sql)
							if exp.err != nil {
								// Loud: every consumer of the reader refuses
								// the same way, nothing is stored, nothing
								// inserted.
								switch q.label {
								case "stored":
									if sqlerr.StateOf(err) != "42P01" {
										t.Errorf("stored: the CTAS over a refused read created a table: %v %v", rdCells1(res), err)
									}
									continue
								case "inserted":
									if err != nil || rdCells1(res) != "[0 <nil>]" {
										t.Errorf("inserted: want [0 <nil>], got %v %v", rdCells1(res), err)
									}
									continue
								}
								if got := sqlerr.StateOf(err); got != exp.err.state {
									t.Errorf("%s: want %s, got %s %v (rows %v)\n  SQL: %s", q.label, exp.err.state, got, err, rdCells1(res), q.sql)
									continue
								}
								for _, part := range exp.err.parts {
									if !strings.Contains(err.Error(), part) {
										t.Errorf("%s: missing %q in %v", q.label, part, err)
									}
								}
								continue
							}
							want, skip := exp.answers[q.label]
							if skip && want == "" {
								continue // a consumer outside this seam (see the cell)
							}
							got := rdCells1(res)
							if err != nil {
								got = "ERR " + sqlerr.StateOf(err)
							}
							if got != want {
								t.Errorf("%s: got %s %v, want %s\n  SQL: %s", q.label, got, err, want, q.sql)
							}
						}
					})
				}
			}
		}
	}
}

type rdRefusal struct {
	state string
	parts []string
}

type rdExpect struct {
	err     *rdRefusal
	proj    string            // the projection of row R
	answers map[string]string // consumer → the rendered answer ("" = not asserted)
}

type rdCell struct {
	name              string
	csvOnly, jsonOnly bool
	sample            func(i int) any // rows 1..R-1's a (nil = NULL)
	csvPast           string          // row R's a as the CSV field is written
	jsonPast          any             // row R's a as JSON (or a raw object line, see line)
	line              func(i, R int) map[string]any
	expect            func(kind string, R, S int, whole bool) rdExpect
}

// rdCells1 renders a result's rows positionally, as the probe printed them.
func rdCells1(r *QueryResult) string {
	if r == nil {
		return "<nil result>"
	}
	var rows []string
	for i := range r.Rows {
		rows = append(rows, fmt.Sprint(r.Cells(i)))
	}
	return strings.Join(rows, " ")
}

func rdWriteCell(t *testing.T, dir string, c rdCell, kind string, R int) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%s_%d.%s", c.name, R, kind))
	var b strings.Builder
	if kind == "csv" {
		b.WriteString("id,a\n")
	}
	for i := 1; i <= R; i++ {
		if c.line != nil {
			raw, err := json.Marshal(c.line(i, R))
			if err != nil {
				t.Fatal(err)
			}
			b.Write(raw)
			b.WriteByte('\n')
			continue
		}
		var v any
		if i == R {
			if kind == "csv" {
				fmt.Fprintf(&b, "%d,%s\n", i, c.csvPast)
				continue
			}
			v = c.jsonPast
		} else {
			v = c.sample(i)
		}
		if kind == "csv" {
			field := ""
			if v != nil {
				field = fmt.Sprint(v)
			}
			fmt.Fprintf(&b, "%d,%s\n", i, field)
			continue
		}
		raw, err := json.Marshal(map[string]any{"id": i, "a": v})
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func rdInts(i int) any { return i }

// rdRefuse is the refusal of row R's value of observed type obs in column a,
// inferred bigint from the first 100 rows, as each reader spells the value.
func rdRefuse(kind string, R int, csvValue, jsonValue, obs string) *rdRefusal {
	v := csvValue
	if kind == "json" {
		v = jsonValue
	}
	return &rdRefusal{state: "22P02", parts: []string{
		fmt.Sprintf("read_%s: ", kind),
		fmt.Sprintf(": row %d column \"a\": value %s (%s) is not of type bigint", R, v, obs),
		"(the column's type was inferred from the file's first 100 rows); sample_size = -1 infers it from every row",
	}}
}

// rdText is the whole-input answer for a column that widens to text.
func rdText(R int, rowR string) rdExpect {
	return rdExpect{proj: "a", answers: map[string]string{
		"sum": "ERR 42883", "count": fmt.Sprintf("[%d]", R), "count_a": fmt.Sprintf("[%d]", R),
		"row": "[" + rowR + "]", "stored": "[" + rowR + "]",
		// A text column compared with an integer is a comparison-typing
		// seam, not this one (PostgreSQL: 42883); not asserted here.
		"where":  "",
		"ctas":   fmt.Sprintf("[SELECT %d]", R),
		"insert": "ERR 42804", "inserted": "[0 <nil>]",
	}}
}

func rdCells() []rdCell {
	widenOrRefuse := func(csvV, jsonV, obs, rowR string) func(string, int, int, bool) rdExpect {
		return func(kind string, R, S int, whole bool) rdExpect {
			if whole {
				return rdText(R, rowR)
			}
			return rdExpect{err: rdRefuse(kind, R, csvV, jsonV, obs)}
		}
	}
	return []rdCell{
		{name: "float", sample: rdInts, csvPast: "0.75", jsonPast: 0.75,
			expect: func(kind string, R, S int, whole bool) rdExpect {
				if !whole {
					return rdExpect{err: rdRefuse(kind, R, `"0.75"`, "0.75", "double precision")}
				}
				return rdExpect{proj: "a", answers: map[string]string{
					"sum": fmt.Sprintf("[%v]", float64(S)+0.75), "count": fmt.Sprintf("[%d]", R),
					"count_a": fmt.Sprintf("[%d]", R), "row": "[0.75]", "stored": "[0.75]",
					"where": fmt.Sprintf("[%d]", R), "ctas": fmt.Sprintf("[SELECT %d]", R),
					// The assignment to bigint rounds 0.75 to 1, as PostgreSQL's does.
					"insert": fmt.Sprintf("[INSERT 0 %d]", R), "inserted": fmt.Sprintf("[%d %d]", R, S+1),
				}}
			}},
		{name: "text", sample: rdInts, csvPast: "abc", jsonPast: "abc",
			expect: widenOrRefuse(`"abc"`, `"abc"`, "text", "abc")},
		{name: "bool", sample: rdInts, csvPast: "true", jsonPast: true,
			expect: widenOrRefuse(`"true"`, "true", "boolean", "true")},
		{name: "date", sample: rdInts, csvPast: "2024-01-02", jsonPast: "2024-01-02",
			expect: widenOrRefuse(`"2024-01-02"`, `"2024-01-02"`, "timestamp", "2024-01-02")},
		// A quoted empty CSV field is the empty string, as COPY reads it.
		{name: "empty", sample: rdInts, csvPast: `""`, jsonPast: "",
			expect: widenOrRefuse(`""`, `""`, "text", "")},
		{name: "nulltext", sample: rdInts, csvPast: "NULL", jsonPast: "NULL",
			expect: widenOrRefuse(`"NULL"`, `"NULL"`, "text", "NULL")},
		{name: "nested_object", jsonOnly: true, sample: rdInts, jsonPast: map[string]any{"x": 1},
			expect: widenOrRefuse("", `{"x":1}`, "object", `{"x":1}`)},
		// A JSON string past a bigint sample; the CSV twin is the quoted
		// number, which COPY reads as the number (below).
		{name: "quoted_number", jsonOnly: true, sample: rdInts, jsonPast: "5",
			expect: widenOrRefuse("", `"5"`, "text", "5")},
		{name: "quoted_number", csvOnly: true, sample: rdInts, csvPast: `"5"`,
			expect: func(kind string, R, S int, whole bool) rdExpect {
				return rdBigint(R, S+5, R, "5")
			}},
		// An unquoted empty CSV field and a JSON null are NULL in every mode.
		{name: "null", sample: rdInts, csvPast: "", jsonPast: nil,
			expect: func(kind string, R, S int, whole bool) rdExpect {
				return rdBigint(R, S, R-1, "<nil>")
			}},
		{name: "past_int64", sample: rdInts, csvPast: "9223372036854775808", jsonPast: json.Number("9223372036854775808"),
			expect: func(kind string, R, S int, whole bool) rdExpect {
				if !whole {
					v := "9223372036854775808"
					if kind == "csv" {
						v = `"` + v + `"`
					}
					return rdExpect{err: &rdRefusal{state: "22003", parts: []string{
						fmt.Sprintf("read_%s: ", kind),
						fmt.Sprintf(": row %d column \"a\": value %s is out of range for type bigint", R, v),
					}}}
				}
				return rdExpect{proj: "a", answers: map[string]string{
					"sum": fmt.Sprintf("[%v]", float64(S)+9223372036854775808), "count": fmt.Sprintf("[%d]", R),
					"count_a": fmt.Sprintf("[%d]", R), "row": "[9.223372036854776e+18]", "stored": "[9.223372036854776e+18]",
					"where": fmt.Sprintf("[%d]", R), "ctas": fmt.Sprintf("[SELECT %d]", R),
					// INSERT of the double 2^63 into bigint is the assignment
					// cast's seam (it wraps; PostgreSQL 22003) — recorded as a
					// filing candidate, not asserted by this gate.
					"insert": "", "inserted": "",
				}}
			}},
		// A sample of dates and a later timestamp: the reader types both as
		// timestamp, so nothing is past the sample.
		{name: "date_then_timestamp", sample: func(i int) any { return fmt.Sprintf("2024-01-%02d", i%28+1) },
			csvPast: "2024-01-02 03:04:05", jsonPast: "2024-01-02 03:04:05",
			expect: func(kind string, R, S int, whole bool) rdExpect {
				return rdExpect{proj: "CAST(a AS VARCHAR)", answers: map[string]string{
					"sum": "ERR 42883", "count": fmt.Sprintf("[%d]", R), "count_a": fmt.Sprintf("[%d]", R),
					"row": "[2024-01-02 03:04:05]", "stored": "[2024-01-02 03:04:05]",
					"where": "", "ctas": fmt.Sprintf("[SELECT %d]", R),
					"insert": "ERR 42804", "inserted": "[0 <nil>]",
				}}
			}},
		// A sample that is all NULL ends with no type: text by default (the
		// value reads, as its text), bigint when every row is read.
		{name: "all_null_sample", sample: func(int) any { return nil }, csvPast: "5", jsonPast: 5,
			expect: func(kind string, R, S int, whole bool) rdExpect {
				if whole {
					return rdBigint(R, 5, 1, "5")
				}
				e := rdText(R, "5")
				e.answers["count_a"] = "[1]"
				return e
			}},
		// A KEY first seen at row R.
		{name: "key", jsonOnly: true, line: func(i, R int) map[string]any {
			o := map[string]any{"id": i, "a": i}
			if i == R {
				o["k"] = 7
			}
			return o
		}, expect: func(kind string, R, S int, whole bool) rdExpect {
			if !whole {
				return rdExpect{err: &rdRefusal{state: "22P04", parts: []string{
					"read_json: ",
					fmt.Sprintf(": row %d: key \"k\" is not a column of the relation (the columns were inferred from the file's first 100 rows); sample_size = -1 infers it from every row", R),
				}}}
			}
			e := rdBigint(R, S+R, R, "")
			e.proj = "k"
			e.answers["row"], e.answers["stored"] = "[7]", "[7]"
			return e
		}},
		// …and one whose only value is a JSON null: no value is lost, so the
		// default reads the row; every row read makes it a (text) column.
		{name: "key_null", jsonOnly: true, line: func(i, R int) map[string]any {
			o := map[string]any{"id": i, "a": i}
			if i == R {
				o["k"] = nil
			}
			return o
		}, expect: func(kind string, R, S int, whole bool) rdExpect {
			e := rdBigint(R, S+R, R, "")
			if whole {
				e.proj = "k"
				e.answers["row"], e.answers["stored"] = "[<nil>]", "[<nil>]"
				return e
			}
			e.proj = "a"
			e.answers["row"], e.answers["stored"] = fmt.Sprintf("[%d]", R), fmt.Sprintf("[%d]", R)
			return e
		}},
		// A FIELD of a nested object first seen at row R.
		{name: "nested_field", jsonOnly: true, line: func(i, R int) map[string]any {
			m := map[string]any{"x": i}
			if i == R {
				m["y"] = 2
			}
			return map[string]any{"id": i, "a": i, "m": m}
		}, expect: func(kind string, R, S int, whole bool) rdExpect {
			if !whole {
				return rdExpect{err: &rdRefusal{state: "22P02", parts: []string{
					"read_json: ",
					fmt.Sprintf(`: row %d column "m" field "y": value 2 is under a field the column's type (record) does not have (the column's type was inferred from the file's first 100 rows); sample_size = -1 infers it from every row`, R),
				}}}
			}
			e := rdBigint(R, S+R, R, "")
			e.proj = "m"
			e.answers["row"] = fmt.Sprintf("[map[x:%d y:2]]", R)
			e.answers["stored"] = e.answers["row"]
			return e
		}},
	}
}

// rdBigint is the answer of a bigint column a whose sum is sum, non-NULL
// count countA and row-R value rowR.
func rdBigint(R, sum, countA int, rowR string) rdExpect {
	where := countA
	return rdExpect{proj: "a", answers: map[string]string{
		"sum": fmt.Sprintf("[%d]", sum), "count": fmt.Sprintf("[%d]", R),
		"count_a": fmt.Sprintf("[%d]", countA), "row": "[" + rowR + "]", "stored": "[" + rowR + "]",
		"where": fmt.Sprintf("[%d]", where), "ctas": fmt.Sprintf("[SELECT %d]", R),
		"insert": fmt.Sprintf("[INSERT 0 %d]", R), "inserted": fmt.Sprintf("[%d %d]", R, sum),
	}}
}

// TestArcRDSampleSizeArgument: the `sample_size` named argument of both
// readers — a count of rows, or -1 for every row — its refusals, and the
// signed spelling the parser used to split. At 9420d256 every cell answered
// as if the argument were absent or was a 42601/positional mismatch.
func TestArcRDSampleSizeArgument(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := t.TempDir()
	c := rdCells()[0] // float: 0.75 at row R
	for _, kind := range []string{"csv", "json"} {
		path := rdWriteCell(t, dir, c, kind, 2200)
		S := 2199 * 2200 / 2
		for _, tc := range []struct{ opt, want, state string }{
			// The count reaches the row: the column widens.
			{"sample_size = 2200", fmt.Sprintf("[%v]", float64(S)+0.75), ""},
			{"sample_size=2200", fmt.Sprintf("[%v]", float64(S)+0.75), ""},
			{"sample_size = +2200", fmt.Sprintf("[%v]", float64(S)+0.75), ""},
			{"sample_size = '-1'", fmt.Sprintf("[%v]", float64(S)+0.75), ""},
			{"sample_size=-1", fmt.Sprintf("[%v]", float64(S)+0.75), ""},
			// It does not: the refusal names the count it used.
			{"sample_size = 2199", "inferred from the file's first 2199 rows); sample_size = -1", "22P02"},
			{"sample_size = 1", "inferred from the file's first 1 rows); sample_size = -1", "22P02"},
			// Not a count.
			{"sample_size = 0", `sample_size must be a number of rows (1 or more) or -1 for every row, not "0"`, "22023"},
			{"sample_size = -2", `not "-2"`, "22023"},
			{"sample_size = 'many'", `not "many"`, "22023"},
		} {
			t.Run(kind+"/"+tc.opt, func(t *testing.T) {
				res, err := db.Query(ctx, fmt.Sprintf("SELECT SUM(a) FROM read_%s('%s', %s)", kind, path, tc.opt))
				if tc.state == "" {
					if err != nil || rdCells1(res) != tc.want {
						t.Fatalf("got %s %v, want %s", rdCells1(res), err, tc.want)
					}
					return
				}
				if sqlerr.StateOf(err) != tc.state || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("want %s %q, got %v", tc.state, tc.want, err)
				}
			})
		}
		// A LIMIT under -1 answers from the first rows; the inference read
		// every row first, so the type is the widened one.
		t.Run(kind+"/limit", func(t *testing.T) {
			res, err := db.Query(ctx, fmt.Sprintf("SELECT a FROM read_%s('%s', sample_size = -1) LIMIT 1", kind, path))
			if err != nil || rdCells1(res) != "[1]" || res.ColumnMetas == nil || res.ColumnMetas[0].TypeName != "FLOAT64" {
				t.Fatalf("got %s %+v %v", rdCells1(res), res, err)
			}
		})
	}
	// -1 reads the input twice, so an input that can be read once is refused.
	fifo := filepath.Join(dir, "once.csv")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	for _, kind := range []string{"csv", "json"} {
		_, err := db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_%s('%s', sample_size = -1)", kind, fifo))
		want := fmt.Sprintf("read_%s: sample_size = -1 reads the input twice, and %q is not a regular file", kind, fifo)
		if sqlerr.StateOf(err) != "0A000" || !strings.Contains(err.Error(), want) {
			t.Errorf("%s fifo: want 0A000 %q, got %v", kind, want, err)
		}
	}
}

// TestArcRDWholeInputGlob: `sample_size = -1` over a glob infers from every
// row of every file — the value in the SECOND file widens the column — and
// still reads each later CSV file's header as a header.
func TestArcRDWholeInputGlob(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var c1, j1 strings.Builder
	c1.WriteString("a\n")
	for i := 1; i <= 150; i++ {
		fmt.Fprintf(&c1, "%d\n", i)
		fmt.Fprintf(&j1, "{\"a\":%d}\n", i)
	}
	write("p1.csv", c1.String())
	write("p2.csv", "a\n1\n0.5\n")
	write("p1.json", j1.String())
	write("p2.json", "{\"a\":1}\n{\"a\":0.5,\"k\":\"late\"}\n")
	for _, tc := range []struct{ kind, sql, want string }{
		{"csv", "SELECT COUNT(*), SUM(a) FROM read_csv('%s/p*.csv', sample_size = -1)", "[152 11326.5]"},
		{"json", "SELECT COUNT(*), SUM(a), MAX(k) FROM read_json('%s/p*.json', sample_size = -1)", "[152 11326.5 late]"},
	} {
		res, err := db.Query(ctx, fmt.Sprintf(tc.sql, dir))
		if err != nil || rdCells1(res) != tc.want {
			t.Errorf("%s: got %s %v, want %s", tc.kind, rdCells1(res), err, tc.want)
		}
		// The default names the second file and its own row.
		_, err = db.Query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_%s('%s/p*.%s')", tc.kind, dir, tc.kind))
		if sqlerr.StateOf(err) == "" || !strings.Contains(err.Error(), filepath.Join(dir, "p2."+tc.kind)+" row 2") {
			t.Errorf("%s default: want the refusal naming p2 row 2, got %v", tc.kind, err)
		}
	}
}
