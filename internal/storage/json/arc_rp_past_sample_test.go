// SPDX-License-Identifier: MIT

package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// rpPaths are every JSON read path: the eager row reader (from bytes, from a
// stream, with coercion), the eager columnar reader (from bytes, from a
// stream) and the incremental StreamReader read_json runs on.
var rpPaths = []string{"eager", "eager_stream", "coercion", "columnar", "columnar_stream", "stream"}

type rpReader interface {
	Next() (*batch.RecordBatch, error)
}

func rpOpen(tb testing.TB, path string, data []byte) (rpReader, error) {
	tb.Helper()
	switch path {
	case "eager":
		return NewReaderFromBytes(data)
	case "eager_stream":
		return NewReaderFromStream(bytes.NewReader(data))
	case "coercion":
		return NewReaderFromBytesWithCoercion(data)
	case "columnar":
		return NewColumnarReader(data)
	case "columnar_stream":
		return NewColumnarReaderFromStream(bytes.NewReader(data))
	case "stream":
		return NewStreamReader(bytes.NewReader(data))
	}
	tb.Fatalf("unknown path %q", path)
	return nil, nil
}

// rpFixture writes n JSONL rows {"a":<first>} (first == "sequence" writes
// 1..n) with row `change` holding `last` instead.
func rpFixture(tb testing.TB, n, change int, first, last string) []byte {
	tb.Helper()
	var b strings.Builder
	for i := 1; i <= n; i++ {
		value := first
		if first == "sequence" {
			value = fmt.Sprint(i)
		}
		if i == change {
			value = last
		}
		fmt.Fprintf(&b, "{\"a\":%s}\n", value)
	}
	return []byte(b.String())
}

// rpRead drains a reader and answers the row count, the sum of column 0
// when it is bigint, and its NULL count.
func rpRead(tb testing.TB, path string, data []byte) (int, int64, int, error) {
	tb.Helper()
	r, err := rpOpen(tb, path, data)
	if err != nil {
		return 0, 0, 0, err
	}
	count, nulls := 0, 0
	var sum int64
	for {
		b, err := r.Next()
		if err != nil {
			return count, sum, nulls, err
		}
		if b == nil {
			break
		}
		for i := 0; i < b.Len; i++ {
			count++
			if b.Columns[0].Nulls.IsNull(i) {
				nulls++
				continue
			}
			if len(b.Columns[0].Int64Data) > 0 {
				sum += b.Columns[0].Int64Data[i]
			}
		}
	}
	return count, sum, nulls, nil
}

// rpCell is one type change: 100+ rows of `first`, then `last`. want lists
// the fragments the 22P02 must carry (the value, its type, the column's); an
// empty want means the column accepts the value.
type rpCell struct {
	name, first, last string
	want              []string
}

var rpCells = []rpCell{
	{"int_float", "7", "0.75", []string{"value 0.75 (double precision) is not of type bigint"}},
	{"int_string", "7", `"oops"`, []string{`value "oops" (text) is not of type bigint`}},
	{"int_bool", "7", "true", []string{"value true (boolean) is not of type bigint"}},
	// The eager row reader has already decoded the number to float64, so
	// only the types are pinned here.
	{"int_overflow", "7", "99999999999999999999", []string{"(double precision) is not of type bigint"}},
	{"int_object", "7", `{"b":1}`, []string{`value {"b":1} (object) is not of type bigint`}},
	{"float_string", "1.25", `"oops"`, []string{`value "oops" (text) is not of type double precision`}},
	{"float_bool", "1.25", "false", []string{"value false (boolean) is not of type double precision"}},
	{"bool_string", "true", `"oops"`, []string{`value "oops" (text) is not of type boolean`}},
	{"bool_number", "true", "1", []string{"value 1 (bigint) is not of type boolean"}},
	{"timestamp_string", `"2024-01-02"`, `"oops"`, []string{`value "oops" (text) is not of type timestamp`}},
	{"timestamp_number", `"2024-01-02"`, "5", []string{"value 5 (bigint) is not of type timestamp"}},
	{"ipv4_string", `"10.0.0.1"`, `"oops"`, []string{`value "oops" (text) is not of type inet`}},
	// A string column holds every value as its text, in every reader.
	{"string_number", `"oops"`, "42", nil},
	{"string_bool", `"oops"`, "true", nil},
	// A double precision column holds a whole number.
	{"float_int", "1.25", "3", nil},
}

// TestArcRPPastSample: a non-NULL value past the 100-row sample that does not
// fit the inferred type is a 22P02 naming the row, the column, the value and
// both types, on every read path, at row 101 (the first row past the
// sample), 2049 (the first row of the second 2048-row batch) and 2200. At
// 16b924d1 these cells read NULL (or 0) with the row counted, or failed as a
// recovered index-out-of-range.
func TestArcRPPastSample(t *testing.T) {
	for _, path := range rpPaths {
		for _, row := range []int{101, 2049, 2200} {
			for _, cell := range rpCells {
				t.Run(fmt.Sprintf("%s/%d/%s", path, row, cell.name), func(t *testing.T) {
					n, _, _, err := rpRead(t, path, rpFixture(t, row, row, cell.first, cell.last))
					if cell.want == nil {
						if err != nil || n != row {
							t.Fatalf("rows=%d err=%v, want %d rows", n, err, row)
						}
						return
					}
					if sqlerr.StateOf(err) != "22P02" {
						t.Fatalf("rows=%d err=%v, want 22P02", n, err)
					}
					want := append([]string{fmt.Sprintf("row %d column \"a\": ", row), "inferred from the file's first 100 rows"}, cell.want...)
					for _, part := range want {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q in: %v", part, err)
						}
					}
				})
			}
		}
		t.Run(path+"/controls", func(t *testing.T) {
			// A conforming file reads every value.
			n, sum, _, err := rpRead(t, path, rpFixture(t, 5000, 0, "sequence", ""))
			if err != nil || n != 5000 || sum != 12502500 {
				t.Fatalf("conforming: rows=%d sum=%d err=%v", n, sum, err)
			}
			// null past the sample is NULL, as it always was.
			n, _, nulls, err := rpRead(t, path, rpFixture(t, 2200, 2200, "7", "null"))
			if err != nil || n != 2200 || nulls != 1 {
				t.Fatalf("null: rows=%d nulls=%d err=%v", n, nulls, err)
			}
			// Inside the sample the inference widens as it always did: a
			// 0.75 at row 100 makes the column double precision.
			n, _, _, err = rpRead(t, path, rpFixture(t, 2200, 100, "7", "0.75"))
			if err != nil || n != 2200 {
				t.Fatalf("widened: rows=%d err=%v", n, err)
			}
		})
	}
}

// TestArcRPPastSampleNested: past the sample, an ARRAY's elements and a
// ROW's fields are checked the way a scalar column is (the columnar readers
// are the only ones that infer nested types).
func TestArcRPPastSampleNested(t *testing.T) {
	cells := []struct {
		name, first, last, want string
	}{
		{"array_element", "[1,2]", `[1,"x"]`, `row 101 column "a" element: value "x" (text) is not of type bigint`},
		{"row_field", `{"x":1,"y":"s"}`, `{"x":"bad","y":"s"}`, `row 101 column "a" field "x": value "bad" (text) is not of type bigint`},
		{"array_scalar", "[1,2]", "5", `row 101 column "a": value 5 (bigint) is not of type array`},
		{"row_array", `{"x":1}`, "[1]", `row 101 column "a": value [1] (array) is not of type record`},
	}
	for _, path := range []string{"columnar", "columnar_stream", "stream"} {
		for _, cell := range cells {
			t.Run(path+"/"+cell.name, func(t *testing.T) {
				_, _, _, err := rpRead(t, path, rpFixture(t, 101, 101, cell.first, cell.last))
				if sqlerr.StateOf(err) != "22P02" || !strings.Contains(err.Error(), cell.want) {
					t.Fatalf("got %v, want 22P02 containing %q", err, cell.want)
				}
			})
		}
		t.Run(path+"/conforming", func(t *testing.T) {
			n, _, _, err := rpRead(t, path, rpFixture(t, 300, 300, `{"x":1,"y":"s"}`, `{"x":2,"y":"t"}`))
			if err != nil || n != 300 {
				t.Fatalf("rows=%d err=%v", n, err)
			}
		})
	}
}

// TestArcRPByteCappedSample: the StreamReader stops sampling at
// maxSampleBytes, so a file whose first 100 objects are large is inferred
// from FEWER rows. A row past that shorter sample is past the sample: at
// f58a653e an int column meeting 0.75 at row 70 read 0 there, with the row
// counted, because the check assumed a sample of 100.
func TestArcRPByteCappedSample(t *testing.T) {
	pad := strings.Repeat("x", 200<<10)
	for _, cell := range []struct{ name, last, want string }{
		{"float", "0.75", "value 0.75 (double precision) is not of type bigint"},
		{"string", `"oops"`, `value "oops" (text) is not of type bigint`},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var b strings.Builder
			for i := 1; i <= 80; i++ {
				v := "1"
				if i == 70 {
					v = cell.last
				}
				fmt.Fprintf(&b, "{\"a\":%s,\"pad\":\"%s\"}\n", v, pad)
			}
			sr, err := NewStreamReader(strings.NewReader(b.String()))
			if err != nil {
				t.Fatal(err)
			}
			for {
				rb, err := sr.Next()
				if err != nil {
					if sqlerr.StateOf(err) != "22P02" {
						t.Fatalf("got %v, want 22P02", err)
					}
					for _, part := range []string{`row 70 column "a": `, cell.want} {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q in: %v", part, err)
						}
					}
					// The message states the sample it was inferred from,
					// which the fixture holds under row 70.
					m := regexp.MustCompile(`first (\d+) rows`).FindStringSubmatch(err.Error())
					if m == nil {
						t.Fatalf("no sample size in: %v", err)
					}
					if n, _ := strconv.Atoi(m[1]); n < 1 || n >= 70 {
						t.Fatalf("sample of %d rows; the fixture must cap it under row 70", n)
					}
					return
				}
				if rb == nil {
					t.Fatal("read every row; want a 22P02 at row 70")
				}
			}
		})
	}
}

// FuzzArcRPPastSample varies the row at which the type changes, the column's
// type (the first value) and the value it changes to (any valid JSON): a read
// never raises a runtime error, and answers either every row or the 22P02 —
// the same answer on every read path.
func FuzzArcRPPastSample(f *testing.F) {
	firsts := []string{"7", "1.25", "true", `"oops"`, `"2024-01-02"`, `"10.0.0.1"`, "[1,2]", `{"x":1}`}
	for _, cell := range rpCells {
		f.Add(uint16(0), uint8(0), cell.last)
	}
	f.Add(uint16(1948), uint8(6), `[1,"x"]`)
	f.Add(uint16(2099), uint8(7), `{"x":"s"}`)
	f.Fuzz(func(t *testing.T, position uint16, first uint8, last string) {
		if !json.Valid([]byte(last)) || strings.ContainsAny(last, "\n\r") {
			t.Skip()
		}
		row := 101 + int(position)%2200
		data := rpFixture(t, row, row, firsts[int(first)%len(firsts)], last)
		refused := map[string]bool{}
		for _, path := range []string{"coercion", "columnar", "stream"} {
			n, _, _, err := rpRead(t, path, data)
			switch {
			case err == nil:
				if n != row {
					t.Fatalf("%s: %d rows, want %d", path, n, row)
				}
			case sqlerr.StateOf(err) == "22P02":
				refused[path] = true
			default:
				t.Fatalf("%s: %v, want rows or 22P02", path, err)
			}
		}
		// The two columnar readers share one scanner and must agree; the
		// eager coercion reader infers no nested types (a nested column is
		// text there), so it is held only to rows-or-22P02.
		if refused["columnar"] != refused["stream"] {
			t.Fatalf("columnar and stream readers disagree: %v", refused)
		}
	})
}
