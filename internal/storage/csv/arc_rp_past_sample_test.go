// SPDX-License-Identifier: MIT

package csv

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

type rpReader interface {
	Next() (*batch.RecordBatch, error)
}

func rpOpen(tb testing.TB, path string, data []byte) (rpReader, error) {
	tb.Helper()
	switch path {
	case "eager":
		return NewReader(data, DefaultConfig())
	case "stream":
		return NewStreamReader(bytes.NewReader(data), DefaultConfig())
	case "stream_no_header":
		// The same rows without their header line, so the row numbers
		// match; the column is then named col0.
		cfg := DefaultConfig()
		cfg.HasHeader = false
		return NewStreamReader(bytes.NewReader(data[bytes.IndexByte(data, '\n')+1:]), cfg)
	}
	tb.Fatalf("unknown path %q", path)
	return nil, nil
}

func rpFixture(tb testing.TB, n, change int, first, last string) []byte {
	tb.Helper()
	var b strings.Builder
	b.WriteString("a,b\n")
	for i := 1; i <= n; i++ {
		value := first
		if first == "sequence" {
			value = fmt.Sprint(i)
		}
		if i == change {
			value = last
		}
		fmt.Fprintf(&b, "%s,x\n", value)
	}
	return []byte(b.String())
}

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

// TestArcRPPastSample: a field past the 100-row sample that does not parse as
// the inferred type is a 22P02 naming the row, the column, the value and both
// types, on the eager, the streaming and the header-less streaming reader, at
// row 101 (the first past the sample), 2049 and 2200 (past one 2048-row
// batch). At 16b924d1 these fields read NULL with the row counted. A text
// column holds every field; an empty field is NULL, as it always was.
func TestArcRPPastSample(t *testing.T) {
	cells := []struct {
		name, first, last, want string
	}{
		{"int_float", "7", "0.75", `value "0.75" (double precision) is not of type bigint`},
		{"int_string", "7", "oops", `value "oops" (text) is not of type bigint`},
		{"int_bool", "7", "true", `value "true" (boolean) is not of type bigint`},
		{"int_overflow", "7", "99999999999999999999", `value "99999999999999999999" is out of range for type bigint`},
		{"float_string", "1.25", "oops", `value "oops" (text) is not of type double precision`},
		{"float_bool", "1.25", "false", `value "false" (boolean) is not of type double precision`},
		{"bool_string", "true", "oops", `value "oops" (text) is not of type boolean`},
		{"timestamp_string", "2024-01-02", "oops", `value "oops" (text) is not of type timestamp`},
		{"ipv4_string", "10.0.0.1", "oops", `value "oops" (text) is not of type inet`},
		// A text column holds every field; a double precision column a
		// whole number.
		{"string_number", "oops", "42", ""},
		{"float_int", "1.25", "3", ""},
	}
	for _, path := range []string{"eager", "stream", "stream_no_header"} {
		for _, row := range []int{101, 2049, 2200} {
			for _, cell := range cells {
				t.Run(fmt.Sprintf("%s/%d/%s", path, row, cell.name), func(t *testing.T) {
					n, _, _, err := rpRead(t, path, rpFixture(t, row, row, cell.first, cell.last))
					if cell.want == "" {
						if err != nil || n != row {
							t.Fatalf("rows=%d err=%v, want %d rows", n, err, row)
						}
						return
					}
					code := "22P02"
					switch cell.name {
					case "int_overflow":
						code = "22003"
					case "timestamp_string":
						code = "22007"
					}
					if sqlerr.StateOf(err) != code {
						t.Fatalf("rows=%d err=%v, want %s", n, err, code)
					}
					column := `column "a"`
					if path == "stream_no_header" {
						column = `column "col0"`
					}
					want := []string{fmt.Sprintf("row %d %s: ", row, column), cell.want}
					if code != "22003" {
						want = append(want, "inferred from the file's first 100 rows")
					}
					for _, part := range want {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q in: %v", part, err)
						}
					}
				})
			}
		}
		t.Run(path+"/controls", func(t *testing.T) {
			n, sum, _, err := rpRead(t, path, rpFixture(t, 5000, 0, "sequence", ""))
			if err != nil || n != 5000 || sum != 12502500 {
				t.Fatalf("conforming: rows=%d sum=%d err=%v", n, sum, err)
			}
			n, _, nulls, err := rpRead(t, path, rpFixture(t, 2200, 2200, "7", ""))
			if err != nil || n != 2200 || nulls != 1 {
				t.Fatalf("empty field: rows=%d nulls=%d err=%v", n, nulls, err)
			}
			// Inside the sample the inference widens as before.
			n, _, _, err = rpRead(t, path, rpFixture(t, 2200, 100, "7", "0.75"))
			if err != nil || n != 2200 {
				t.Fatalf("widened: rows=%d err=%v", n, err)
			}
		})
	}
}

// TestArcRPCSVFieldGrammar: past the sample a field is read with PostgreSQL's
// input function for the column's type, so it refuses exactly what COPY
// refuses, with COPY's SQLSTATE. Every row is PostgreSQL 17.11's answer for
// the text (pg_input_is_valid / pg_input_error_info, measured into
// tooling/arcs/rp_reader_past_sample/rp_finisher/pg_input_functions_17.11.txt).
// At f58a653e ' 5', 't', 'of', '0x1F', '1_000', ' 1.5' refused and the
// overflows were 22P02.
func TestArcRPCSVFieldGrammar(t *testing.T) {
	cells := []struct {
		first, field string
		want         string // the value read, or the SQLSTATE
	}{
		{"7", " 5", "5"}, {"7", "5 ", "5"}, {"7", "+5", "5"}, {"7", "017", "17"}, {"7", "1_000", "1000"},
		{"7", "0x1F", "31"}, {"7", "0o17", "15"}, {"7", "0b101", "5"}, {"7", "-9223372036854775808", "-9223372036854775808"},
		{"7", "1e3", "22P02"}, {"7", "1.0", "22P02"}, {"7", "_1", "22P02"}, {"7", "0x", "22P02"},
		{"7", "12345678901234567890", "22003"}, {"7", "9223372036854775808", "22003"}, {"7", "0x8000000000000000", "22003"},
		{"1.25", " 1.5", "1.5"}, {"1.25", ".5", "0.5"}, {"1.25", "inf", "+Inf"}, {"1.25", "-Infinity", "-Inf"}, {"1.25", "NaN", "NaN"},
		{"1.25", "0x1p-2", "0.25"}, {"1.25", "0x10", "16"},
		{"1.25", "1e999", "22003"}, {"1.25", "1e-400", "22003"}, {"1.25", "1_000", "22P02"}, {"1.25", "1,5", "22P02"},
		{"true", "t", "true"}, {"true", "tr", "true"}, {"true", " true", "true"}, {"true", "y", "true"}, {"true", "on", "true"},
		{"true", "of", "false"}, {"true", "n", "false"}, {"true", "0", "false"},
		{"true", "o", "22P02"}, {"true", "01", "22P02"}, {"true", "truex", "22P02"},
		{"2024-01-02", " 2024-01-02", "2024-01-02"}, {"2024-01-02", "nope", "22007"},
		{"10.0.0.1", " 10.0.0.1", "22P02"}, {"10.0.0.1", "256.0.0.1", "22P02"},
	}
	for _, cell := range cells {
		t.Run(cell.first+"/"+cell.field, func(t *testing.T) {
			var b strings.Builder
			b.WriteString("a\n")
			for i := 0; i < 100; i++ {
				b.WriteString(cell.first + "\n")
			}
			fmt.Fprintf(&b, "%q\n", cell.field)
			r, err := NewStreamReader(strings.NewReader(b.String()), DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			var got string
			for {
				rb, err := r.Next()
				if err != nil {
					got = sqlerr.StateOf(err)
					break
				}
				if rb == nil {
					break
				}
				v := rb.Columns[0]
				switch {
				case v.Nulls.IsNull(rb.Len - 1):
					got = "NULL"
				case len(v.Int64Data) > 0 && rb.Schema[0].Type == parquet.TypeTimestamp:
					got = time.UnixMicro(v.Int64Data[rb.Len-1]).UTC().Format("2006-01-02")
				case len(v.Int64Data) > 0:
					got = fmt.Sprint(v.Int64Data[rb.Len-1])
				case len(v.Float64Data) > 0:
					got = fmt.Sprint(v.Float64Data[rb.Len-1])
				case len(v.BoolData) > 0:
					got = fmt.Sprint(v.BoolData[rb.Len-1])
				}
			}
			if got != cell.want {
				t.Fatalf("field %q in a column sampled from %q: got %s, want %s", cell.field, cell.first, got, cell.want)
			}
		})
	}
}
