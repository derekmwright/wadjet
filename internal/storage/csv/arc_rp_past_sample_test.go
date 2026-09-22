// SPDX-License-Identifier: MIT

package csv

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
		{"int_overflow", "7", "99999999999999999999", `value "99999999999999999999" (double precision) is not of type bigint`},
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
					if sqlerr.StateOf(err) != "22P02" {
						t.Fatalf("rows=%d err=%v, want 22P02", n, err)
					}
					column := `column "a"`
					if path == "stream_no_header" {
						column = `column "col0"`
					}
					for _, part := range []string{fmt.Sprintf("row %d %s: ", row, column), cell.want, "inferred from the file's first 100 rows"} {
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
