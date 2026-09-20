// SPDX-License-Identifier: MIT
package csv

import (
	"bytes"
	"fmt"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"strings"
	"testing"
)

type rpReader interface {
	Next() (*batch.RecordBatch, error)
}

func rpOpen(tb testing.TB, path string, data []byte) (rpReader, error) {
	tb.Helper()
	switch path {
	case "eager":
		return NewReader(data, DefaultConfig())
	default:
		return NewStreamReader(bytes.NewReader(data), DefaultConfig())
	}
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
func TestArcRPPastSample(t *testing.T) {
	for _, path := range []string{"eager", "stream"} {
		for _, row := range []int{101, 2049, 2200} {
			for _, cell := range []struct {
				name, first, last string
				accepts           bool
			}{
				{"int_float", "7", "0.75", false}, {"int_string", "7", "oops", false}, {"int_bool", "7", "true", false}, {"float_string", "1.25", "oops", false}, {"bool_string", "true", "oops", false}, {"string_number", "oops", "42", true},
			} {
				t.Run(fmt.Sprintf("%s/%d/%s", path, row, cell.name), func(t *testing.T) {
					n, _, _, err := rpRead(t, path, rpFixture(t, row, row, cell.first, cell.last))
					if cell.accepts {
						if err != nil || n != row {
							t.Fatalf("rows=%d err=%v", n, err)
						}
						return
					}
					if sqlerr.StateOf(err) != "22P02" {
						t.Fatalf("want 22P02, got %v", err)
					}
					for _, part := range []string{fmt.Sprintf("row %d", row), `column "a"`, "read_csv", "first 100 rows"} {
						if !strings.Contains(err.Error(), part) {
							t.Errorf("missing %q: %v", part, err)
						}
					}
				})
			}
		}
		t.Run(path+"/controls", func(t *testing.T) {
			n, sum, _, err := rpRead(t, path, rpFixture(t, 5000, 0, "sequence", ""))
			if err != nil || n != 5000 || sum != 12502500 {
				t.Fatalf("rows=%d sum=%d err=%v", n, sum, err)
			}
			n, _, nulls, err := rpRead(t, path, rpFixture(t, 2200, 2200, "7", ""))
			if err != nil || n != 2200 || nulls != 1 {
				t.Fatalf("rows=%d nulls=%d err=%v", n, nulls, err)
			}
		})
	}
}
