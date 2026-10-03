// SPDX-License-Identifier: MIT

package csv

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/fileinput"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func rdInputs(t *testing.T, bodies ...string) []fileinput.Input {
	t.Helper()
	dir := t.TempDir()
	var inputs []fileinput.Input
	for i, body := range bodies {
		p := filepath.Join(dir, fmt.Sprintf("f%d.csv", i+1))
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		name := ""
		if len(bodies) > 1 {
			name = filepath.Base(p)
		}
		inputs = append(inputs, fileinput.Input{Name: name, Open: func() (io.ReadCloser, error) { return os.Open(p) }})
	}
	return inputs
}

func rdDrain(r *Reader) (int, error) {
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

// rdColumn writes header a and rows 1..n of a, with row `at` = past.
func rdColumn(n, at int, past string) string {
	var b strings.Builder
	b.WriteString("id,a\n")
	for i := 1; i <= n; i++ {
		v := fmt.Sprint(i)
		if i == at {
			v = past
		}
		fmt.Fprintf(&b, "%d,%s\n", i, v)
	}
	return b.String()
}

// TestArcRDSampleSize (#1242): ReaderConfig.SampleSize — a positive count
// samples that many rows, WholeInput every row in a first pass that keeps
// none of them. The inference of the whole input is the SAME promotion the
// sample uses, so the past-sample value widens exactly as it would inside
// the sample: int then 0.75 is double precision, int then `abc` text, int
// then `true` text; an all-NULL head followed by 5 is bigint.
func TestArcRDSampleSize(t *testing.T) {
	for _, tc := range []struct {
		past string
		want parquet.TypeID
	}{
		{"0.75", parquet.TypeFloat64},
		{"abc", parquet.TypeString},
		{"true", parquet.TypeString},
		{"2024-01-02", parquet.TypeString},
		{`""`, parquet.TypeString},
		{"9223372036854775808", parquet.TypeFloat64},
		{`"5"`, parquet.TypeInt64},
		{"", parquet.TypeInt64},
	} {
		for _, at := range []int{101, 2049, 2200} {
			t.Run(fmt.Sprintf("%s/%d", tc.past, at), func(t *testing.T) {
				inputs := rdInputs(t, rdColumn(at, at, tc.past))
				cfg := DefaultConfig()
				cfg.SampleSize = WholeInput
				r, err := NewFilesReader(inputs, cfg)
				if err != nil {
					t.Fatal(err)
				}
				if got := r.Schema()[1].Type; got != tc.want {
					t.Fatalf("whole input: a is %s, want %s", got, tc.want)
				}
				if len(r.rows) != 0 {
					t.Fatalf("a whole-input reader buffered %d rows", len(r.rows))
				}
				if n, err := rdDrain(r); err != nil || n != at {
					t.Fatalf("read %d rows, %v", n, err)
				}
				// A count that reaches the row agrees with every row; one that
				// stops before it refuses naming the count.
				cfg.SampleSize = at
				r, err = NewFilesReader(inputs, cfg)
				if err != nil || r.Schema()[1].Type != tc.want {
					t.Fatalf("sample %d: %v %v", at, r.Schema(), err)
				}
				cfg.SampleSize = at - 1
				r, _ = NewFilesReader(inputs, cfg)
				_, err = rdDrain(r)
				if tc.want == parquet.TypeInt64 {
					if err != nil {
						t.Fatalf("sample %d: %v", at-1, err)
					}
					return
				}
				if sqlerr.StateOf(err) == "" || !strings.Contains(err.Error(), fmt.Sprintf("first %d rows); sample_size = -1 infers it from every row", at-1)) {
					t.Fatalf("sample %d: %v", at-1, err)
				}
			})
		}
	}
	// An all-NULL head: text by default, the value's type over every row.
	var b strings.Builder
	b.WriteString("a\n")
	for i := 1; i < 2200; i++ {
		b.WriteString("\n")
	}
	b.WriteString("5\n")
	for _, tc := range []struct {
		size int
		want parquet.TypeID
	}{{0, parquet.TypeString}, {WholeInput, parquet.TypeInt64}} {
		cfg := DefaultConfig()
		cfg.SampleSize = tc.size
		r, err := NewFilesReader(rdInputs(t, b.String()), cfg)
		if err != nil || r.Schema()[0].Type != tc.want {
			t.Errorf("all-NULL head, sample %d: %v %v", tc.size, r.Schema(), err)
		}
	}
}

// TestArcRDWholeInputShapes: a whole-input pass over two files with
// headers (the second file's header is a header, its value widens the
// column), header=false, a header-only file, and an empty input.
func TestArcRDWholeInputShapes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleSize = WholeInput
	r, err := NewFilesReader(rdInputs(t, rdColumn(150, 0, ""), "id,a\n151,0.5\n"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Schema()[1].Type != parquet.TypeFloat64 {
		t.Fatalf("glob: %v", r.Schema())
	}
	if n, err := rdDrain(r); err != nil || n != 151 {
		t.Fatalf("glob: %d %v", n, err)
	}
	noHeader := cfg
	noHeader.HasHeader = false
	r, err = NewFilesReader(rdInputs(t, "1,x\n2,y\n3.5,z\n"), noHeader)
	if err != nil || len(r.Schema()) != 2 || r.Schema()[0].Name != "col0" || r.Schema()[0].Type != parquet.TypeFloat64 {
		t.Fatalf("header=false: %v %v", r.Schema(), err)
	}
	if n, err := rdDrain(r); err != nil || n != 3 {
		t.Fatalf("header=false: %d %v", n, err)
	}
	r, err = NewFilesReader(rdInputs(t, "id,a\n"), cfg)
	if err != nil || len(r.Schema()) != 2 || r.Schema()[1].Type != parquet.TypeString {
		t.Fatalf("header only: %v %v", r.Schema(), err)
	}
	r, err = NewFilesReader(rdInputs(t, ""), cfg)
	if err != nil || len(r.Schema()) != 0 {
		t.Fatalf("empty: %v %v", r.Schema(), err)
	}
	// A record of the wrong width is refused at the first pass.
	_, err = NewFilesReader(rdInputs(t, "id,a\n1,2\n3\n"), cfg)
	if sqlerr.StateOf(err) != "22P04" {
		t.Fatalf("short record: %v", err)
	}
}

// TestArcRDPlannedSchemaChecksEveryRow: the execution of a whole-input read
// takes the plan's schema and does not infer again; an input changed after
// the plan read it is refused, and the message says the type came from every
// row (and offers no sample_size, which was already -1).
func TestArcRDPlannedSchemaChecksEveryRow(t *testing.T) {
	schema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64, Nullable: true}, {Name: "a", Type: parquet.TypeInt64, Nullable: true}}
	r := NewFilesReaderWithSchema(rdInputs(t, rdColumn(3000, 2500, "x")), DefaultConfig(), schema)
	_, err := rdDrain(r)
	want := `row 2500 column "a": value "x" (text) is not of type bigint (the column's type was inferred from every row of the input)`
	if sqlerr.StateOf(err) != "22P02" || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "sample_size") {
		t.Fatalf("want %q, got %v", want, err)
	}
}
