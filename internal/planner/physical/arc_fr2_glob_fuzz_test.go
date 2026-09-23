// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// FuzzArcFR2Glob reads a glob of one to three files cut from the fuzzer's
// bytes, through read_csv (with and without a header), read_json and
// read_parquet, on the plan-time schema read and on the execution. The
// property: the readers never panic, and every refusal of a CSV or JSON
// input carries a SQLSTATE — a malformed file is an error a client can
// classify, never a silent stop and never an internal failure.
//
// read_parquet's files are VALID Parquet whose column name and type the
// fuzzer picks, so the target exercises the glob's one-schema rule rather
// than the Parquet decoder, which has its own fuzz targets.
func FuzzArcFR2Glob(f *testing.F) {
	f.Add([]byte("a,b\n1,x\n"), []byte("a,b\n2,\"y\"\n"), []byte(""), uint8(0))
	f.Add([]byte("[{\"a\":1},{\"a\":2}]"), []byte("[{\"a\":3}]"), []byte("{\"a\":4}\n"), uint8(2))
	f.Add([]byte("a\n\"unterminated\n"), []byte("a\n1\n"), []byte("a\r\n2\r\n"), uint8(1))
	f.Add([]byte("{\"a\":[1]}\n{\"a\":[1.5]}\n"), []byte("[1,2]"), []byte("]"), uint8(2))
	f.Add([]byte("a"), []byte("b"), []byte("a"), uint8(3))
	f.Fuzz(func(t *testing.T, f1, f2, f3 []byte, mode uint8) {
		dir := t.TempDir()
		files := [][]byte{f1, f2, f3}[:1+int(mode>>4)%3]
		fn, ext, args := "read_json", "json", map[string]string(nil)
		switch mode % 4 {
		case 0:
			fn, ext = "read_csv", "csv"
		case 1:
			fn, ext, args = "read_csv", "csv", map[string]string{"header": "false"}
		case 3:
			fn, ext = "read_parquet", "parquet"
		}
		for i, body := range files {
			if ext == "parquet" {
				body = fr2FuzzParquet(t, body)
			}
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d.%s", i, ext)), body, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		path := filepath.Join(dir, "*."+ext)
		fargs := []string{path}
		if _, _, err := readReaderSchema(fn, fargs, args); err != nil && sqlerr.StateOf(err) == "" {
			t.Fatalf("the plan-time read refused with no SQLSTATE: %v", err)
		}
		src, err := buildTableFunctionSource(fn, fargs, args)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		err = src.Init(t.Context())
		for err == nil {
			var b interface{ ToRows() []map[string]any }
			bt, nerr := src.Next(t.Context())
			if nerr != nil {
				err = nerr
				break
			}
			if bt == nil {
				break
			}
			b = bt
			_ = b.ToRows()
		}
		if err != nil && ext != "parquet" && sqlerr.StateOf(err) == "" {
			t.Fatalf("%s refused with no SQLSTATE: %v", fn, err)
		}
	})
}

// fr2FuzzParquet writes a valid Parquet file whose one column the fuzzer's
// bytes name and type: the first byte picks bigint, double precision or
// text, the second the name (a or b), the rest are the values.
func fr2FuzzParquet(t *testing.T, body []byte) []byte {
	t.Helper()
	col := parquet.Column{Name: "a", Type: parquet.TypeInt64}
	if len(body) > 0 && body[0]%3 == 1 {
		col.Type = parquet.TypeFloat64
	} else if len(body) > 0 && body[0]%3 == 2 {
		col.Type = parquet.TypeString
	}
	if len(body) > 1 && body[1]%2 == 1 {
		col.Name = "b"
	}
	var rows []map[string]any
	for i := 2; i < len(body) && i < 64; i++ {
		var v any
		switch col.Type {
		case parquet.TypeInt64:
			v = int64(body[i])
		case parquet.TypeFloat64:
			v = float64(body[i]) / 3
		default:
			v = string(body[i : i+1])
		}
		rows = append(rows, map[string]any{col.Name: v})
	}
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{col}}, parquet.DefaultWriterConfig())
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
	return buf.Bytes()
}
