// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestArcFR2AParquetGlobReadsEveryFileByItsOwnFooter reads a glob of two
// files PyArrow 25 wrote (testdata/fr2_pyarrow_glob: several row groups, the
// second file's columns in the reverse order) and holds
// the answer to two things: the values PyArrow was given, and each file read
// alone. At 962117da the glob was the files' bytes concatenated and every
// read failed with `invalid magic` (#1240).
func TestArcFR2AParquetGlobReadsEveryFileByItsOwnFooter(t *testing.T) {
	dir := filepath.Join("testdata", "fr2_pyarrow_glob")
	read := func(path string) []map[string]any {
		t.Helper()
		src, err := buildTableFunctionSource("read_parquet", []string{path}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := src.Init(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		return drainTableFunc(t, src)
	}
	got := read(filepath.Join(dir, "*.parquet"))
	if len(got) != 11 {
		t.Fatalf("rows = %d, want 11", len(got))
	}
	for i, r := range got {
		id := int64(i + 1)
		name := any(fmt.Sprintf("n%d", id))
		if id%4 == 0 {
			name = nil
		}
		want := fmt.Sprint(map[string]any{
			"id": id, "score": float64(id) * 1.5, "name": name, "ok": id%2 == 0,
			"amt": fmt.Sprintf("%d.%02d", id/100, id%100), "tags": []any{fmt.Sprintf("t%d", id), fmt.Sprintf("u%d", id)},
		})
		if g := fmt.Sprint(map[string]any{
			"id": r["id"], "score": r["score"], "name": r["name"], "ok": r["ok"],
			"amt": fmt.Sprint(r["amt"]), "tags": r["tags"],
		}); g != want {
			t.Fatalf("row %d = %s, PyArrow wrote %s", i, g, want)
		}
	}
	var alone []map[string]any
	for _, f := range []string{"part-000.parquet", "part-001.parquet"} {
		alone = append(alone, read(filepath.Join(dir, f))...)
	}
	if fmt.Sprint(alone) != fmt.Sprint(got) {
		t.Fatalf("the glob and its files read alone disagree:\n glob %v\nalone %v", got, alone)
	}
}

// TestArcFR2AGlobHoldsOneFileOpenAtATime: a reader over a 100-file glob opens
// each file when it reaches it and closes it before the next — it never
// buffers every file (as fetchGlob did for Parquet) nor holds them all open.
// Counted from /proc/self/fd after every batch, for every format.
func TestArcFR2AGlobHoldsOneFileOpenAtATime(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd")
	}
	for _, f := range []struct{ fn, ext string }{{"read_csv", "csv"}, {"read_json", "json"}, {"read_parquet", "parquet"}} {
		t.Run(f.fn, func(t *testing.T) {
			dir := t.TempDir()
			for i := 0; i < 100; i++ {
				var body []byte
				switch f.ext {
				case "csv":
					body = []byte(fmt.Sprintf("a\n%d\n%d\n", i, i+1000))
				case "json":
					body = []byte(fmt.Sprintf("[{\"a\":%d},{\"a\":%d}]", i, i+1000))
				case "parquet":
					var buf bytes.Buffer
					w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{{Name: "a", Type: parquet.TypeInt64}}}, parquet.DefaultWriterConfig())
					if err != nil {
						t.Fatal(err)
					}
					if err := w.WriteRows([]map[string]any{{"a": int64(i)}, {"a": int64(i + 1000)}}); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					body = buf.Bytes()
				}
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("p%03d.%s", i, f.ext)), body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			openInDir := func() int {
				ents, _ := os.ReadDir("/proc/self/fd")
				n := 0
				for _, e := range ents {
					if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(target, dir) {
						n++
					}
				}
				return n
			}
			src, err := buildTableFunctionSource(f.fn, []string{filepath.Join(dir, "*."+f.ext)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := src.Init(t.Context()); err != nil {
				t.Fatal(err)
			}
			maxOpen, rows := openInDir(), 0
			for {
				b, err := src.Next(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				maxOpen = max(maxOpen, openInDir())
				if b == nil {
					break
				}
				rows += b.Len
			}
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if rows != 200 {
				t.Fatalf("rows = %d, want 200", rows)
			}
			if maxOpen > 1 {
				t.Fatalf("%d files of the glob open at once, want at most 1", maxOpen)
			}
			if n := openInDir(); n != 0 {
				t.Fatalf("%d files still open after Close", n)
			}
		})
	}
}

// BenchmarkArcFR2Glob100 reads a 100-file glob of 2000 rows per file through
// each reader: the memory and time a glob costs now that it is a sequence of
// files rather than one concatenated stream (JSON arrays / NDJSON, CSV) or
// one concatenated buffer (Parquet, which could not be read at all).
func BenchmarkArcFR2Glob100(b *testing.B) {
	for _, f := range []struct{ name, fn, ext string }{
		{"csv", "read_csv", "csv"}, {"ndjson", "read_json", "json"}, {"parquet", "read_parquet", "parquet"},
	} {
		b.Run(f.name, func(b *testing.B) {
			dir := b.TempDir()
			for i := 0; i < 100; i++ {
				var body bytes.Buffer
				switch f.ext {
				case "csv":
					body.WriteString("id,name,score\n")
					for r := 0; r < 2000; r++ {
						fmt.Fprintf(&body, "%d,n%d,%d.5\n", i*2000+r, r, r)
					}
				case "json":
					for r := 0; r < 2000; r++ {
						fmt.Fprintf(&body, "{\"id\":%d,\"name\":\"n%d\",\"score\":%d.5}\n", i*2000+r, r, r)
					}
				case "parquet":
					rows := make([]map[string]any, 2000)
					for r := range rows {
						rows[r] = map[string]any{"id": int64(i*2000 + r), "name": fmt.Sprintf("n%d", r), "score": float64(r) + 0.5}
					}
					w, err := parquet.NewWriter(&body, parquet.Schema{Columns: []parquet.Column{
						{Name: "id", Type: parquet.TypeInt64}, {Name: "name", Type: parquet.TypeString}, {Name: "score", Type: parquet.TypeFloat64},
					}}, parquet.DefaultWriterConfig())
					if err != nil {
						b.Fatal(err)
					}
					if err := w.WriteRows(rows); err != nil {
						b.Fatal(err)
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("p%03d.%s", i, f.ext)), body.Bytes(), 0o600); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src, err := buildTableFunctionSource(f.fn, []string{filepath.Join(dir, "*."+f.ext)}, nil)
				if err != nil {
					b.Fatal(err)
				}
				if err := src.Init(b.Context()); err != nil {
					b.Fatal(err)
				}
				rows := 0
				for {
					bt, err := src.Next(b.Context())
					if err != nil {
						b.Fatal(err)
					}
					if bt == nil {
						break
					}
					rows += bt.Len
				}
				src.Close()
				if rows != 200000 {
					b.Fatalf("rows = %d", rows)
				}
			}
		})
	}
}

// TestArcFR2AParquetReadIsBoundedByARowGroup (review B3): read_parquet
// decodes a member ROW GROUP by row group and hands each on before the next
// is decoded. Through FR2 round 1 a member was decoded whole into one merged
// batch, so peak RSS followed the FILE: a 500 MB file (100 row groups of
// 5,000 rows × 1 KiB) held 2.7 GB and a two-file 1 GB glob 2.84 GiB.
//
// The fixture is the review's shape (5,000-row row groups, a 1 KiB string
// per row, uncompressed) at 40 row groups — a 205 MB file whose whole decode
// is ~1.1 GiB — read alone and as a two-file glob in a CHILD process, whose
// peak RSS (VmHWM) is the measurement. The bound, 256 MiB, is under a
// quarter of the file-sized decode and far above one row group's ~6 MiB, so
// it holds any incremental reader and fails any file-sized one.
func TestArcFR2AParquetReadIsBoundedByARowGroup(t *testing.T) {
	if p := os.Getenv("FR2_RSS_CHILD"); p != "" {
		src, err := buildTableFunctionSource("read_parquet", []string{p}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := src.Init(t.Context()); err != nil {
			t.Fatal(err)
		}
		rows := 0
		for {
			b, err := src.Next(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			rows += b.Len
		}
		src.Close()
		status, _ := os.ReadFile("/proc/self/status")
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				fmt.Printf("FR2RSS rows=%d %s\n", rows, strings.Join(strings.Fields(line), " "))
			}
		}
		return
	}
	if testing.Short() {
		t.Skip("-short: writes a 205 MB fixture twice and measures a child's peak RSS")
	}
	if _, err := os.Stat("/proc/self/status"); err != nil {
		t.Skip("no /proc/self/status")
	}
	dir := t.TempDir()
	payload := strings.Repeat("x", 1024)
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64}, {Name: "payload", Type: parquet.TypeString},
	}}, parquet.WriterConfig{RowGroupSize: 5000, PageBufferSize: 256 << 10, Compression: parquet.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 5000)
	for g := 0; g < 40; g++ {
		for i := range rows {
			rows[i] = map[string]any{"a": int64(g*5000 + i), "payload": payload}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"p000.parquet", "p001.parquet"} {
		if err := os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	size := buf.Len()
	buf = bytes.Buffer{}
	const bound = 256 << 10 // KiB
	for _, c := range []struct{ name, path, rows string }{
		{"one_file", filepath.Join(dir, "p000.parquet"), "200000"},
		{"two_file_glob", filepath.Join(dir, "*.parquet"), "400000"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run", "^TestArcFR2AParquetReadIsBoundedByARowGroup$")
			cmd.Env = append(os.Environ(), "FR2_RSS_CHILD="+c.path)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			var gotRows, hwm int
			for _, line := range strings.Split(string(out), "\n") {
				if strings.HasPrefix(line, "FR2RSS ") {
					fmt.Sscanf(line, "FR2RSS rows=%d VmHWM: %d kB", &gotRows, &hwm)
				}
			}
			t.Logf("%s: %d bytes per file, rows %d, peak RSS %d KiB (bound %d KiB)", c.name, size, gotRows, hwm, bound)
			if fmt.Sprint(gotRows) != c.rows {
				t.Fatalf("rows = %d, want %s\n%s", gotRows, c.rows, out)
			}
			if hwm == 0 || hwm > bound {
				t.Fatalf("peak RSS %d KiB, bound %d KiB: the member is not read row group by row group", hwm, bound)
			}
		})
	}
}
