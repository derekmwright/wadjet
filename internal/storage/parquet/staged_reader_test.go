package parquet

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// stagedTestSchema covers every flat storage class the scan path decodes:
// integers, floats, bool, string (incl. empty), bytes, and nullables.
func stagedTestSchema() Schema {
	return Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "i32", Type: TypeInt32, Nullable: true},
			{Name: "name", Type: TypeString},
			{Name: "note", Type: TypeString, Nullable: true},
			{Name: "score", Type: TypeFloat64, Nullable: true},
			{Name: "ratio", Type: TypeFloat32},
			{Name: "ok", Type: TypeBool},
			{Name: "blob", Type: TypeBytes, Nullable: true},
		},
	}
}

func stagedTestRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{
			"id":    int64(i),
			"name":  fmt.Sprintf("row-%05d", i),
			"ratio": float64(i%97) / 97.0,
			"ok":    i%3 == 0,
		}
		if i%5 != 0 {
			r["i32"] = int64(i % 1000)
		}
		if i%7 != 0 {
			r["note"] = fmt.Sprintf("note text %d with some repeated payload payload payload", i)
		} else if i%14 == 0 {
			r["note"] = "" // empty string, present
		}
		if i%4 != 0 {
			r["score"] = float64(i) * 1.5
		}
		if i%6 != 0 {
			r["blob"] = []byte{byte(i), byte(i >> 8), 0x00, 0xFF}
		}
		rows[i] = r
	}
	return rows
}

func writeStagedTestFile(t *testing.T, comp Compression, rowGroupSize int, n int) []byte {
	t.Helper()
	cfg := DefaultWriterConfig()
	cfg.Compression = comp
	cfg.RowGroupSize = rowGroupSize
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, stagedTestSchema(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(stagedTestRows(n)); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestStagedReader_ParityAllCodecs verifies the staged (pread) reader
// returns byte-identical results to the whole-file slice reader for
// every codec, across multiple row groups, via both a bytes ReaderAt
// and a real *os.File.
func TestStagedReader_ParityAllCodecs(t *testing.T) {
	codecs := []struct {
		name string
		comp Compression
	}{
		{"none", CompressionNone},
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
		{"gzip", CompressionGzip},
		{"lz4", CompressionLZ4},
	}
	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			// Small row-group size forces several groups (multi-chunk staging).
			data := writeStagedTestFile(t, c.comp, 256, 1000)

			ref, err := NewReaderFromBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			if ref.NumRowGroups() < 2 {
				t.Fatalf("fixture produced %d row groups, want >= 2", ref.NumRowGroups())
			}
			want, err := ref.ReadRows(nil)
			if err != nil {
				t.Fatal(err)
			}

			staged, err := NewReaderAt(newBytesReaderAt(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			got, err := staged.ReadRows(nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("staged bytes-ReaderAt rows differ from slice reader")
			}

			path := filepath.Join(t.TempDir(), "staged.parquet")
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			fromFile, err := NewReaderAt(f, int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			got2, err := fromFile.ReadRows(nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got2) {
				t.Fatalf("staged file rows differ from slice reader")
			}
		})
	}
}

// flakyReaderAt serves reads normally until failAfterOpen is set, then
// fails every read — simulating a chunk read hitting a vanished or
// truncated file after a successful open.
type flakyReaderAt struct {
	data *bytesReaderAt
	fail bool
}

func (r *flakyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.fail {
		return 0, fmt.Errorf("injected read failure at offset %d", off)
	}
	return r.data.ReadAt(p, off)
}

// TestStagedReader_ReadErrorSurfaces verifies a failed chunk staging
// read surfaces as an error — never as a silently empty column. Silent
// empties on the scan path would decode as all-NULL vectors: corrupt
// results, the worst failure class this package has.
func TestStagedReader_ReadErrorSurfaces(t *testing.T) {
	data := writeStagedTestFile(t, CompressionSnappy, 1024, 100)
	src := &flakyReaderAt{data: newBytesReaderAt(data)}
	staged, err := NewReaderAt(src, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	src.fail = true
	if _, err := staged.ReadRows(nil); err == nil {
		t.Fatal("ReadRows succeeded with failing chunk reads; want error")
	}
	fr := staged.FileReader()
	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		t.Fatal("ColumnPages returned nil")
	}
	defer pr.Close()
	if _, err := pr.NextPage(); err == nil {
		t.Fatal("NextPage succeeded with failing chunk read; want error")
	}
}

// TestStagedReader_CloseReturnsBuffer verifies staged readers return
// their chunk buffer exactly once and tolerate double Close.
func TestStagedReader_CloseReturnsBuffer(t *testing.T) {
	data := writeStagedTestFile(t, CompressionNone, 1024, 200)
	staged, err := NewReaderAt(newBytesReaderAt(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	fr := staged.FileReader()
	pr := fr.ColumnPages(0, 0)
	if pr == nil {
		t.Fatal("ColumnPages returned nil")
	}
	if _, err := pr.NextPage(); err != nil {
		t.Fatal(err)
	}
	if pr.owned == nil {
		t.Fatal("staged reader has no owned buffer after NextPage")
	}
	if err := pr.Close(); err != nil {
		t.Fatal(err)
	}
	if pr.owned != nil || pr.data != nil {
		t.Fatal("Close did not release the staged buffer")
	}
	if err := pr.Close(); err != nil {
		t.Fatal("double Close errored")
	}
}

func TestChunkPool(t *testing.T) {
	cases := []struct {
		n       int
		wantCap int
	}{
		{1, chunkPoolMinCap},
		{chunkPoolMinCap, chunkPoolMinCap},
		{chunkPoolMinCap + 1, chunkPoolMinCap * 2},
		{chunkPoolMaxCap, chunkPoolMaxCap},
	}
	for _, c := range cases {
		b := getChunkBuf(c.n)
		if len(b) != c.n {
			t.Fatalf("getChunkBuf(%d) len = %d", c.n, len(b))
		}
		if cap(b) != c.wantCap {
			t.Fatalf("getChunkBuf(%d) cap = %d, want %d", c.n, cap(b), c.wantCap)
		}
		putChunkBuf(b)
	}
	// Oversized: allocated fresh, exact length, silently not pooled.
	big := getChunkBuf(chunkPoolMaxCap + 1)
	if len(big) != chunkPoolMaxCap+1 {
		t.Fatalf("oversized len = %d", len(big))
	}
	putChunkBuf(big) // must not panic or pool
}
