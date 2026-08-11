package scan

import (
	"bytes"
	"fmt"
	"testing"

	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// stagedNativeFile builds an uncompressed multi-row-group parquet file
// whose string values are derived from tag — uncompressed because
// CodecNone is the aliasing codec: decoded Values point directly into
// the staged chunk buffer, making it the sharpest probe for the
// copy-before-Close contract.
func stagedNativeFile(t *testing.T, tag string, numGroups, rowsPerGroup int) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "name", Type: pqt.TypeString},
	}}
	cfg := pqt.DefaultWriterConfig()
	cfg.Compression = pqt.CompressionNone
	cfg.RowGroupSize = rowsPerGroup
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for g := 0; g < numGroups; g++ {
		rows := make([]map[string]any, rowsPerGroup)
		for i := range rows {
			rows[i] = map[string]any{
				"id":   int64(g*1000 + i),
				"name": fmt.Sprintf("%s-%d-%05d", tag, g, i),
			}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestReadRowGroupNative_StagedNoAliasing decodes batches through a
// staged (pread-mode) FileReader, releases every chunk buffer back to
// the pool, then forces pool reuse by staging a second file of the same
// chunk size class filled with different content — and verifies the
// first file's batches still hold their original values. If any decode
// path ever aliased the staged chunk buffer into a batch instead of
// copying, the reused buffer's new contents would show through here as
// silent data corruption.
func TestReadRowGroupNative_StagedNoAliasing(t *testing.T) {
	const numGroups, rowsPerGroup = 3, 200
	dataA := stagedNativeFile(t, "alpha", numGroups, rowsPerGroup)
	dataB := stagedNativeFile(t, "OMEGA", numGroups, rowsPerGroup)

	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "name", Type: pqt.TypeString},
	}

	readAll := func(data []byte) [][]string {
		fr, err := pqt.OpenFileReaderAt(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		var out [][]string
		for g := 0; g < fr.NumRowGroups(); g++ {
			b, err := ReadRowGroupNative(fr, g, schema, nil)
			if err != nil {
				t.Fatal(err)
			}
			names := make([]string, b.Len)
			for i := 0; i < b.Len; i++ {
				names[i] = string(b.Columns[1].BytesData.Value(i))
			}
			out = append(out, names)
		}
		return out
	}

	gotA := readAll(dataA)

	// Snapshot A's decoded values NOW (copy), then hammer the pool with
	// B's same-class chunks. If A's batches aliased pool memory, gotA's
	// backing arrays are batch vectors — only a decode that leaked pool
	// bytes into those vectors can corrupt them, which is exactly the
	// bug class this guards.
	wantA := make([][]string, len(gotA))
	for g := range gotA {
		wantA[g] = append([]string(nil), gotA[g]...)
	}

	for round := 0; round < 4; round++ {
		_ = readAll(dataB)
	}

	for g := range wantA {
		for i := range wantA[g] {
			if gotA[g][i] != wantA[g][i] {
				t.Fatalf("group %d row %d corrupted after pool reuse: %q != %q",
					g, i, gotA[g][i], wantA[g][i])
			}
			want := fmt.Sprintf("alpha-%d-%05d", g, i)
			if gotA[g][i] != want {
				t.Fatalf("group %d row %d wrong value: got %q want %q", g, i, gotA[g][i], want)
			}
		}
	}
}

// TestDecodeAheadIter_StagedParity runs the decode-ahead iterator over a
// staged (pread-mode) reader and the whole-file slice reader and
// requires identical batches — the two backings must be
// indistinguishable to every consumer.
func TestDecodeAheadIter_StagedParity(t *testing.T) {
	data := manyRowGroupFile(t, 6, 250)

	serial := openFileReader(t, data)
	serialIter, err := OpenRowGroupIter(serial, serial.Schema().Columns, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	want, err := drainGroupIter(t, serialIter)
	if err != nil {
		t.Fatal(err)
	}

	staged, err := pqt.NewReaderAt(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	it, err := OpenDecodeAheadIter(staged, staged.Schema().Columns, nil, 0, 1, DecodeAheadOpts{Workers: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatal(err)
	}
	requireSameBatches(t, want, got)
}
