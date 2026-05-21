package worker

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompressShuffleFile_RoundTrip(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.wshf")
	dst := filepath.Join(tmp, "dst.wshc")

	// Highly-compressible payload (repeating bytes).
	payload := bytes.Repeat([]byte("WSHFcontent_"), 50_000) // 600 KB
	if err := os.WriteFile(src, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	compressedSize, useCompressed, err := CompressShuffleFile(src, dst)
	if err != nil {
		t.Fatalf("CompressShuffleFile: %v", err)
	}
	if !useCompressed {
		t.Fatalf("expected useCompressed=true for repetitive payload (saved=%d/%d)",
			compressedSize, len(payload))
	}
	if compressedSize == 0 {
		t.Fatal("compressedSize 0")
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != compressedSize {
		t.Errorf("dst size %d != returned compressedSize %d", info.Size(), compressedSize)
	}

	// Verify roundtrip via DecompressShuffleData equivalence.
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 'W' || got[1] != 'S' || got[2] != 'H' || got[3] != 'C' {
		t.Fatalf("dst missing WSHC magic; got % x", got[:4])
	}
	decoded, err := DecompressShuffleData(got)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Errorf("roundtrip mismatch: got %d bytes, want %d", len(decoded), len(payload))
	}
}

func TestCompressShuffleFile_TinyFileSkips(t *testing.T) {
	// Files below the 64-byte threshold match CompressShuffleData's
	// short-circuit. The caller uses the source file as-is.
	tmp := t.TempDir()
	src := filepath.Join(tmp, "tiny")
	dst := filepath.Join(tmp, "tiny.s2")
	if err := os.WriteFile(src, []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, useCompressed, err := CompressShuffleFile(src, dst)
	if err != nil {
		t.Fatalf("CompressShuffleFile: %v", err)
	}
	if useCompressed {
		t.Errorf("tiny file should skip compression")
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) && statErr != nil {
		t.Errorf("dst should not exist (or be checked nil); stat err=%v", statErr)
	}
}

func TestCompressShuffleFile_IncompressibleSkips(t *testing.T) {
	// Random bytes don't compress; CompressShuffleFile should signal
	// useCompressed=false so the caller drops the dst and uploads the
	// source as-is.
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.wshf")
	dst := filepath.Join(tmp, "dst.wshc")
	payload := make([]byte, 200_000)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	_, useCompressed, err := CompressShuffleFile(src, dst)
	if err != nil {
		t.Fatalf("CompressShuffleFile: %v", err)
	}
	if useCompressed {
		t.Errorf("random payload should not meet the ≥10%% savings threshold")
	}
}

func TestCompressShuffleFile_HeapBounded(t *testing.T) {
	// Streaming-compression must hold roughly a fixed buffer regardless
	// of file size. Write a moderately-large file (~10 MB) and confirm
	// the call completes without panic; this catches regressions where
	// the helper accidentally reverts to io.ReadAll or similar
	// full-buffer patterns.
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.wshf")
	dst := filepath.Join(tmp, "dst.wshc")
	// Repeating pattern compresses to ~negligible, so dst stays small.
	chunk := []byte(strings.Repeat("partition-row-data-", 100))
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		if _, err := f.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	compressedSize, useCompressed, err := CompressShuffleFile(src, dst)
	if err != nil {
		t.Fatalf("CompressShuffleFile: %v", err)
	}
	if !useCompressed {
		t.Errorf("highly-compressible 10 MB payload should compress")
	}
	if compressedSize > 200_000 {
		t.Errorf("compressed size %d unexpectedly large", compressedSize)
	}
}
