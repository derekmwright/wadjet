package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// FileSketches bundles every supported column's HLL + reservoir sample
// for one parquet file into a single object-store blob. Storing these
// sketches inline in the catalog manifest blew the NATS payload limit
// at SF100 (63 lineitem files × 16 cols × 18 KB ≈ 18 MB > 1 MB max),
// so the catalog now writes sketches to the object store and only
// keeps a small reference in the manifest.
//
// One blob per parquet file (≈ 16 cols × 18 KB ≈ 300 KB at SF100).
// The blob is read once per file during AggregateColumnStats and the
// individual sketches feed cross-file merges. Lazy: not loaded unless
// the planner asks for the table's aggregated stats.
//
// Wire format (binary, version-1):
//
//	[4]   magic "WSKB" (Wadjet SKetches Bundle)
//	[1]   version (1)
//	[1]   reserved
//	[2]   column count K (uint16 LE)
//	then K entries, each:
//	  [2]  column name length (uint16 LE)
//	  [N]  column name bytes
//	  [4]  HLL bytes length (uint32 LE; 0 = no HLL)
//	  [M]  HLL bytes (HLL wire format, see hll.go)
//	  [4]  Sample bytes length (uint32 LE; 0 = no Sample)
//	  [P]  Sample bytes (Sample wire format, see sample.go)
const (
	sketchesMagic   = "WSKB"
	sketchesVersion = 1
)

// FileSketchesEntry is the in-memory form of one column's sketches.
type FileSketchesEntry struct {
	Column string
	HLL    []byte
	Sample []byte
}

// EncodeFileSketches serializes a per-column sketch map to the v1 wire
// format. Empty input returns nil (no blob to upload).
func EncodeFileSketches(entries []FileSketchesEntry) []byte {
	if len(entries) == 0 {
		return nil
	}
	var buf bytes.Buffer
	hdr := make([]byte, 8)
	copy(hdr[0:4], sketchesMagic)
	hdr[4] = sketchesVersion
	binary.LittleEndian.PutUint16(hdr[6:8], uint16(len(entries)))
	buf.Write(hdr)
	for _, e := range entries {
		var lb [4]byte
		// column name
		binary.LittleEndian.PutUint16(lb[:2], uint16(len(e.Column)))
		buf.Write(lb[:2])
		buf.WriteString(e.Column)
		// HLL
		binary.LittleEndian.PutUint32(lb[:], uint32(len(e.HLL)))
		buf.Write(lb[:])
		buf.Write(e.HLL)
		// Sample
		binary.LittleEndian.PutUint32(lb[:], uint32(len(e.Sample)))
		buf.Write(lb[:])
		buf.Write(e.Sample)
	}
	return buf.Bytes()
}

// DecodeFileSketches parses a v1 sketches blob.
func DecodeFileSketches(r io.Reader) ([]FileSketchesEntry, error) {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("sketches: read header: %w", err)
	}
	if string(hdr[0:4]) != sketchesMagic {
		return nil, fmt.Errorf("sketches: bad magic %q", string(hdr[0:4]))
	}
	if hdr[4] != sketchesVersion {
		return nil, fmt.Errorf("sketches: unsupported version %d", hdr[4])
	}
	n := int(binary.LittleEndian.Uint16(hdr[6:8]))
	out := make([]FileSketchesEntry, 0, n)
	for i := 0; i < n; i++ {
		var nameLen [2]byte
		if _, err := io.ReadFull(r, nameLen[:]); err != nil {
			return nil, fmt.Errorf("sketches: entry %d name length: %w", i, err)
		}
		nameBytes := make([]byte, binary.LittleEndian.Uint16(nameLen[:]))
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, fmt.Errorf("sketches: entry %d name: %w", i, err)
		}
		var lb [4]byte
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return nil, fmt.Errorf("sketches: entry %d hll length: %w", i, err)
		}
		hllLen := binary.LittleEndian.Uint32(lb[:])
		hllBytes := make([]byte, hllLen)
		if hllLen > 0 {
			if _, err := io.ReadFull(r, hllBytes); err != nil {
				return nil, fmt.Errorf("sketches: entry %d hll: %w", i, err)
			}
		}
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return nil, fmt.Errorf("sketches: entry %d sample length: %w", i, err)
		}
		sampleLen := binary.LittleEndian.Uint32(lb[:])
		sampleBytes := make([]byte, sampleLen)
		if sampleLen > 0 {
			if _, err := io.ReadFull(r, sampleBytes); err != nil {
				return nil, fmt.Errorf("sketches: entry %d sample: %w", i, err)
			}
		}
		out = append(out, FileSketchesEntry{Column: string(nameBytes), HLL: hllBytes, Sample: sampleBytes})
	}
	return out, nil
}

// sketchesObjectKey returns the canonical object-store path for one
// parquet file's sketches bundle. The basename of the parquet path is
// reused so the sketch object lives in a parallel directory layout.
func sketchesObjectKey(table, parquetPath string) string {
	base := parquetPath
	if i := bytes.LastIndexByte([]byte(parquetPath), '/'); i >= 0 {
		base = parquetPath[i+1:]
	}
	// Strip a .parquet suffix; keep .sketches as the new extension.
	if l := len(base); l >= 8 && base[l-8:] == ".parquet" {
		base = base[:l-8]
	}
	return fmt.Sprintf("stats/%s/%s.sketches", table, base)
}

// putFileSketches uploads a bundled sketches blob and returns the
// object-store key. Empty entries returns "" (nothing to upload).
func (c *Catalog) putFileSketches(ctx context.Context, table, parquetPath string, entries []FileSketchesEntry) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	data := EncodeFileSketches(entries)
	if len(data) == 0 {
		return "", nil
	}
	key := sketchesObjectKey(table, parquetPath)
	if _, err := c.store.Put(ctx, c.bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		return "", fmt.Errorf("upload sketches %s: %w", key, err)
	}
	return key, nil
}

// UploadFileSketches is the exported entry point for the ingest path.
// It bundles per-column sketches into a single object-store blob and
// returns the canonical key.
func (c *Catalog) UploadFileSketches(ctx context.Context, table, parquetPath string, entries []FileSketchesEntry) (string, error) {
	return c.putFileSketches(ctx, table, parquetPath, entries)
}

// loadFileSketches fetches a previously-uploaded sketches blob.
// Returns nil entries on best-effort error so AggregateColumnStats
// can degrade to the legacy inline path or to heuristic NDV.
func (c *Catalog) loadFileSketches(ctx context.Context, key string) ([]FileSketchesEntry, error) {
	if key == "" {
		return nil, nil
	}
	rc, _, err := c.store.Get(ctx, c.bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetch sketches %s: %w", key, err)
	}
	defer rc.Close()
	return DecodeFileSketches(rc)
}

// Compile-time guarantee that we hold a usable Store.
var _ objstore.Store = (objstore.Store)(nil)
