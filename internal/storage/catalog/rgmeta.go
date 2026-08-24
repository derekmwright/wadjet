package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// FileRGMeta carries one parquet file's row-group-level metadata: per-RG
// row counts and per-column min/max/null statistics — exactly what
// buildRGUnits needs from the file footer to enumerate and prune row
// groups at plan time.
//
// The catalog persists ALL files' RGMeta for a table in a single
// object-store blob (see rgMetaObjectKey). Before this existed, every
// scan read every file's footer over the network as a barrier before
// any data download — 600 S3 range-read round-trips per lineitem scan
// at SF10, ~2-5s of pure metadata latency on EVERY query. The footer
// contents are static per file, so one blob GET per (table, manifest
// version) replaces all of them; scans of files not covered by the
// blob (fresh ingest, never-analyzed tables) fall back to footer reads.
//
// A blob entry can never be WRONG, only missing or superfluous: entries
// are keyed by object paths and data files are immutable, so a stale
// blob still describes exactly the files it covers. That makes a fixed
// blob key with atomic overwrite safe — no versioning needed.
//
// The blob is written by AnalyzeTable (which decodes every file anyway)
// and encoded in a binary format so stat values keep their exact native
// types (int64/float64/string/bool, the closed set statsToNative
// produces). Routing them through the JSON manifest instead would
// degrade int64→float64 and non-UTF-8 strings, which is tolerable for
// CBO selectivity but not for pruning decisions.
//
// Wire format (binary, version-1):
//
//	[4]   magic "WRGM" (Wadjet Row-Group Metadata)
//	[1]   version (1)
//	[3]   reserved
//	[4]   file count (uint32 LE)
//	then per file:
//	  [2]  path length (uint16 LE) + path bytes
//	  [4]  row group count (uint32 LE)
//	  then per row group:
//	    [8]  num rows (int64 LE)
//	    [2]  column count (uint16 LE)
//	    then per column:
//	      [2]  name length (uint16 LE) + name bytes
//	      [8]  null count (int64 LE)
//	      [1]  has stats (0/1)
//	      min value: [1] type tag + payload
//	      max value: [1] type tag + payload
//
// Value tags: 0 = nil, 1 = bool (1 byte), 2 = int64 (8 bytes LE),
// 3 = float64 (8 bytes LE), 4 = string ([4] length uint32 LE + bytes).
const (
	rgMetaMagic   = "WRGM"
	rgMetaVersion = 1
)

const (
	rgMetaTagNil    = 0
	rgMetaTagBool   = 1
	rgMetaTagInt64  = 2
	rgMetaTagFloat  = 3
	rgMetaTagString = 4
)

// rgMetaMaxStringLen caps decoded stat-string allocations so a corrupt
// blob can't ask for gigabytes. Real min/max strings are column values
// (comments, names) — far below this.
const rgMetaMaxStringLen = 1 << 20

// FileRGMeta is the per-file unit of the table RG-metadata blob.
type FileRGMeta struct {
	Path   string
	Groups []parquet.RowGroupStats
}

// EncodeTableRGMeta serializes all files' row-group metadata to the v1
// wire format. Empty input returns nil (no blob to upload).
func EncodeTableRGMeta(files []FileRGMeta) []byte {
	if len(files) == 0 {
		return nil
	}
	var buf bytes.Buffer
	hdr := make([]byte, 12)
	copy(hdr[0:4], rgMetaMagic)
	hdr[4] = rgMetaVersion
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(files)))
	buf.Write(hdr)
	var lb [8]byte
	writeU16Str := func(s string) {
		binary.LittleEndian.PutUint16(lb[:2], uint16(len(s)))
		buf.Write(lb[:2])
		buf.WriteString(s)
	}
	writeValue := func(v any) {
		switch tv := v.(type) {
		case nil:
			buf.WriteByte(rgMetaTagNil)
		case bool:
			buf.WriteByte(rgMetaTagBool)
			if tv {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		case int64:
			buf.WriteByte(rgMetaTagInt64)
			binary.LittleEndian.PutUint64(lb[:], uint64(tv))
			buf.Write(lb[:])
		case float64:
			buf.WriteByte(rgMetaTagFloat)
			binary.LittleEndian.PutUint64(lb[:], math.Float64bits(tv))
			buf.Write(lb[:])
		case string:
			buf.WriteByte(rgMetaTagString)
			binary.LittleEndian.PutUint32(lb[:4], uint32(len(tv)))
			buf.Write(lb[:4])
			buf.WriteString(tv)
		default:
			// Not a type statsToNative produces — store nothing rather
			// than an approximation the pruner might act on.
			buf.WriteByte(rgMetaTagNil)
		}
	}
	for _, f := range files {
		writeU16Str(f.Path)
		binary.LittleEndian.PutUint32(lb[:4], uint32(len(f.Groups)))
		buf.Write(lb[:4])
		for _, rg := range f.Groups {
			binary.LittleEndian.PutUint64(lb[:], uint64(rg.NumRows))
			buf.Write(lb[:])
			binary.LittleEndian.PutUint16(lb[:2], uint16(len(rg.Columns)))
			buf.Write(lb[:2])
			for name, cs := range rg.Columns {
				writeU16Str(name)
				binary.LittleEndian.PutUint64(lb[:], uint64(cs.NullCount))
				buf.Write(lb[:])
				if cs.HasStats {
					buf.WriteByte(1)
				} else {
					buf.WriteByte(0)
				}
				writeValue(cs.MinValue)
				writeValue(cs.MaxValue)
			}
		}
	}
	return buf.Bytes()
}

// DecodeTableRGMeta parses a v1 RG-metadata blob into a by-path map,
// the shape buildRGUnits consumes.
func DecodeTableRGMeta(r io.Reader) (map[string][]parquet.RowGroupStats, error) {
	hdr := make([]byte, 12)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("rgmeta: read header: %w", err)
	}
	if string(hdr[0:4]) != rgMetaMagic {
		return nil, fmt.Errorf("rgmeta: bad magic %q", string(hdr[0:4]))
	}
	if hdr[4] != rgMetaVersion {
		return nil, fmt.Errorf("rgmeta: unsupported version %d", hdr[4])
	}
	fileCount := int(binary.LittleEndian.Uint32(hdr[8:12]))
	var lb [8]byte
	readU16Str := func() (string, error) {
		if _, err := io.ReadFull(r, lb[:2]); err != nil {
			return "", err
		}
		b := make([]byte, binary.LittleEndian.Uint16(lb[:2]))
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		return string(b), nil
	}
	readValue := func() (any, error) {
		if _, err := io.ReadFull(r, lb[:1]); err != nil {
			return nil, err
		}
		switch lb[0] {
		case rgMetaTagNil:
			return nil, nil
		case rgMetaTagBool:
			if _, err := io.ReadFull(r, lb[:1]); err != nil {
				return nil, err
			}
			return lb[0] != 0, nil
		case rgMetaTagInt64:
			if _, err := io.ReadFull(r, lb[:]); err != nil {
				return nil, err
			}
			return int64(binary.LittleEndian.Uint64(lb[:])), nil
		case rgMetaTagFloat:
			if _, err := io.ReadFull(r, lb[:]); err != nil {
				return nil, err
			}
			return math.Float64frombits(binary.LittleEndian.Uint64(lb[:])), nil
		case rgMetaTagString:
			if _, err := io.ReadFull(r, lb[:4]); err != nil {
				return nil, err
			}
			n := binary.LittleEndian.Uint32(lb[:4])
			if n > rgMetaMaxStringLen {
				return nil, fmt.Errorf("rgmeta: stat string length %d exceeds cap", n)
			}
			b := make([]byte, n)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, err
			}
			return string(b), nil
		default:
			return nil, fmt.Errorf("rgmeta: unknown value tag %d", lb[0])
		}
	}

	out := make(map[string][]parquet.RowGroupStats, fileCount)
	for fi := 0; fi < fileCount; fi++ {
		path, err := readU16Str()
		if err != nil {
			return nil, fmt.Errorf("rgmeta: file %d path: %w", fi, err)
		}
		if _, err := io.ReadFull(r, lb[:4]); err != nil {
			return nil, fmt.Errorf("rgmeta: file %s rg count: %w", path, err)
		}
		rgCount := int(binary.LittleEndian.Uint32(lb[:4]))
		groups := make([]parquet.RowGroupStats, 0, min(rgCount, 1024))
		for gi := 0; gi < rgCount; gi++ {
			if _, err := io.ReadFull(r, lb[:]); err != nil {
				return nil, fmt.Errorf("rgmeta: file %s rg %d rows: %w", path, gi, err)
			}
			rg := parquet.RowGroupStats{NumRows: int64(binary.LittleEndian.Uint64(lb[:]))}
			if _, err := io.ReadFull(r, lb[:2]); err != nil {
				return nil, fmt.Errorf("rgmeta: file %s rg %d col count: %w", path, gi, err)
			}
			colCount := int(binary.LittleEndian.Uint16(lb[:2]))
			rg.Columns = make(map[string]parquet.ColumnStats, colCount)
			for ci := 0; ci < colCount; ci++ {
				name, err := readU16Str()
				if err != nil {
					return nil, fmt.Errorf("rgmeta: file %s rg %d col %d name: %w", path, gi, ci, err)
				}
				var cs parquet.ColumnStats
				if _, err := io.ReadFull(r, lb[:]); err != nil {
					return nil, fmt.Errorf("rgmeta: col %s null count: %w", name, err)
				}
				cs.NullCount = int64(binary.LittleEndian.Uint64(lb[:]))
				if _, err := io.ReadFull(r, lb[:1]); err != nil {
					return nil, fmt.Errorf("rgmeta: col %s has-stats: %w", name, err)
				}
				cs.HasStats = lb[0] != 0
				if cs.MinValue, err = readValue(); err != nil {
					return nil, fmt.Errorf("rgmeta: col %s min: %w", name, err)
				}
				if cs.MaxValue, err = readValue(); err != nil {
					return nil, fmt.Errorf("rgmeta: col %s max: %w", name, err)
				}
				rg.Columns[name] = cs
			}
			groups = append(groups, rg)
		}
		out[path] = groups
	}
	return out, nil
}

// rgMetaObjectKey returns the fixed object-store path for a table's
// RG-metadata blob. Fixed (not versioned) is deliberate: entries are
// keyed by immutable file paths, so overwriting in place can never make
// a concurrent reader see wrong stats — only older/newer coverage.
func rgMetaObjectKey(table string) string {
	return fmt.Sprintf("stats/%s/rgmeta.wrgm", table)
}

// PutTableRGMeta uploads the table's RG-metadata blob and returns its
// object-store key. Empty input returns "" (nothing uploaded).
func (c *Catalog) PutTableRGMeta(ctx context.Context, table string, files []FileRGMeta) (string, error) {
	data := EncodeTableRGMeta(files)
	if len(data) == 0 {
		return "", nil
	}
	key := rgMetaObjectKey(table)
	if _, err := c.store.Put(ctx, c.bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		return "", fmt.Errorf("upload rgmeta %s: %w", key, err)
	}
	return key, nil
}

// rgMetaCacheEntry is a memoized decoded RG-metadata blob, validated the
// same way as aggStatsCacheEntry: by the manifest's KV revision.
// The map is shared with callers — treat it as immutable.
type rgMetaCacheEntry struct {
	rev    uint64
	byPath map[string][]parquet.RowGroupStats
}

// TableRGMeta returns the table's persisted row-group metadata as a
// by-path map, or nil when the table has no blob (never analyzed).
// Best-effort: fetch/decode failures return nil, nil so scans degrade
// to per-file footer reads instead of failing.
//
// The decoded blob is memoized per table, keyed by the manifest's KV
// revision — the same invalidation contract as AggregateColumnStats. In
// the 22-query benchmark process the blob is fetched from the store once
// per table, not once per query.
func (c *Catalog) TableRGMeta(ctx context.Context, tableName string) (map[string][]parquet.RowGroupStats, error) {
	manifest, rev, err := c.manifestWithRevision(tableName)
	if err != nil || manifest == nil || manifest.RGMetaKey == "" {
		return nil, nil
	}

	c.rgMetaMu.Lock()
	if e, ok := c.rgMetaCache[tableName]; ok && e.rev == rev {
		c.rgMetaMu.Unlock()
		return e.byPath, nil
	}
	c.rgMetaMu.Unlock()

	rc, _, err := c.store.Get(ctx, c.bucket, manifest.RGMetaKey)
	if err != nil {
		return nil, nil
	}
	byPath, err := DecodeTableRGMeta(rc)
	rc.Close()
	if err != nil {
		return nil, nil
	}

	c.rgMetaMu.Lock()
	if c.rgMetaCache == nil {
		c.rgMetaCache = make(map[string]rgMetaCacheEntry)
	}
	c.rgMetaCache[tableName] = rgMetaCacheEntry{rev: rev, byPath: byPath}
	c.rgMetaMu.Unlock()
	return byPath, nil
}
