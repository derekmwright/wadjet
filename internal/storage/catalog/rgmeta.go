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

// FileRGMeta preserves per-file row counts and min/max/null stats for pruning.
// AnalyzeTable stores one table blob; uncovered files fall back to footers.
// Path-keyed entries assume IMMUTABLE objects, so stale entries may be missing
// or superfluous but must still describe the same bytes; overwrite violates this.
// Binary WRGM v1 preserves native bounds, including CIDR's sort key AND text;
// JSON numeric/string coercions are unsafe for pruning.
// Unknown tags are errors; TableRGMeta treats decode failure as no blob and
// falls back to footers, preserving answers when adding tags without a version bump.
// See docs/internals/catalog-row-group-metadata-blob.md for the design.
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
	// rgMetaTagCidrInet keeps a confirmed CIDR bound BOXED across the blob
	// (#523). Writing it as a plain string would lose the box, and the box
	// is the whole confirmation: scan.compareValuesOK refuses to compare a
	// parquet.CidrInetBound against an ordinary string, so a bound that
	// came back unboxed would prune nothing and the coordinator's rgmeta
	// path would silently forgo pruning that a footer read gets — green on
	// every gate, because a withheld prune is never a wrong answer.
	rgMetaTagCidrInet = 5
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
		case parquet.CidrInetBound:
			buf.WriteByte(rgMetaTagCidrInet)
			for _, part := range [2]string{tv.Key, tv.Text} {
				binary.LittleEndian.PutUint32(lb[:4], uint32(len(part)))
				buf.Write(lb[:4])
				buf.WriteString(part)
			}
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
		case rgMetaTagCidrInet:
			var parts [2]string
			for i := range parts {
				if _, err := io.ReadFull(r, lb[:4]); err != nil {
					return nil, err
				}
				n := binary.LittleEndian.Uint32(lb[:4])
				if n > rgMetaMaxStringLen {
					return nil, fmt.Errorf("rgmeta: cidr bound length %d exceeds cap", n)
				}
				b := make([]byte, n)
				if _, err := io.ReadFull(r, b); err != nil {
					return nil, err
				}
				parts[i] = string(b)
			}
			return parquet.CidrInetBound{Key: parts[0], Text: parts[1]}, nil
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
