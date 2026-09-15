package parquet

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Compression identifies a compression codec for Parquet pages.
type Compression int

const (
	CompressionSnappy Compression = iota // default
	CompressionZstd
	CompressionGzip
	CompressionLZ4
	CompressionNone
)

func (c Compression) String() string {
	switch c {
	case CompressionSnappy:
		return "snappy"
	case CompressionZstd:
		return "zstd"
	case CompressionGzip:
		return "gzip"
	case CompressionLZ4:
		return "lz4"
	case CompressionNone:
		return "none"
	default:
		return fmt.Sprintf("unknown(%d)", int(c))
	}
}

// ParseCompression parses a compression name string.
func ParseCompression(s string) (Compression, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "snappy":
		return CompressionSnappy, nil
	case "zstd":
		return CompressionZstd, nil
	case "gzip":
		return CompressionGzip, nil
	case "lz4":
		return CompressionLZ4, nil
	case "none", "uncompressed":
		return CompressionNone, nil
	default:
		return 0, fmt.Errorf("unknown compression: %q (supported: snappy, zstd, gzip, lz4, none)", s)
	}
}

// WriterConfig configures the Parquet writer.
type WriterConfig struct {
	RowGroupSize   int         // target number of rows per row group (default 128*1024)
	PageBufferSize int         // target data page size in bytes (default 256 KB)
	Compression    Compression // compression codec (default Snappy)
}

// DefaultWriterConfig returns a default writer configuration.
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		RowGroupSize:   128 * 1024,
		PageBufferSize: 256 * 1024,
		Compression:    CompressionSnappy,
	}
}

// Writer writes rows to a Parquet file using the native Parquet writer.
type Writer struct {
	schema Schema
	config WriterConfig
	nw     *NativeWriter
}

// NewWriter creates a Parquet writer that writes to the given io.Writer.
func NewWriter(w io.Writer, schema Schema, cfg WriterConfig) (*Writer, error) {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 128 * 1024
	}
	// The native writer deep-copies the schema (#973) and validates that copy
	// (#970); this one holds THAT copy, not the caller's and not a second copy
	// of its own, so prepareRows and the decomposition below it read one and
	// the same schema, validated once. The refusal is RETURNED here, which is
	// this constructor's contract, and latched there, which is all the other
	// one's signature allows.
	nw := NewNativeWriter(w, schema, cfg)
	if nw.err != nil {
		return nil, nw.err
	}
	return &Writer{
		schema: nw.schema,
		config: cfg,
		nw:     nw,
	}, nil
}

// WriteRows writes a batch of rows to the Parquet file.
// Values for network types (IPv4, IPv6, MAC) are converted from their string
// representations to the internal binary format before writing.
func (w *Writer) WriteRows(rows []map[string]any) error {
	// Before prepareRows, which converts network/temporal values IN THE
	// CALLER'S OWN MAPS: a write this writer will not perform must leave
	// them as it found them (ErrWriterClosed, #972).
	if err := w.nw.checkWritable(); err != nil {
		return err
	}
	if err := w.prepareRows(rows); err != nil {
		return err
	}
	return w.nw.WriteMapRows(rows)
}

// prepareRows converts network/typed values in maps to parquet-compatible
// representations. It returns an error for a value that cannot be stored
// without corruption — an invalid DATE string, which used to become the
// epoch silently (#560) — so the write fails instead of persisting a wrong
// value under the caller's date.
func (w *Writer) prepareRows(rows []map[string]any) error {
	// Fast path: check if any columns need conversion
	needsConversion := false
	for _, col := range w.schema.Columns {
		switch col.Type {
		case TypeIPv4, TypeIPv6, TypeMAC, TypePort, TypeProtocol, TypeDuration, TypeUUID, TypeDate, TypeTimestamp:
			needsConversion = true
		}
	}
	if !needsConversion {
		return nil
	}

	for _, row := range rows {
		for _, col := range w.schema.Columns {
			val, ok := row[col.Name]
			if !ok || val == nil {
				continue
			}
			// A TEXT value for any type with a text input form goes through
			// the ONE grammar (convertNetworkLiteral). This arm used to carry
			// a FOURTH set of parsers — net.ParseIP for the two address types,
			// net.ParseMAC for MAC — which silently left an unparsed string in
			// place for the leaf to refuse later, and read spellings the
			// writer's own door did not (#627).
			if s, isText := val.(string); isText && hasNetworkLiteralForm(col.Type) {
				conv, err := convertNetworkLiteral(col.Type, s)
				if err != nil {
					return fmt.Errorf("column %q: %w", col.Name, err)
				}
				// A nil is the EMPTY literal, which is absence; the leaf
				// writes it as NULL exactly as a missing key would be.
				row[col.Name] = conv
				continue
			}
			switch col.Type {
			case TypePort, TypeProtocol:
				// This used to narrow int / int64 / float64 to an int32 HERE,
				// with a bare Go conversion, on the CALLER's map — so
				// int64(4294967297) became 1 before the native writer ever saw
				// the number it was asked to store, and the leaf's own range
				// check could not see it either (#890). The narrowing belongs
				// at the leaf, where a refusal names the column and the row;
				// int32LeafValue is that one rule, so this arm only has to
				// stop mangling the value on the way past.
				if err := CheckInt32LeafValue(col.Type, val); err != nil {
					return fmt.Errorf("column %q: %w", col.Name, err)
				}
			case TypeDuration:
				// Same rule as PORT/PROTOCOL above: the numeric arms used to
				// widen here with a bare conversion — float64 included, which
				// is implementation-defined for a NaN — and int64LeafValue is
				// the one checked rule. What stays is the TEMPORAL
				// normalisation, which the leaf cannot do because a
				// time.Duration and a text literal need this column's own
				// accept-set.
				if norm, ok, err := normalizeTemporalBox(col.Type, val); err != nil {
					return fmt.Errorf("column %q: %w", col.Name, err)
				} else if ok {
					row[col.Name] = norm
				}
			case TypeTimestamp:
				if norm, ok, err := normalizeTemporalBox(col.Type, val); err != nil {
					return fmt.Errorf("column %q: %w", col.Name, err)
				} else if ok {
					row[col.Name] = norm
				}
			case TypeDate:
				// Both a text literal and a time.Time land here — the second
				// is the box the SQL INSERT path produces, and it used to
				// fall past every converter into toInt32's zero (#673).
				if norm, ok, err := normalizeTemporalBox(col.Type, val); err != nil {
					return fmt.Errorf("column %q: %w", col.Name, err)
				} else if ok {
					row[col.Name] = norm
				}
			}
		}
	}
	return nil
}

// Close finalizes the Parquet file and closes the writer.
func (w *Writer) Close() error {
	return w.nw.Close()
}

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// parseUUIDForWrite converts a UUID string to raw 16 bytes for parquet storage,
// through the type's one grammar (PgUUIDPton). It used to strip EVERY hyphen,
// which accepted `'a-0eebc99…'` that PostgreSQL refuses, and had no arm for the
// braced spelling that PostgreSQL accepts — wrong in both directions at once.
func parseUUIDForWrite(s string) []byte {
	raw, st := PgUUIDPton(s)
	if st != NetTextOK {
		return nil
	}
	out := make([]byte, 16)
	copy(out, raw[:])
	return out
}

// parseDateForWrite converts a date string "2006-01-02" to days since epoch.
//
// Computed from t.Unix() (civil-days arithmetic), not t.Sub(epochDate):
// Sub returns a time.Duration, which saturates at ±math.MaxInt64 ns
// (~292 years) rather than reporting an overflow, so a 4-digit-year date
// before 1678 or after 2262 previously wrote a silently WRONG day count —
// a real data-corruption path, since this runs at ingest
// (batch.parseDateString / kernel.parseDateToDays, #451).
func parseDateForWrite(s string) int32 {
	d, _ := ParseDateDays(s)
	return d
}

// parseDateForWriteChecked is parseDateForWrite with the error surfaced: an
// unparseable or nonexistent calendar date ('not-a-date', '2026-02-30',
// month 13, day 32) is rejected instead of being silently written as the
// epoch (day 0 = 1970-01-01) — ingest-time data CORRUPTION, since the
// original text is gone and 1970-01-01 reads back in its place (#560). It is
// the thin checked wrapper the map-row write path (prepareRows) and the
// ingest boundary (ingest.checkType, via ValidateDateString) use; the accept
// set and classification live in ParseDateDays.
func parseDateForWriteChecked(s string) (int32, error) {
	return ParseDateDays(s)
}

// ValidateDateString reports whether s is a date the writer can store without
// corrupting it. The ingest boundary (ingest.checkType) uses it to reject an
// unparseable or nonexistent calendar date before the writer turns it into
// the epoch (#560).
func ValidateDateString(s string) error {
	_, err := ParseDateDays(s)
	return err
}
