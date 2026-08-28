package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
	if err := ValidateWriteSchema(schema); err != nil {
		return nil, err
	}

	return &Writer{
		schema: schema,
		config: cfg,
		nw:     NewNativeWriter(w, schema, cfg),
	}, nil
}

// WriteRows writes a batch of rows to the Parquet file.
// Values for network types (IPv4, IPv6, MAC) are converted from their string
// representations to the internal binary format before writing.
func (w *Writer) WriteRows(rows []map[string]any) error {
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
		case TypeIPv4, TypeIPv6, TypeMAC, TypePort, TypeProtocol, TypeDuration, TypeUUID, TypeDate:
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
			switch col.Type {
			case TypeIPv4:
				if s, ok := val.(string); ok {
					ip := net.ParseIP(s)
					if ip != nil {
						if ip4 := ip.To4(); ip4 != nil {
							row[col.Name] = int64(binary.BigEndian.Uint32(ip4))
						}
					}
				}
			case TypeIPv6:
				if s, ok := val.(string); ok {
					ip := net.ParseIP(s)
					if ip != nil {
						row[col.Name] = []byte(ip.To16())
					}
				}
			case TypeMAC:
				if s, ok := val.(string); ok {
					hw, err := net.ParseMAC(s)
					if err == nil && len(hw) == 6 {
						var n uint64
						for _, b := range hw {
							n = (n << 8) | uint64(b)
						}
						row[col.Name] = int64(n)
					}
				}
			case TypePort, TypeProtocol:
				switch tv := val.(type) {
				case int:
					row[col.Name] = int32(tv)
				case int64:
					row[col.Name] = int32(tv)
				case float64:
					row[col.Name] = int32(tv)
				}
			case TypeDuration:
				switch tv := val.(type) {
				case int:
					row[col.Name] = int64(tv)
				case int32:
					row[col.Name] = int64(tv)
				case float64:
					row[col.Name] = int64(tv)
				}
			case TypeUUID:
				if s, ok := val.(string); ok {
					raw := parseUUIDForWrite(s)
					if raw != nil {
						row[col.Name] = raw
					}
				}
			case TypeDate:
				if s, ok := val.(string); ok {
					d, err := parseDateForWriteChecked(s)
					if err != nil {
						return fmt.Errorf("column %q: %w", col.Name, err)
					}
					row[col.Name] = d
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

// parseUUIDForWrite converts a UUID string to raw 16 bytes for parquet storage.
func parseUUIDForWrite(s string) []byte {
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return nil
	}
	raw := make([]byte, 16)
	for i := 0; i < 16; i++ {
		hi := unhex(clean[i*2])
		lo := unhex(clean[i*2+1])
		if hi == 0xFF || lo == 0xFF {
			return nil
		}
		raw[i] = hi<<4 | lo
	}
	return raw
}

func unhex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 0xFF
	}
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
