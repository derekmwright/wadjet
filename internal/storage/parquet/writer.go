package parquet

import (
	"fmt"
	"io"
	"strings"

	goparquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// Compression identifies a compression codec for Parquet pages.
//
// Note: the underlying parquet-go gzip.Codec has a quirk where its zero value
// (Level=0) maps to gzip.NoCompression rather than gzip.DefaultCompression.
// Caelum works around this by explicitly setting Level to DefaultCompression
// when CompressionGzip is selected.
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

func compressionCodec(c Compression) goparquet.WriterOption {
	switch c {
	case CompressionZstd:
		return goparquet.Compression(&zstd.Codec{})
	case CompressionGzip:
		// Work around parquet-go quirk: gzip.Codec{} zero-initializes Level
		// to 0 which is gzip.NoCompression, not gzip.DefaultCompression (-1).
		return goparquet.Compression(&gzip.Codec{Level: gzip.DefaultCompression})
	case CompressionLZ4:
		return goparquet.Compression(&lz4.Codec{})
	case CompressionNone:
		return goparquet.Compression(&uncompressed.Codec{})
	default:
		return goparquet.Compression(&snappy.Codec{})
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

// Writer writes rows to a Parquet file.
type Writer struct {
	schema Schema
	config WriterConfig
	pw     *goparquet.GenericWriter[map[string]any]
}

// NewWriter creates a Parquet writer that writes to the given io.Writer.
func NewWriter(w io.Writer, schema Schema, cfg WriterConfig) (*Writer, error) {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 128 * 1024
	}

	parquetSchema, err := buildParquetSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("building parquet schema: %w", err)
	}

	opts := []goparquet.WriterOption{
		parquetSchema,
		compressionCodec(cfg.Compression),
		goparquet.CreatedBy("caelum", "0.1.0", ""),
	}
	if cfg.PageBufferSize > 0 {
		opts = append(opts, goparquet.PageBufferSize(cfg.PageBufferSize))
	}

	pw := goparquet.NewGenericWriter[map[string]any](w, opts...)

	return &Writer{
		schema: schema,
		config: cfg,
		pw:     pw,
	}, nil
}

// WriteRows writes a batch of rows to the Parquet file.
func (w *Writer) WriteRows(rows []map[string]any) error {
	_, err := w.pw.Write(rows)
	return err
}

// Close finalizes the Parquet file and closes the writer.
func (w *Writer) Close() error {
	return w.pw.Close()
}

func buildParquetSchema(schema Schema) (*goparquet.Schema, error) {
	nodes := make([]goparquet.Node, len(schema.Columns))
	for i, col := range schema.Columns {
		node, err := columnToNode(col)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		nodes[i] = node
	}
	group := goparquet.Group{}
	for i, col := range schema.Columns {
		group[col.Name] = nodes[i]
	}
	return goparquet.NewSchema("caelum", group), nil
}

func columnToNode(col Column) (goparquet.Node, error) {
	var node goparquet.Node
	switch col.Type {
	case TypeBool:
		node = goparquet.Leaf(goparquet.BooleanType)
	case TypeInt32:
		node = goparquet.Int(32)
	case TypeInt64:
		node = goparquet.Int(64)
	case TypeFloat32:
		node = goparquet.Leaf(goparquet.FloatType)
	case TypeFloat64:
		node = goparquet.Leaf(goparquet.DoubleType)
	case TypeString:
		node = goparquet.String()
	case TypeBytes:
		node = goparquet.Leaf(goparquet.ByteArrayType)
	case TypeTimestamp:
		node = goparquet.Timestamp(goparquet.Millisecond)
	default:
		return nil, fmt.Errorf("unsupported type: %v", col.Type)
	}

	if col.Nullable {
		node = goparquet.Optional(node)
	}
	return node, nil
}
