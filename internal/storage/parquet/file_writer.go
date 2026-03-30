package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// NativeWriter writes Parquet files without depending on parquet-go.
// It uses PLAIN encoding, RLE definition levels, and configurable compression.
//
// Usage:
//
//	w := NewNativeWriter(out, schema, cfg)
//	w.WriteMapRows(rows)
//	w.Close()
type NativeWriter struct {
	w      io.Writer
	schema Schema
	config WriterConfig
	codec  CompressionCodec

	// Accumulates rows until row group flush.
	colBufs []columnBuffer
	numRows int

	written int64 // total bytes written to w so far
	rowGroups []RowGroup
}

// columnBuffer accumulates values for a single column.
type columnBuffer struct {
	col      Column
	physical PhysicalType

	// PLAIN-encoded value data for fixed-width types.
	data []byte
	// For BYTE_ARRAY: separate offsets and packed data.
	offsets []uint32
	packed  []byte
	// For BOOLEAN: bit-packed into bytes.
	boolBuf []byte
	boolPos int

	nulls    []bool // true = null at this row position
	numNulls int64
	count    int // total values (including nulls)

	// Statistics tracking.
	hasStats bool
	minI32, maxI32 int32
	minI64, maxI64 int64
	minF32, maxF32 float32
	minF64, maxF64 float64
	minBytes, maxBytes []byte
}

// NewNativeWriter creates a Parquet writer that writes to the given io.Writer.
func NewNativeWriter(w io.Writer, schema Schema, cfg WriterConfig) *NativeWriter {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 128 * 1024
	}

	codec := CodecSnappy
	switch cfg.Compression {
	case CompressionZstd:
		codec = CodecZstd
	case CompressionGzip:
		codec = CodecGzip
	case CompressionNone:
		codec = CodecNone
	case CompressionLZ4:
		codec = CodecNone // fall back to uncompressed for LZ4 (rarely used)
	default:
		codec = CodecSnappy
	}

	nw := &NativeWriter{
		w:      w,
		schema: schema,
		config: cfg,
		codec:  codec,
	}
	nw.initColumns()
	return nw
}

func (nw *NativeWriter) initColumns() {
	nw.colBufs = make([]columnBuffer, len(nw.schema.Columns))
	for i, col := range nw.schema.Columns {
		nw.colBufs[i] = columnBuffer{
			col:      col,
			physical: wadjetTypeToPhysical(col.Type),
		}
		if col.Nullable {
			nw.colBufs[i].nulls = make([]bool, 0, 1024)
		}
	}
}

func wadjetTypeToPhysical(t TypeID) PhysicalType {
	switch t {
	case TypeBool:
		return PhysicalBoolean
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		return PhysicalInt32
	case TypeInt64, TypeIPv4, TypeMAC, TypeDuration, TypeTimestamp:
		return PhysicalInt64
	case TypeFloat32:
		return PhysicalFloat
	case TypeFloat64:
		return PhysicalDouble
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		return PhysicalByteArray
	case TypeDecimal:
		return PhysicalInt64
	default:
		return PhysicalByteArray
	}
}

// WriteMapRows writes rows from map[string]any format.
// Network types are converted to their binary storage format.
func (nw *NativeWriter) WriteMapRows(rows []map[string]any) error {
	for _, row := range rows {
		for i := range nw.colBufs {
			cb := &nw.colBufs[i]
			val, ok := row[cb.col.Name]
			if !ok || val == nil {
				cb.appendNull()
				continue
			}
			cb.appendValue(val)
		}
		nw.numRows++

		if nw.numRows >= nw.config.RowGroupSize {
			if err := nw.flushRowGroup(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close flushes any remaining rows and writes the file footer.
func (nw *NativeWriter) Close() error {
	// Write magic header if this is the first write.
	if nw.written == 0 {
		if err := nw.writeBytes([]byte("PAR1")); err != nil {
			return err
		}
	}

	// Flush remaining rows.
	if nw.numRows > 0 {
		if err := nw.flushRowGroup(); err != nil {
			return err
		}
	}

	// Build and write footer.
	return nw.writeFooter()
}

func (nw *NativeWriter) writeBytes(b []byte) error {
	n, err := nw.w.Write(b)
	nw.written += int64(n)
	return err
}

func (nw *NativeWriter) flushRowGroup() error {
	// Write magic header on first flush.
	if nw.written == 0 {
		if err := nw.writeBytes([]byte("PAR1")); err != nil {
			return err
		}
	}

	rgOffset := nw.written
	numRows := int64(nw.numRows)
	var totalSize, totalCompressed int64

	columns := make([]ColumnChunk, len(nw.colBufs))
	for i := range nw.colBufs {
		cb := &nw.colBufs[i]
		chunkOffset := nw.written

		uncompressed, compressed, err := nw.writeColumnChunk(cb)
		if err != nil {
			return fmt.Errorf("writing column %s: %w", cb.col.Name, err)
		}
		totalSize += uncompressed
		totalCompressed += compressed

		columns[i] = ColumnChunk{
			FileOffset: chunkOffset,
			MetaData: &ColumnMetaData{
				Type:                  cb.physical,
				Encodings:             []Encoding{EncodingPlain, EncodingRLE},
				PathInSchema:          []string{cb.col.Name},
				Codec:                 nw.codec,
				NumValues:             int64(cb.count),
				TotalUncompressedSize: uncompressed,
				TotalCompressedSize:   compressed,
				DataPageOffset:        chunkOffset,
				Statistics:            cb.buildStats(),
			},
		}

		// Reset column buffer for next row group.
		cb.reset()
	}

	nw.rowGroups = append(nw.rowGroups, RowGroup{
		Columns:             columns,
		TotalByteSize:       totalSize,
		NumRows:             numRows,
		FileOffset:          rgOffset,
		TotalCompressedSize: totalCompressed,
	})

	nw.numRows = 0
	return nil
}

func (nw *NativeWriter) writeColumnChunk(cb *columnBuffer) (uncompressed, compressed int64, err error) {
	// Build page data: definition levels (if nullable) + PLAIN-encoded values.
	var pageBuf bytes.Buffer

	maxDefLevel := int32(0)
	if cb.col.Nullable {
		maxDefLevel = 1
	}

	// Write definition levels (RLE encoded with 4-byte LE length prefix).
	if maxDefLevel > 0 {
		defData := encodeDefLevelsRLE(cb.nulls, cb.count)
		// 4-byte LE length prefix.
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(defData)))
		pageBuf.Write(lenBuf[:])
		pageBuf.Write(defData)
	}

	// Write PLAIN-encoded values.
	pageBuf.Write(cb.plainData())

	uncompressedData := pageBuf.Bytes()
	uncompressedSize := int32(len(uncompressedData))

	// Compress.
	compressedData, err := compressPage(uncompressedData, nw.codec)
	if err != nil {
		return 0, 0, fmt.Errorf("compressing page: %w", err)
	}
	compressedSize := int32(len(compressedData))

	// Build page header.
	ph := &PageHeader{
		Type:                 PageDataV1,
		UncompressedPageSize: uncompressedSize,
		CompressedPageSize:   compressedSize,
		DataPageHeader: &DataPageHeader{
			NumValues:               int32(cb.count),
			Encoding:                EncodingPlain,
			DefinitionLevelEncoding: EncodingRLE,
			RepetitionLevelEncoding: EncodingRLE,
		},
	}

	headerBytes := EncodePageHeader(ph)

	// Write header + compressed data.
	if err := nw.writeBytes(headerBytes); err != nil {
		return 0, 0, err
	}
	if err := nw.writeBytes(compressedData); err != nil {
		return 0, 0, err
	}

	totalUncompressed := int64(len(headerBytes)) + int64(uncompressedSize)
	totalCompressed := int64(len(headerBytes)) + int64(compressedSize)
	return totalUncompressed, totalCompressed, nil
}

func (nw *NativeWriter) writeFooter() error {
	totalRows := int64(0)
	for _, rg := range nw.rowGroups {
		totalRows += rg.NumRows
	}

	// Build schema elements (flattened schema tree).
	schemaElements := buildSchemaElements(nw.schema)

	md := &FileMetaData{
		Version:   1,
		Schema:    schemaElements,
		NumRows:   totalRows,
		RowGroups: nw.rowGroups,
		CreatedBy: "wadjet (native writer)",
		KeyValueMetadata: []KeyValue{
			{Key: "wadjet.version", Value: "0.1.0"},
		},
	}

	footerBytes := EncodeFileMetaData(md)

	if err := nw.writeBytes(footerBytes); err != nil {
		return err
	}

	// Write footer length (4 bytes LE).
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footerBytes)))
	if err := nw.writeBytes(lenBuf[:]); err != nil {
		return err
	}

	// Write magic trailer.
	return nw.writeBytes([]byte("PAR1"))
}

// buildSchemaElements creates the flattened schema tree for the footer.
func buildSchemaElements(schema Schema) []SchemaElement {
	// Root element (message).
	root := SchemaElement{
		Name:        "wadjet_schema",
		NumChildren: int32(len(schema.Columns)),
	}
	elements := []SchemaElement{root}

	for _, col := range schema.Columns {
		se := SchemaElement{
			Name: col.Name,
		}
		pt := wadjetTypeToPhysical(col.Type)
		se.Type = &pt

		if col.Nullable {
			se.RepetitionType = FieldOptional
		} else {
			se.RepetitionType = FieldRequired
		}

		// Set converted type and logical type for annotations.
		switch col.Type {
		case TypeString, TypeCIDR:
			ct := ConvertedUTF8
			se.ConvertedType = &ct
			se.LogicalType = &LogicalType{Type: LogicalString}
		case TypeTimestamp:
			ct := ConvertedTimestampMillis
			se.ConvertedType = &ct
			se.LogicalType = &LogicalType{
				Type:            LogicalTimestampMillis,
				IsAdjustedToUTC: true,
			}
		case TypeDate:
			ct := ConvertedDate
			se.ConvertedType = &ct
			se.LogicalType = &LogicalType{Type: LogicalDate}
		case TypeInt32:
			ct := ConvertedInt32
			se.ConvertedType = &ct
			se.LogicalType = &LogicalType{Type: LogicalInteger, BitWidth: 32, IsSigned: true}
		case TypeInt64:
			ct := ConvertedInt64
			se.ConvertedType = &ct
			se.LogicalType = &LogicalType{Type: LogicalInteger, BitWidth: 64, IsSigned: true}
		case TypeDecimal:
			ct := ConvertedDecimal
			se.ConvertedType = &ct
			prec := col.Precision
			if prec <= 0 {
				prec = 38
			}
			se.Precision = int32(prec)
			se.Scale = int32(col.Scale)
			se.LogicalType = &LogicalType{
				Type:      LogicalDecimal,
				Precision: prec,
				Scale:     col.Scale,
			}
		}

		elements = append(elements, se)
	}
	return elements
}

// --- Column buffer methods ---

func (cb *columnBuffer) appendNull() {
	if cb.nulls != nil {
		cb.nulls = append(cb.nulls, true)
	}
	cb.numNulls++
	cb.count++
}

func (cb *columnBuffer) appendValue(val any) {
	if cb.nulls != nil {
		cb.nulls = append(cb.nulls, false)
	}
	cb.count++

	switch cb.physical {
	case PhysicalBoolean:
		b := toBool(val)
		cb.appendBool(b)
	case PhysicalInt32:
		v := toInt32(val, cb.col.Type)
		cb.appendInt32(v)
	case PhysicalInt64:
		v := toInt64(val, cb.col.Type)
		cb.appendInt64(v)
	case PhysicalFloat:
		v := toFloat32(val)
		cb.appendFloat32(v)
	case PhysicalDouble:
		v := toFloat64(val)
		cb.appendFloat64(v)
	case PhysicalByteArray:
		b := toBytes(val, cb.col.Type)
		cb.appendByteArray(b)
	}
}

func (cb *columnBuffer) appendBool(v bool) {
	if cb.boolPos%8 == 0 {
		cb.boolBuf = append(cb.boolBuf, 0)
	}
	if v {
		cb.boolBuf[len(cb.boolBuf)-1] |= 1 << (cb.boolPos % 8)
	}
	cb.boolPos++
}

func (cb *columnBuffer) appendInt32(v int32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(v))
	cb.data = append(cb.data, buf[:]...)
	cb.updateStatsI32(v)
}

func (cb *columnBuffer) appendInt64(v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	cb.data = append(cb.data, buf[:]...)
	cb.updateStatsI64(v)
}

func (cb *columnBuffer) appendFloat32(v float32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
	cb.data = append(cb.data, buf[:]...)
	cb.updateStatsF32(v)
}

func (cb *columnBuffer) appendFloat64(v float64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	cb.data = append(cb.data, buf[:]...)
	cb.updateStatsF64(v)
}

func (cb *columnBuffer) appendByteArray(b []byte) {
	cb.offsets = append(cb.offsets, uint32(len(cb.packed)))
	cb.packed = append(cb.packed, b...)
	cb.updateStatsBytes(b)
}

func (cb *columnBuffer) plainData() []byte {
	switch cb.physical {
	case PhysicalBoolean:
		return cb.boolBuf
	case PhysicalByteArray:
		// PLAIN byte array: each value prefixed with 4-byte LE length.
		var buf bytes.Buffer
		for i := 0; i < len(cb.offsets); i++ {
			start := cb.offsets[i]
			var end uint32
			if i+1 < len(cb.offsets) {
				end = cb.offsets[i+1]
			} else {
				end = uint32(len(cb.packed))
			}
			val := cb.packed[start:end]
			var lenBuf [4]byte
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(val)))
			buf.Write(lenBuf[:])
			buf.Write(val)
		}
		return buf.Bytes()
	default:
		return cb.data
	}
}

func (cb *columnBuffer) reset() {
	cb.data = cb.data[:0]
	cb.offsets = cb.offsets[:0]
	cb.packed = cb.packed[:0]
	cb.boolBuf = cb.boolBuf[:0]
	cb.boolPos = 0
	if cb.nulls != nil {
		cb.nulls = cb.nulls[:0]
	}
	cb.numNulls = 0
	cb.count = 0
	cb.hasStats = false
	cb.minBytes = nil
	cb.maxBytes = nil
}

func (cb *columnBuffer) updateStatsI32(v int32) {
	if !cb.hasStats {
		cb.minI32, cb.maxI32 = v, v
		cb.hasStats = true
	} else {
		if v < cb.minI32 {
			cb.minI32 = v
		}
		if v > cb.maxI32 {
			cb.maxI32 = v
		}
	}
}

func (cb *columnBuffer) updateStatsI64(v int64) {
	if !cb.hasStats {
		cb.minI64, cb.maxI64 = v, v
		cb.hasStats = true
	} else {
		if v < cb.minI64 {
			cb.minI64 = v
		}
		if v > cb.maxI64 {
			cb.maxI64 = v
		}
	}
}

func (cb *columnBuffer) updateStatsF32(v float32) {
	if !cb.hasStats || v < cb.minF32 {
		cb.minF32 = v
	}
	if !cb.hasStats || v > cb.maxF32 {
		cb.maxF32 = v
	}
	cb.hasStats = true
}

func (cb *columnBuffer) updateStatsF64(v float64) {
	if !cb.hasStats || v < cb.minF64 {
		cb.minF64 = v
	}
	if !cb.hasStats || v > cb.maxF64 {
		cb.maxF64 = v
	}
	cb.hasStats = true
}

func (cb *columnBuffer) updateStatsBytes(b []byte) {
	if !cb.hasStats {
		cb.minBytes = append([]byte(nil), b...)
		cb.maxBytes = append([]byte(nil), b...)
		cb.hasStats = true
	} else {
		if bytes.Compare(b, cb.minBytes) < 0 {
			cb.minBytes = append(cb.minBytes[:0], b...)
		}
		if bytes.Compare(b, cb.maxBytes) > 0 {
			cb.maxBytes = append(cb.maxBytes[:0], b...)
		}
	}
}

func (cb *columnBuffer) buildStats() *Statistics {
	s := &Statistics{NullCount: cb.numNulls}
	if !cb.hasStats {
		return s
	}
	switch cb.physical {
	case PhysicalInt32:
		s.MinValue = make([]byte, 4)
		s.MaxValue = make([]byte, 4)
		binary.LittleEndian.PutUint32(s.MinValue, uint32(cb.minI32))
		binary.LittleEndian.PutUint32(s.MaxValue, uint32(cb.maxI32))
	case PhysicalInt64:
		s.MinValue = make([]byte, 8)
		s.MaxValue = make([]byte, 8)
		binary.LittleEndian.PutUint64(s.MinValue, uint64(cb.minI64))
		binary.LittleEndian.PutUint64(s.MaxValue, uint64(cb.maxI64))
	case PhysicalFloat:
		s.MinValue = make([]byte, 4)
		s.MaxValue = make([]byte, 4)
		binary.LittleEndian.PutUint32(s.MinValue, math.Float32bits(cb.minF32))
		binary.LittleEndian.PutUint32(s.MaxValue, math.Float32bits(cb.maxF32))
	case PhysicalDouble:
		s.MinValue = make([]byte, 8)
		s.MaxValue = make([]byte, 8)
		binary.LittleEndian.PutUint64(s.MinValue, math.Float64bits(cb.minF64))
		binary.LittleEndian.PutUint64(s.MaxValue, math.Float64bits(cb.maxF64))
	case PhysicalByteArray:
		s.MinValue = append([]byte(nil), cb.minBytes...)
		s.MaxValue = append([]byte(nil), cb.maxBytes...)
	}
	return s
}

// --- Type conversion helpers ---

func toBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	default:
		return false
	}
}

func toInt32(v any, colType TypeID) int32 {
	switch t := v.(type) {
	case int:
		return int32(t)
	case int32:
		return t
	case int64:
		return int32(t)
	case float64:
		return int32(t)
	case string:
		if colType == TypeDate {
			return parseDateForWrite(t)
		}
		return 0
	default:
		return 0
	}
}

func toInt64(v any, colType TypeID) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		return convertStringToInt64(t, colType)
	default:
		return 0
	}
}

func toFloat32(v any) float32 {
	switch t := v.(type) {
	case float32:
		return t
	case float64:
		return float32(t)
	case int:
		return float32(t)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
}

func toBytes(v any, colType TypeID) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return convertStringToBytes(t, colType)
	default:
		return nil
	}
}

// convertStringToInt64 handles network type string-to-int64 conversion.
func convertStringToInt64(s string, colType TypeID) int64 {
	switch colType {
	case TypeIPv4:
		return ipv4StringToInt64(s)
	case TypeMAC:
		return macStringToInt64(s)
	default:
		return 0
	}
}

// convertStringToBytes handles network type string-to-bytes conversion.
func convertStringToBytes(s string, colType TypeID) []byte {
	switch colType {
	case TypeIPv6:
		return ipv6StringToBytes(s)
	case TypeUUID:
		b := parseUUIDForWrite(s)
		if b != nil {
			return b
		}
		return []byte(s)
	default:
		return []byte(s)
	}
}

// --- RLE encoding for definition levels ---

// encodeDefLevelsRLE encodes definition levels using RLE/bit-packing hybrid.
// For nullable columns, bitWidth=1: 0=null, 1=present.
func encodeDefLevelsRLE(nulls []bool, count int) []byte {
	if count == 0 {
		return nil
	}

	var buf []byte

	// Encode as consecutive RLE runs.
	i := 0
	for i < count {
		val := byte(1) // present
		if i < len(nulls) && nulls[i] {
			val = 0 // null
		}

		// Count consecutive same values.
		runLen := 1
		for i+runLen < count {
			nextVal := byte(1)
			if i+runLen < len(nulls) && nulls[i+runLen] {
				nextVal = 0
			}
			if nextVal != val {
				break
			}
			runLen++
		}

		// RLE header: count << 1 (LSB=0 for RLE mode).
		buf = appendVarint(buf, uint64(runLen)<<1)
		// Value: 1 byte (bitWidth=1 → ceil(1/8) = 1).
		buf = append(buf, val)
		i += runLen
	}

	return buf
}

func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// --- Compression ---

func compressPage(data []byte, codec CompressionCodec) ([]byte, error) {
	switch codec {
	case CodecNone:
		return data, nil
	case CodecSnappy:
		return snappy.Encode(nil, data), nil
	case CodecZstd:
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, fmt.Errorf("creating zstd encoder: %w", err)
		}
		defer enc.Close()
		return enc.EncodeAll(data, nil), nil
	case CodecGzip:
		var buf bytes.Buffer
		w, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
		if err != nil {
			return nil, fmt.Errorf("creating gzip writer: %w", err)
		}
		if _, err := w.Write(data); err != nil {
			return nil, fmt.Errorf("gzip write: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("gzip close: %w", err)
		}
		return buf.Bytes(), nil
	default:
		return data, nil
	}
}

// Network type conversion helpers (reuse writer.go functions where possible).
func ipv4StringToInt64(s string) int64 {
	// Simple IPv4 parser — avoid net.ParseIP allocation.
	var ip [4]byte
	idx := 0
	octet := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			ip[idx] = byte(octet)
			idx++
			octet = 0
			if idx >= 4 {
				return 0
			}
		} else if s[i] >= '0' && s[i] <= '9' {
			octet = octet*10 + int(s[i]-'0')
		} else {
			return 0
		}
	}
	if idx == 3 {
		ip[idx] = byte(octet)
		return int64(binary.BigEndian.Uint32(ip[:]))
	}
	return 0
}

func macStringToInt64(s string) int64 {
	// Parse "00:11:22:33:44:55" format.
	if len(s) != 17 {
		return 0
	}
	var n uint64
	for i := 0; i < 6; i++ {
		hi := unhex(s[i*3])
		lo := unhex(s[i*3+1])
		if hi == 0xFF || lo == 0xFF {
			return 0
		}
		n = (n << 8) | uint64(hi<<4|lo)
	}
	return int64(n)
}

func ipv6StringToBytes(s string) []byte {
	// IPv6 parsing is complex. The existing Writer.prepareRows already
	// converts IPv6 strings to 16-byte binary before calling WriteRows,
	// so the NativeWriter receives pre-converted []byte values via toBytes.
	// This fallback just stores as string bytes for direct API usage.
	return []byte(s)
}
