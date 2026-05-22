package scan

import (
	"encoding/binary"
	"fmt"
	"math"
	"runtime"

	"golang.org/x/sync/errgroup"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ReadFileBatchesNative reads all row groups from a Parquet file using our
// custom FileReader (no parquet-go dependency). Returns one RecordBatch per
// row group for schemas without unsupported types.
func ReadFileBatchesNative(fr *pqt.FileReader, schema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	return ReadFileBatchesNativeShard(fr, schema, selectedCols, 0, 1)
}

// ReadFileBatchesNativeShard reads only the row-group slice assigned to one
// shard. With shardCount=1 the behavior matches ReadFileBatchesNative.
//
// Row-group ownership: shardIdx i reads row groups in [i*total/count,
// (i+1)*total/count). When total < count, the early shards each read one
// row group and later shards read nothing — a degenerate but correct split.
func ReadFileBatchesNativeShard(fr *pqt.FileReader, schema []pqt.Column, selectedCols []string, shardIdx, shardCount int) ([]*batch.RecordBatch, error) {
	if shardCount < 1 {
		shardCount = 1
	}
	if shardIdx < 0 || shardIdx >= shardCount {
		return nil, nil
	}

	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}

	if HasUnsupportedColumnarTypes(readSchema) {
		return nil, fmt.Errorf("native reader does not support Array/Map types yet")
	}

	total := fr.NumRowGroups()
	startRg, endRg := rowGroupRangeForShard(total, shardIdx, shardCount)
	var batches []*batch.RecordBatch
	for rgIdx := startRg; rgIdx < endRg; rgIdx++ {
		b, err := ReadRowGroupNative(fr, rgIdx, readSchema, nil)
		if err != nil {
			return nil, err
		}
		if b != nil {
			batches = append(batches, b)
		}
	}
	return batches, nil
}

// rowGroupRangeForShard returns the half-open [start, end) row-group range
// for shardIdx out of shardCount total shards over total row groups. The
// union of ranges over [0, shardCount) covers exactly [0, total).
func rowGroupRangeForShard(total, shardIdx, shardCount int) (int, int) {
	if total <= 0 || shardCount <= 1 {
		return 0, total
	}
	start := shardIdx * total / shardCount
	end := (shardIdx + 1) * total / shardCount
	if shardIdx == shardCount-1 {
		end = total
	}
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	return start, end
}

// ReadRowGroupNative reads a row group using our custom page reader,
// bypassing parquet-go entirely for the data path.
func ReadRowGroupNative(fr *pqt.FileReader, rgIdx int, schema []pqt.Column, pool *batch.BatchPool) (*batch.RecordBatch, error) {
	numRows := int(fr.RowGroupNumRows(rgIdx))
	if numRows == 0 {
		return nil, nil
	}

	var b *batch.RecordBatch
	if pool != nil {
		b = pool.GetForSize(numRows)
	} else {
		b = batch.NewRecordBatch(schema, numRows)
	}

	leaves := fr.Leaves()
	rg := fr.RowGroupMeta(rgIdx)
	if rg == nil {
		return nil, fmt.Errorf("row group %d metadata not found", rgIdx)
	}

	// Build leaf-name-to-index mapping for column lookup.
	leafByName := make(map[string]int, len(leaves))
	for i, leaf := range leaves {
		leafByName[leaf.Name] = i
	}
	// Also map by path for qualified name lookup.
	leafByPath := make(map[string]int, len(leaves))
	for i, leaf := range leaves {
		if len(leaf.Path) >= 2 {
			key := leaf.Path[len(leaf.Path)-2] + "." + leaf.Path[len(leaf.Path)-1]
			leafByPath[key] = i
		}
	}

	// Per-column reads write only to their own b.Columns[i] slot, and
	// FileReader is read-only after construction (immutable meta + leaves;
	// each ColumnPages call allocates a fresh ColumnPageReader). Parallelize
	// the per-column work to use idle worker cores: 2026-05-22 SF100 Q18
	// profile showed cachedFileStreamSource.Next at 22.7% cum CPU with the
	// per-column loop serial within a single fragment goroutine.
	limit := len(schema)
	if gp := runtime.GOMAXPROCS(0); limit > gp {
		limit = gp
	}
	g := new(errgroup.Group)
	g.SetLimit(limit)
	for i, col := range schema {
		i, col := i, col
		// ROW: read each child field as a separate leaf column.
		if col.Type == pqt.TypeRow && len(col.Fields) > 0 {
			g.Go(func() error {
				for j, field := range col.Fields {
					key := col.Name + "." + field.Name
					childIdx, ok := leafByPath[key]
					if !ok {
						for k := 0; k < numRows; k++ {
							b.Columns[i].Children[j].Nulls.SetNull(k)
						}
						continue
					}
					if err := readColumnNative(b.Columns[i].Children[j], fr, rgIdx, childIdx, numRows, field.Type); err != nil {
						return fmt.Errorf("reading ROW field %s.%s: %w", col.Name, field.Name, err)
					}
				}
				return nil
			})
			continue
		}

		colIdx, ok := leafByName[col.Name]
		if !ok {
			// Try aliases.
			found := false
			for _, alias := range columnAliases[col.Name] {
				if idx, ok := leafByName[alias]; ok {
					colIdx = idx
					found = true
					break
				}
			}
			if !found {
				// Column not in parquet file — leave as all nulls.
				// Run synchronously: trivial work, no decode.
				for j := 0; j < numRows; j++ {
					b.Columns[i].Nulls.SetNull(j)
				}
				continue
			}
		}

		ci := colIdx
		g.Go(func() error {
			if err := readColumnNative(b.Columns[i], fr, rgIdx, ci, numRows, col.Type); err != nil {
				return fmt.Errorf("reading column %s: %w", col.Name, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return b, nil
}

// readColumnNative reads all pages from a column chunk into a Vector using
// our custom ColumnPageReader.
func readColumnNative(vec *batch.Vector, fr *pqt.FileReader, rgIdx, colIdx, numRows int, catalogType pqt.TypeID) error {
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return fmt.Errorf("column %d not found in row group %d", colIdx, rgIdx)
	}
	defer pr.Close()

	// Detect file type from schema tree.
	leaves := fr.Leaves()
	var fileType pqt.TypeID
	if colIdx < len(leaves) {
		fileType = pqt.TypeIDFromSchemaNode(leaves[colIdx])
	} else {
		fileType = catalogType
	}

	// Get max definition level from schema.
	maxDefLevel := 0
	if colIdx < len(leaves) {
		maxDefLevel = leaves[colIdx].MaxDefLevel
	}

	coerce := storageClass(fileType) != storageClass(catalogType)

	// Read dictionary if present.
	dict, err := pr.NextDictionary()
	if err != nil {
		return fmt.Errorf("reading dictionary: %w", err)
	}

	offset := 0
	for {
		page, err := pr.NextPage()
		if err != nil {
			return fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}

		pageRows := page.NumValues
		if pageRows == 0 {
			continue
		}

		data := page.Data
		// Resolve dictionary indices to actual values.
		if dict != nil {
			data = resolveNativeDictionary(dict, data, fileType)
		}

		defLevels := page.DefinitionLevels
		hasNulls := page.NumNulls > 0

		if defLevels == nil || !hasNulls {
			if coerce {
				if err := copyNativeCoercedDirect(vec, offset, data, pageRows, fileType, catalogType); err != nil {
					return err
				}
			} else {
				copyNativeDataDirect(vec, offset, data, pageRows, fileType)
			}
		} else {
			if coerce {
				if err := copyNativeCoercedScatter(vec, offset, data, defLevels, int32(maxDefLevel), pageRows, fileType, catalogType); err != nil {
					return err
				}
			} else {
				copyNativeDataScatter(vec, offset, data, defLevels, int32(maxDefLevel), pageRows, fileType)
			}
		}

		offset += pageRows
	}

	return nil
}

// resolveNativeDictionary resolves INT32 dictionary indices to actual values.
func resolveNativeDictionary(dict *pqt.DictionaryData, page pqt.Values, fileType pqt.TypeID) pqt.Values {
	indices := page.Int32()
	n := page.Count()

	switch fileType {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := dict.Data.Int64()
		dst := make([]int64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return pqt.PlainInt64Values(dst)

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := dict.Data.Int32()
		dst := make([]int32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return pqt.PlainInt32Values(dst)

	case pqt.TypeFloat64:
		src := dict.Data.Double()
		dst := make([]float64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return pqt.PlainFloat64Values(dst)

	case pqt.TypeFloat32:
		src := dict.Data.Float()
		dst := make([]float32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return pqt.PlainFloat32Values(dst)

	case pqt.TypeVector:
		// VECTOR stored as FIXED_LEN_BYTE_ARRAY: resolve dictionary indices.
		dictData, dictOffsets := dict.Data.ByteArray()
		var buf []byte
		offsets := make([]uint32, n+1)
		for i := 0; i < n; i++ {
			idx := indices[i]
			offsets[i] = uint32(len(buf))
			buf = append(buf, dictData[dictOffsets[idx]:dictOffsets[idx+1]]...)
		}
		offsets[n] = uint32(len(buf))
		return pqt.ByteArrayValues(buf, offsets)

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		dictData, dictOffsets := dict.Data.ByteArray()
		var buf []byte
		offsets := make([]uint32, n+1)
		for i := 0; i < n; i++ {
			idx := indices[i]
			offsets[i] = uint32(len(buf))
			buf = append(buf, dictData[dictOffsets[idx]:dictOffsets[idx+1]]...)
		}
		offsets[n] = uint32(len(buf))
		return pqt.ByteArrayValues(buf, offsets)

	default:
		// Fallback for unknown types: treat as byte array.
		dictData, dictOffsets := dict.Data.ByteArray()
		var buf []byte
		offsets := make([]uint32, n+1)
		for i := 0; i < n; i++ {
			idx := indices[i]
			offsets[i] = uint32(len(buf))
			buf = append(buf, dictData[dictOffsets[idx]:dictOffsets[idx+1]]...)
		}
		offsets[n] = uint32(len(buf))
		return pqt.ByteArrayValues(buf, offsets)
	}
}

// copyNativeDataDirect copies non-null page data into a Vector.
func copyNativeDataDirect(vec *batch.Vector, offset int, data pqt.Values, n int, typ pqt.TypeID) {
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := data.Int64()
		copy(vec.Int64Data[offset:offset+n], src[:n])

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := data.Int32()
		copy(vec.Int32Data[offset:offset+n], src[:n])

	case pqt.TypeFloat64:
		src := data.Double()
		copy(vec.Float64Data[offset:offset+n], src[:n])

	case pqt.TypeFloat32:
		src := data.Float()
		copy(vec.Float32Data[offset:offset+n], src[:n])

	case pqt.TypeBool:
		boolBytes := data.Boolean()
		for i := 0; i < n; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			vec.BoolData[offset+i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
		}

	case pqt.TypeVector:
		// FIXED_LEN_BYTE_ARRAY: decode little-endian float32 bytes into Float32Data.
		rawData, offsets := data.ByteArray()
		dim := vec.VectorDim
		if dim <= 0 {
			break
		}
		for i := 0; i < n; i++ {
			var val []byte
			if offsets != nil {
				val = rawData[offsets[i]:offsets[i+1]]
			} else if i*dim*4+dim*4 <= len(rawData) {
				val = rawData[i*dim*4 : (i+1)*dim*4]
			}
			dstOff := (offset + i) * dim
			for j := 0; j < dim && j*4+4 <= len(val); j++ {
				vec.Float32Data[dstOff+j] = math.Float32frombits(binary.LittleEndian.Uint32(val[j*4:]))
			}
		}

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			vec.BytesData.BulkSet(offset, rawData, offsets, n)
		} else {
			// PLAIN encoding: 4-byte length prefix per value.
			pos := 0
			for i := 0; i < n; i++ {
				if pos+4 > len(rawData) {
					break
				}
				length := int(binary.LittleEndian.Uint32(rawData[pos:]))
				pos += 4
				if pos+length > len(rawData) {
					break
				}
				vec.BytesData.Set(offset+i, rawData[pos:pos+length])
				pos += length
			}
		}
	}
}

// copyNativeDataScatter copies nullable page data into a Vector using
// int32 definition levels from our custom page reader.
func copyNativeDataScatter(vec *batch.Vector, offset int, data pqt.Values, defLevels []int32, maxDefLevel int32, n int, typ pqt.TypeID) {
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := data.Int64()
		scatterRunsNative(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Int64Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := data.Int32()
		scatterRunsNative(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Int32Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeFloat64:
		src := data.Double()
		scatterRunsNative(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Float64Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeFloat32:
		src := data.Float()
		scatterRunsNative(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Float32Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeBool:
		boolBytes := data.Boolean()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				byteIdx := valIdx / 8
				bitIdx := uint(valIdx % 8)
				vec.BoolData[offset+i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeVector:
		// FIXED_LEN_BYTE_ARRAY with nullable scatter.
		rawData, offsets := data.ByteArray()
		dim := vec.VectorDim
		if dim <= 0 {
			break
		}
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				var val []byte
				if offsets != nil {
					val = rawData[offsets[valIdx]:offsets[valIdx+1]]
				}
				dstOff := (offset + i) * dim
				for j := 0; j < dim && j*4+4 <= len(val); j++ {
					vec.Float32Data[dstOff+j] = math.Float32frombits(binary.LittleEndian.Uint32(val[j*4:]))
				}
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		rawData, offsets := data.ByteArray()
		valIdx := 0
		if offsets != nil {
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					start := offsets[valIdx]
					end := offsets[valIdx+1]
					vec.BytesData.Set(offset+i, rawData[start:end])
					valIdx++
				} else {
					vec.Nulls.SetNull(offset + i)
					vec.BytesData.Set(offset+i, nil)
				}
			}
		} else {
			// PLAIN encoding fallback.
			pos := 0
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					if pos+4 > len(rawData) {
						break
					}
					length := int(binary.LittleEndian.Uint32(rawData[pos:]))
					pos += 4
					vec.BytesData.Set(offset+i, rawData[pos:pos+length])
					pos += length
					valIdx++
				} else {
					vec.Nulls.SetNull(offset + i)
					vec.BytesData.Set(offset+i, nil)
				}
			}
		}
	}
}

// scatterRunsNative is like scatterRunsTyped but for int32 definition levels.
func scatterRunsNative(
	defLevels []int32, maxDefLevel int32, n int,
	onValid func(dstStart, srcStart, count int),
	onNull func(dstStart, count int),
) {
	valIdx := 0
	i := 0
	for i < n {
		if defLevels[i] == maxDefLevel {
			runStart := i
			srcStart := valIdx
			for i < n && defLevels[i] == maxDefLevel {
				i++
				valIdx++
			}
			onValid(runStart, srcStart, i-runStart)
		} else {
			runStart := i
			for i < n && defLevels[i] != maxDefLevel {
				i++
			}
			onNull(runStart, i-runStart)
		}
	}
}

// copyNativeCoercedDirect converts non-null page data from fileType to catalogType.
func copyNativeCoercedDirect(vec *batch.Vector, offset int, data pqt.Values, n int, fileType, catalogType pqt.TypeID) error {
	switch {
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeInt32:
		src := data.Int64()
		for i := 0; i < n; i++ {
			vec.Int32Data[offset+i] = int32(src[i])
		}
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeFloat64:
		src := data.Int64()
		for i := 0; i < n; i++ {
			vec.Float64Data[offset+i] = float64(src[i])
		}
	case (fileType == pqt.TypeDate || fileType == pqt.TypeInt32) && catalogType == pqt.TypeString:
		src := data.Int32()
		for i := 0; i < n; i++ {
			vec.BytesData.Set(offset+i, []byte(batch.FormatDate(src[i])))
		}
	default:
		return fmt.Errorf("unsupported type coercion: %v → %v", fileType, catalogType)
	}
	return nil
}

// copyNativeCoercedScatter converts nullable page data with type coercion.
func copyNativeCoercedScatter(vec *batch.Vector, offset int, data pqt.Values, defLevels []int32, maxDefLevel int32, n int, fileType, catalogType pqt.TypeID) error {
	switch {
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeInt32:
		src := data.Int64()
		scatterRunsNative(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			for i := 0; i < count; i++ {
				vec.Int32Data[offset+dstStart+i] = int32(src[srcStart+i])
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeFloat64:
		src := data.Int64()
		scatterRunsNative(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			for i := 0; i < count; i++ {
				vec.Float64Data[offset+dstStart+i] = float64(src[srcStart+i])
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})
	case (fileType == pqt.TypeDate || fileType == pqt.TypeInt32) && catalogType == pqt.TypeString:
		src := data.Int32()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				vec.BytesData.Set(offset+i, []byte(batch.FormatDate(src[valIdx])))
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
				vec.BytesData.Set(offset+i, nil)
			}
		}
	default:
		return fmt.Errorf("unsupported type coercion: %v → %v", fileType, catalogType)
	}
	return nil
}
