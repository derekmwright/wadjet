package ingest

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// computeColumnHLLs builds a HyperLogLog++ sketch for every column in
// the schema, scanning the row map per column. Returns a map keyed by
// column name; columns whose values are all nil are omitted.
//
// Called from flushBuffer alongside extractColumnStats so the catalog
// gets table-level NDV estimates the planner can consult for cardinality
// and dynamic-filter decisions.
func computeColumnHLLs(rows []map[string]any, schema parquet.Schema) map[string]*catalog.HLL {
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]*catalog.HLL, len(schema.Columns))
	for _, col := range schema.Columns {
		// HLL works on hashable scalar values. Skip nested types (Array,
		// Row, Map) — NDV isn't well-defined for them and the parquet
		// readers don't return them as single values anyway.
		switch col.Type {
		case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap:
			continue
		}
		var h catalog.HLL
		any := false
		for _, row := range rows {
			v, ok := row[col.Name]
			if !ok || v == nil {
				continue
			}
			any = true
			addValueToHLL(&h, v, col.Type)
		}
		if any {
			out[col.Name] = &h
		}
	}
	return out
}

// addValueToHLL hashes a value according to its declared parquet type
// and inserts the hash into the sketch.
//
// For numeric/temporal types we serialize a canonical 8-byte LE form to
// keep the hash stable across int32→int64 widening. For strings we hash
// the bytes directly. For bool we use a single byte 0/1.
func addValueToHLL(h *catalog.HLL, v any, t parquet.TypeID) {
	switch t {
	case parquet.TypeBool:
		b := byte(0)
		if bv, ok := v.(bool); ok && bv {
			b = 1
		}
		h.AddBytes([]byte{b})
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeDate,
		parquet.TypePort, parquet.TypeProtocol:
		h.AddInt64(asInt64(v))
	case parquet.TypeFloat32, parquet.TypeFloat64:
		bits := math.Float64bits(asFloat64(v))
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], bits)
		h.AddBytes(buf[:])
	case parquet.TypeTimestamp:
		// Timestamps stored as int64 ms epoch.
		switch tv := v.(type) {
		case time.Time:
			h.AddInt64(tv.UnixMilli())
		default:
			h.AddInt64(asInt64(v))
		}
	case parquet.TypeDuration:
		switch tv := v.(type) {
		case time.Duration:
			h.AddInt64(int64(tv))
		default:
			h.AddInt64(asInt64(v))
		}
	case parquet.TypeString, parquet.TypeBytes,
		parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR,
		parquet.TypeMAC, parquet.TypeUUID, parquet.TypeDecimal:
		switch sv := v.(type) {
		case string:
			h.AddBytes([]byte(sv))
		case []byte:
			h.AddBytes(sv)
		default:
			h.AddBytes([]byte(fmt.Sprintf("%v", v)))
		}
	default:
		// Unknown type — hash a stringified form. Conservative; better
		// than skipping (we can still distinguish "different repr" values).
		h.AddBytes([]byte(fmt.Sprintf("%v", v)))
	}
}

func asInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int32:
		return int64(tv)
	case int:
		return int64(tv)
	case uint64:
		return int64(tv)
	case uint32:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	case bool:
		if tv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func asFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int32:
		return float64(tv)
	case int:
		return float64(tv)
	default:
		return 0
	}
}
