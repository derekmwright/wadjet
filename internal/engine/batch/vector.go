package batch

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// formatIPv4 formats a uint32 IPv4 address as a string without allocating net.IP.
func formatIPv4(v uint32) string {
	var buf [15]byte // max "255.255.255.255"
	n := 0
	for i := 3; i >= 0; i-- {
		if i < 3 {
			buf[n] = '.'
			n++
		}
		octet := v >> (uint(i) * 8) & 0xFF
		if octet >= 100 {
			buf[n] = '0' + byte(octet/100)
			n++
			octet %= 100
			buf[n] = '0' + byte(octet/10)
			n++
			buf[n] = '0' + byte(octet%10)
			n++
		} else if octet >= 10 {
			buf[n] = '0' + byte(octet/10)
			n++
			buf[n] = '0' + byte(octet%10)
			n++
		} else {
			buf[n] = '0' + byte(octet)
			n++
		}
	}
	return string(buf[:n])
}

// formatMAC formats a uint64 (lower 48 bits) as a MAC address without allocating net.HardwareAddr.
func formatMAC(v uint64) string {
	const hex = "0123456789abcdef"
	var buf [17]byte // "aa:bb:cc:dd:ee:ff"
	for i := 0; i < 6; i++ {
		if i > 0 {
			buf[i*3-1] = ':'
		}
		b := byte(v >> uint((5-i)*8))
		buf[i*3] = hex[b>>4]
		buf[i*3+1] = hex[b&0xf]
	}
	return string(buf[:17])
}

// TypeID is an alias for the parquet TypeID used throughout the engine.
type TypeID = parquet.TypeID

const (
	TypeBool      = parquet.TypeBool
	TypeInt32     = parquet.TypeInt32
	TypeInt64     = parquet.TypeInt64
	TypeFloat32   = parquet.TypeFloat32
	TypeFloat64   = parquet.TypeFloat64
	TypeString    = parquet.TypeString
	TypeBytes     = parquet.TypeBytes
	TypeTimestamp = parquet.TypeTimestamp
	TypeIPv4     = parquet.TypeIPv4
	TypeIPv6     = parquet.TypeIPv6
	TypeCIDR     = parquet.TypeCIDR
	TypeMAC      = parquet.TypeMAC
	TypePort     = parquet.TypePort
	TypeProtocol = parquet.TypeProtocol
	TypeDuration = parquet.TypeDuration
	TypeUUID     = parquet.TypeUUID
	TypeDate     = parquet.TypeDate
	TypeDecimal  = parquet.TypeDecimal
)

// BytesColumn stores variable-length byte data (strings, binary) with zero
// per-row allocations using an offset/data layout.
type BytesColumn struct {
	Offsets []uint32 // len = num_rows + 1
	Data    []byte   // contiguous buffer
}

// NewBytesColumn creates a new BytesColumn with the given capacity.
// Pre-allocates offsets for positional access (all offsets start at 0 = empty strings).
func NewBytesColumn(capacity int) BytesColumn {
	offsets := make([]uint32, capacity+1)
	return BytesColumn{
		Offsets: offsets,
		Data:    make([]byte, 0, capacity*16), // estimate 16 bytes per value
	}
}

// Set writes a value at positional index i. The BytesColumn must have been
// created with NewBytesColumn(capacity >= i+1). Values must be set in order
// (i = 0, 1, 2, ...) because later offsets depend on prior data length.
func (bc *BytesColumn) Set(i int, val []byte) {
	bc.Data = append(bc.Data, val...)
	bc.Offsets[i+1] = uint32(len(bc.Data))
}

// Value returns the byte slice at position i.
func (bc *BytesColumn) Value(i int) []byte {
	start := bc.Offsets[i]
	end := bc.Offsets[i+1]
	return bc.Data[start:end]
}

// StringValue returns the string at position i.
func (bc *BytesColumn) StringValue(i int) string {
	return string(bc.Value(i))
}

// Len returns the number of values.
func (bc *BytesColumn) Len() int {
	return len(bc.Offsets) - 1
}

// Reset clears the bytes column for reuse.
func (bc *BytesColumn) Reset() {
	for i := range bc.Offsets {
		bc.Offsets[i] = 0
	}
	bc.Data = bc.Data[:0]
}

// Vector holds a single column of data. Uses typed slices instead of interface{}.
type Vector struct {
	Type        TypeID
	Len         int
	Nulls       Bitmap
	BoolData    []bool
	Int32Data   []int32
	Int64Data   []int64
	Float32Data []float32
	Float64Data []float64
	BytesData   BytesColumn
	DecimalData DecimalColumn // for TypeDecimal
}

// NewVector creates a new vector of the given type and length.
func NewVector(typ TypeID, length int) *Vector {
	return NewVectorWithScale(typ, length, 0)
}

// NewVectorWithScale creates a new vector with scale metadata (used for DECIMAL).
func NewVectorWithScale(typ TypeID, length int, scale int) *Vector {
	v := &Vector{
		Type:  typ,
		Len:   length,
		Nulls: NewBitmap(length),
	}
	switch typ {
	case TypeBool:
		v.BoolData = make([]bool, length)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		v.Int32Data = make([]int32, length)
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		v.Int64Data = make([]int64, length)
	case TypeFloat32:
		v.Float32Data = make([]float32, length)
	case TypeFloat64:
		v.Float64Data = make([]float64, length)
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		v.BytesData = NewBytesColumn(length)
	case TypeDecimal:
		v.DecimalData = NewDecimalColumn(length, scale)
	}
	return v
}

// GetValue returns the value at position i as an interface{}.
// Note: returns boxed values for numeric types (unavoidable with any return type).
// Prefer typed accessors (GetInt64, GetFloat64, etc.) in hot paths.
func (v *Vector) GetValue(i int) any {
	if v.Nulls.IsNullFast(i) {
		return nil
	}
	// Hot types first as if-chain for better branch prediction
	switch v.Type {
	case TypeInt64, TypeTimestamp:
		return v.Int64Data[i]
	case TypeFloat64:
		return v.Float64Data[i]
	case TypeString:
		return v.BytesData.StringValue(i)
	case TypeInt32:
		return v.Int32Data[i]
	case TypeBool:
		return v.BoolData[i]
	case TypeFloat32:
		return v.Float32Data[i]
	case TypeBytes:
		return v.BytesData.Value(i)
	case TypeIPv4:
		return formatIPv4(uint32(v.Int64Data[i]))
	case TypeIPv6:
		raw := v.BytesData.Value(i)
		if len(raw) == 16 {
			return net.IP(raw).String()
		}
		return ""
	case TypeCIDR:
		return v.BytesData.StringValue(i)
	case TypeMAC:
		return formatMAC(uint64(v.Int64Data[i]))
	case TypePort, TypeProtocol:
		return v.Int32Data[i]
	case TypeDuration:
		return v.Int64Data[i]
	case TypeUUID:
		raw := v.BytesData.Value(i)
		if len(raw) == 16 {
			return formatUUID(raw)
		}
		return ""
	case TypeDate:
		return formatDate(v.Int32Data[i])
	case TypeDecimal:
		return v.DecimalData.Data[i].FormatDecimal(v.DecimalData.Scale)
	default:
		return nil
	}
}

// SetValue sets the value at position i from an interface{}.
// For string/bytes types, values must be set in sequential order (i = 0, 1, 2, ...).
func (v *Vector) SetValue(i int, val any) {
	if val == nil {
		v.Nulls.SetNull(i)
		// For bytes/string columns, write a zero-length entry to keep offsets aligned
		if v.Type == TypeString || v.Type == TypeBytes || v.Type == TypeIPv6 || v.Type == TypeCIDR || v.Type == TypeUUID {
			v.BytesData.Set(i, nil)
		}
		return
	}
	v.Nulls.SetValid(i)
	switch v.Type {
	case TypeBool:
		v.BoolData[i] = val.(bool)
	case TypeInt32:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case float64:
			v.Int32Data[i] = int32(tv)
		}
	case TypeInt64, TypeTimestamp:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		}
	case TypeFloat32:
		switch tv := val.(type) {
		case float32:
			v.Float32Data[i] = tv
		case float64:
			v.Float32Data[i] = float32(tv)
		}
	case TypeFloat64:
		switch tv := val.(type) {
		case float64:
			v.Float64Data[i] = tv
		case float32:
			v.Float64Data[i] = float64(tv)
		case int64:
			v.Float64Data[i] = float64(tv)
		case int:
			v.Float64Data[i] = float64(tv)
		}
	case TypeString:
		switch tv := val.(type) {
		case string:
			v.BytesData.Set(i, []byte(tv))
		case []byte:
			v.BytesData.Set(i, tv)
		default:
			// Coerce non-string values to string representation
			v.BytesData.Set(i, []byte(fmt.Sprint(val)))
		}
	case TypeBytes:
		v.BytesData.Set(i, val.([]byte))
	case TypeIPv4:
		switch tv := val.(type) {
		case string:
			ip := net.ParseIP(tv)
			if ip == nil {
				return
			}
			ip4 := ip.To4()
			if ip4 == nil {
				return
			}
			v.Int64Data[i] = int64(binary.BigEndian.Uint32(ip4))
		case int64:
			v.Int64Data[i] = tv
		case int32:
			v.Int64Data[i] = int64(tv)
		default:
			return
		}
	case TypeIPv6:
		s, ok := val.(string)
		if !ok {
			v.BytesData.Set(i, nil)
			return
		}
		ip := net.ParseIP(s)
		if ip == nil {
			v.BytesData.Set(i, nil)
			return
		}
		ip6 := ip.To16()
		v.BytesData.Set(i, []byte(ip6))
	case TypeCIDR:
		s, ok := val.(string)
		if !ok {
			v.BytesData.Set(i, nil)
			return
		}
		v.BytesData.Set(i, []byte(s))
	case TypeMAC:
		switch tv := val.(type) {
		case string:
			hw, err := net.ParseMAC(tv)
			if err != nil || len(hw) != 6 {
				return
			}
			var n uint64
			for _, b := range hw {
				n = (n << 8) | uint64(b)
			}
			v.Int64Data[i] = int64(n)
		case int64:
			v.Int64Data[i] = tv
		default:
			return
		}
	case TypePort, TypeProtocol:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case float64:
			v.Int32Data[i] = int32(tv)
		}
	case TypeDuration:
		switch tv := val.(type) {
		case int64:
			v.Int64Data[i] = tv
		case int:
			v.Int64Data[i] = int64(tv)
		case int32:
			v.Int64Data[i] = int64(tv)
		case float64:
			v.Int64Data[i] = int64(tv)
		}
	case TypeUUID:
		switch tv := val.(type) {
		case string:
			raw := parseUUID(tv)
			v.BytesData.Set(i, raw)
		case []byte:
			v.BytesData.Set(i, tv)
		default:
			v.BytesData.Set(i, nil)
		}
	case TypeDate:
		switch tv := val.(type) {
		case int32:
			v.Int32Data[i] = tv
		case int:
			v.Int32Data[i] = int32(tv)
		case int64:
			v.Int32Data[i] = int32(tv)
		case string:
			v.Int32Data[i] = parseDateString(tv)
		}
	case TypeDecimal:
		switch tv := val.(type) {
		case Int128:
			v.DecimalData.Data[i] = tv
		case int64:
			v.DecimalData.Data[i] = Int128From(tv)
		case int:
			v.DecimalData.Data[i] = Int128From(int64(tv))
		case int32:
			v.DecimalData.Data[i] = Int128From(int64(tv))
		case float64:
			v.DecimalData.Data[i] = Int128FromFloat64(tv, v.DecimalData.Scale)
		case float32:
			v.DecimalData.Data[i] = Int128FromFloat64(float64(tv), v.DecimalData.Scale)
		case string:
			v.DecimalData.Data[i] = ParseDecimalString(tv, v.DecimalData.Scale)
		}
	}
}

// --- Typed accessors (zero-allocation hot path) ---

// GetInt64 returns the int64 value at position i. Returns (0, false) if null.
func (v *Vector) GetInt64(i int) (int64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Int64Data[i], true
}

// GetFloat64 returns the float64 value at position i. Returns (0, false) if null.
func (v *Vector) GetFloat64(i int) (float64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Float64Data[i], true
}

// GetInt32 returns the int32 value at position i. Returns (0, false) if null.
func (v *Vector) GetInt32(i int) (int32, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Int32Data[i], true
}

// GetFloat32 returns the float32 value at position i. Returns (0, false) if null.
func (v *Vector) GetFloat32(i int) (float32, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	return v.Float32Data[i], true
}

// GetBool returns the bool value at position i. Returns (false, false) if null.
func (v *Vector) GetBool(i int) (bool, bool) {
	if v.Nulls.IsNullFast(i) {
		return false, false
	}
	return v.BoolData[i], true
}

// GetString returns the string value at position i. Returns ("", false) if null.
func (v *Vector) GetString(i int) (string, bool) {
	if v.Nulls.IsNullFast(i) {
		return "", false
	}
	return v.BytesData.StringValue(i), true
}

// GetNumericFloat64 returns any numeric column value as float64 without boxing.
// Handles Int32, Int64, Float32, Float64, Timestamp types.
func (v *Vector) GetNumericFloat64(i int) (float64, bool) {
	if v.Nulls.IsNullFast(i) {
		return 0, false
	}
	switch v.Type {
	case TypeInt64, TypeTimestamp:
		return float64(v.Int64Data[i]), true
	case TypeFloat64:
		return v.Float64Data[i], true
	case TypeInt32:
		return float64(v.Int32Data[i]), true
	case TypeFloat32:
		return float64(v.Float32Data[i]), true
	case TypeDecimal:
		return v.DecimalData.Data[i].ToFloat64(v.DecimalData.Scale), true
	default:
		return 0, false
	}
}

// String returns a debug representation of the vector.
func (v *Vector) String() string {
	return fmt.Sprintf("Vector{type=%v, len=%d, nulls=%d}", v.Type, v.Len, v.Nulls.NullCount())
}

// formatUUID formats 16 raw bytes as a UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func formatUUID(b []byte) string {
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
}

// parseUUID parses a UUID string into 16 raw bytes.
// Accepts "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" or "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".
func parseUUID(s string) []byte {
	// Remove dashes
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
	_, err := hex.Decode(raw, clean)
	if err != nil {
		return nil
	}
	return raw
}

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// formatDate formats days-since-epoch as "2006-01-02".
func formatDate(days int32) string {
	t := epochDate.AddDate(0, 0, int(days))
	return t.Format("2006-01-02")
}

// parseDateString parses "2006-01-02" to days since epoch.
func parseDateString(s string) int32 {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0
	}
	return int32(t.Sub(epochDate).Hours() / 24)
}
