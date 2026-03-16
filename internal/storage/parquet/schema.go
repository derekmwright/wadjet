// Package parquet provides Parquet file reading and writing on top of objstore.
package parquet

import (
	"fmt"
	"strings"
)

// TypeID identifies the data type of a column.
type TypeID int

const (
	TypeBool TypeID = iota
	TypeInt32
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeBytes
	TypeTimestamp
	TypeIPv4     // stored as uint32 (4 bytes)
	TypeIPv6     // stored as [16]byte via BytesColumn
	TypeCIDR     // stored as string (e.g., "192.168.1.0/24")
	TypeMAC      // stored as uint64 (6 bytes, upper 2 zero)
	TypePort     // stored as uint16 in Int32Data
	TypeProtocol // IANA protocol number, stored as uint8 in Int32Data
	TypeDuration // nanoseconds, stored as int64
	TypeUUID     // 128-bit UUID stored as 16-byte ByteArray
	TypeDate     // calendar date stored as int32 (days since 1970-01-01)
	TypeDecimal  // fixed-point DECIMAL(p,s) stored as scaled Int128
	TypeArray    // variable-length array of a single element type
	TypeRow      // struct/row with named fields
	TypeMap      // key-value map, stored as ARRAY(ROW("key","value"))
)

func (t TypeID) String() string {
	switch t {
	case TypeBool:
		return "BOOL"
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeFloat32:
		return "FLOAT32"
	case TypeFloat64:
		return "FLOAT64"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeIPv4:
		return "IPV4"
	case TypeIPv6:
		return "IPV6"
	case TypeCIDR:
		return "CIDR"
	case TypeMAC:
		return "MAC"
	case TypePort:
		return "PORT"
	case TypeProtocol:
		return "PROTOCOL"
	case TypeDuration:
		return "DURATION"
	case TypeUUID:
		return "UUID"
	case TypeDate:
		return "DATE"
	case TypeDecimal:
		return "DECIMAL"
	case TypeArray:
		return "ARRAY"
	case TypeRow:
		return "ROW"
	case TypeMap:
		return "MAP"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

// ParseTypeID parses a type name string into a TypeID.
// Supports parameterized types like "DECIMAL(10,2)".
func ParseTypeID(s string) (TypeID, error) {
	upper := strings.ToUpper(strings.TrimSpace(s))
	// Handle parameterized types
	if strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") {
		return TypeDecimal, nil
	}
	if strings.HasPrefix(upper, "ARRAY") {
		return TypeArray, nil
	}
	if strings.HasPrefix(upper, "ROW") || strings.HasPrefix(upper, "STRUCT") {
		return TypeRow, nil
	}
	if strings.HasPrefix(upper, "MAP") {
		return TypeMap, nil
	}
	switch upper {
	case "BOOL", "BOOLEAN":
		return TypeBool, nil
	case "INT32", "INT", "INTEGER":
		return TypeInt32, nil
	case "INT64", "BIGINT", "LONG":
		return TypeInt64, nil
	case "FLOAT32", "FLOAT":
		return TypeFloat32, nil
	case "FLOAT64", "DOUBLE":
		return TypeFloat64, nil
	case "STRING", "VARCHAR", "TEXT":
		return TypeString, nil
	case "BYTES", "BINARY", "VARBINARY":
		return TypeBytes, nil
	case "TIMESTAMP", "DATETIME":
		return TypeTimestamp, nil
	case "IPV4", "IP":
		return TypeIPv4, nil
	case "IPV6":
		return TypeIPv6, nil
	case "CIDR":
		return TypeCIDR, nil
	case "MAC", "MACADDR":
		return TypeMAC, nil
	case "PORT":
		return TypePort, nil
	case "PROTOCOL", "PROTO":
		return TypeProtocol, nil
	case "DURATION", "INTERVAL":
		return TypeDuration, nil
	case "UUID", "GUID":
		return TypeUUID, nil
	case "DATE":
		return TypeDate, nil
	default:
		return 0, fmt.Errorf("unknown type: %s", s)
	}
}

// ParseDecimalParams extracts precision and scale from a type string like "DECIMAL(10,2)".
// Returns default (38, 0) if no parameters are specified.
func ParseDecimalParams(s string) (precision, scale int) {
	upper := strings.ToUpper(strings.TrimSpace(s))
	precision, scale = 38, 0 // defaults

	idx := strings.Index(upper, "(")
	if idx < 0 {
		return
	}
	end := strings.Index(upper, ")")
	if end < 0 {
		return
	}
	params := upper[idx+1 : end]
	parts := strings.Split(params, ",")
	if len(parts) >= 1 {
		if p := strings.TrimSpace(parts[0]); p != "" {
			fmt.Sscanf(p, "%d", &precision)
		}
	}
	if len(parts) >= 2 {
		if s := strings.TrimSpace(parts[1]); s != "" {
			fmt.Sscanf(s, "%d", &scale)
		}
	}
	return
}

// ResolveColumn parses a type string into a fully-resolved Column with nested types.
// Handles: "INT64", "ARRAY(STRING)", "ROW(name STRING, age INT32)", "MAP(STRING, INT64)".
func ResolveColumn(name, typeStr string) (Column, error) {
	upper := strings.ToUpper(strings.TrimSpace(typeStr))

	// Check for parameterized nested types
	if idx := strings.Index(upper, "("); idx >= 0 {
		base := strings.TrimSpace(upper[:idx])
		// Extract inner params (everything between outer parens)
		end := strings.LastIndex(upper, ")")
		if end < 0 {
			return Column{}, fmt.Errorf("unmatched parenthesis in type: %s", typeStr)
		}
		inner := strings.TrimSpace(upper[idx+1 : end])

		switch base {
		case "ARRAY":
			elemCol, err := ResolveColumn("element", inner)
			if err != nil {
				return Column{}, fmt.Errorf("invalid ARRAY element type: %w", err)
			}
			return Column{Name: name, Type: TypeArray, Nullable: true, ElementType: &elemCol}, nil

		case "MAP":
			parts := splitTopLevel(inner)
			if len(parts) != 2 {
				return Column{}, fmt.Errorf("MAP requires exactly 2 type parameters, got %d", len(parts))
			}
			keyCol, err := ResolveColumn("key", parts[0])
			if err != nil {
				return Column{}, fmt.Errorf("invalid MAP key type: %w", err)
			}
			valCol, err := ResolveColumn("value", parts[1])
			if err != nil {
				return Column{}, fmt.Errorf("invalid MAP value type: %w", err)
			}
			entryCol := Column{
				Name: "entry", Type: TypeRow, Fields: []Column{keyCol, valCol},
			}
			return Column{Name: name, Type: TypeMap, Nullable: true, ElementType: &entryCol}, nil

		case "ROW", "STRUCT":
			fields, err := parseRowFields(inner)
			if err != nil {
				return Column{}, err
			}
			return Column{Name: name, Type: TypeRow, Nullable: true, Fields: fields}, nil

		case "DECIMAL", "NUMERIC":
			p, s := ParseDecimalParams(typeStr)
			return Column{Name: name, Type: TypeDecimal, Nullable: true, Precision: p, Scale: s}, nil
		}
	}

	// Simple type
	tid, err := ParseTypeID(upper)
	if err != nil {
		return Column{}, err
	}
	col := Column{Name: name, Type: tid, Nullable: true}
	if tid == TypeDecimal {
		col.Precision, col.Scale = ParseDecimalParams(typeStr)
	}
	return col, nil
}

// splitTopLevel splits a string by commas, but only at the top level (not inside parentheses).
func splitTopLevel(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

// parseRowFields parses "name1 TYPE1, name2 TYPE2, ..." into Column definitions.
func parseRowFields(s string) ([]Column, error) {
	parts := splitTopLevel(s)
	fields := make([]Column, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Split into name and type — first token is the name, rest is the type
		spaceIdx := strings.IndexByte(part, ' ')
		if spaceIdx < 0 {
			return nil, fmt.Errorf("ROW field must have name and type: %q", part)
		}
		fieldName := strings.TrimSpace(part[:spaceIdx])
		fieldType := strings.TrimSpace(part[spaceIdx+1:])
		col, err := ResolveColumn(fieldName, fieldType)
		if err != nil {
			return nil, fmt.Errorf("invalid ROW field %q: %w", fieldName, err)
		}
		fields = append(fields, col)
	}
	return fields, nil
}

// Column defines a column in a Parquet schema.
type Column struct {
	Name      string  `json:"name"`
	Type      TypeID  `json:"type"`
	Nullable  bool    `json:"nullable"`
	Precision int     `json:"precision,omitempty"` // for DECIMAL: max digits (1-38)
	Scale     int     `json:"scale,omitempty"`     // for DECIMAL: digits after decimal point
	ElementType *Column  `json:"element_type,omitempty"` // for ARRAY: element column definition
	Fields      []Column `json:"fields,omitempty"`       // for ROW/MAP: child field definitions
}

// Schema defines the schema for a Parquet file.
type Schema struct {
	Columns []Column `json:"columns"`
}

// ColumnIndex returns the index of a named column, or -1 if not found.
func (s *Schema) ColumnIndex(name string) int {
	for i, c := range s.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// HasColumn returns true if the schema contains a column with the given name.
func (s *Schema) HasColumn(name string) bool {
	return s.ColumnIndex(name) >= 0
}

// ColumnNames returns the names of all columns.
func (s *Schema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}
