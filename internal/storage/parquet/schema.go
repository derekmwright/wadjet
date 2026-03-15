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
	TypeIPv4 // stored as uint32 (4 bytes)
	TypeIPv6 // stored as [16]byte via BytesColumn
	TypeCIDR // stored as string (e.g., "192.168.1.0/24")
	TypeMAC  // stored as uint64 (6 bytes, upper 2 zero)
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
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

// ParseTypeID parses a type name string into a TypeID.
func ParseTypeID(s string) (TypeID, error) {
	switch strings.ToUpper(s) {
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
	default:
		return 0, fmt.Errorf("unknown type: %s", s)
	}
}

// Column defines a column in a Parquet schema.
type Column struct {
	Name     string `json:"name"`
	Type     TypeID `json:"type"`
	Nullable bool   `json:"nullable"`
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
