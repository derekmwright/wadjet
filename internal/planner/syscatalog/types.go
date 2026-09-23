// SPDX-License-Identifier: MIT

package syscatalog

import (
	_ "embed"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TypeInfo is what the catalog says about a column's type: the OID and names
// pg_attribute, pg_type, format_type() and information_schema.columns report.
//
// It is derived from the ENGINE type by the same rule the wire's
// RowDescription uses (pgwire.pgColumnOID for a stored column), so a client
// that reads a column's type from the catalog and then from a result set is
// told the same thing twice. A catalog saying `text` beside a RowDescription
// saying int4 is the contradiction #834 was; the pgwire package asserts the
// two rules agree for every engine type.
type TypeInfo struct {
	OID int64
	// Typname is pg_type.typname — information_schema's udt_name.
	Typname string
	// SQLName is format_type()'s rendering without a modifier.
	SQLName string
	// DataType is information_schema.columns.data_type: the SQL name, or
	// ARRAY for an array.
	DataType string
	Typmod   int32
	Len      int16
	ByVal    bool
	Align    string
	Storage  string
	// Collation is pg_attribute.attcollation: 100 ("default") for a
	// collatable type, 0 otherwise.
	Collation int64
	// Ndims is pg_attribute.attndims.
	Ndims int32

	NumericPrecision      any // int32 or nil
	NumericPrecisionRadix any
	NumericScale          any
	DatetimePrecision     any
	CharOctetLength       any
}

// FormatType is format_type(oid, typmod) for this column.
func (t TypeInfo) FormatType() string {
	if t.OID == 1700 && t.Typmod >= 4 {
		p := (t.Typmod - 4) >> 16
		s := (t.Typmod - 4) & 0xFFFF
		return fmt.Sprintf("numeric(%d,%d)", p, s)
	}
	return t.SQLName
}

// scalar type descriptions, keyed by the OID the wire declares.
var scalarTypes = map[int64]TypeInfo{
	16:   {OID: 16, Typname: "bool", SQLName: "boolean", Len: 1, ByVal: true, Align: "c", Storage: "p"},
	17:   {OID: 17, Typname: "bytea", SQLName: "bytea", Len: -1, Align: "i", Storage: "x"},
	20:   {OID: 20, Typname: "int8", SQLName: "bigint", Len: 8, ByVal: true, Align: "d", Storage: "p"},
	23:   {OID: 23, Typname: "int4", SQLName: "integer", Len: 4, ByVal: true, Align: "i", Storage: "p"},
	25:   {OID: 25, Typname: "text", SQLName: "text", Len: -1, Align: "i", Storage: "x", Collation: 100},
	700:  {OID: 700, Typname: "float4", SQLName: "real", Len: 4, ByVal: true, Align: "i", Storage: "p"},
	701:  {OID: 701, Typname: "float8", SQLName: "double precision", Len: 8, ByVal: true, Align: "d", Storage: "p"},
	1082: {OID: 1082, Typname: "date", SQLName: "date", Len: 4, ByVal: true, Align: "i", Storage: "p"},
	1114: {OID: 1114, Typname: "timestamp", SQLName: "timestamp without time zone", Len: 8, ByVal: true, Align: "d", Storage: "p"},
	1700: {OID: 1700, Typname: "numeric", SQLName: "numeric", Len: -1, Align: "i", Storage: "m"},
	2950: {OID: 2950, Typname: "uuid", SQLName: "uuid", Len: 16, Align: "c", Storage: "p"},
}

// arrayOIDs pairs an element OID with its array type's OID, as pg_type's
// typarray does on 17.11 — the same pairing pgwire.pgArrayOID declares.
var arrayOIDs = map[int64]int64{
	16: 1000, 17: 1001, 20: 1016, 23: 1007, 25: 1009, 700: 1021, 701: 1022,
	1082: 1182, 1114: 1115, 1700: 1231, 2950: 2951,
}

// ScalarOID is the OID a stored column of engine type t declares on the wire:
// pgwire.pgTypeOID's rule, restated here because the catalog must say what
// the wire says and pgwire imports this package, not the other way round.
func ScalarOID(t parquet.TypeID) int64 {
	switch t {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		return 23
	case parquet.TypeInt64, parquet.TypeDuration:
		return 20
	case parquet.TypeFloat32:
		return 700
	case parquet.TypeFloat64:
		return 701
	case parquet.TypeBool:
		return 16
	case parquet.TypeTimestamp:
		return 1114
	case parquet.TypeDate:
		return 1082
	case parquet.TypeDecimal:
		return 1700
	case parquet.TypeBytes:
		return 17
	case parquet.TypeUUID:
		return 2950
	}
	return 25
}

// ColumnOID is the OID a stored column declares: its scalar OID, or for an
// ARRAY the array type of its element's — text[] for an element the wire
// declares as text, and for a nested, ROW or MAP element (pgwire's
// pgArrayColumnOID).
func ColumnOID(c parquet.Column) int64 {
	if c.Type != parquet.TypeArray {
		return ScalarOID(c.Type)
	}
	if c.ElementType == nil {
		return 25
	}
	switch c.ElementType.Type {
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap:
		return 25
	}
	if a, ok := arrayOIDs[ScalarOID(c.ElementType.Type)]; ok {
		return a
	}
	return 1009
}

// TypeOf describes a stored column's type the way PostgreSQL's catalog
// describes the type the wire declares for it.
func TypeOf(c parquet.Column) TypeInfo {
	oid := ColumnOID(c)
	if c.Type == parquet.TypeArray && oid != 25 {
		elem := scalarTypes[ScalarOID(c.ElementType.Type)]
		return TypeInfo{
			OID: oid, Typname: "_" + elem.Typname, SQLName: elem.SQLName + "[]",
			DataType: "ARRAY", Typmod: -1, Len: -1, Align: arrayAlign(elem.Align),
			Storage: "x", Collation: elem.Collation, Ndims: 1,
		}
	}
	info := scalarTypes[oid]
	info.DataType = info.SQLName
	info.Typmod = -1
	switch oid {
	case 20:
		info.NumericPrecision, info.NumericPrecisionRadix, info.NumericScale = int32(64), int32(2), int32(0)
	case 23:
		info.NumericPrecision, info.NumericPrecisionRadix, info.NumericScale = int32(32), int32(2), int32(0)
	case 700:
		info.NumericPrecision, info.NumericPrecisionRadix = int32(24), int32(2)
	case 701:
		info.NumericPrecision, info.NumericPrecisionRadix = int32(53), int32(2)
	case 1700:
		info.NumericPrecisionRadix = int32(10)
		if c.Type == parquet.TypeDecimal && c.Precision > 0 {
			info.Typmod = int32((c.Precision<<16)|(c.Scale&0xFFFF)) + 4
			info.NumericPrecision, info.NumericScale = int32(c.Precision), int32(c.Scale)
		}
	case 1082:
		info.DatetimePrecision = int32(0)
	case 1114:
		info.DatetimePrecision = int32(6)
	case 25:
		info.CharOctetLength = int32(1073741824)
	}
	return info
}

func arrayAlign(elemAlign string) string {
	if elemAlign == "d" {
		return "d"
	}
	return "i"
}

//go:embed pg17_types.csv
var typesCSV string

var (
	typesOnce sync.Once
	typeRows  []map[string]string
)

// PgTypeRows is pg_type's rows for the types this server declares — every
// scalar the wire can declare, the handful of others a driver's type cache
// asks for by OID (int2, oid, name, "char", varchar, bpchar, timestamptz),
// and the array type of each — read off a live 17.11 catalog.
func PgTypeRows() []map[string]string {
	typesOnce.Do(func() {
		r := csv.NewReader(strings.NewReader(typesCSV))
		recs, err := r.ReadAll()
		if err != nil || len(recs) < 2 {
			panic(fmt.Sprintf("syscatalog: pg17_types.csv: %v", err))
		}
		head := recs[0]
		for _, rec := range recs[1:] {
			row := make(map[string]string, len(head))
			for i, h := range head {
				row[h] = rec[i]
			}
			typeRows = append(typeRows, row)
		}
	})
	return typeRows
}

// TypeName is the pg_type.typname and format_type() rendering of an OID this
// catalog lists, or "" — for format_type() and the regtype rendering.
func TypeName(oid int64) (typname, sqlName string, ok bool) {
	if info, ok := scalarTypes[oid]; ok {
		return info.Typname, info.SQLName, true
	}
	for _, row := range PgTypeRows() {
		if row["oid"] == strconv.FormatInt(oid, 10) {
			typname := row["typname"]
			return typname, sqlNameOf(typname), true
		}
	}
	return "", "", false
}

// sqlNameOf is format_type()'s SQL spelling of a pg_type name — the standard
// names for the built-in types PostgreSQL renders by their SQL name, and an
// array as its element's name with [].
func sqlNameOf(typname string) string {
	if strings.HasPrefix(typname, "_") {
		return sqlNameOf(typname[1:]) + "[]"
	}
	switch typname {
	case "bool":
		return "boolean"
	case "int2":
		return "smallint"
	case "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "float4":
		return "real"
	case "float8":
		return "double precision"
	case "varchar":
		return "character varying"
	case "bpchar":
		return "character"
	case "timestamp":
		return "timestamp without time zone"
	case "timestamptz":
		return "timestamp with time zone"
	case "char":
		return `"char"`
	}
	return typname
}
