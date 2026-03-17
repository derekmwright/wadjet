// Package dbscan provides table-function sources for querying external
// SQL databases (PostgreSQL, MySQL) and producing columnar RecordBatches.
package dbscan

import (
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

const defaultBatchSize = 2048

// Scanner reads rows from a *sql.Rows result set into columnar RecordBatches.
type Scanner struct {
	schema []parquet.Column
	rows   *sql.Rows
	done   bool
}

// NewScanner creates a Scanner from an open *sql.Rows. The caller should
// not close the Rows — the Scanner will close them when it returns nil
// from Next() or when Close() is called.
func NewScanner(rows *sql.Rows) (*Scanner, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("dbscan: reading column types: %w", err)
	}

	schema := make([]parquet.Column, len(colTypes))
	for i, ct := range colTypes {
		schema[i] = parquet.Column{
			Name:     ct.Name(),
			Type:     mapSQLType(ct),
			Nullable: isNullable(ct),
		}
	}

	return &Scanner{schema: schema, rows: rows}, nil
}

// Schema returns the inferred schema.
func (s *Scanner) Schema() []parquet.Column {
	return s.schema
}

// Next returns the next batch of rows. Returns nil when all rows are consumed.
func (s *Scanner) Next() (*batch.RecordBatch, error) {
	if s.done {
		return nil, nil
	}

	b := batch.NewRecordBatch(s.schema, defaultBatchSize)
	row := 0

	// Pre-allocate scan destinations
	numCols := len(s.schema)
	dests := make([]any, numCols)
	for i, col := range s.schema {
		dests[i] = scanDest(col.Type)
	}

	for row < defaultBatchSize && s.rows.Next() {
		if err := s.rows.Scan(dests...); err != nil {
			return nil, fmt.Errorf("dbscan: scanning row: %w", err)
		}

		for col := range s.schema {
			writeValue(b.Columns[col], row, dests[col], s.schema[col].Type)
		}
		row++
	}

	if err := s.rows.Err(); err != nil {
		return nil, fmt.Errorf("dbscan: iterating rows: %w", err)
	}

	if row == 0 {
		s.done = true
		return nil, nil
	}

	// Trim batch to actual row count
	b.Len = row
	return b, nil
}

// Close releases the underlying sql.Rows.
func (s *Scanner) Close() error {
	if s.rows != nil {
		return s.rows.Close()
	}
	return nil
}

// mapSQLType maps a *sql.ColumnType to a Caelum TypeID.
func mapSQLType(ct *sql.ColumnType) parquet.TypeID {
	return mapDBTypeName(ct.DatabaseTypeName())
}

// mapDBTypeName maps a database type name string to a Caelum TypeID.
func mapDBTypeName(dbType string) parquet.TypeID {
	switch dbType {
	// Boolean
	case "BOOL", "BOOLEAN", "BIT":
		return parquet.TypeBool

	// Integers
	case "INT2", "SMALLINT", "TINYINT":
		return parquet.TypeInt32
	case "INT4", "INT", "INTEGER", "MEDIUMINT":
		return parquet.TypeInt64
	case "INT8", "BIGINT", "SERIAL", "BIGSERIAL":
		return parquet.TypeInt64

	// Floats
	case "FLOAT4", "REAL", "FLOAT":
		return parquet.TypeFloat64
	case "FLOAT8", "DOUBLE", "DOUBLE PRECISION":
		return parquet.TypeFloat64
	case "NUMERIC", "DECIMAL":
		// Use float64 for NUMERIC/DECIMAL (safe for analytics, not exact)
		return parquet.TypeFloat64

	// Date/Time
	case "DATE":
		return parquet.TypeDate
	case "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITHOUT TIME ZONE",
		"TIMESTAMP WITH TIME ZONE", "DATETIME":
		return parquet.TypeTimestamp

	// Network (PostgreSQL-specific)
	case "INET", "CIDR":
		return parquet.TypeString
	case "MACADDR", "MACADDR8":
		return parquet.TypeString

	// UUID
	case "UUID":
		return parquet.TypeString

	// Binary
	case "BYTEA", "BLOB", "BINARY", "VARBINARY":
		return parquet.TypeBytes

	// JSON
	case "JSON", "JSONB":
		return parquet.TypeString

	default:
		// TEXT, VARCHAR, CHAR, NAME, and everything else → string
		return parquet.TypeString
	}
}

func isNullable(ct *sql.ColumnType) bool {
	nullable, ok := ct.Nullable()
	return !ok || nullable // default to nullable if unknown
}

// scanDest returns a pointer to an appropriate Go value for scanning.
func scanDest(typ parquet.TypeID) any {
	switch typ {
	case parquet.TypeBool:
		return new(sql.NullBool)
	case parquet.TypeInt32:
		return new(sql.NullInt64) // scan as int64, narrow later
	case parquet.TypeInt64:
		return new(sql.NullInt64)
	case parquet.TypeFloat64:
		return new(sql.NullFloat64)
	case parquet.TypeTimestamp:
		return new(sql.NullTime)
	case parquet.TypeDate:
		return new(sql.NullTime)
	case parquet.TypeBytes:
		return new(sql.RawBytes)
	default:
		return new(sql.NullString)
	}
}

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// writeValue writes a scanned Go value into a vector column.
func writeValue(vec *batch.Vector, row int, dest any, typ parquet.TypeID) {
	switch typ {
	case parquet.TypeBool:
		v := dest.(*sql.NullBool)
		if !v.Valid {
			vec.Nulls.SetNull(row)
			return
		}
		vec.BoolData[row] = v.Bool

	case parquet.TypeInt32:
		v := dest.(*sql.NullInt64)
		if !v.Valid {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Int32Data[row] = int32(v.Int64)

	case parquet.TypeInt64:
		v := dest.(*sql.NullInt64)
		if !v.Valid {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Int64Data[row] = v.Int64

	case parquet.TypeFloat64:
		v := dest.(*sql.NullFloat64)
		if !v.Valid || math.IsNaN(v.Float64) {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Float64Data[row] = v.Float64

	case parquet.TypeTimestamp:
		v := dest.(*sql.NullTime)
		if !v.Valid {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Int64Data[row] = v.Time.UnixMicro()

	case parquet.TypeDate:
		v := dest.(*sql.NullTime)
		if !v.Valid {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Int32Data[row] = int32(v.Time.Sub(epochDate).Hours() / 24)

	case parquet.TypeBytes:
		v := dest.(*sql.RawBytes)
		if v == nil || *v == nil {
			vec.Nulls.SetNull(row)
			vec.BytesData.Set(row, nil)
			return
		}
		// Copy to avoid driver reuse of buffer
		cp := make([]byte, len(*v))
		copy(cp, *v)
		vec.BytesData.Set(row, cp)

	default: // TypeString and everything else
		v := dest.(*sql.NullString)
		if !v.Valid {
			vec.Nulls.SetNull(row)
			vec.BytesData.Set(row, nil)
			return
		}
		vec.BytesData.Set(row, []byte(v.String))
	}
}
