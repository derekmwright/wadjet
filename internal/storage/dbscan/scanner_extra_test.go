package dbscan

import (
	"database/sql"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestWriteValue_Int32(t *testing.T) {
	schema := []parquet.Column{{Name: "small", Type: parquet.TypeInt32, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)

	val := &sql.NullInt64{Int64: 42, Valid: true}
	nullVal := &sql.NullInt64{Valid: false}

	writeValue(b.Columns[0], 0, val, parquet.TypeInt32)
	writeValue(b.Columns[0], 1, nullVal, parquet.TypeInt32)

	if b.Columns[0].Int32Data[0] != 42 {
		t.Errorf("expected 42, got %d", b.Columns[0].Int32Data[0])
	}
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("expected null")
	}
}

func TestWriteValue_Timestamp_Null(t *testing.T) {
	schema := []parquet.Column{{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	nullVal := &sql.NullTime{Valid: false}
	writeValue(b.Columns[0], 0, nullVal, parquet.TypeTimestamp)

	if !b.Columns[0].Nulls.IsNull(0) {
		t.Error("expected null")
	}
}

func TestWriteValue_Date_Null(t *testing.T) {
	schema := []parquet.Column{{Name: "dt", Type: parquet.TypeDate, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	nullVal := &sql.NullTime{Valid: false}
	writeValue(b.Columns[0], 0, nullVal, parquet.TypeDate)

	if !b.Columns[0].Nulls.IsNull(0) {
		t.Error("expected null")
	}
}

func TestWriteValue_Float64_Null(t *testing.T) {
	schema := []parquet.Column{{Name: "f", Type: parquet.TypeFloat64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	nullVal := &sql.NullFloat64{Valid: false}
	writeValue(b.Columns[0], 0, nullVal, parquet.TypeFloat64)

	if !b.Columns[0].Nulls.IsNull(0) {
		t.Error("expected null")
	}
}

func TestMapDBTypeName_Additional(t *testing.T) {
	tests := []struct {
		dbType string
		want   parquet.TypeID
	}{
		{"MEDIUMINT", parquet.TypeInt64},
		{"BIGSERIAL", parquet.TypeInt64},
		{"FLOAT", parquet.TypeFloat64},
		{"DOUBLE PRECISION", parquet.TypeFloat64},
		{"TIMESTAMP WITHOUT TIME ZONE", parquet.TypeTimestamp},
		{"TIMESTAMP WITH TIME ZONE", parquet.TypeTimestamp},
		{"MACADDR8", parquet.TypeString},
		{"VARBINARY", parquet.TypeBytes},
		{"NAME", parquet.TypeString},
		{"CHAR", parquet.TypeString},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			got := mapDBTypeName(tt.dbType)
			if got != tt.want {
				t.Errorf("mapDBTypeName(%q) = %v, want %v", tt.dbType, got, tt.want)
			}
		})
	}
}

func TestScanDest_AllTypes(t *testing.T) {
	types := []parquet.TypeID{
		parquet.TypeBool,
		parquet.TypeInt32,
		parquet.TypeInt64,
		parquet.TypeFloat64,
		parquet.TypeTimestamp,
		parquet.TypeDate,
		parquet.TypeBytes,
		parquet.TypeString,
		parquet.TypeIPv4,     // should fall through to string
		parquet.TypeFloat32,  // should fall through to string
		parquet.TypeDuration, // should fall through to string
	}

	for _, typ := range types {
		dest := scanDest(typ)
		if dest == nil {
			t.Errorf("scanDest(%v) returned nil", typ)
		}
	}
}

func TestWriteValue_DefaultCase_String(t *testing.T) {
	// The default case in writeValue handles TypeString and any unrecognized type
	// Note: types like IPv4, IPv6, etc. use different vector storage but
	// the dbscan writeValue maps them all to the string/default path
	schema := []parquet.Column{{Name: "col", Type: parquet.TypeString, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)

	val := &sql.NullString{String: "test-value", Valid: true}
	nullVal := &sql.NullString{Valid: false}

	writeValue(b.Columns[0], 0, val, parquet.TypeString)
	writeValue(b.Columns[0], 1, nullVal, parquet.TypeString)

	if b.Columns[0].BytesData.StringValue(0) != "test-value" {
		t.Errorf("expected 'test-value', got %q", b.Columns[0].BytesData.StringValue(0))
	}
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("expected null")
	}
}

func TestWriteValue_Bool_False(t *testing.T) {
	schema := []parquet.Column{{Name: "flag", Type: parquet.TypeBool, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	val := &sql.NullBool{Bool: false, Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeBool)

	if b.Columns[0].BoolData[0] {
		t.Error("expected false")
	}
	if b.Columns[0].Nulls.IsNull(0) {
		t.Error("should not be null")
	}
}

func TestWriteValue_EmptyString(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	val := &sql.NullString{String: "", Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeString)

	if b.Columns[0].BytesData.StringValue(0) != "" {
		t.Errorf("expected empty string, got %q", b.Columns[0].BytesData.StringValue(0))
	}
	if b.Columns[0].Nulls.IsNull(0) {
		t.Error("empty string should not be null")
	}
}

func TestWriteValue_Date_SpecificValue(t *testing.T) {
	schema := []parquet.Column{{Name: "dt", Type: parquet.TypeDate, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	// 2024-01-01
	dt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	val := &sql.NullTime{Time: dt, Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeDate)

	expectedDays := int32(dt.Sub(epochDate).Hours() / 24)
	if b.Columns[0].Int32Data[0] != expectedDays {
		t.Errorf("expected %d days, got %d", expectedDays, b.Columns[0].Int32Data[0])
	}
}

func TestWriteValue_Bytes_EmptySlice(t *testing.T) {
	schema := []parquet.Column{{Name: "data", Type: parquet.TypeBytes, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	raw := sql.RawBytes([]byte{})
	writeValue(b.Columns[0], 0, &raw, parquet.TypeBytes)

	got := b.Columns[0].BytesData.Value(0)
	if len(got) != 0 {
		t.Errorf("expected empty bytes, got %x", got)
	}
}

func TestWriteValue_LargeInt64(t *testing.T) {
	schema := []parquet.Column{{Name: "big", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	val := &sql.NullInt64{Int64: 9223372036854775807, Valid: true} // max int64
	writeValue(b.Columns[0], 0, val, parquet.TypeInt64)

	if b.Columns[0].Int64Data[0] != 9223372036854775807 {
		t.Errorf("expected max int64, got %d", b.Columns[0].Int64Data[0])
	}
}
