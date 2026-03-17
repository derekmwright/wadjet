package dbscan

import (
	"database/sql"
	"math"
	"testing"
	"time"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestMapDBTypeName(t *testing.T) {
	tests := []struct {
		dbType string
		want   parquet.TypeID
	}{
		{"BOOL", parquet.TypeBool},
		{"BOOLEAN", parquet.TypeBool},
		{"BIT", parquet.TypeBool},
		{"INT2", parquet.TypeInt32},
		{"SMALLINT", parquet.TypeInt32},
		{"TINYINT", parquet.TypeInt32},
		{"INT4", parquet.TypeInt64},
		{"INT", parquet.TypeInt64},
		{"INTEGER", parquet.TypeInt64},
		{"INT8", parquet.TypeInt64},
		{"BIGINT", parquet.TypeInt64},
		{"SERIAL", parquet.TypeInt64},
		{"FLOAT4", parquet.TypeFloat64},
		{"REAL", parquet.TypeFloat64},
		{"FLOAT8", parquet.TypeFloat64},
		{"DOUBLE", parquet.TypeFloat64},
		{"NUMERIC", parquet.TypeFloat64},
		{"DECIMAL", parquet.TypeFloat64},
		{"DATE", parquet.TypeDate},
		{"TIMESTAMP", parquet.TypeTimestamp},
		{"TIMESTAMPTZ", parquet.TypeTimestamp},
		{"DATETIME", parquet.TypeTimestamp},
		{"BYTEA", parquet.TypeBytes},
		{"BLOB", parquet.TypeBytes},
		{"BINARY", parquet.TypeBytes},
		{"UUID", parquet.TypeString},
		{"JSON", parquet.TypeString},
		{"JSONB", parquet.TypeString},
		{"TEXT", parquet.TypeString},
		{"VARCHAR", parquet.TypeString},
		{"INET", parquet.TypeString},
		{"CIDR", parquet.TypeString},
		{"MACADDR", parquet.TypeString},
		{"UNKNOWN_TYPE", parquet.TypeString}, // fallback
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

func TestWriteValue_Bool(t *testing.T) {
	schema := []parquet.Column{{Name: "active", Type: parquet.TypeBool, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)

	trueVal := &sql.NullBool{Bool: true, Valid: true}
	nullVal := &sql.NullBool{Valid: false}

	writeValue(b.Columns[0], 0, trueVal, parquet.TypeBool)
	writeValue(b.Columns[0], 1, nullVal, parquet.TypeBool)

	if !b.Columns[0].BoolData[0] {
		t.Error("expected true")
	}
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("expected null")
	}
}

func TestWriteValue_Int64(t *testing.T) {
	schema := []parquet.Column{{Name: "count", Type: parquet.TypeInt64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)

	val := &sql.NullInt64{Int64: 42, Valid: true}
	nullVal := &sql.NullInt64{Valid: false}

	writeValue(b.Columns[0], 0, val, parquet.TypeInt64)
	writeValue(b.Columns[0], 1, nullVal, parquet.TypeInt64)

	if b.Columns[0].Int64Data[0] != 42 {
		t.Errorf("expected 42, got %d", b.Columns[0].Int64Data[0])
	}
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("expected null")
	}
}

func TestWriteValue_Float64(t *testing.T) {
	schema := []parquet.Column{{Name: "score", Type: parquet.TypeFloat64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	val := &sql.NullFloat64{Float64: 3.14, Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeFloat64)

	if b.Columns[0].Float64Data[0] != 3.14 {
		t.Errorf("expected 3.14, got %f", b.Columns[0].Float64Data[0])
	}
}

func TestWriteValue_Float64_NaN(t *testing.T) {
	schema := []parquet.Column{{Name: "score", Type: parquet.TypeFloat64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	val := &sql.NullFloat64{Float64: math.NaN(), Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeFloat64)

	if !b.Columns[0].Nulls.IsNull(0) {
		t.Error("NaN should map to null")
	}
}

func TestWriteValue_String(t *testing.T) {
	schema := []parquet.Column{{Name: "name", Type: parquet.TypeString, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)

	val := &sql.NullString{String: "alice", Valid: true}
	nullVal := &sql.NullString{Valid: false}

	writeValue(b.Columns[0], 0, val, parquet.TypeString)
	writeValue(b.Columns[0], 1, nullVal, parquet.TypeString)

	if b.Columns[0].BytesData.StringValue(0) != "alice" {
		t.Errorf("expected alice, got %s", b.Columns[0].BytesData.StringValue(0))
	}
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("expected null")
	}
}

func TestWriteValue_Timestamp(t *testing.T) {
	schema := []parquet.Column{{Name: "created", Type: parquet.TypeTimestamp, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	ts := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)
	val := &sql.NullTime{Time: ts, Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeTimestamp)

	got := b.Columns[0].Int64Data[0]
	want := ts.UnixMicro()
	if got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
}

func TestWriteValue_Date(t *testing.T) {
	schema := []parquet.Column{{Name: "dt", Type: parquet.TypeDate, Nullable: true}}
	b := batch.NewRecordBatch(schema, 1)

	dt := time.Date(2026, 3, 17, 0, 0, 0, 0, time.UTC)
	val := &sql.NullTime{Time: dt, Valid: true}
	writeValue(b.Columns[0], 0, val, parquet.TypeDate)

	got := b.Columns[0].Int32Data[0]
	want := int32(dt.Sub(epochDate).Hours() / 24)
	if got != want {
		t.Errorf("expected %d days, got %d", want, got)
	}
}

func TestWriteValue_Bytes(t *testing.T) {
	schema := []parquet.Column{{Name: "data", Type: parquet.TypeBytes, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)

	raw := sql.RawBytes([]byte{0xCA, 0xFE})
	writeValue(b.Columns[0], 0, &raw, parquet.TypeBytes)

	got := b.Columns[0].BytesData.Value(0)
	if len(got) != 2 || got[0] != 0xCA || got[1] != 0xFE {
		t.Errorf("expected [CA FE], got %x", got)
	}

	var nilRaw sql.RawBytes
	writeValue(b.Columns[0], 1, &nilRaw, parquet.TypeBytes)
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("expected null")
	}
}

func TestScanDest(t *testing.T) {
	cases := []struct {
		typ  parquet.TypeID
		want string
	}{
		{parquet.TypeBool, "*sql.NullBool"},
		{parquet.TypeInt32, "*sql.NullInt64"},
		{parquet.TypeInt64, "*sql.NullInt64"},
		{parquet.TypeFloat64, "*sql.NullFloat64"},
		{parquet.TypeTimestamp, "*sql.NullTime"},
		{parquet.TypeDate, "*sql.NullTime"},
		{parquet.TypeBytes, "*sql.RawBytes"},
		{parquet.TypeString, "*sql.NullString"},
	}

	for _, c := range cases {
		dest := scanDest(c.typ)
		got := typeName(dest)
		if got != c.want {
			t.Errorf("scanDest(%v) = %s, want %s", c.typ, got, c.want)
		}
	}
}

func typeName(v any) string {
	switch v.(type) {
	case *sql.NullBool:
		return "*sql.NullBool"
	case *sql.NullInt64:
		return "*sql.NullInt64"
	case *sql.NullFloat64:
		return "*sql.NullFloat64"
	case *sql.NullString:
		return "*sql.NullString"
	case *sql.NullTime:
		return "*sql.NullTime"
	case *sql.RawBytes:
		return "*sql.RawBytes"
	default:
		return "unknown"
	}
}
