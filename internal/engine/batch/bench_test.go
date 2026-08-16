package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func BenchmarkNewRecordBatch(b *testing.B) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
		{Name: "ts", Type: parquet.TypeTimestamp},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewRecordBatch(schema, 2048)
	}
}

func BenchmarkVectorSetValueInt64(b *testing.B) {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}
	bb := NewRecordBatch(schema, 2048)
	v := bb.Columns[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			v.SetValue(j, int64(j))
		}
	}
}

func BenchmarkVectorSetValueString(b *testing.B) {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeString}}
	bb := NewRecordBatch(schema, 2048)
	v := bb.Columns[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			v.SetValue(j, "hello_world")
		}
	}
}

func BenchmarkVectorGetValueInt64(b *testing.B) {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeInt64}}
	bb := NewRecordBatch(schema, 2048)
	v := bb.Columns[0]
	for j := 0; j < 2048; j++ {
		v.SetValue(j, int64(j))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			_ = v.GetValue(j)
		}
	}
}

func BenchmarkVectorGetValueFloat64(b *testing.B) {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeFloat64}}
	bb := NewRecordBatch(schema, 2048)
	v := bb.Columns[0]
	for j := 0; j < 2048; j++ {
		v.SetValue(j, float64(j)*1.5)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 2048; j++ {
			_ = v.GetValue(j)
		}
	}
}

func BenchmarkColumnByName(b *testing.B) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
		{Name: "c", Type: parquet.TypeFloat64},
		{Name: "d", Type: parquet.TypeBool},
	}
	bb := NewRecordBatch(schema, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bb.ColumnByName("c")
	}
}

func BenchmarkColumnIndex(b *testing.B) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
		{Name: "c", Type: parquet.TypeFloat64},
		{Name: "d", Type: parquet.TypeBool},
	}
	bb := NewRecordBatch(schema, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bb.ColumnIndex("c")
	}
}
