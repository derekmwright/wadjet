package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Sweep regression (2026-06-12): the coordinator's merge-path typed helpers
// silently dropped Decimal — copyVectorValue wrote nothing (zeros out) and
// extractValue returned nil so every Decimal group key encoded identically,
// collapsing distinct groups into one.

func TestCoordCopyVectorValue_Decimal(t *testing.T) {
	src := batch.NewVectorWithScale(parquet.TypeDecimal, 2, 2)
	src.DecimalData.Data[0] = batch.Int128From(12345)
	src.DecimalData.Data[1] = batch.Int128From(-678)
	dst := batch.NewVectorWithScale(parquet.TypeDecimal, 2, 2)

	copyVectorValue(dst, 0, src, 1, parquet.TypeDecimal)
	copyVectorValue(dst, 1, src, 0, parquet.TypeDecimal)

	if dst.DecimalData.Data[0] != src.DecimalData.Data[1] || dst.DecimalData.Data[1] != src.DecimalData.Data[0] {
		t.Fatalf("decimal copy dropped values: dst=%+v src=%+v", dst.DecimalData.Data, src.DecimalData.Data)
	}
}

func TestExtractValue_DecimalDistinctKeys(t *testing.T) {
	v := batch.NewVectorWithScale(parquet.TypeDecimal, 2, 2)
	v.DecimalData.Data[0] = batch.Int128From(100)
	v.DecimalData.Data[1] = batch.Int128From(200)

	a := extractValue(v, 0, parquet.TypeDecimal)
	b := extractValue(v, 1, parquet.TypeDecimal)
	if a == nil || b == nil {
		t.Fatal("extractValue returned nil for Decimal — group keys would collapse")
	}
	if a == b {
		t.Fatalf("distinct decimal keys extracted equal: %v == %v", a, b)
	}
}
