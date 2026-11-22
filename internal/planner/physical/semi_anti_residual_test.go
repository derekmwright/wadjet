// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A SEMI/ANTI RESIDUAL COMPARES AS POSTGRESQL DOES, FOR EVERY OPERATOR THE
// DECORRELATION EMITS AND EVERY OPERAND TYPE (#1681, line 1681b). Each want is
// PostgreSQL 17.11's answer to the same comparison of two literals
// (postgres:17-alpine, --locale=C --encoding=UTF8; measured 2026-11-22). The
// residual compiler compared numeric, bytea and the network types by their
// printed TEXT, compared NaN IEEE-style, read the wrong array when the two
// operands differ in type, and did not recognize IS [NOT] DISTINCT FROM at all
// (no filter: the probe tested existence only).
func TestSemiAntiFilterPostgresComparisons(t *testing.T) {
	c := func(ty parquet.TypeID) parquet.Column { return parquet.Column{Type: ty, Nullable: true} }
	dec := func(p, s int) parquet.Column {
		col := c(parquet.TypeDecimal)
		col.Precision, col.Scale = p, s
		return col
	}
	nan := math.NaN()
	cases := []struct {
		name   string
		pc, bc parquet.Column
		pv     any
		op     string
		bv     any
		want   bool
	}{
		{"NaN > 12.5", c(parquet.TypeFloat64), c(parquet.TypeFloat64), nan, ">", 12.5, true},
		{"NaN = NaN", c(parquet.TypeFloat64), c(parquet.TypeFloat64), nan, "=", nan, true},
		{"NaN <> NaN", c(parquet.TypeFloat64), c(parquet.TypeFloat64), nan, "!=", nan, false},
		{"12.5 < NaN", c(parquet.TypeFloat64), c(parquet.TypeFloat64), 12.5, "<", nan, true},
		{"NaN >= NaN", c(parquet.TypeFloat64), c(parquet.TypeFloat64), nan, ">=", nan, true},
		{"NaN <= 12.5", c(parquet.TypeFloat64), c(parquet.TypeFloat64), nan, "<=", 12.5, false},
		{"float4 NaN > 12.5", c(parquet.TypeFloat32), c(parquet.TypeFloat32), float32(nan), ">", float32(12.5), true},
		{"numeric 12.25 > 9.25", dec(18, 4), dec(18, 4), "12.2500", ">", "9.2500", true},
		{"inet 10.0.0.12 > 10.0.0.9", c(parquet.TypeIPv4), c(parquet.TypeIPv4), "10.0.0.12", ">", "10.0.0.9", true},
		{"bytea \\x0c > \\x0900", c(parquet.TypeBytes), c(parquet.TypeBytes), []byte{0x0c}, ">", []byte{0x09, 0x00}, true},
		{"bytea \\x09 < \\x0900", c(parquet.TypeBytes), c(parquet.TypeBytes), []byte{0x09}, "<", []byte{0x09, 0x00}, true},
		{"NULL IS DISTINCT FROM 5", c(parquet.TypeInt64), c(parquet.TypeInt64), nil, "is distinct from", int64(5), true},
		{"NULL IS DISTINCT FROM NULL", c(parquet.TypeInt64), c(parquet.TypeInt64), nil, "is distinct from", nil, false},
		{"5 IS NOT DISTINCT FROM 5", c(parquet.TypeInt64), c(parquet.TypeInt64), int64(5), "is not distinct from", int64(5), true},
		{"5 IS NOT DISTINCT FROM NULL", c(parquet.TypeInt64), c(parquet.TypeInt64), int64(5), "is not distinct from", nil, false},
		{"NaN IS DISTINCT FROM NaN", c(parquet.TypeFloat64), c(parquet.TypeFloat64), nan, "is distinct from", nan, false},
		{"int8 9 > float8 9.5", c(parquet.TypeInt64), c(parquet.TypeFloat64), int64(9), ">", 9.5, false},
		{"int8 10 > float8 9.5", c(parquet.TypeInt64), c(parquet.TypeFloat64), int64(10), ">", 9.5, true},
		{"int4 9 < int8 12", c(parquet.TypeInt32), c(parquet.TypeInt64), int32(9), "<", int64(12), true},
		{"numeric(10,2) 9.50 > numeric(18,4) 9.25", dec(10, 2), dec(18, 4), "9.50", ">", "9.2500", true},
		{"inet 2001:db8::12 > 2001:db8::9", c(parquet.TypeIPv6), c(parquet.TypeIPv6), "2001:db8::12", ">", "2001:db8::9", true},
		{"cidr 10.12.0.0/16 > 10.9.0.0/24", c(parquet.TypeCIDR), c(parquet.TypeCIDR), "10.12.0.0/16", ">", "10.9.0.0/24", true},
		{"cidr 10.0.0.0/8 < 10.0.0.0/16", c(parquet.TypeCIDR), c(parquet.TypeCIDR), "10.0.0.0/8", "<", "10.0.0.0/16", true},
		{"macaddr ..:12 > ..:09", c(parquet.TypeMAC), c(parquet.TypeMAC), "00:00:00:00:00:12", ">", "00:00:00:00:00:09", true},
		{"date 2024-01-09 < timestamp 2024-01-01 12:00", c(parquet.TypeDate), c(parquet.TypeTimestamp), "2024-01-09", "<", "2024-01-01 12:00:00", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pc, bc := tc.pc, tc.bc
			pc.Name, bc.Name = "pv", "bv"
			probe := batch.NewRecordBatch([]parquet.Column{pc}, 1)
			build := batch.NewRecordBatch([]parquet.Column{bc}, 1)
			probe.Columns[0].SetValue(0, tc.pv)
			build.Columns[0].SetValue(0, tc.bv)
			filter := "pv " + tc.op + " bv"
			f, err := buildSemiAntiFilter(filter)
			if err != nil {
				t.Fatalf("%q: %v", filter, err)
			}
			if f == nil {
				t.Fatalf("%q compiled to no filter: the probe would test existence only", filter)
			}
			got := func() (got bool) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("%q panicked: %v", filter, r)
					}
				}()
				return f(probe, 0, build, 0)
			}()
			if got != tc.want {
				t.Errorf("%s: matched=%v, PostgreSQL 17.11 answers %v", tc.name, got, tc.want)
			}
		})
	}
}

// A conjunct the residual compiler does not know is an error, never a dropped
// conjunct: `a > b AND c IS DISTINCT FROM d` answered `a > b` alone.
func TestSemiAntiFilterEveryConjunctCounts(t *testing.T) {
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	probe := batch.NewRecordBatch([]parquet.Column{i64("a"), i64("c")}, 1)
	build := batch.NewRecordBatch([]parquet.Column{i64("b"), i64("d")}, 1)
	probe.Columns[0].SetValue(0, int64(1))
	build.Columns[0].SetValue(0, int64(0))
	probe.Columns[1].SetValue(0, int64(5))
	build.Columns[1].SetValue(0, int64(5))
	for _, filter := range []string{"a > b AND c is distinct from d", "c is distinct from d AND a > b"} {
		f, err := buildSemiAntiFilter(filter)
		if err != nil || f == nil {
			t.Fatalf("%q: filter=%v err=%v", filter, f != nil, err)
		}
		if f(probe, 0, build, 0) {
			t.Errorf("%q matched (1 > 0 AND 5 IS DISTINCT FROM 5): the second conjunct was dropped", filter)
		}
	}
	_, err := buildSemiAntiFilter("a > b AND c between d")
	if err == nil || sqlerr.StateOf(err) != "0A000" {
		t.Errorf("an unparseable conjunct: err=%v, want a 0A000 refusal", err)
	}
	if cols := extractFilterBuildColumns("a > b AND c is distinct from d"); len(cols) != 2 {
		t.Errorf("build store columns %v: the IS DISTINCT FROM operand must be stored", cols)
	}
}

// A VIEW operand on a typed kernel's type is read through its base.
func TestSemiAntiFilterReadsAView(t *testing.T) {
	base := batch.NewVector(batch.TypeInt64, 3)
	base.Int64Data[0], base.Int64Data[1], base.Int64Data[2] = 7, 1, 9
	view := &batch.Vector{Type: batch.TypeInt64, Len: 1, Nulls: batch.NewBitmap(1), Base: base, Indices: []uint32{2}}
	probe := &batch.RecordBatch{Schema: []parquet.Column{{Name: "a", Type: parquet.TypeInt64, Nullable: true}}, Columns: []*batch.Vector{view}, Len: 1}
	build := batch.NewRecordBatch([]parquet.Column{{Name: "b", Type: parquet.TypeInt64, Nullable: true}}, 1)
	build.Columns[0].SetValue(0, int64(8))
	f, err := buildSemiAntiFilter("a > b")
	if err != nil {
		t.Fatal(err)
	}
	if !f(probe, 0, build, 0) {
		t.Error("view row 0 reads base row 2 (9): 9 > 8 should match")
	}
}

// The probe's workers share one filter: the general evaluators are pooled, so
// concurrent candidates on the engine's comparison neither race nor answer
// each other's pair (run under -race).
func TestSemiAntiFilterGeneralConcurrent(t *testing.T) {
	dec := parquet.Column{Name: "pv", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}
	bdec := dec
	bdec.Name = "bv"
	const n = 64
	probe := batch.NewRecordBatch([]parquet.Column{dec}, n)
	build := batch.NewRecordBatch([]parquet.Column{bdec}, n)
	for i := 0; i < n; i++ {
		probe.Columns[0].SetValue(i, fmt.Sprintf("%d.2500", i))
		build.Columns[0].SetValue(i, fmt.Sprintf("%d.2500", n-1-i))
	}
	f, err := buildSemiAntiFilter("pv > bv")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rep := 0; rep < 200; rep++ {
				for i := 0; i < n; i++ {
					want := i > n-1-i
					if got := f(probe, i, build, i); got != want {
						select {
						case errs <- fmt.Sprintf("row %d: %d.25 > %d.25 matched=%v", i, i, n-1-i, got):
						default:
						}
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
