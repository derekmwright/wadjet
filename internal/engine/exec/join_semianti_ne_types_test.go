// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// neCrossFilter is `probe.pv <> build.v` for an integer build value and a
// float8 probe value as PostgreSQL 17.11 evaluates it: the integer is cast to
// float8 (measured: 1152921504606846979::int8 <> 1152921504606846976::float8
// is false, 3::int8 <> 3.0::float8 false, 3::int8 <> 'NaN'::float8 true).
func neCrossFilter(probe *batch.RecordBatch, pr int, build *batch.RecordBatch, br int) bool {
	pc, bc := probe.Columns[probe.ColumnIndex("pv")], build.Columns[build.ColumnIndex("v")]
	if pc.Nulls.IsNull(pr) || bc.Nulls.IsNull(br) {
		return false
	}
	x := pc.Float64Data[pr]
	return x != x || float64(bc.Int64Data[br]) != x
}

// A `<>` whose probe value is a float is not an integer comparison (#1681,
// line 1681c). The distinct-pair build used to enable on the build value's
// type alone and read the float through the integer reader, which answered
// NULL for every probe row: EXISTS false everywhere, NOT EXISTS true
// everywhere. It must stay off for such a pair, so the residual decides.
func TestSemiAntiNEDeclinesCrossTypePair(t *testing.T) {
	const two60 = int64(1) << 60
	buildSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64, Nullable: true}}
	probeSchema := []parquet.Column{{Name: "id", Type: parquet.TypeString}, {Name: "k", Type: parquet.TypeInt64}, {Name: "pv", Type: parquet.TypeFloat64, Nullable: true}}
	buildRows := []map[string]any{
		{"k": int64(1), "v": int64(3)},
		// Two distinct int8 values that float8 cannot tell apart: a pair of
		// distinct integers is not a pair of distinct values here.
		{"k": int64(2), "v": two60 + 3}, {"k": int64(2), "v": two60 + 7},
	}
	probeRows := []map[string]any{
		{"id": "a", "k": int64(1), "pv": 3.0},            // 3 <> 3.0 is false
		{"id": "b", "k": int64(1), "pv": 3.5},            // 3 <> 3.5 is true
		{"id": "c", "k": int64(1), "pv": math.NaN()},     // 3 <> NaN is true
		{"id": "d", "k": int64(2), "pv": float64(two60)}, // both equal 2^60 in float8
		{"id": "e", "k": int64(1), "pv": nil},
	}
	for _, tc := range []struct {
		jt   JoinType
		want string
	}{{SemiJoin, "[b c]"}, {AntiJoin, "[a d e]"}} {
		hj := NewHashJoin(tc.jt, []string{"k"}, []string{"k"})
		hj.SemiAntiFilter = neCrossFilter
		hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
		hj.ProbeSchemaHint, hj.BuildSchemaHint = probeSchema, buildSchema
		if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
			t.Fatal(err)
		}
		if hj.neActive {
			t.Errorf("%v: the distinct-pair build enabled for an int8 build value against a float8 probe value", tc.jt)
		}
		sink := &CollectSink{}
		pipe := &Pipeline{Source: NewSliceSource(probeSchema, probeRows), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, r := range sink.Rows {
			ids = append(ids, fmt.Sprint(r["id"]))
		}
		sort.Strings(ids)
		if got := fmt.Sprint(ids); got != tc.want {
			t.Errorf("%v: got %s, want %s (PostgreSQL 17.11)", tc.jt, got, tc.want)
		}
	}
}

// neExactPair is the whole enabling rule: integer against integer of either
// width, and date / port / protocol against their own type, and nothing else
// the integer reader accepts.
func TestSemiAntiNEExactPairs(t *testing.T) {
	exact := map[[2]parquet.TypeID]bool{
		{parquet.TypeInt64, parquet.TypeInt64}: true, {parquet.TypeInt64, parquet.TypeInt32}: true,
		{parquet.TypeInt32, parquet.TypeInt64}: true, {parquet.TypeInt32, parquet.TypeInt32}: true,
		{parquet.TypeDate, parquet.TypeDate}: true, {parquet.TypePort, parquet.TypePort}: true,
		{parquet.TypeProtocol, parquet.TypeProtocol}: true,
	}
	types := []parquet.TypeID{parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64,
		parquet.TypeDecimal, parquet.TypeDate, parquet.TypeTimestamp, parquet.TypeDuration, parquet.TypePort,
		parquet.TypeProtocol, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeBool, parquet.TypeString}
	for _, b := range types {
		for _, p := range types {
			if got, want := neExactPair(b, p), exact[[2]parquet.TypeID{b, p}]; got != want {
				t.Errorf("neExactPair(build %v, probe %v) = %v, want %v", b, p, got, want)
			}
		}
	}
}

// Undeclared value types leave the optimization off (the generic filtered
// build stores the batches), and an int4 probe against an int8 build, which
// compares exactly, still enables.
func TestSemiAntiNEEnablesOnlyOnDeclaredExactTypes(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}}
	rows := []map[string]any{{"k": int64(1), "v": int64(3)}}
	for _, tc := range []struct {
		name  string
		probe []parquet.Column
		want  bool
	}{
		{"undeclared", nil, false},
		{"int4 probe", []parquet.Column{{Name: "pv", Type: parquet.TypeInt32}}, true},
		{"numeric probe", []parquet.Column{{Name: "pv", Type: parquet.TypeDecimal}}, false},
		{"float4 probe", []parquet.Column{{Name: "pv", Type: parquet.TypeFloat32}}, false},
	} {
		hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
		hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
		hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
		hj.ProbeSchemaHint, hj.BuildSchemaHint = tc.probe, buildSchema
		if err := hj.Build(context.Background(), NewSliceSource(buildSchema, rows)); err != nil {
			t.Fatal(err)
		}
		if hj.neActive != tc.want {
			t.Errorf("%s: neActive=%v, want %v", tc.name, hj.neActive, tc.want)
		}
	}
}

// A probe stream that does not carry the type the plan declared is refused:
// the distinct-pair build stored no batches for the residual to fall back to,
// and reading it as integer storage would answer a different question.
func TestSemiAntiNEProbeTypeMismatchRefuses(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}}
	hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
	hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
	hj.ProbeSchemaHint = []parquet.Column{{Name: "pv", Type: parquet.TypeInt64}}
	hj.BuildSchemaHint = buildSchema
	if err := hj.Build(context.Background(), NewSliceSource(buildSchema, []map[string]any{{"k": int64(1), "v": int64(3)}})); err != nil {
		t.Fatal(err)
	}
	if !hj.neActive {
		t.Fatal("fixture: the distinct-pair build should enable on the declared int8 pair")
	}
	probeSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "pv", Type: parquet.TypeFloat64}}
	pipe := &Pipeline{Source: NewSliceSource(probeSchema, []map[string]any{{"k": int64(1), "pv": 3.5}}), Ops: []UnaryOperator{hj.Probe()}, Sink: &CollectSink{}}
	if err := pipe.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "compares exactly") {
		t.Fatalf("err = %v, want the declared-type refusal", err)
	}
}
