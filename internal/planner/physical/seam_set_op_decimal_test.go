// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSetOpWidenLadder pins the common type of a set operation's arms against
// what live postgres:17-alpine resolves for the same pair. The two DECIMAL
// rungs are the ones #533's fix added; the rest is the pre-existing ladder,
// asserted so widening a rung cannot quietly reorder it.
func TestSetOpWidenLadder(t *testing.T) {
	d, i32, i64 := parquet.TypeDecimal, parquet.TypeInt32, parquet.TypeInt64
	f32, f64 := parquet.TypeFloat32, parquet.TypeFloat64
	for _, tc := range []struct {
		a, b parquet.TypeID
		want parquet.TypeID
		ok   bool
	}{
		{i32, i32, i32, true},
		{i32, i64, i64, true},
		{i32, f64, f64, true},
		{f32, f64, f64, true},
		{f32, f32, f32, true},
		{d, d, d, true},
		// `numeric UNION ALL bigint` is numeric in PostgreSQL: an integer
		// converts to numeric implicitly, and not the other way.
		{d, i32, d, true},
		{d, i64, d, true},
		{i64, d, d, true},
		// `numeric UNION ALL double precision` is double precision: float8
		// is the PREFERRED type of PostgreSQL's numeric category.
		{d, f64, f64, true},
		{f64, d, f64, true},
		// float4 is PREFERRED too, so it beats every EXACT type it meets and
		// only float8 beats it. Verified live on postgres:17-alpine with
		// pg_typeof over the union itself, both arm orders:
		//
		//	real ∪ integer          → real
		//	real ∪ bigint           → real
		//	real ∪ numeric(9,2)     → real
		//	real ∪ double precision → double precision
		//
		// This rung is a VALUE question, not only an OID one: a real column
		// holding 0.1 renders 0.1, and the same value under a double
		// precision result renders 0.10000000149011612 — the float32 spelled
		// to float64 precision, which is a number neither engine holds.
		// `CREATE TABLE t (x FLOAT)` declares a FLOAT32 column here, so a
		// plain DDL table reaches it.
		{d, f32, f32, true},
		{f32, d, f32, true},
		{f32, i32, f32, true},
		{i32, f32, f32, true},
		{f32, i64, f32, true},
		{i64, f32, f32, true},
		{f64, f32, f64, true},
		// Nothing outside the numeric family widens: making the files line
		// up by rendering a number as text would answer a different query.
		{d, parquet.TypeString, 0, false},
		{d, parquet.TypeTimestamp, 0, false},
		{parquet.TypeString, parquet.TypeString, parquet.TypeString, true},
	} {
		got, ok := SetOpWiden(tc.a, tc.b)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("setOpWiden(%s, %s) = (%s, %v), want (%s, %v)", tc.a, tc.b, got, ok, tc.want, tc.ok)
		}
	}
}

// TestSetOpDecimalTarget pins the (p,s) every arm is coerced to.
//
// The scale is the maximum, which is the only choice that moves no value.
// The precision is rebuilt from the widest INTEGER part rather than taken as
// max(precision), because max(precision) is not a bound on the widened
// values — the (18,2) with (9,4) case below is where the two rules part, and
// where max(precision) would declare a type too small for its own values.
func TestSetOpDecimalTarget(t *testing.T) {
	dec := func(p, s int) SetOpColType {
		return SetOpColType{Typ: parquet.TypeDecimal, Known: true,
			Dec: logical.DecimalMeta{Precision: p, Scale: s}, DecKnown: true}
	}
	intArm := func(t parquet.TypeID) SetOpColType {
		return SetOpColType{Typ: t, Known: true}
	}
	for _, tc := range []struct {
		name   string
		arms   []SetOpColType
		wantP  int
		wantS  int
		wantOk bool
	}{
		{"same_type", []SetOpColType{dec(9, 2), dec(9, 2)}, 9, 2, true},
		// The issue's own pair: 7 integer digits and 14, so 14 + scale 4.
		{"the_issue", []SetOpColType{dec(9, 2), dec(18, 4)}, 18, 4, true},
		{"reversed", []SetOpColType{dec(18, 4), dec(9, 2)}, 18, 4, true},
		{"three_arms", []SetOpColType{dec(9, 2), dec(18, 4), dec(38, 10)}, 38, 10, true},
		// Where max(precision) is WRONG: 16 integer digits at scale 4 needs
		// 20, and max(precision) would say 18.
		{"integer_part_beats_max_precision", []SetOpColType{dec(18, 2), dec(9, 4)}, 20, 4, true},
		// Capped at the Int128 carrier's width; a value that does not fit is
		// then an error at coercion, never a wrapped number.
		{"capped_at_38", []SetOpColType{dec(38, 2), dec(38, 10)}, 38, 10, true},
		// An integer arm contributes its whole range's digits.
		{"with_int32", []SetOpColType{dec(9, 2), intArm(parquet.TypeInt32)}, 12, 2, true},
		{"with_int64", []SetOpColType{dec(9, 2), intArm(parquet.TypeInt64)}, 21, 2, true},
		{"only_ints", []SetOpColType{intArm(parquet.TypeInt32), intArm(parquet.TypeInt64)}, 19, 0, true},
		// An arm whose (p,s) nothing resolved: no target, which
		// reconcileSetOpArmTypes turns into a plan-time refusal (#551).
		{"unresolved_arm", []SetOpColType{dec(9, 2),
			{Typ: parquet.TypeDecimal, Known: true}}, 0, 0, false},
		{"non_numeric_arm", []SetOpColType{dec(9, 2),
			{Typ: parquet.TypeString, Known: true}}, 0, 0, false},
		{"no_arms", nil, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SetOpDecimalTarget(tc.arms)
			if ok != tc.wantOk {
				t.Fatalf("setOpDecimalTarget ok = %v, want %v", ok, tc.wantOk)
			}
			if ok && (got.Precision != tc.wantP || got.Scale != tc.wantS) {
				t.Errorf("setOpDecimalTarget = DECIMAL(%d,%d), want DECIMAL(%d,%d)",
					got.Precision, got.Scale, tc.wantP, tc.wantS)
			}
		})
	}
}

// TestSetOpDecimalTargetNeverNarrowsAnArm is the property the coercion
// operator relies on to refuse a downward move: whatever the arms are, the
// target's scale is at least every arm's own.
func TestSetOpDecimalTargetNeverNarrowsAnArm(t *testing.T) {
	for _, arms := range [][]SetOpColType{
		{{Typ: parquet.TypeDecimal, Known: true, DecKnown: true, Dec: logical.DecimalMeta{Precision: 38, Scale: 10}},
			{Typ: parquet.TypeDecimal, Known: true, DecKnown: true, Dec: logical.DecimalMeta{Precision: 9, Scale: 0}}},
		{{Typ: parquet.TypeDecimal, Known: true, DecKnown: true, Dec: logical.DecimalMeta{Precision: 5, Scale: 5}},
			{Typ: parquet.TypeInt64, Known: true}},
		{{Typ: parquet.TypeInt32, Known: true},
			{Typ: parquet.TypeDecimal, Known: true, DecKnown: true, Dec: logical.DecimalMeta{Precision: 38, Scale: 38}}},
	} {
		want, ok := SetOpDecimalTarget(arms)
		if !ok {
			t.Fatalf("setOpDecimalTarget declined for %v", arms)
		}
		for _, a := range arms {
			s := 0
			if a.Typ == parquet.TypeDecimal {
				s = a.Dec.Scale
			}
			if want.Scale < s {
				t.Errorf("target scale %d is below arm scale %d — a set operation must never scale an arm DOWN",
					want.Scale, s)
			}
		}
	}
}
