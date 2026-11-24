// SPDX-License-Identifier: MIT

// This file holds the representative rule: which member's stored
// representation a group keeps when two SQL-equal members meet (ADR-0024's
// 2026-11-24 amendment, #1713). The order itself is batch's
// (batch/representative.go); this file applies it to whole key tuples at
// every place two members of one group meet: the consume hit paths, the
// clone merge (mergeSinkState) and the spill run merge (kWayMerger).
package exec

import (
	"math"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// RepresentativeReplacements counts the times a group's stored
// representative was replaced by a smaller one (any meet point). A test can
// read it to prove a meet point ran the rule.
var RepresentativeReplacements atomic.Int64

// SetRepresentativeEligibility names the aggregate whose input marks a row
// as an ELIGIBLE representative (input > 0): the INTERSECT / EXCEPT counting
// aggregate's left-arm tag, so the group's representative is a LEFT member
// whenever the group has one, as PostgreSQL's is. Members of one eligibility
// class are ordered by the representative order. Must precede the first
// Consume. An unknown name leaves every member eligible.
func (h *HashAggregate) SetRepresentativeEligibility(aggOutputCol string) {
	h.repEligAgg = 0
	for i, a := range h.Aggs {
		if a.OutputCol == aggOutputCol {
			h.repEligAgg = i + 1
			h.repActive = true
			return
		}
	}
}

// resolveRepCols records which group-key positions carry a type whose
// SQL-equal values can differ in representation. Called wherever
// groupColTypes is (re)assigned.
func (h *HashAggregate) resolveRepCols() {
	var cols []int
	var cmps []func(*batch.Vector, int, any) int
	for i, t := range h.groupColTypes {
		if batch.RepEligibleType(t) {
			cols = append(cols, i)
			cmps = append(cmps, repRowCmpFor(t))
		}
	}
	h.repRowCmp = cmps
	if len(cols) != len(h.repDecMaxEff) {
		h.repDecMaxEff = make([]int, len(cols))
		for j := range h.repDecMaxEff {
			h.repDecMaxEff[j] = -1
		}
	}
	h.repCols = cols
	h.repActive = len(h.repCols) > 0 || h.repEligAgg > 0
}

// repNoteBoxes raises repDecMaxEff to cover a new group's DECIMAL boxes.
// Every place a group is created with boxed key values calls it; a
// replacement only lowers a group's display scale, so the bound stays one.
func (h *HashAggregate) repNoteBoxes(keyVals []any) {
	for j, i := range h.repCols {
		if h.groupColTypes[i] != batch.TypeDecimal || i >= len(keyVals) {
			continue
		}
		if s, ok := keyVals[i].(string); ok {
			if e := batch.DecimalTextFractionDigits(s); e > h.repDecMaxEff[j] {
				h.repDecMaxEff[j] = e
			}
		}
	}
}

// repFoldBound folds another aggregate's bound into h's (a merge or an
// adoption brings o's groups into h).
func (h *HashAggregate) repFoldBound(o *HashAggregate) {
	if len(o.repDecMaxEff) != len(h.repDecMaxEff) {
		// Positions unknown on one side: give up the bound (no batch is
		// skipped), which is always safe.
		for j := range h.repDecMaxEff {
			h.repDecMaxEff[j] = 1 << 30
		}
		return
	}
	for j, e := range o.repDecMaxEff {
		if e > h.repDecMaxEff[j] {
			h.repDecMaxEff[j] = e
		}
	}
}

// repCanSkipBatch reports whether no row of b can be a smaller
// representative than a group already holds or than another row of b, so
// the batch's hits need no comparison. It is the ordinary case's cost: one
// test per batch. Per eligible key column:
//   - DECIMAL: every row's effective display scale is the one carrier scale
//     S (a uniform DScaleCarrier code, or a uniform explicit code at least
//     S), and no group holds a box printed at a scale above S
//     (repDecMaxEff). Every member then prints at S or higher, and a
//     smaller representative needs a lower scale.
//   - FLOAT: no row is a zero or a NaN. Any other float value has one
//     representation, so a hit always meets its own bits.
//   - CIDR: never (its text varies freely).
//
// A set operation's eligibility class never skips.
func (h *HashAggregate) repCanSkipBatch(b *batch.RecordBatch) bool {
	if h.repEligAgg > 0 {
		return false
	}
	for j, i := range h.repCols {
		idx := h.groupColIdx[i]
		if idx < 0 || idx >= len(b.Columns) {
			return false
		}
		v := b.Columns[idx]
		if v.Base != nil {
			return false
		}
		switch h.groupColTypes[i] {
		case batch.TypeDecimal:
			dc := &v.DecimalData
			if dc.DScale != nil {
				return false
			}
			s := dc.Scale
			if s < 0 {
				s = 0
			}
			code := dc.DScaleCode(0)
			if code == batch.DScaleUnknown || (code != batch.DScaleCarrier && int(code) < s) {
				return false
			}
			if h.repDecMaxEff[j] > s {
				return false
			}
		case batch.TypeFloat64:
			n := b.Len
			if n > len(v.Float64Data) {
				n = len(v.Float64Data)
			}
			for _, f := range v.Float64Data[:n] {
				if f == 0 || f != f {
					return false
				}
			}
		case batch.TypeFloat32:
			n := b.Len
			if n > len(v.Float32Data) {
				n = len(v.Float32Data)
			}
			for _, f := range v.Float32Data[:n] {
				if f == 0 || f != f {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// repAccEligible reads a stored group's eligibility from its accumulators:
// the eligibility aggregate's SUM is positive exactly when the group holds an
// eligible member, and then its representative is one (eligible members
// always win).
func (h *HashAggregate) repAccEligible(accs []kernel.Accumulator) bool {
	ai := h.repEligAgg - 1
	if ai < 0 || ai >= len(accs) {
		return true
	}
	a := &accs[ai]
	return a.SumI64 > 0 || a.SumF64 > 0 || (!a.SumDec.IsZero() && !a.SumDec.IsNegative())
}

// repRowEligible reads row's eligibility from the eligibility aggregate's
// input column.
func (h *HashAggregate) repRowEligible(b *batch.RecordBatch, row int) bool {
	ai := h.repEligAgg - 1
	if ai < 0 || ai >= len(h.aggColIdx) {
		return true
	}
	idx := h.aggColIdx[ai]
	if idx < 0 || idx >= len(b.Columns) {
		return true
	}
	f, ok := b.Columns[idx].GetNumericFloat64(row)
	return ok && f > 0
}

// repStoredKeys is a stored group's tuple for the rule. A group the
// typed-generic path deferred (and a later migration reified) has no boxed
// tuple: its key is the canonical value its serialized key rebuilds, which
// is then boxed into the group so a smaller member can replace it.
func (h *HashAggregate) repStoredKeys(ext *groupStateExtras, serKey string) []any {
	if ext.keyValues == nil && len(h.repCols) > 0 && len(h.GroupingSets) == 0 {
		ext.keyValues = decodeSerializedKey(serKey, h.groupColTypes)
	}
	return ext.keyValues
}

// repRowCompare orders row's key tuple against a stored tuple of the same
// group: lexicographic over the eligible key positions in key order.
// Positions where the stored box is nil (a NULL key, or a column outside
// the grouping set) are skipped. Negative means the row is the smaller.
func (h *HashAggregate) repRowCompare(b *batch.RecordBatch, row int, stored []any) int {
	for j, i := range h.repCols {
		if i >= len(stored) || stored[i] == nil {
			continue
		}
		idx := h.groupColIdx[i]
		if idx < 0 {
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			continue
		}
		if c := h.repRowCmp[j](v, row, stored[i]); c != 0 {
			return c
		}
	}
	return 0
}

// repRowCmpFor resolves a key column's row-against-box comparison once per
// aggregate (resolveRepCols), so a hit dispatches through it rather than
// switching on the type per row.
func repRowCmpFor(t batch.TypeID) func(v *batch.Vector, row int, stored any) int {
	switch t {
	case batch.TypeDecimal:
		return func(v *batch.Vector, row int, stored any) int {
			if s, ok := stored.(string); ok {
				return batch.RepCompareDecimalRow(v, row, s)
			}
			return 0
		}
	case batch.TypeFloat64:
		return func(v *batch.Vector, row int, stored any) int {
			if f, ok := stored.(float64); ok {
				return batch.RepCompareFloat64(floatRowValue64(v, row), f)
			}
			return 0
		}
	case batch.TypeFloat32:
		return func(v *batch.Vector, row int, stored any) int {
			if f, ok := stored.(float32); ok {
				return batch.RepCompareFloat32(floatRowValue32(v, row), f)
			}
			return 0
		}
	case batch.TypeCIDR:
		return func(v *batch.Vector, row int, stored any) int {
			if s, ok := stored.(string); ok {
				return batch.RepCompareText(cidrRowText(v, row), s)
			}
			return 0
		}
	}
	return func(*batch.Vector, int, any) int { return 0 }
}

// cidrRowText is row's stored CIDR text, unboxed (valid for the compare).
func cidrRowText(v *batch.Vector, row int) string {
	for v.Base != nil {
		row = int(v.Indices[row])
		v = v.Base
	}
	return v.BytesData.UnsafeStringValue(row)
}

func floatRowValue64(v *batch.Vector, row int) float64 {
	for v.Base != nil {
		row = int(v.Indices[row])
		v = v.Base
	}
	return v.Float64Data[row]
}

func floatRowValue32(v *batch.Vector, row int) float32 {
	for v.Base != nil {
		row = int(v.Indices[row])
		v = v.Base
	}
	return v.Float32Data[row]
}

// repBoxCompare orders two stored tuples of one group the same way.
func (h *HashAggregate) repBoxCompare(a, b []any) int {
	for _, i := range h.repCols {
		if i >= len(a) || i >= len(b) {
			continue
		}
		if c := batch.RepCompareBox(h.groupColTypes[i], a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}

// repHit applies the rule when row meets a group whose stored tuple is
// stored (and whose accumulators, for eligibility, are accs — read BEFORE
// row is accumulated). It rewrites stored IN PLACE with the row's whole
// tuple when the row is the smaller representative: every key position the
// stored tuple holds, never a mix of two members.
func (h *HashAggregate) repHit(b *batch.RecordBatch, row int, stored []any, accs []kernel.Accumulator) {
	if h.repEligAgg > 0 {
		re, se := h.repRowEligible(b, row), h.repAccEligible(accs)
		if re != se {
			if re {
				h.repAdoptRow(b, row, stored)
			}
			return
		}
	}
	if len(h.repCols) > 0 && h.repRowCompare(b, row, stored) < 0 {
		h.repAdoptRow(b, row, stored)
	}
}

func (h *HashAggregate) repAdoptRow(b *batch.RecordBatch, row int, stored []any) {
	for i, idx := range h.groupColIdx {
		if i >= len(stored) || idx < 0 || stored[i] == nil {
			// nil stays nil: a NULL key (the row's is NULL too, being one
			// group) or a column outside this grouping set.
			continue
		}
		stored[i] = b.Columns[idx].GetValueOf(row, false)
	}
	RepresentativeReplacements.Add(1)
}

// repSoAHit is repHit on the eager generic SoA path (a DECIMAL or float key
// among several columns): the group's boxed tuple is its state's.
// Eligibility never applies here (a counting aggregate takes processRow).
func (h *HashAggregate) repSoAHit(b *batch.RecordBatch, row int, g int32) {
	if gs := h.strGroupStates[g]; gs != nil && gs.extras != nil && gs.extras.keyValues != nil {
		if h.repRowCompare(b, row, gs.extras.keyValues) < 0 {
			h.repAdoptRow(b, row, gs.extras.keyValues)
		}
	}
}

// repCompactHit is repSoAHit on the compact multi-column path (a FLOAT32
// key beside narrow columns), whose states live in intGroupStates.
func (h *HashAggregate) repCompactHit(b *batch.RecordBatch, row int, g int32) {
	if gs := h.intGroupStates[g]; gs != nil && gs.extras != nil && gs.extras.keyValues != nil {
		if h.repRowCompare(b, row, gs.extras.keyValues) < 0 {
			h.repAdoptRow(b, row, gs.extras.keyValues)
		}
	}
}

// repMergeGroup applies the rule when a clone's group (src) meets the
// primary's (dst): dst's tuple becomes src's, in place, when src's is the
// smaller representative.
func (h *HashAggregate) repMergeGroup(dst, src *groupStateExtras) {
	if dst == nil || src == nil || dst.keyValues == nil || src.keyValues == nil {
		return
	}
	if h.repEligAgg > 0 {
		de, se := h.repAccEligible(dst.accs), h.repAccEligible(src.accs)
		if de != se {
			if se {
				copy(dst.keyValues, src.keyValues)
				RepresentativeReplacements.Add(1)
			}
			return
		}
	}
	if len(h.repCols) > 0 && h.repBoxCompare(src.keyValues, dst.keyValues) < 0 {
		copy(dst.keyValues, src.keyValues)
		RepresentativeReplacements.Add(1)
	}
}

// repDeferredFloatHit is repHit for the typed-generic path's deferred
// groups, whose state is nil while their key is the canonical value the key
// bytes rebuild (+0, the one canonical NaN). A row whose float key is not
// canonical and orders before the canonical value gets the group a boxed
// state carrying its own tuple, boxFloatMemberKey's form.
func (h *HashAggregate) repDeferredFloatHit(b *batch.RecordBatch, row int, g int32, kcols []keySerCol) {
	if gs := h.strGroupStates[g]; gs != nil && gs.extras != nil && gs.extras.keyValues != nil {
		if h.repRowCompare(b, row, gs.extras.keyValues) < 0 {
			h.repAdoptRow(b, row, gs.extras.keyValues)
		}
		return
	}
	if !floatKeyNotCanonical(kcols, row) {
		return
	}
	for i := range kcols {
		c := &kcols[i]
		var cmp int
		switch c.kind {
		case 3:
			if c.nulls.IsNullFast(row) {
				continue
			}
			f := c.f64[row]
			cmp = batch.RepCompareFloat64(f, math.Float64frombits(keyFloat64bits(f)))
		case 4:
			if c.nulls.IsNullFast(row) {
				continue
			}
			f := c.f32[row]
			cmp = batch.RepCompareFloat32(f, math.Float32frombits(keyFloat32bits(f)))
		}
		if cmp != 0 {
			if cmp < 0 {
				h.boxFloatMemberKey(b, row, g)
				RepresentativeReplacements.Add(1)
			}
			return
		}
	}
}

// repPartialCompare orders two spill-run tuples of one group (kWayMerger):
// the eligibility class first when the aggregate has one, then the
// representative order over the eligible key positions.
func repPartialCompare(keyTypes []batch.TypeID, eligAgg int, a, b *partialGroup) int {
	if eligAgg > 0 {
		ea, eb := partialAccEligible(a, eligAgg-1), partialAccEligible(b, eligAgg-1)
		if ea != eb {
			if ea {
				return -1
			}
			return 1
		}
	}
	for i, t := range keyTypes {
		if !batch.RepEligibleType(t) || i >= len(a.KeyVals) || i >= len(b.KeyVals) {
			continue
		}
		ka, kb := &a.KeyVals[i], &b.KeyVals[i]
		if ka.Tag != kb.Tag || ka.IsNull() {
			continue
		}
		var c int
		switch ka.Tag {
		case partialTagString:
			// A DECIMAL key's run value is its printed box.
			switch t {
			case batch.TypeDecimal:
				c = repCompareDecimalBytes(ka.Bytes, kb.Bytes)
			case batch.TypeCIDR:
				c = repCompareTextBytes(ka.Bytes, kb.Bytes)
			}
		case partialTagFloat64:
			c = batch.RepCompareFloat64(ka.F64, kb.F64)
		case partialTagFloat32:
			c = batch.RepCompareFloat32(float32(ka.F64), float32(kb.F64))
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func partialAccEligible(g *partialGroup, ai int) bool {
	if ai < 0 || ai >= len(g.Accs) {
		return true
	}
	a := &g.Accs[ai]
	return a.SumI64 > 0 || a.SumF64 > 0 || (!a.SumDec.IsZero() && !a.SumDec.IsNegative())
}

// repCompareDecimalBytes is batch.RepCompareDecimalText over byte slices,
// without converting them.
func repCompareDecimalBytes(a, b []byte) int {
	fd := func(s []byte) int {
		for i := len(s) - 1; i >= 0; i-- {
			if s[i] == '.' {
				return len(s) - 1 - i
			}
		}
		return 0
	}
	if da, db := fd(a), fd(b); da != db {
		if da < db {
			return -1
		}
		return 1
	}
	return bytesCompare(a, b)
}

// repCompareTextBytes is batch.RepCompareText over byte slices.
func repCompareTextBytes(a, b []byte) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return bytesCompare(a, b)
}
