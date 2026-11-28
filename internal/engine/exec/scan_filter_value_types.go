// SPDX-License-Identifier: MIT

package exec

import "github.com/derekmwright/wadjet/internal/engine/batch"

// ScanFilterValueTypes is, for each key pair i, the type in which this build's
// scan filters carry key i's VALUES — the dynamic min/max range
// (BuildKeyRange) and the bloom scan filter (BloomScanFilter) — or
// KeyTypeUnresolved where that is not one proved type (#1677 line r).
//
// The range is recorded off the build vector in the build column's OWN type
// and units (updateKeyMinMax), so its type is the build column's type. The
// bloom holds the keys at the PAIR's resolved common type (KeyTypes). The two
// agree, and the answer is the build column's type, only when the pair is not
// keyed at a different common type; where it is — DATE against TIMESTAMP
// keyed as the midnight's milliseconds, INTEGER against BIGINT keyed as eight
// bytes, an integer against NUMERIC keyed as a decimal — the probe side is
// promoted at the join, and a filter in either side's own units says nothing
// the probe's stored statistics can be compared with. Such a key answers
// KeyTypeUnresolved, as does a key whose build column was never recorded.
//
// Read it after Build and before PruneBuildColumns, which may drop the
// recorded build schema.
func (h *HashJoin) ScanFilterValueTypes() []batch.TypeID {
	out := make([]batch.TypeID, len(h.LeftKeys))
	for i := range out {
		out[i] = KeyTypeUnresolved
		own, ok := h.buildKeyType(i)
		if !ok {
			continue
		}
		if KeyTypeAt(h.KeyTypes, i, own) != own {
			continue
		}
		out[i] = own
	}
	return out
}
