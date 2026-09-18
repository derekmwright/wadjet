// SPDX-License-Identifier: MIT

package batch

// VectorAcceptsText reports whether SetValue/SetValueChecked has a string arm
// for this type, instead of raising TypeMismatchError (#361).
// UNKNOWN set-operation literals use the resolved type's input function;
// unsupported shapes must refuse at plan time with 0A000 (ADR-0012 item 12).
// Keep this list equal to the writers' actual accept-set:
// TestVectorAcceptsTextIsWhatSetValueDoes exercises every type.
// See docs/internals/batch-vector-text-input-capability.md for the design.
func VectorAcceptsText(t TypeID) bool {
	switch t {
	case TypeString, TypeBytes, TypeIPv4, TypeIPv6, TypeCIDR, TypeMAC,
		TypeUUID, TypeDate, TypeDecimal,
		// PORT and PROTOCOL read their own text form since #1137 — the IANA
		// name and decimal digits, parquet.NetworkTextValue — so a set
		// operation whose resolved type is one of them no longer has to refuse
		// an UNKNOWN literal arm with 0A000.
		TypePort, TypeProtocol:
		return true
	}
	return false
}
