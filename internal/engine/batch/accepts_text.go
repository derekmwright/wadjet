package batch

// VectorAcceptsText reports whether a vector of this type can STORE a Go
// string — that is, whether SetValue/SetValueChecked has a string arm for it
// rather than raising the #361 guard.
//
// It is not a style question: a caller that hands a typed vector the text form
// of its value gets a stored value for the types listed here and a
// TypeMismatchError panic for the rest, and the two are far apart. The set
// operation's UNKNOWN-typed literal arm is the caller that has to know
// (ADR-0012 item 12): PostgreSQL gives a quoted literal the type the other
// arms resolve to and parses it with THAT type's input function, so
// `SELECT c_ipv4 … UNION ALL SELECT '10.0.0.9'` is inet — and the arm's
// pipeline produces the literal as a STRING box, which reaches the result
// column's vector unchanged. Where this returns false the planner refuses the
// shape at PLAN time with 0A000 rather than letting a deterministic parse
// failure surface mid-execution (on the stage DAG, after three retries of it).
//
// The list is held to what the code actually does by
// TestVectorAcceptsTextIsWhatSetValueDoes, which tries a string into a vector
// of EVERY type and compares. Adding a string arm to SetValue without updating
// this function fails that test, which is the point: this is a statement about
// SetValue, not a second opinion about it.
func VectorAcceptsText(t TypeID) bool {
	switch t {
	case TypeString, TypeBytes, TypeIPv4, TypeIPv6, TypeCIDR, TypeMAC,
		TypeUUID, TypeDate, TypeDecimal:
		return true
	}
	return false
}
