// SPDX-License-Identifier: MIT

package physical

import (
	"testing"
)

// TestParseJoinKeys_PreservesQualifiers locks in the physical.ParseJoinKeys behavior
// after the Q07 fix. Stripping qualifiers in physical.CleanExpr would re-introduce
// the ambiguity that broke Q07/Q08 (probe-side lookup of unqualified
// "n_regionkey" matches both "n1.n_regionkey" and "n2.n_regionkey").
func TestParseJoinKeys_PreservesQualifiers(t *testing.T) {
	left, right, _ := parseJoinKeys("n1.n_regionkey = r_regionkey")
	if len(left) != 1 || left[0] != "n1.n_regionkey" {
		t.Errorf("left = %v, want [n1.n_regionkey]", left)
	}
	if len(right) != 1 || right[0] != "r_regionkey" {
		t.Errorf("right = %v, want [r_regionkey]", right)
	}
	// Compound:
	left, right, _ = parseJoinKeys("a.k1 = b.k1 AND a.k2 = b.k2")
	if len(left) != 2 || left[0] != "a.k1" || left[1] != "a.k2" {
		t.Errorf("compound left = %v, want [a.k1 a.k2]", left)
	}
	if len(right) != 2 || right[0] != "b.k1" || right[1] != "b.k2" {
		t.Errorf("compound right = %v, want [b.k1 b.k2]", right)
	}
	// Unqualified passes through:
	left, right, _ = parseJoinKeys("s_suppkey = l_suppkey")
	if left[0] != "s_suppkey" || right[0] != "l_suppkey" {
		t.Errorf("unqualified: left=%v right=%v", left, right)
	}
}
