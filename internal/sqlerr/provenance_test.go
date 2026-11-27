// SPDX-License-Identifier: MIT

package sqlerr

import (
	"errors"
	"fmt"
	"testing"
)

// TestRefusalIdentityAndProvenance: a refusal's identity is read where it
// was raised, through framing, the door's sentence and an outer code; a code
// attached by Wrap to an uncoded chain is Attached, and one wrapped around a
// raised code is not (#1680c).
func TestRefusalIdentityAndProvenance(t *testing.T) {
	r := Refuse("typmod", "an interval typmod is not supported")
	for _, c := range []struct {
		why      string
		err      error
		reason   string
		attached bool
		state    string
	}{
		{"the refusal", r, "typmod", false, "0A000"},
		{"framed", fmt.Errorf("planning: %w", r), "typmod", false, "0A000"},
		{"through the door's sentence", Sentence(fmt.Errorf("planning: %w", r)), "typmod", false, "0A000"},
		{"under an outer code", Wrap("42000", r), "typmod", false, "42000"},
		{"a raised code without identity", New("22008", "interval out of range"), "", false, "22008"},
		{"a code attached to an uncoded failure", Wrap("0A000", errors.New("unsupported shuffle type: 255")), "", true, "0A000"},
		{"the same, framed", fmt.Errorf("stage: %w", Wrap("0A000", errors.New("x"))), "", true, "0A000"},
		{"uncoded", errors.New("plain"), "", false, ""},
	} {
		if got := ReasonOf(c.err); got != c.reason {
			t.Errorf("%s: ReasonOf = %q, want %q", c.why, got, c.reason)
		}
		if got := Attached(c.err); got != c.attached {
			t.Errorf("%s: Attached = %v, want %v", c.why, got, c.attached)
		}
		if got := StateOf(c.err); got != c.state {
			t.Errorf("%s: StateOf = %q, want %q", c.why, got, c.state)
		}
	}
	if r.Error() != "an interval typmod is not supported" {
		t.Errorf("the sentence carries no identity: %q", r.Error())
	}
}
