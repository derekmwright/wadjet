// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "testing"

// TestGKMultisetMatches shows the order-free comparison of a kept-unsorted
// pin (#1689) accepts a reordering and nothing else: a differing value, a
// missing row, a duplicated row, a changed header and an error all fail.
func TestGKMultisetMatches(t *testing.T) {
	const want = "{numeric,int} rows=3 4.50,1 | -7.00,1 | NULL,1"
	for _, tc := range []struct {
		name, got string
		ok        bool
	}{
		{"same", want, true},
		{"reordered", "{numeric,int} rows=3 NULL,1 | 4.50,1 | -7.00,1", true},
		{"differentValue", "{numeric,int} rows=3 4.50,1 | -7.01,1 | NULL,1", false},
		{"missingRow", "{numeric,int} rows=2 4.50,1 | -7.00,1", false},
		{"duplicatedRow", "{numeric,int} rows=3 4.50,1 | 4.50,1 | NULL,1", false},
		{"duplicateReplacesRow", "{numeric,int} rows=3 4.50,1 | -7.00,1 | -7.00,1", false},
		{"differentHeader", "{text,int} rows=3 4.50,1 | -7.00,1 | NULL,1", false},
		{"error", "ERR 42803 column must appear", false},
	} {
		if got := gkMultisetMatches(tc.got, want); got != tc.ok {
			t.Errorf("%s: gkMultisetMatches(%q) = %v, want %v", tc.name, tc.got, got, tc.ok)
		}
	}
	if !gkMultisetMatches("ERR 42803 column x must appear", "ERR 42803 column x") {
		t.Error("an error answer must still compare by SQLSTATE and message")
	}
}
