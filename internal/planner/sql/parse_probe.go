// SPDX-License-Identifier: MIT

package sql

import "sync/atomic"

// parseProbe, when set, is told the text of every Parse call. It is the seam
// the parse-count gates read (arc CI3 round 2): "a subquery body is parsed
// once per statement" is a property a gate can only hold by counting, and a
// counter installed and removed by hand holds nothing after the review that
// installed it.
var parseProbe atomic.Pointer[func(string)]

// SetParseProbe installs f as the parse probe and returns the function that
// restores the previous one. A nil f removes the probe. Tests only: the probe
// sees every statement of the process, whichever goroutine parses it.
func SetParseProbe(f func(sql string)) (restore func()) {
	var next *func(string)
	if f != nil {
		next = &f
	}
	prev := parseProbe.Swap(next)
	return func() { parseProbe.Store(prev) }
}
