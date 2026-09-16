// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"sort"
	"strings"
)

// o2SortRender compares a result as a MULTISET: it sorts the row segments of
// an f1Render string, keeping the `cols=[…] rows=N` header first. A statement
// with no ORDER BY of its own makes no claim about the order of its rows, and
// pinning one would be pinning an implementation detail (ADR-0013).
func o2SortRender(rendered string) string {
	parts := strings.Split(rendered, " | ")
	if len(parts) < 2 {
		return rendered
	}
	head, body := parts[0], append([]string(nil), parts[1:]...)
	sort.Strings(body)
	return head + " | " + strings.Join(body, " | ")
}

// o2KeySeq is the SEQUENCE of a result's sort-key columns, rendered as
// `v0,v1 | v0,v1 | …`. A statement whose own ORDER BY leaves ties makes no
// claim about which tied row comes first, but it does claim the KEY sequence,
// and that is fully determined by the multiset of key tuples.
func o2KeySeq(run func(string) (string, error), c o2Cell) string {
	rendered, err := run(c.sql)
	if err != nil {
		return "ERR: " + err.Error()
	}
	parts := strings.Split(rendered, " | ")
	if len(parts) < 2 {
		return ""
	}
	var out []string
	for _, row := range parts[1:] {
		cells := strings.Split(row, ",")
		var keys []string
		for _, i := range c.keyCols {
			if i < len(cells) {
				keys = append(keys, cells[i])
			}
		}
		out = append(out, strings.Join(keys, ","))
	}
	return strings.Join(out, " | ")
}
