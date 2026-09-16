// SPDX-License-Identifier: MIT

package physical

func ColSet(cols []string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}
