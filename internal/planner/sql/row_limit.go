package sql

import "strconv"

// AppendRowLimit bounds a read only when appending LIMIT preserves meaning.
// EXISTS needs one row; scalar subqueries need enough to detect a second row
// and raise 21000, not an IN-set-size 54000/WADJET_IN_SET_MAX refusal.
// IN still needs its set and retains that bound.
// Return unchanged for existing LIMIT/OFFSET, set operations, nil info or n<=0.
// info must parse this sql; text-only callers use WithRowLimit.
// See docs/internals/sql-subquery-row-read-limits.md for the design.
func AppendRowLimit(sql string, info *SelectInfo, n int) string {
	if info == nil || n <= 0 {
		return sql
	}
	if info.Limit != "" || info.Offset != "" || info.Union != nil {
		return sql
	}
	return sql + " LIMIT " + strconv.Itoa(n)
}

// WithRowLimit is AppendRowLimit for a caller that holds only the subquery
// text. A statement that does not parse is returned unchanged: this function
// decides how much to READ, never what is legal, and the compiler and the
// runner both raise on their own for text they cannot use.
func WithRowLimit(sql string, n int) string {
	parsed, err := Parse(sql)
	if err != nil {
		return sql
	}
	info, err := ExtractSelect(parsed)
	if err != nil || info == nil {
		return sql
	}
	return AppendRowLimit(sql, info, n)
}
