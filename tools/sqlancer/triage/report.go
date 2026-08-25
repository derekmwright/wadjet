package triage

import (
	"fmt"
	"io"
	"strings"
)

// orderedCategories is the order counts and findings print in — genuine
// violations first (most to least specific), then the two noise buckets.
var orderedCategories = []Category{
	CategoryTLPResultSet,
	CategoryTLPAggregate,
	CategoryNoREC,
	CategoryPQS,
	CategoryCERT,
	CategoryCrashEcho,
	CategoryUnexpectedError,
}

// Print writes a human-readable summary of r to w: counts per category,
// followed by every retained finding (every genuine violation, plus any
// captured panic/fatal-error lines) with its source location and either
// its best-effort minimized query (when one was extracted) or its raw
// captured detail (a crash-echo panic/fatal-error line never has a
// "query" to extract, so it renders its Header/Detail text instead —
// omitting that would leave every crash Finding printed with nothing
// under its header line at all).
func (r *Report) Print(w io.Writer) {
	fmt.Fprintln(w, "SQLancer triage report")
	fmt.Fprintln(w, "======================")
	for _, cat := range orderedCategories {
		fmt.Fprintf(w, "%-32s %d\n", cat.String()+":", r.Counts[cat])
	}
	fmt.Fprintln(w)

	if len(r.Findings) == 0 {
		fmt.Fprintln(w, "No genuine violations or captured panics.")
		return
	}

	fmt.Fprintln(w, "Findings")
	fmt.Fprintln(w, "--------")
	for n, f := range r.Findings {
		fmt.Fprintf(w, "[%d] %s (%s:%d)\n", n+1, f.Category, f.Source, f.Line)
		if f.OracleCheck != "" {
			fmt.Fprintf(w, "    oracle check: %s\n", f.OracleCheck)
		}
		if len(f.Queries) > 0 {
			for _, q := range f.Queries {
				fmt.Fprintf(w, "    query: %s\n", q)
			}
		} else {
			for _, line := range strings.Split(f.Detail, "\n") {
				fmt.Fprintf(w, "    %s\n", line)
			}
		}
		fmt.Fprintln(w)
	}
}
