package harness

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
)

// SliceConfig describes a local-mode data slice. The harness uses these
// to choose how many sample files to load into the catalog and what
// GOMEMLIMIT each worker process is started with.
type SliceConfig struct {
	Name          Slice
	LineitemFiles int
	OrdersFiles   int
	GoMemLimit    int64 // bytes; passed to worker via GOMEMLIMIT env
	ExpectSpill   bool  // if true and total spill bytes == 0, fail the run
}

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

// SliceConfigs maps each Slice to its configuration.
var SliceConfigs = map[Slice]SliceConfig{
	SliceSmall: {
		Name:          SliceSmall,
		LineitemFiles: 4,
		OrdersFiles:   1,
		GoMemLimit:    4 * GB,
		ExpectSpill:   false,
	},
	SliceLarge: {
		Name:          SliceLarge,
		LineitemFiles: 12,
		OrdersFiles:   3,
		GoMemLimit:    8 * GB,
		ExpectSpill:   true,
	},
}

// LoadQuery returns the SQL text for the given TPC-H query name (e.g. "q05").
// Uses SF100 scale factor for Q11 fraction calculation.
func LoadQuery(name string) (string, error) {
	num, err := parseQueryNum(name)
	if err != nil {
		return "", err
	}
	qd := tpch.GetQuery(num, tpch.SF100)
	if qd.SQL == "" {
		return "", fmt.Errorf("unknown TPC-H query %q", name)
	}
	return qd.SQL, nil
}

// AllTPCHQueries returns the names of all 22 TPC-H queries in canonical order.
func AllTPCHQueries() []string {
	out := make([]string, 22)
	for i := 0; i < 22; i++ {
		out[i] = fmt.Sprintf("q%02d", i+1)
	}
	return out
}

// SelectQueries resolves the --queries flag to a final ordered list.
// An empty input means all 22 TPC-H queries plus all micros.
func SelectQueries(requested []string) []string {
	if len(requested) == 0 {
		out := AllTPCHQueries()
		out = append(out, "micro_reverse_bloom", "micro_grace_hash_join", "micro_hash_agg_high_card")
		return out
	}
	return requested
}

// parseQueryNum extracts the integer query number from names like "q05" or "q5".
func parseQueryNum(name string) (int, error) {
	s := strings.TrimPrefix(name, "q")
	if s == name {
		return 0, fmt.Errorf("query name %q must start with 'q'", name)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid query name %q: %w", name, err)
	}
	if n < 1 || n > 22 {
		return 0, fmt.Errorf("query number %d out of range [1,22]", n)
	}
	return n, nil
}
