// SPDX-License-Identifier: MIT

// This file holds stage cost for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// QueryCost summarizes the estimated cost of a query across all scan stages.
type QueryCost struct {
	TotalBytes int64
	TotalRows  int64
	TotalFiles int
	HasFilter  bool
	HasLimit   bool
}

func HasFilterOrPartition(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeFilter {
		return true
	}
	if n.Type == logical.NodeScan && (len(n.PartitionFilter) > 0 || len(n.ScanPredicates) > 0) {
		return true
	}
	for _, c := range n.Children {
		if HasFilterOrPartition(c) {
			return true
		}
	}
	return false
}

func HasLimit(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeLimit {
		return true
	}
	for _, c := range n.Children {
		if HasLimit(c) {
			return true
		}
	}
	return false
}

// QueryLimitSQLState is the SQLSTATE a cost-guard rejection carries:
// PostgreSQL class 53 (insufficient resources), 53400
// configuration_limit_exceeded. The limit is a configured bound, not a
// property of the statement, so a client can tell "your administrator caps
// this" apart from a syntax or type error and stop retrying (#803).
const QueryLimitSQLState = "53400"

// enforceQueryLimits checks estimated query cost against the limits in force:
// the deployment's (Planner.QueryLimits, from `query_limits:` and its per-role
// overrides) narrowed by the calling identity's, which an ABAC `query_limit`
// obligation puts on the context. See identity_limits.go for why the two
// arrive by different routes and meet here.
func (p *Planner) EnforceQueryLimits(ctx context.Context, node *logical.Node) error {
	limits := tightestLimits(p.QueryLimits, IdentityQueryLimitsFromContext(ctx))
	if limits == nil {
		return nil
	}
	cost := p.EstimatePlanScanCost(ctx, node)

	if limits.MaxScanBytes > 0 && cost.TotalBytes > limits.MaxScanBytes {
		return sqlerr.New(QueryLimitSQLState, "query would scan %s (%d bytes) across %d files, exceeding limit of %s — add a WHERE clause or partition filter",
			FormatBytes(cost.TotalBytes), cost.TotalBytes, cost.TotalFiles, FormatBytes(limits.MaxScanBytes))
	}
	if limits.MaxScanRows > 0 && cost.TotalRows > limits.MaxScanRows {
		return sqlerr.New(QueryLimitSQLState, "query would scan %d rows across %d files, exceeding limit of %d rows — add a WHERE clause or LIMIT",
			cost.TotalRows, cost.TotalFiles, limits.MaxScanRows)
	}
	if limits.MaxScanFiles > 0 && cost.TotalFiles > limits.MaxScanFiles {
		return sqlerr.New(QueryLimitSQLState, "query would scan %d files, exceeding limit of %d — add a partition filter",
			cost.TotalFiles, limits.MaxScanFiles)
	}
	if limits.RequireFilterAboveBytes > 0 && cost.TotalBytes > limits.RequireFilterAboveBytes && !cost.HasFilter {
		return sqlerr.New(QueryLimitSQLState, "query scans %s without a WHERE clause (filter required above %s)",
			FormatBytes(cost.TotalBytes), FormatBytes(limits.RequireFilterAboveBytes))
	}
	if limits.RequireLimitAboveRows > 0 && cost.TotalRows > limits.RequireLimitAboveRows && !cost.HasLimit {
		return sqlerr.New(QueryLimitSQLState, "query scans %d rows without a LIMIT (limit required above %d rows)",
			cost.TotalRows, limits.RequireLimitAboveRows)
	}
	return nil
}

func FormatBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1fTB", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
