// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// ErrTableLessSelectDistributed hands plans containing a Dual to
// Coordinator.ExecuteSQL's local pipeline: the DAG cannot dispatch a dual stage
// with no dependencies or scan files (#806).
// The refusal routes the whole query, including mixed dual/real-scan plans such
// as SELECT 1 UNION ALL SELECT c FROM big; those lose distribution.
// Removing that cost requires a distributed single-row source, fragment builder
// and wire tag.
// See docs/internals/table-less-distributed-handoff.md for the design.
var ErrTableLessSelectDistributed = errors.New(
	"a table-less SELECT has no distributed stage")

// refuseTableLessSelect returns a typed refusal for the first source in the
// plan that reads no stored table: a Dual node, and a TABLE-FUNCTION scan —
// generate_series, unnest, a file reader, and the system catalog relations
// (pg_catalog.*, information_schema.*), which are materialized from the
// catalog by the pipeline that scans them.
//
// A table-function scan emits a SCAN stage, which refuseUnbuildableStages
// exempts because a scan stage resolves its files from the catalog at
// dispatch — and a table function has no files to resolve, so every such
// statement failed three task attempts later with `stage scan-0 has no
// dependencies and no ScanFiles` on every DAG door. It is the #806 shape
// through a second source kind, refused at the same seam.
func refuseTableLessSelect(n *logical.Node) error {
	if n == nil {
		return nil
	}
	if n.Type == logical.NodeDual {
		return fmt.Errorf("%w: a SELECT with no FROM clause emits a `dual` stage"+
			" with no dependencies and no scan files, which the dispatcher cannot"+
			" build task inputs for",
			ErrTableLessSelectDistributed)
	}
	if n.Type == logical.NodeScan && n.IsTableFunc {
		return fmt.Errorf("%w: the table function %s reads no stored table, so its"+
			" scan stage has no files the dispatcher can build task inputs from",
			ErrTableLessSelectDistributed, n.FuncName)
	}
	for _, child := range n.Children {
		if err := refuseTableLessSelect(child); err != nil {
			return err
		}
	}
	return nil
}
