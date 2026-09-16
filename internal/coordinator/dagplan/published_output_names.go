// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// republishDeclaredSchema renames a plan-declared output schema's columns to
// the published names, positionally — the same rename CollectSink.OutputNames
// applies to the sink's own schema, for the copy the GATHER carries.
func republishDeclaredSchema(projNode *logical.Node, cols []parquet.Column) []parquet.Column {
	names := physical.DAGPublishedOutputNames(projNode)
	if len(names) == 0 || len(names) != len(cols) {
		return cols
	}
	for i := range cols {
		if names[i] != "" {
			cols[i].Name = names[i]
		}
	}
	return cols
}
