// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// taskCompileOptions are the compile options every expression a task
// compiles from its text takes: its query's PostgreSQL numeric categories by
// column name (distributed.Task.PGCategories), which is how ROUND and the
// integer cast know whether a FLOAT64 column a previous stage wrote is a
// float8 or a float-carried numeric (#381).
func taskCompileOptions(task distributed.Task) []expr.CompileOption {
	if len(task.PGCategories) == 0 {
		return nil
	}
	cats := make(map[string]expr.PGCategory, len(task.PGCategories))
	for k, v := range task.PGCategories {
		cats[k] = expr.PGCategory(v)
	}
	return []expr.CompileOption{expr.WithInputPGCategories(cats)}
}
