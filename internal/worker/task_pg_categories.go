// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// taskCompileOptions are the compile options every expression a task
// compiles from its text takes: the statement clock (expr.WithStatementClock)
// and its query's PostgreSQL numeric categories by
// column name (distributed.Task.PGCategories), which is how ROUND and the
// integer cast know whether a FLOAT64 column a previous stage wrote is a
// float8 or a float-carried numeric (#381).
func taskCompileOptions(ctx context.Context, task distributed.Task) []expr.CompileOption {
	opts := []expr.CompileOption{expr.WithStatementClock(ctx)}
	if len(task.PGCategories) == 0 {
		return opts
	}
	cats := make(map[string]expr.PGCategory, len(task.PGCategories))
	for k, v := range task.PGCategories {
		cats[k] = expr.PGCategory(v)
	}
	return append(opts, expr.WithInputPGCategories(cats))
}
