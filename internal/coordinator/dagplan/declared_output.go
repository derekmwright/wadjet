// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ProjectionOutputType is inferProjectionType for callers outside this
// package. The worker's pre-aggregate projection compiles a derived GROUP BY
// key from its SQL TEXT and has no catalog to resolve the columns in it, so it
// needs the same rule the planner applies to a SELECT-list expression — the
// same reason distributed.AggSpec.InputType is carried on the spec.
//
// It used to declare every derived key String, which is right only when the
// expression returns one: CAST(l_shipdate AS DATE) evaluates to an epoch-day
// number, and a String vector stored it as the DIGITS of that number, so the
// stage DAG grouped by "8039" where the single-process path grouped by
// 1992-01-05 (#340).
//
// Only a DECIDED type is taken. A polymorphic declaration that answered with
// its own fallback (expr.Guessed) has decided nothing here, because the caller
// holds no column types for it to consult: COALESCE(n_name, n_comment) would
// answer Float64 from coalesce's numeric fallback, and a Float64 vector drops
// every string it is handed — 1 group where there are 25 (#331/#333). The
// caller's fallback stands in those cases, exactly as before.
func ProjectionOutputType(node plansql.Node, fallback parquet.TypeID) expr.DeclType {
	if t, c := physical.NodeDeclaredType(node, physical.ColDecls{}); c == expr.Decided {
		return t
	}
	return expr.Decl(fallback)
}

// inferRenameExprDecl types an expression the GATHER will evaluate, against
// the scope the producer below the output projection emits.
//
// It is the same call attachScanSelectProjections makes for a SELECT item its
// own fragment computes — one rule for a computed column's type, whichever
// operator ends up computing it. A scope it cannot read leaves the rename
// undeclared, and the gather keeps the runtime detections it had.
func inferRenameExprDecl(astExpr plansql.Node, scope *logical.Node) (expr.DeclType, bool) {
	if astExpr == nil || scope == nil || len(scope.Children) != 1 {
		return expr.DeclType{}, false
	}
	child := scope.Children[0]
	// physical.EmittedColDecls, not physical.InputColDecls. The scope is the node BELOW the
	// output projection, and what the gather's expression reads is what that
	// node EMITS: a WINDOW's `__win_N` slots, and an AGGREGATE's `__agg_N`
	// outputs. `inputColTypes` has a Window arm (#729) and NO Aggregate arm,
	// so the window half of this family was typed and the aggregate half fell
	// through to the STRING fallback — `physical.EmittedColTypes`' aggregate arm
	// already declares each output from `physical.AggSpecOutputType`, which is the same
	// rule the stage's own AggSpec carries. One rule, both slot families.
	//
	// And the declaration is made only when the inference DECIDED. A fallback
	// is not a declaration: typed STRING and declared anyway,
	// `CASE WHEN MAX(id) > 0 THEN MAX(c_date) ELSE NULL END` built a String
	// vector and `SetValue` rendered the epoch day `16195` where PostgreSQL
	// and the single-process path say `2014-05-05`. An undecided rename keeps
	// `evalExprColumn`'s float64 arm — what it had before the declaration
	// existed — so a shape this walk cannot type is never made worse by it.
	d, conf := physical.InferProjectionDeclTypeConf(astExpr, parquet.TypeString,
		physical.StrictIntArithCols(child), physical.EmittedColDecls(child))
	if conf == expr.Undecided {
		return expr.DeclType{}, false
	}
	return d, true
}
