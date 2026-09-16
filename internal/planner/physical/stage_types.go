// SPDX-License-Identifier: MIT

// This file holds stage types for the physical planner, governed by ADR-0010 and ADR-0026.
package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// PhysicalPlan represents an executable query plan.
type PhysicalPlan struct {
	Pipeline *exec.Pipeline
	Cleanup  func() // optional: called after pipeline finishes to clean up spill files
	// OutputSchema is the PLAN-DERIVED output schema: the SELECT list's
	// column names with the types the catalog says they carry. It answers
	// the question a zero-row result leaves open, since every other source
	// of a result schema in this engine reads it off a batch that never
	// arrived (#416, DeclaredOutputSchema). Advisory: a consumed batch
	// always wins.
	OutputSchema []parquet.Column
}

// ProjectExprSpec is one SELECT-list item a scan fragment must emit: Name is
// the output column, Expr the SQL text the worker compiles and evaluates
// (bare column references become passthrough copies). Type is the plan-time
// inferred output type for computed expressions (inferProjectionTypeCols) —
// the worker cannot resolve it from the input schema because the output
// column doesn't exist there. A bare passthrough leaves Type at its zero
// value and the worker never consults it there (a ColRef resolves by
// DirectCopy instead).
type ProjectExprSpec struct {
	Fields []parquet.Column
	Expr   string
	Name   string
	Type   parquet.TypeID
	// TypeKnown distinguishes a DECLARED Type from the zero value, which
	// TypeBool shares — the same shape as AggSpec.OutputTypeKnown (#354,
	// #371). A computed BOOLEAN expression (a comparison, LIKE, IS NULL, a
	// boolean literal — anything inferProjectionTypeCols resolves to
	// TypeBool) otherwise reads as "not set": projectOpFromSpecs drops it
	// off the wire, and the worker's buildSelectProjection then guesses
	// STRING for a column that IS a bool, so a pgwire client asking for the
	// true OID gets a boxed "true"/"false" string instead (#445).
	TypeKnown bool
	// Precision and Scale carry a computed DECIMAL's declaration alongside
	// Type, for the reason DecimalCoercion carries the same pair: a DECIMAL
	// is an unscaled integer plus a scale, and a worker that learns only the
	// TypeID builds the output vector at scale 0 and reads every value back
	// a hundredfold out (ADR-0024 item 2; #529, #555).
	Precision int
	Scale     int
	// SourceSlot names the input column by POSITION rather than by name, for
	// a projection whose input publishes the name TWICE. It is
	// exec.ProjectColumn.SourceIdx on the wire, and it exists for the same
	// reason that field does: `batch.RecordBatch.ColumnIndex` answers with the
	// FIRST match, so two specs reading one name read one column and the
	// other's value is unreachable (#575, #785).
	//
	// The producer that can publish a name twice is an AGGREGATE — a group
	// key and an aggregate output may share it (ADR-0026 §3a) — and the class
	// of each spec is what says which of the two it means.
	//
	// SourceSlotSet is required because 0 is a valid slot.
	SourceSlot    int
	SourceSlotSet bool
}

// DecimalCoercion is one column that must arrive as DECIMAL(Precision, Scale).
type DecimalCoercion struct {
	Name      string
	Precision int
	Scale     int
}

// PrettyPrint renders the plan for EXPLAIN VERBOSE.
//
// A PhysicalPlan is a single-process PIPELINE and says so. It used to print
// the distributed stage list, which meant `EXPLAIN VERBOSE` through the
// embedded API — and through the `wadjet` binary — printed a DAG the embedded
// engine never executes, emitted only to be printed. The stage list belongs to
// the distributed planner and `wadjetd` still prints it, from there.
func (p *PhysicalPlan) PrettyPrint() string {
	return "Single-stage local execution"
}
