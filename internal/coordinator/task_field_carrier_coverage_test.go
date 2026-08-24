package coordinator

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// Every field of distributed.Task must be consciously classified against
// the two walkers that enumerate a task's file-carrying fields by hand:
// stampTaskDeleteMarkers (delete_markers.go, #491) and
// annotateTaskPeerLocations (peer_locations.go). Both walk the identical
// nine sites today — Files, InputFiles, BuildFiles, Inputs,
// PreScannedInputs, ScanFileFilter, FusedJoins[].BuildFiles,
// Operators[].{InputFiles,BuildFiles}, PreComputedAggregates[].CacheFiles
// — which is what makes one classification answer for both; if a future
// change makes them diverge, split "walked" into two columns rather than
// stretching this test to paper over the difference.
//
// The trap this guards against already bit annotateTaskPeerLocations once:
// dispatchFinalAggregateFanout's merge tasks carried their input list ONLY
// in OpShuffleSource.InputFiles, a carrier neither walker's original
// author had reason to think about, and it went unnoticed as a hint miss
// (fell through to S3, slow but not wrong) rather than the loud failure a
// missed carrier costs stampTaskDeleteMarkers — a task reading a marked
// file through an unwalked carrier answers with the deleted rows still in
// it, silently. A new field named like a file list (a string slice or a
// map to one, directly or through a nested Operators/FusedJoins-shaped
// struct) must be added to both walkers' addAll calls AND classified
// "walked" below, or explicitly justified as not carrying row-bearing
// file paths — never left to fall through the cracks between the two.
func TestTaskFieldCarrierCoverage(t *testing.T) {
	const (
		walked      = "walked"       // addAll'd by both stampTaskDeleteMarkers and annotateTaskPeerLocations
		notFile     = "not-a-file"   // field's type carries no row-bearing file path at all
		notRowFile  = "not-row-data" // carries an S3 key, but not one a DELETE marker could apply to
		acceptedGap = "accepted-gap" // does carry file paths; deliberately unwalked, reasoned below
	)

	classified := map[string]string{
		// Identity / routing — no file paths.
		"ID": notFile, "QueryID": notFile, "StageID": notFile, "Type": notFile,
		"ClusterID": notFile, "TableName": notFile, "Attempt": notFile,
		"DegradedMemory": notFile, "EstimatedBytes": notFile,
		"Priority": notFile, "PriorityDeep": notFile,

		// SQLText is SQL, not a file path — the pipeline path's whole
		// reason for being exempt from a wire-level stamp (see the
		// comment above asyncCtx in SubmitSQL, coordinator.go).
		"SQLText": notFile, "DataBucket": notFile,

		// The file-carrying fields both walkers cover today.
		"PreScannedInputs": walked, "ScanFileFilter": walked, "Files": walked,
		"InputFiles": walked, "BuildFiles": walked, "Inputs": walked,
		"FusedJoins": walked, "Operators": walked, "PreComputedAggregates": walked,

		"ScanShardIndex": notFile, "ScanShardCount": notFile, "PartialAggregate": notFile,
		// PartitionFilter maps a partition COLUMN to its VALUE (e.g.
		// region → "east"), not a file.
		"PartitionFilter": notFile,
		// Columns/ColumnTypes name output columns and their declared
		// types, never a path.
		"Columns": notFile, "ColumnTypes": notFile,
		"FilterExprs": notFile, "PostFilterExprs": notFile,
		"ScanAggGroupBy": notFile, "ScanAggSpecs": notFile,
		"GroupByCols": notFile, "Aggregates": notFile,
		"RowLimit": notFile, "SortKeys": notFile, "Limit": notFile,
		"MergePreSorted": notFile, "MergePartials": notFile,
		"JoinType": notFile, "JoinLeftKeys": notFile, "JoinRightKeys": notFile,
		"BuildTableAlias": notFile, "QualifyAllBuildCols": notFile,
		// BuildColOrigins maps a bare column name to its owning scan
		// ALIAS, not to a file.
		"BuildColOrigins": notFile,
		"JoinFilter":      notFile, "BuildFilterExprs": notFile,
		"JoinBuildSchema": notFile, "JoinProbeSchema": notFile,
		"ShuffleKeys": notFile, "NumPartitions": notFile,
		"ComputedCols": notFile, "DropCols": notFile,
		"PartialAggKeys": notFile, "PartialAggSpecs": notFile, "PartitionID": notFile,
		// DynamicFilterSpec.BloomBucket/BloomKey DO name an S3 object, but
		// it is a materialized bloom filter blob — a boolean summary, not
		// rows a DELETE removes any of. There is nothing for a delete
		// marker to intersect.
		"DynamicFilters": notRowFile,
		"TraceID":        notFile, "SpanID": notFile, "TraceFlags": notFile,
		"IdentityName": notFile, "IdentityRole": notFile,
		"PolicyDecisionJSON": notFile,
		"ResultBucket":       notFile,
		// ResultPrefix/Output/ReplySubject are WRITE destinations — where
		// this task's own output goes, never something it reads.
		"ResultPrefix": notFile,
		// InputLocations is a peer-address HINT keyed by files already
		// named on one of the walked fields above; it introduces no new
		// file path of its own.
		"InputLocations":   notFile,
		"AffinityWorkerID": notFile,
		// EagerInputs[alias].Replay[].Files DOES carry file paths — the
		// one deliberate exception. Eager consumer dispatch
		// (docs/design/eager-consumer-dispatch.md) only ever races a
		// COMPUTE stage's own dispatched tasks (EagerInput.ProducerTaskIDs
		// must name real tasks); a pass-through leaf scan dispatches no
		// task at all (dispatchPipelineStage's OutputSinglePart
		// fast-path) and so can never be an eager producer. Every file an
		// EagerInputs entry can name is therefore a downstream STAGE
		// OUTPUT whose own producer task was walked (and, if it read
		// marked base files, stamped) when IT was dispatched — the same
		// argument that keeps PreScannedInputs safe unwalked-in-effect
		// even though it IS walked. Filed as a documented gap rather than
		// silently exempted: if eager dispatch ever grows a path from a
		// leaf scan directly, this classification must be revisited.
		"EagerInputs":    acceptedGap,
		"FetchToken":     notFile,
		"AsyncUpload":    notFile,
		"UploadPolicy":   notFile,
		"Output":         notFile,
		"ReplySubject":   notFile,
		"GatherOrdering": notFile, "GatherLimit": notFile,
		"StageType": notFile, "BuildRowHint": notFile, "SemiAntiKeyOnly": notFile,
		// DeleteMarkers is the walkers' OWN OUTPUT, not an input to walk —
		// stamping it from itself would be circular.
		"DeleteMarkers": notFile,
		"CreatedAt":     notFile,
	}

	tp := reflect.TypeOf(distributed.Task{})
	for i := 0; i < tp.NumField(); i++ {
		name := tp.Field(i).Name
		if _, ok := classified[name]; !ok {
			t.Errorf("distributed.Task field %q is not classified for the file-carrier walk; "+
				"classify it in this test and, if it carries a row-bearing file path, add it to "+
				"stampTaskDeleteMarkers AND annotateTaskPeerLocations", name)
		}
	}
	for name := range classified {
		if _, ok := tp.FieldByName(name); !ok {
			t.Errorf("classified field %q no longer exists on distributed.Task", name)
		}
	}
}
