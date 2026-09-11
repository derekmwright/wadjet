package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrCompactionConflict means input identity or applied delete-marker state
// changed since the output snapshot (ADR-0030; #893/#894/#895).
// Validation refuses before CAS: nothing was published and the losing writer
// may discard its output and replan from the current manifest.
// A publication ERROR is different: a timeout/refusal may leave commit outcome
// unknown, so retain the uploaded bytes rather than deleting live output.
// See docs/internals/catalog-compaction-conflict-boundary.md for the design.
var ErrCompactionConflict = errors.New("this compaction output was cut from a snapshot the table no longer has")

var (
	// ErrCompactionInputMoved: one of the files the output replaces is no
	// longer in the partition — another writer has already consumed it.
	ErrCompactionInputMoved = fmt.Errorf("%w: an input file is no longer in the partition", ErrCompactionConflict)

	// ErrCompactionDeletesAdvanced: the delete markers on an input are not
	// the ones the output was cut against. Publishing would republish a row
	// a committed DELETE removed, or drop a row nobody deleted.
	ErrCompactionDeletesAdvanced = fmt.Errorf("%w: an input's delete markers changed after the output was written", ErrCompactionConflict)
)

// CompactionCommit is ONE compaction publication: the input files it consumes,
// the replacement it publishes, and the delete-marker snapshot the replacement
// was cut from.
//
// Every field is part of the commit's precondition, not just of its effect.
// See Catalog.CommitCompaction.
type CompactionCommit struct {
	Table      string
	PartPath   string
	PartValues map[string]string

	// Inputs are the file paths the output replaces. Every one must still be
	// in PartPath's file list at commit time.
	Inputs []string

	// Output is the replacement file, or nil when every input row was
	// delete-filtered away and nothing was written. A nil Output is a
	// publication like any other — the inputs and their markers still go
	// away in the same CAS.
	Output *FileEntry

	// AppliedDeletes is the delete-marker snapshot the output was cut
	// against: per input path, the set of row indices the merge skipped. The
	// manifest's markers for those paths must be EXACTLY this at commit
	// time. A marker that arrived since names a row the output still
	// carries, and publishing would resurrect it; a marker that vanished
	// since means the output dropped a row the table still has.
	AppliedDeletes map[string]map[int64]bool
}

// CommitCompaction removes inputs and their markers and publishes the output
// in ONE conditional manifest transaction, or changes none (#893).
// Every input must remain in PartPath, else ErrCompactionInputMoved (#895).
// Each input's current marker set must equal the output's applied snapshot,
// else ErrCompactionDeletesAdvanced (#894); atomicity alone is insufficient.
// Unrelated ingest, DML on other files and other-partition compaction do not
// invalidate these predicates and may commit alongside this replacement.
// See docs/internals/catalog-compaction-atomic-publication.md for the design.
func (c *Catalog) CommitCompaction(_ context.Context, cc CompactionCommit) error {
	if len(cc.Inputs) == 0 {
		return fmt.Errorf("compaction commit for table %q names no input files", cc.Table)
	}

	// The output is an object this engine just wrote, so it carries the
	// ownership marker AddNewFiles stamps (#494, ADR-0020 layer 0). Copied,
	// not stamped in place: the entry belongs to the caller.
	var output *FileEntry
	if cc.Output != nil {
		owned := *cc.Output
		owned.EngineWritten = true
		output = &owned
	}

	// A registration naming the output path must not be racing its
	// publication, and the output path must not be mid-retirement. Both are
	// impossible for a freshly minted UUIDv7, and both are cheap to prove
	// rather than assume.
	var registered []string
	if output != nil {
		registered = []string{output.Path}
	}
	if err := c.beginRegistration(registered); err != nil {
		return err
	}
	defer c.endRegistration(registered)

	c.invalidateManifestCache(cc.Table)
	key := c.key("manifest." + cc.Table)
	const maxRetries = 10

	for retry := 0; retry < maxRetries; retry++ {
		raw, rev, err := c.kv.Get(key)
		if err != nil {
			return fmt.Errorf("reading manifest for %q: %w", cc.Table, err)
		}
		var manifest PartitionManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}

		partIdx := -1
		for i := range manifest.Partitions {
			if manifest.Partitions[i].Path == cc.PartPath {
				partIdx = i
				break
			}
		}
		if partIdx < 0 {
			return fmt.Errorf("partition %q of table %q: %w",
				manifestPartitionLabel(cc.PartPath), cc.Table, ErrCompactionInputMoved)
		}

		// (1) Input identity.
		inPartition := make(map[string]bool, len(manifest.Partitions[partIdx].Files))
		for _, f := range manifest.Partitions[partIdx].Files {
			inPartition[f.Path] = true
		}
		for _, p := range cc.Inputs {
			if !inPartition[p] {
				return fmt.Errorf("input %q of table %q: %w", p, cc.Table, ErrCompactionInputMoved)
			}
		}

		// (2) The delete-marker snapshot the output was cut against.
		current := DeletedRowsByFile(manifest.DeleteMarkers)
		for _, p := range cc.Inputs {
			if !sameRowSet(current[p], cc.AppliedDeletes[p]) {
				return fmt.Errorf("input %q of table %q (manifest marks %d rows, the output applied %d): %w",
					p, cc.Table, len(current[p]), len(cc.AppliedDeletes[p]), ErrCompactionDeletesAdvanced)
			}
		}

		// (3) The replacement's path must not already name a file the
		// partition carries — same #494 rule mergeNewFileEntries enforces,
		// checked before the filter below mutates the slice.
		removeSet := make(map[string]bool, len(cc.Inputs))
		for _, p := range cc.Inputs {
			removeSet[p] = true
		}
		if output != nil {
			for _, f := range manifest.Partitions[partIdx].Files {
				if !removeSet[f.Path] && f.Path == output.Path {
					return fmt.Errorf("file path %q already exists in partition %q: refusing to add a duplicate compaction output (#494)",
						output.Path, cc.PartPath)
				}
			}
		}

		// (4) The mutation: inputs out, their markers out, replacement in.
		files := make([]FileEntry, 0, len(manifest.Partitions[partIdx].Files))
		for _, f := range manifest.Partitions[partIdx].Files {
			if !removeSet[f.Path] {
				files = append(files, f)
			}
		}
		if output != nil {
			files = append(files, *output)
		}
		manifest.Partitions[partIdx].Files = files

		var keepMarkers []DeleteMarker
		for _, dm := range manifest.DeleteMarkers {
			if !removeSet[dm.FilePath] {
				keepMarkers = append(keepMarkers, dm)
			}
		}
		manifest.DeleteMarkers = keepMarkers
		manifest.UpdatedAt = time.Now().UTC()

		updated, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("marshaling manifest: %w", err)
		}
		if _, err := c.kv.Update(key, updated, rev); err == ErrRevisionMismatch {
			casBackoff(retry)
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("compaction commit failed after %d CAS retries (table %q)", maxRetries, cc.Table)
}

// manifestPartitionLabel names the unpartitioned partition in an error
// message, where an empty string reads as a missing value.
func manifestPartitionLabel(path string) string {
	if path == "" {
		return "(unpartitioned)"
	}
	return path
}

// sameRowSet reports whether two row-index sets hold the same members. Nil and
// empty are the same set: a file with no markers and a file whose marker entry
// was merged away both mean "no row of this file is deleted".
func sameRowSet(a, b map[int64]bool) bool {
	na, nb := 0, 0
	for _, v := range a {
		if v {
			na++
		}
	}
	for _, v := range b {
		if v {
			nb++
		}
	}
	if na != nb {
		return false
	}
	for k, v := range a {
		if v && !b[k] {
			return false
		}
	}
	return true
}
