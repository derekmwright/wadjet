package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ErrDMLTargetMoved reports that a DML statement's manifest change cannot be
// committed because the files it read are no longer the files the table has.
//
// It is the catalog's half of #691. A DELETE/UPDATE/MERGE reads a manifest,
// scans the files it names, and records which ROW OF WHICH FILE it affected.
// Between that read and the commit, compaction can rewrite those files:
// mergeGroup calls RemoveFiles (which strips the markers for the paths it
// removes) and then AddNewFiles, so the statement's markers arrive naming
// files the table no longer has. AddDeleteMarkers used to accept them —
// `dm.FilePath` was only ever a map key there — and the manifest gained a
// marker pointing at nothing:
//
//	DELETE FROM u WHERE id = 1   →  "DELETE 1", and row 1 is still there
//	UPDATE u SET n = 99 …        →  "UPDATE 1", and the table holds 1:10 AND 1:99
//
// both reported as success. Reproduced deterministically on all three doors.
//
// The statement retries when it sees this; a statement that has exhausted its
// retries reports it, and the DML layer gives it PostgreSQL's 40001
// (serialization_failure) — the class a client is expected to retry.
var ErrDMLTargetMoved = errors.New("the files this statement read are no longer in the table's manifest")

// ErrDMLRowSuperseded reports that a DML statement's manifest change cannot be
// committed because ANOTHER STATEMENT has already superseded a row this one is
// about to supersede.
//
// It is the row-level half of the same rule ErrDMLTargetMoved states over
// files, and it is what #691 left open — ADR-0030 said so in its own words:
// "Two writers racing each other … both succeed, and the second one's markers
// are valid because the files did not move … Closing it needs a conflict rule
// over ROWS, which this record does not decide." This is that rule.
//
// The window is the ordinary one: each statement reads the manifest, scans the
// files it names, records WHICH ROW OF WHICH FILE it affected, and commits at
// the end. Two statements over the same row both see it live, both write a
// replacement, and both mark the copy they read — so the manifest ends up
// naming BOTH replacements and the key is present twice. Measured on
// v0.18.22, `UPDATE … n = 111 WHERE id = 1` against `UPDATE … n = 222 WHERE
// id = 1`:
//
//	table afterwards:  1:111:a  1:222:a  2:20:b  3:30:c
//	both statements:   UPDATE 1
//
// The same window resurrects a deleted row (an UPDATE whose scan predates a
// concurrent DELETE re-publishes the row it read) and reports `DELETE 1` over
// a row that is still readable.
//
// A statement that sees this redoes itself against the manifest that replaced
// the one it read, exactly as ErrDMLTargetMoved makes it redo; the outcome is
// then one of the two serial orders PostgreSQL could have produced.
var ErrDMLRowSuperseded = errors.New("a row this statement supersedes was already superseded by another statement")

// PendingFile is a data file already written to the object store but NOT yet
// in the manifest, waiting to be committed together with the delete markers
// that supersede what it replaces.
type PendingFile struct {
	PartValues map[string]string
	PartPath   string
	Entry      FileEntry
}

// CommitDML commits one DML statement's whole manifest change in a SINGLE CAS:
// the files it wrote and the delete markers that remove the rows they replace,
// or neither.
//
// Two properties, and both are load-bearing:
//
//  1. **Validation, over files and over rows.** Every marker names a file the
//     manifest STILL HOLDS at commit time (`ErrDMLTargetMoved`), and no marker
//     names a (file, row) the manifest ALREADY MARKS (`ErrDMLRowSuperseded`).
//     The first is the check `AddDeleteMarkers` never had — it decodes the
//     manifest and never looks at `Partitions`. The second is the one #691
//     left open, and it rests on an invariant the DML door keeps: a statement
//     filters its scan through `DeletedRowsByFile` before it matches anything
//     (`deleteOnce`, `updateOnce` and `readMergeTarget` all do, which is
//     #674's rule), so it NEVER mints a marker for a row the manifest it read
//     already marked. An incoming (file, row) that is marked here was
//     therefore marked by another statement SINCE this one read, and that is
//     exactly the conflict.
//
//     Both predicates are exactly right rather than merely conservative. A
//     concurrent write that did not touch this statement's files leaves its
//     markers valid; two statements over DIFFERENT rows of the same file both
//     commit, because their marker sets are disjoint. A blunt "the revision
//     moved" test would be wrong in both directions — it fails on any
//     unrelated write, and an UPDATE's own ingest moves the revision.
//
//  2. **Atomicity.** An UPDATE or MERGE used to commit twice — the ingester's
//     AddNewFiles per flushed file, then AddDeleteMarkers — so a refusal at
//     the second commit left the replacement rows beside the originals they
//     were supposed to replace. Here the replacement files ride in the same
//     CAS as the markers, so a refused statement has written nothing to the
//     manifest and can simply be retried.
//
// What remains outside it: the parquet objects a refused attempt already wrote
// stay in the object store, unreferenced, until the orphan sweep reclaims
// them. That is a leak of bytes on a rare retry, never a wrong row — the
// manifest is the only thing that decides which rows exist.
func (c *Catalog) CommitDML(_ context.Context, tableName string, newFiles []PendingFile, markers []DeleteMarker) error {
	if len(newFiles) == 0 && len(markers) == 0 {
		return nil
	}
	c.invalidateManifestCache(tableName)
	key := c.key("manifest." + tableName)
	const maxRetries = 10

	// The rewrite outputs are objects this engine just wrote, so they carry
	// the ownership marker AddNewFiles stamps (#494, ADR-0020 layer 0).
	owned := make([]PendingFile, len(newFiles))
	copy(owned, newFiles)
	for i := range owned {
		owned[i].Entry.EngineWritten = true
	}

	for retry := 0; retry < maxRetries; retry++ {
		raw, rev, err := c.kv.Get(key)
		if err != nil {
			return fmt.Errorf("reading manifest for %q: %w", tableName, err)
		}
		var manifest PartitionManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}

		// (1) Validation, before anything is merged in.
		live := make(map[string]bool)
		for _, p := range manifest.Partitions {
			for _, f := range p.Files {
				live[f.Path] = true
			}
		}
		superseded := DeletedRowsByFile(manifest.DeleteMarkers)
		for _, dm := range markers {
			if !live[dm.FilePath] {
				return fmt.Errorf("delete marker for %q in table %q: %w",
					dm.FilePath, tableName, ErrDMLTargetMoved)
			}
			// The row half. See ErrDMLRowSuperseded: the caller filtered the
			// rows the manifest it READ had already marked, so anything marked
			// HERE was marked by a statement that committed in between.
			gone := superseded[dm.FilePath]
			for _, idx := range dm.RowIndices {
				if gone[idx] {
					return fmt.Errorf("row %d of %q in table %q: %w",
						idx, dm.FilePath, tableName, ErrDMLRowSuperseded)
				}
			}
		}

		// (2) The new files.
		for _, pf := range owned {
			found := false
			for i := range manifest.Partitions {
				if manifest.Partitions[i].Path != pf.PartPath {
					continue
				}
				merged, mErr := mergeNewFileEntries(manifest.Partitions[i].Files, []FileEntry{pf.Entry})
				if mErr != nil {
					return mErr
				}
				manifest.Partitions[i].Files = merged
				found = true
				break
			}
			if !found {
				manifest.Partitions = append(manifest.Partitions, PartitionEntry{
					Path:   pf.PartPath,
					Values: pf.PartValues,
					Files:  []FileEntry{pf.Entry},
				})
			}
		}

		// (3) The markers, merged with what is already there.
		manifest.DeleteMarkers = mergeDeleteMarkers(manifest.DeleteMarkers, markers)
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
	// 40001, not a stateless error: exhausting the CAS retries under pure
	// CONTENTION is the same "retry this statement" answer as losing the
	// target-moved race, and it reached the client as the blanket 42000 while
	// its sibling reached it as 40001 (review P2).
	return sqlerr.Wrap("40001",
		fmt.Errorf("DML commit failed after %d CAS retries (table %q)", maxRetries, tableName))
}

// mergeDeleteMarkers folds incoming markers into existing ones, one entry per
// file path, preserving the earliest CreatedAt per file.
//
// Extracted from AddDeleteMarkers so the two commit paths cannot drift: the
// merge rule is what decides which rows a reader skips.
func mergeDeleteMarkers(existing, incoming []DeleteMarker) []DeleteMarker {
	rows := make(map[string]map[int64]bool)
	times := make(map[string]time.Time)
	add := func(dms []DeleteMarker) {
		for _, dm := range dms {
			if rows[dm.FilePath] == nil {
				rows[dm.FilePath] = make(map[int64]bool)
			}
			for _, idx := range dm.RowIndices {
				rows[dm.FilePath][idx] = true
			}
			if !dm.CreatedAt.IsZero() {
				if t, ok := times[dm.FilePath]; !ok || dm.CreatedAt.Before(t) {
					times[dm.FilePath] = dm.CreatedAt
				}
			}
		}
	}
	add(existing)
	add(incoming)

	now := time.Now().UTC()
	var out []DeleteMarker
	for filePath, indices := range rows {
		idx := make([]int64, 0, len(indices))
		for i := range indices {
			idx = append(idx, i)
		}
		createdAt := now
		if t, ok := times[filePath]; ok {
			createdAt = t
		}
		out = append(out, DeleteMarker{FilePath: filePath, RowIndices: idx, CreatedAt: createdAt})
	}
	return out
}
