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

// ErrDMLRowSuperseded means another statement already superseded a row this
// statement is about to supersede (#691, ADR-0030).
// It complements ErrDMLTargetMoved's file check, preventing duplicate UPDATE
// replacements and resurrection after DELETE even when files did not move.
// Redo the statement against the new manifest, as for ErrDMLTargetMoved,
// so conflicting statements yield a serial outcome.
// See docs/internals/catalog-dml-row-conflict.md for the design.
var ErrDMLRowSuperseded = errors.New("a row this statement supersedes was already superseded by another statement")

// PendingFile is a data file already written to the object store but NOT yet
// in the manifest, waiting to be committed together with the delete markers
// that supersede what it replaces.
type PendingFile struct {
	PartValues map[string]string
	PartPath   string
	Entry      FileEntry
}

// CommitDML publishes replacement files AND delete markers in one CAS, or neither.
// Every marker's file must remain live (ErrDMLTargetMoved), and its row must
// not already be marked (ErrDMLRowSuperseded; #691).
// DML scans must exclude DeletedRowsByFile before matching (#674), so an
// already-marked incoming row proves another statement committed since the read.
// Disjoint rows of one file and unrelated writes may both commit; a revision
// change alone is not a conflict. A refused statement may retry safely.
// Uploaded objects from refused attempts remain unreferenced until orphan
// reclaim; only the manifest decides row visibility.
// See docs/internals/catalog-dml-atomic-publication.md for the design.
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
	registered := make([]string, len(owned))
	for i := range owned {
		owned[i].Entry.EngineWritten = true
		registered[i] = owned[i].Entry.Path
	}

	// The registration side of the object-retirement interlock (#896,
	// retire.go), the same one addFiles takes: no cleanup sweep may retire
	// the bytes at a path this statement is registering.
	if err := c.beginRegistration(registered); err != nil {
		return err
	}
	defer c.endRegistration(registered)

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
