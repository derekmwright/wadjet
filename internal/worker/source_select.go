package worker

import (
	"fmt"
	"strings"

	"github.com/citc-tech/wadjet/internal/engine/exec"
)

// inputFileKind classifies a list of S3 keys for one input alias.
type inputFileKind int

const (
	inputKindUnknown    inputFileKind = iota
	inputKindPartitioned               // "partition=NNNN/*.wshf" — upstream Repartition stage output
	inputKindShuffleFlat               // "*.wshf" without partition segments — upstream Replicate/pipeline output
	inputKindParquet                   // "*.parquet" — table scan
)

func (k inputFileKind) String() string {
	switch k {
	case inputKindPartitioned:
		return "partitioned"
	case inputKindShuffleFlat:
		return "shuffle-flat"
	case inputKindParquet:
		return "parquet"
	default:
		return "unknown"
	}
}

// classifyInputFiles inspects a file list for one input alias and decides
// which Source implementation to use. Mixed kinds in a single alias are a
// planner bug; we fail loudly rather than silently read the wrong source.
func classifyInputFiles(files []string) (inputFileKind, error) {
	if len(files) == 0 {
		return inputKindUnknown, fmt.Errorf("empty file list")
	}
	first := kindOf(files[0])
	if first == inputKindUnknown {
		return inputKindUnknown, fmt.Errorf("unrecognized file pattern: %q", files[0])
	}
	for _, f := range files[1:] {
		k := kindOf(f)
		if k != first {
			return inputKindUnknown, fmt.Errorf("mixed file kinds for alias: first=%s (%q), other=%s (%q)",
				first, files[0], k, f)
		}
	}
	return first, nil
}

func kindOf(key string) inputFileKind {
	switch {
	case strings.Contains(key, "partition="):
		return inputKindPartitioned
	case strings.HasSuffix(key, ".wshf"):
		return inputKindShuffleFlat
	case strings.HasSuffix(key, ".parquet"):
		return inputKindParquet
	default:
		return inputKindUnknown
	}
}

// sourceForAlias returns a streaming Source over the given S3 keys, suitable
// for wiring into planner.StreamingSources. Used by pipeline tasks in the
// native-DAG executor to consume upstream stage output or pre-scanned tables
// by alias.
//
// Today all three file kinds share cachedFileStreamSource because it works
// for parquet and .wshf alike. The classification step validates that the
// file patterns within an alias are consistent (so a planner bug that mixes
// partitioned output with flat output fails fast instead of silently
// concatenating mismatched chunk streams).
func (e *Executor) sourceForAlias(queryID, bucket, alias string, files []string) (exec.Source, error) {
	return e.sourceForAliasWithProjection(queryID, bucket, alias, files, nil)
}

// sourceForAliasWithProjection hints that the parquet reader should only
// materialise the named columns. Safe to call with a mix of schema and
// derived column names — the source skips projection when any requested
// column is missing from the file schema. WSHF payloads ignore the hint.
func (e *Executor) sourceForAliasWithProjection(queryID, bucket, alias string, files []string, projectColumns []string) (exec.Source, error) {
	kind, err := classifyInputFiles(files)
	if err != nil {
		return nil, fmt.Errorf("alias %q: %w", alias, err)
	}
	_ = kind
	if len(projectColumns) > 0 {
		return newCachedFileStreamSourceWithProjection(e, queryID, bucket, files, projectColumns), nil
	}
	return newCachedFileStreamSource(e, queryID, bucket, files), nil
}
