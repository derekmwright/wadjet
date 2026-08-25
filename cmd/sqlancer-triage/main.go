// Command sqlancer-triage classifies SQLancer soak-log output (and the
// wadjet server logs captured next to it) into genuine oracle violations,
// wadjet-crash echoes, and ordinary SQL-surface noise. See
// tools/sqlancer/triage's package doc and tools/sqlancer/README.md's
// "Triage protocol" for why this exists instead of a grep.
//
// Usage:
//
//	sqlancer-triage <file-or-dir>...
//
// Each argument may be a plain-text log, a ".gz" log (transparently
// decompressed), or a directory (walked recursively for *.log/*.log.gz,
// skipping "data" and "nats" subdirectories — a soak pair's wadjet
// data/NATS store dirs, never log output).
//
// Exit status is non-zero if any argument couldn't be classified at all,
// or if any file was only partially scanned (e.g. a line past this
// package's line-length cap, or a corrupt gzip stream) — the printed
// report still runs to completion using whatever was read, but a non-zero
// exit means its counts are a floor, not a complete answer, and a script
// driving this command should not treat that report as authoritative.
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/tools/sqlancer/triage"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: sqlancer-triage <file-or-dir>...")
		os.Exit(2)
	}

	files, err := collectFiles(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "no .log or .log.gz files found in the given arguments")
		os.Exit(1)
	}

	report := triage.NewReport()
	hadError := false
	for _, f := range files {
		if err := report.ClassifyFile(f); err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v\n", err)
			hadError = true
		}
	}
	report.Print(os.Stdout)

	if hadError {
		fmt.Fprintln(os.Stderr, "\nsqlancer-triage: one or more sources were unreadable or only partially scanned (see warnings above) — the counts above may undercount")
		os.Exit(1)
	}
}

func collectFiles(args []string) ([]string, error) {
	var files []string
	for _, arg := range args {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", arg, err)
		}
		if !info.IsDir() {
			files = append(files, arg)
			continue
		}
		err = filepath.WalkDir(arg, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "data" || d.Name() == "nats" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".log") || strings.HasSuffix(path, ".log.gz") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walking %s: %w", arg, err)
		}
	}
	sort.Strings(files)
	return files, nil
}
