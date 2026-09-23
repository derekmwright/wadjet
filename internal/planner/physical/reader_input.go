// SPDX-License-Identifier: MIT

package physical

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/fileinput"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// readerInputs is a reader's input as the SEQUENCE of files it names: a URL
// or a single path is one input the caller names in its errors; a glob is
// every file it matches in name order, each named, each opened only when the
// reader reaches it. A reader decodes each file on its own (fileinput).
func readerInputs(path string) ([]fileinput.Input, error) {
	if isURL(path) {
		return []fileinput.Input{{Open: func() (io.ReadCloser, error) {
			rc, err := openHTTP(path)
			if errors.Is(err, objstore.ErrNotFound) {
				return nil, sqlerr.New("58P01", "could not open %q for reading: not found", path)
			}
			return rc, err
		}}}, nil
	}
	files, err := readerFiles(path)
	if err != nil {
		return nil, err
	}
	name := func(p string) string {
		if isGlob(path) {
			return p
		}
		return "" // the caller names a single path
	}
	inputs := make([]fileinput.Input, len(files))
	for i, p := range files {
		inputs[i] = fileinput.Input{Name: name(p), Open: func() (io.ReadCloser, error) {
			f, _, err := openInputFile(p)
			return f, err
		}}
	}
	return inputs, nil
}

// readerInitError names the reader and its input in an error a reader's
// constructor raised while reading its sample — as a later batch's error is
// named (the source's Next) — unless it is an input that could not be
// opened, whose message names the path already.
func readerInitError(fn, path string, err error) error {
	if unopenable(err) != nil {
		return fmt.Errorf("%s: %w", fn, err)
	}
	return fmt.Errorf("%s: %s: %w", fn, path, err)
}

// readerFiles is the local files a reader's path names: the path itself, or
// every FILE a glob matches, in name order. A directory a glob matches is not
// one of its files (DuckDB reads the same glob the same way). A glob that
// matches no file is 58P01, as a path that names none is.
func readerFiles(path string) ([]string, error) {
	if !isGlob(path) {
		return []string{path}, nil
	}
	matches, err := filepath.Glob(path)
	if err != nil {
		return nil, sqlerr.New("22023", "invalid glob pattern %q: %v", path, err)
	}
	sort.Strings(matches)
	files := matches[:0]
	for _, m := range matches {
		if fi, err := os.Stat(m); err == nil && fi.IsDir() {
			continue
		}
		files = append(files, m)
	}
	if len(files) == 0 {
		return nil, sqlerr.New("58P01", "could not open %q for reading: no file matches the pattern", path)
	}
	return files, nil
}

// openInputFile opens one local input file for reading, with the SQLSTATE
// PostgreSQL's COPY FROM and pg_read_file raise for an input they cannot
// open (measured on 17.11, #1245): 58P01 undefined_file when it does not
// exist, 42501 insufficient_privilege when it may not be read, 42809
// wrong_object_type when it is a directory. Through v0.24.0 the error was
// the bare os error, with no SQLSTATE at any door.
func openInputFile(path string) (*os.File, fs.FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, inputOpenError(path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, inputOpenError(path, err)
	}
	if fi.IsDir() {
		f.Close()
		return nil, nil, sqlerr.New("42809", "%q is a directory", path)
	}
	return f, fi, nil
}

// inputOpenError is PostgreSQL's SQLSTATE for an input that cannot be
// opened; an error of another kind keeps its own words and no class.
func inputOpenError(path string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return sqlerr.New("58P01", "could not open file %q for reading: no such file or directory", path)
	case errors.Is(err, fs.ErrPermission):
		return sqlerr.New("42501", "could not open file %q for reading: permission denied", path)
	}
	return fmt.Errorf("could not open file %q for reading: %w", path, err)
}

// readerInputReachable reports, without reading it, whether a local
// reader's input exists and can be opened: the refusal a statement over it
// gets at PLAN time — and EXPLAIN with it, as EXPLAIN over a relation that
// does not exist is 42P01 on PostgreSQL — rather than at its first batch.
func readerInputReachable(path string) error {
	files, err := readerFiles(path)
	if err != nil {
		return err
	}
	for _, p := range files {
		fi, err := os.Stat(p)
		if err != nil {
			return inputOpenError(p, err)
		}
		if fi.IsDir() {
			return sqlerr.New("42809", "%q is a directory", p)
		}
	}
	return nil
}
