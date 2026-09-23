// SPDX-License-Identifier: MIT

// Package fileinput is the input of a file reader (read_csv, read_json,
// read_parquet): a SEQUENCE of files, each decoded on its own by its
// format's reader, rather than one byte stream.
//
// A glob is several files, and a file format frames a FILE: a CSV header,
// a JSON document (a top-level array is closed by its `]`), a Parquet
// footer. Concatenating the matched files' bytes and handing the stream to
// a single-file decoder read the second file's header as a data row, stopped
// a JSON array glob at the first file's `]` (#1262) and made a Parquet glob
// unreadable (#1240). A reader given a sequence opens each file only when it
// reaches it, holds at most one open at a time, and names the file a row
// came from.
package fileinput

import "io"

// Input is one file of a reader's input.
type Input struct {
	// Name is the file a message about one of its rows names. It is empty
	// for an input that is the reader's WHOLE input (a single path or a
	// URL): the caller already prefixes that path to every error.
	Name string
	// Open opens the file. It is called once, when the reader reaches it.
	Open func() (io.ReadCloser, error)
}

// Reader is an input that is already open: the reader's whole input, with
// no name (the caller names it).
func Reader(r io.Reader) []Input {
	return []Input{{Open: func() (io.ReadCloser, error) { return io.NopCloser(r), nil }}}
}
