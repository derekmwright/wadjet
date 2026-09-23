// SPDX-License-Identifier: MIT

package csv

import (
	"io"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// recordScanner reads the records of ONE file in PostgreSQL's CSV format —
// the grammar `COPY … FROM … WITH (FORMAT csv)` reads (PostgreSQL 17 §COPY,
// "CSV Format"), with the default QUOTE and ESCAPE (`"`) and the default NULL
// (an unquoted empty field):
//
//   - a field is NULL only when it is EMPTY and NO part of it was quoted: `,,`
//     is NULL, `,"",` is the empty string, `, ""` is a space (#1259);
//   - a quote may open anywhere in a field and closes at the next lone quote;
//     inside it a doubled quote is one quote and a delimiter or a line break
//     is data (`x"y,z"w` is the field `xy,zw`; `"x" y` is `x y`);
//   - whitespace is data, quoted or not;
//   - a line ends at LF, CR or CRLF, and the three may be mixed in one file
//     (COPY fixes the first one and refuses a different one later with 22P04;
//     this reader answered them all as line ends through v0.24.0, on every
//     path, and keeps that — ADR-0012 §5's superset rule);
//   - a file that ends inside a quote is 22P04 "unterminated CSV quoted
//     field" (#1248: encoding/csv's error ended the 100-row sample as if it
//     were the end of the file, and the query answered the rows before it);
//   - a blank line is ONE empty unquoted field (the Reader skips it in a file
//     of more than one column, as it always has, where COPY refuses it).
//
// Two differences from PostgreSQL 17 are deliberate (docs/postgres-differences.md):
// a line holding `\.` is data, not an end-of-data marker (PostgreSQL 18 stopped
// honouring it in files too, since it silently dropped every later row), and
// a UTF-8 byte-order mark at the start of a file is skipped (it would
// otherwise be part of the first header name).
//
// Encoding/csv could not be kept: it discards whether a field was quoted, so
// `""` and an empty field are the same string, and it refuses `x"y"z` and
// `"x" y`, which PostgreSQL reads.
type recordScanner struct {
	r     io.Reader
	buf   []byte
	pos   int
	end   int
	eof   bool
	comma byte

	line    int // 1-based line the scanner is on
	started bool

	rec   []byte // the current record's field bytes, unescaped
	ends  []int  // end offset in rec of each field
	nulls []bool // per field; only meaningful when anyNull
}

const scanChunk = 64 << 10

func newRecordScanner(r io.Reader, comma byte) *recordScanner {
	if comma == 0 {
		comma = ','
	}
	return &recordScanner{r: r, comma: comma, line: 1, buf: make([]byte, scanChunk)}
}

// fill reads more input; it reports false at the end of the file.
func (s *recordScanner) fill() (bool, error) {
	if s.eof {
		return false, nil
	}
	for {
		n, err := s.r.Read(s.buf)
		s.pos, s.end = 0, n
		if n > 0 {
			if err == io.EOF {
				s.eof = true
			}
			return true, nil
		}
		if err == io.EOF {
			s.eof = true
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}

// peek returns the next byte without consuming it.
func (s *recordScanner) peek() (byte, bool, error) {
	if s.pos >= s.end {
		ok, err := s.fill()
		if !ok || err != nil {
			return 0, false, err
		}
	}
	return s.buf[s.pos], true, nil
}

// next reads one record: its fields (one string backs them all), which of
// them are NULL (nil when none is), and the line it starts on. It answers
// io.EOF when the file has no further record.
func (s *recordScanner) next() (fields []string, nulls []bool, line int, err error) {
	if !s.started {
		s.started = true
		if err := s.skipBOM(); err != nil {
			return nil, nil, 0, err
		}
	}
	if _, ok, err := s.peek(); err != nil {
		return nil, nil, 0, err
	} else if !ok {
		return nil, nil, 0, io.EOF
	}
	line = s.line
	s.rec = s.rec[:0]
	s.ends = s.ends[:0]
	s.nulls = s.nulls[:0]
	anyNull := false
	fieldStart, sawQuote, inQuote := 0, false, false
	endField := func() {
		null := !sawQuote && len(s.rec) == fieldStart
		anyNull = anyNull || null
		s.ends = append(s.ends, len(s.rec))
		s.nulls = append(s.nulls, null)
		fieldStart, sawQuote = len(s.rec), false
	}
	for {
		if s.pos >= s.end {
			ok, err := s.fill()
			if err != nil {
				return nil, nil, 0, err
			}
			if !ok {
				if inQuote {
					return nil, nil, 0, sqlerr.New("22P04", "line %d: unterminated CSV quoted field", line)
				}
				endField()
				break
			}
		}
		c := s.buf[s.pos]
		if inQuote {
			// The run up to the next quote is data, line breaks included.
			i := s.pos
			for i < s.end && s.buf[i] != '"' {
				if s.buf[i] == '\n' {
					s.line++
				}
				i++
			}
			s.rec = append(s.rec, s.buf[s.pos:i]...)
			s.pos = i
			if i == s.end {
				continue
			}
			s.pos++ // the quote
			nc, ok, err := s.peek()
			if err != nil {
				return nil, nil, 0, err
			}
			if ok && nc == '"' {
				s.rec = append(s.rec, '"')
				s.pos++
				continue
			}
			inQuote = false
			continue
		}
		switch c {
		case s.comma:
			s.pos++
			endField()
			continue
		case '"':
			s.pos++
			sawQuote, inQuote = true, true
			continue
		case '\n':
			s.pos++
			s.line++
			endField()
		case '\r':
			s.pos++
			nc, ok, err := s.peek()
			if err != nil {
				return nil, nil, 0, err
			}
			if ok && nc == '\n' {
				s.pos++
			}
			s.line++
			endField()
		default:
			// An unquoted run of data up to the next byte with a meaning.
			i := s.pos + 1
			for i < s.end {
				b := s.buf[i]
				if b == s.comma || b == '"' || b == '\n' || b == '\r' {
					break
				}
				i++
			}
			s.rec = append(s.rec, s.buf[s.pos:i]...)
			s.pos = i
			continue
		}
		break
	}
	str := string(s.rec)
	fields = make([]string, len(s.ends))
	start := 0
	for i, e := range s.ends {
		fields[i] = str[start:e]
		start = e
	}
	if anyNull {
		nulls = make([]bool, len(s.nulls))
		copy(nulls, s.nulls)
	}
	return fields, nulls, line, nil
}

// skipBOM skips a UTF-8 byte-order mark at the very start of the file.
func (s *recordScanner) skipBOM() error {
	for s.end-s.pos < 3 && !s.eof {
		// Gather up to three bytes without losing any already buffered.
		rest := append([]byte(nil), s.buf[s.pos:s.end]...)
		n, err := s.r.Read(s.buf[len(rest):])
		copy(s.buf, rest)
		s.pos, s.end = 0, len(rest)+n
		if err == io.EOF {
			s.eof = true
		} else if err != nil {
			return err
		}
	}
	if s.end-s.pos >= 3 && s.buf[s.pos] == 0xEF && s.buf[s.pos+1] == 0xBB && s.buf[s.pos+2] == 0xBF {
		s.pos += 3
	}
	return nil
}
