// SPDX-License-Identifier: MIT

package csv

import (
	"bytes"
	"io"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The record scanner reads COPY-style CSV quoting and field boundaries.
// Only empty unquoted fields are NULL; quotes may open inside a field.
// Unterminated quotes raise 22P04; Reader checks record widths. Blank lines,
// mixed endings and extra trailing empty fields retain the documented
// extensions. See ADR-0039 §3 and docs/sql-reference.md.
type recordScanner struct {
	r     io.Reader
	buf   []byte
	pos   int
	end   int
	eof   bool
	comma []byte // the delimiter's UTF-8 bytes: one, or up to four for a non-ASCII rune

	line    int // 1-based line the scanner is on
	started bool

	rec   []byte // the current record's field bytes, unescaped
	ends  []int  // end offset in rec of each field
	nulls []bool // per field; only meaningful when anyNull
}

const scanChunk = 64 << 10

// newRecordScanner scans r with the delimiter comma, which may be any rune:
// a field boundary is the rune's whole UTF-8 sequence, so a multibyte
// delimiter (`§`, `界`) never splits a character (the reader's configuration
// has always been a rune; a byte conversion cut it).
func newRecordScanner(r io.Reader, comma rune) *recordScanner {
	if comma == 0 {
		comma = ','
	}
	return &recordScanner{r: r, comma: utf8.AppendRune(nil, comma), line: 1, buf: make([]byte, scanChunk)}
}

// fill reads more input after the unconsumed bytes, which it moves to the
// front of the buffer; it reports false at the end of the file.
func (s *recordScanner) fill() (bool, error) {
	if s.eof {
		return false, nil
	}
	rest := copy(s.buf, s.buf[s.pos:s.end])
	s.pos, s.end = 0, rest
	for {
		n, err := s.r.Read(s.buf[rest:])
		s.end = rest + n
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

// isComma reports whether the delimiter starts at s.pos, reading ahead as
// far as the delimiter's length needs.
func (s *recordScanner) isComma() (bool, error) {
	if s.buf[s.pos] != s.comma[0] {
		return false, nil
	}
	for s.end-s.pos < len(s.comma) {
		ok, err := s.fill()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return bytes.Equal(s.buf[s.pos:s.pos+len(s.comma)], s.comma), nil
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
		if c == s.comma[0] {
			is, err := s.isComma()
			if err != nil {
				return nil, nil, 0, err
			}
			if is {
				s.pos += len(s.comma)
				endField()
				continue
			}
		}
		switch c {
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
				if b == s.comma[0] || b == '"' || b == '\n' || b == '\r' {
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
	for s.end-s.pos < 3 {
		ok, err := s.fill()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	if s.end-s.pos >= 3 && s.buf[s.pos] == 0xEF && s.buf[s.pos+1] == 0xBB && s.buf[s.pos+2] == 0xBF {
		s.pos += 3
	}
	return nil
}
