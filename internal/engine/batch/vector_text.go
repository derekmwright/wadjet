// SPDX-License-Identifier: MIT

package batch

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ParseVectorText reads pgvector's text form — `[1,2,3]` — into the
// []float32 box the writer, the ingest boundary and batch.Vector all store.
//
// Without it a VECTOR literal fell to convertUnquoted's default arm and was
// passed through as a STRING, which nothing downstream converts: the writer's
// FIXED_LEN_BYTE_ARRAY leaf took the string's own bytes, so
// `INSERT INTO t (v) VALUES ('[1,2]')` into a VECTOR(2) reported success and
// left a page whose body is 5 bytes where the header promises 8 — the table
// unreadable, for a value of the RIGHT width. The file's own comment claimed
// this could not happen ("an array/row/map/vector literal is a composite
// expression the parser refuses, so convertValue never receives one"), and a
// quoted vector literal is a single string token the parser accepts.
//
// PostgreSQL's vector input function is the model: brackets required, a
// comma-separated list of numbers inside, whitespace ignorable, and anything
// else 22P02 with the literal quoted back.
func ParseVectorText(s string) ([]float32, error) {
	bad := func() error {
		return sqlerr.New("22P02", "invalid input syntax for type vector: %s", sqlerr.Quote(s))
	}
	t := strings.TrimSpace(s)
	if len(t) < 2 || t[0] != '[' || t[len(t)-1] != ']' {
		return nil, bad()
	}
	inner := strings.TrimSpace(t[1 : len(t)-1])
	if inner == "" {
		// `[]` is a zero-component vector. It parses; the caller refuses it
		// against the declared dimension, which is where the width lives.
		return []float32{}, nil
	}
	parts := strings.Split(inner, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			// NaN and the infinities have no vector value in pgvector either.
			return nil, bad()
		}
		out[i] = float32(f)
	}
	return out, nil
}
