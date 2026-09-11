// Package semvergen generates the version corpus every semver gate runs over.
//
// It is ONE generator, shared by the expression unit gates, the five-arm census
// and the public-API gates, for the reason every shared fixture in
// internal/oracle exists: a corpus written twice is two corpora, and the pairs
// that break a sort key are exactly the ones a second author would not think
// to list. The generator is SEEDED, so a failure names a reproducible version
// string rather than a run.
//
// WHAT IT CONCENTRATES ON. Not "plausible versions" — the shapes that break an
// ORDERING: equal cores carrying different pre-releases, numeric identifiers
// beside alphanumeric ones, identifiers containing '-' beside deeper
// identifier lists (the pair that inverts under a '.'-joined key), and numbers
// past int32 and past 2^53.
package semvergen

import (
	"fmt"
	"math/rand"
	"strings"
)

// Edge is the hand-picked part of every corpus: the specification's own §11.4
// precedence chain, the two pairs a text sort gets backwards, the `v` prefix,
// build metadata, and the two strings that are not versions at all. It is
// returned in a FIXED order, and gates index into it.
func Edge() []string {
	return []string{
		// The specification's §11.4 example, in its own order.
		"1.0.0-alpha",      // 0
		"1.0.0-alpha.1",    // 1
		"1.0.0-alpha.beta", // 2
		"1.0.0-beta",       // 3
		"1.0.0-beta.2",     // 4
		"1.0.0-beta.11",    // 5
		"1.0.0-rc.1",       // 6
		"1.0.0",            // 7
		// The two pairs a text sort inverts, measured on PostgreSQL 17.11.
		"1.2.3",  // 8
		"1.2.10", // 9
		"1.10.0", // 10
		// The `v` prefix, which normalizes onto 1.2.3 above.
		"v1.2.3", // 11
		// Build metadata, which has no precedence.
		"2.0.0+build.7", // 12
		// The pair that inverts under a '.'-joined sort key.
		"1.0.0-alpha-x", // 13
		// Not versions: a leading zero, and text.
		"01.2.3",        // 14
		"not-a-version", // 15
	}
}

// Corpus returns n distinct version strings generated from the specification's
// grammar with the given seed. Every string it returns parses, and none of them
// is one of Edge's — a gate that names an edge version in a predicate must
// match exactly the rows it put there, and a generated duplicate would silently
// add one.
func Corpus(seed int64, n int) []string {
	rng := rand.New(rand.NewSource(seed))
	alnum := []string{
		"alpha", "beta", "rc", "x", "z", "a-b", "alpha-x", "alphax",
		"alpha-", "b", "beta-1", "SNAPSHOT", "Alpha", "0a", "a0",
	}
	num := []string{"0", "1", "2", "9", "10", "11", "100", "9223372036854775807"}
	cores := []int64{0, 1, 2, 9, 10, 11, 100, 2147483647, 4294967296, 9223372036854775807}
	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	for _, e := range Edge() {
		seen[e] = true
	}
	for len(out) < n {
		var b strings.Builder
		fmt.Fprintf(&b, "%d.%d.%d",
			cores[rng.Intn(len(cores))], cores[rng.Intn(len(cores))], cores[rng.Intn(len(cores))])
		if rng.Intn(100) < 70 {
			fields := 1 + rng.Intn(4)
			b.WriteByte('-')
			for f := 0; f < fields; f++ {
				if f > 0 {
					b.WriteByte('.')
				}
				if rng.Intn(2) == 0 {
					b.WriteString(num[rng.Intn(len(num))])
				} else {
					b.WriteString(alnum[rng.Intn(len(alnum))])
				}
			}
		}
		if rng.Intn(100) < 15 {
			b.WriteString("+build.")
			b.WriteString(num[rng.Intn(len(num))])
		}
		s := b.String()
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}
