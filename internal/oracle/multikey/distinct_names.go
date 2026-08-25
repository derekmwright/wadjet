package multikey

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DISTINCT-NAME arm.
//
// The shared-schema arm (multikey.go) is where #562 was found, and it gates
// the wrong half of the pass. Its two relations carry ONE schema, so every
// correlated conjunct reads `s = s` — a name that resolves on both sides,
// which extractRightJoinKeys cannot attribute and therefore DECLINES. That
// arm proves the decline is right; it never once makes the narrowing fire.
//
// These tables give the outer, inner and wide relations DIFFERENT column
// prefixes (p_ / q_ / w_), so `b.q_s = a.p_s AND b.q_n = a.p_n` attributes
// cleanly, the pass narrows the build side to Project(q_s, q_n) → Distinct,
// and the code path #562 lives on is the one under test.
//
// It found a second silent-zero on that path immediately: the Project aliases
// every key to its BARE name, so a key the condition spells QUALIFIED is
// renamed out from under it. See the dn_selfjoin_* entries.
const (
	DNOuter = "dn_outer"
	DNInner = "dn_inner"
	DNWide  = "dn_wide"
	DNDim   = "dn_dim"
)

// Row counts, mirroring the shared-schema arm: Wide > 3 × Outer is the
// estimator's semi/anti swap threshold, Inner < Outer is its control.
const (
	DNOuterRows = 40
	DNInnerRows = 24
	DNWideRows  = 260
)

// dnPrefix is the per-relation column prefix that makes every key name
// attributable to exactly one side.
func dnPrefix(table string) string {
	switch table {
	case DNOuter:
		return "p_"
	case DNInner:
		return "q_"
	case DNWide:
		return "w_"
	}
	return "k_"
}

func dnRows(table string) int {
	switch table {
	case DNOuter:
		return DNOuterRows
	case DNInner:
		return DNInnerRows
	case DNWide:
		return DNWideRows
	}
	return 5
}

// DNSchema is one distinct-name table's schema.
func DNSchema(table string) parquet.Schema {
	if table == DNDim {
		return parquet.Schema{Columns: []parquet.Column{
			{Name: "k_k", Type: parquet.TypeInt32},
			{Name: "k_label", Type: parquet.TypeString, Nullable: true},
		}}
	}
	p := dnPrefix(table)
	return parquet.Schema{Columns: []parquet.Column{
		{Name: p + "id", Type: parquet.TypeInt64},
		{Name: p + "s", Type: parquet.TypeString, Nullable: true},
		{Name: p + "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: p + "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: p + "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
}

// DNData builds one distinct-name table's rows. Same coprime periods and the
// same NULL discipline as the shared-schema arm — the FIRST column of a pair
// nulls on the probe side and the SECOND on the build side — so the two arms
// differ in exactly one thing: whether the pass can attribute the keys.
func DNData(table string) []map[string]any {
	if table == DNDim {
		out := make([]map[string]any, 5)
		for i := range out {
			out[i] = map[string]any{"k_k": int32(i), "k_label": fmt.Sprintf("l%d", i)}
		}
		return out
	}
	p := dnPrefix(table)
	out := make([]map[string]any, dnRows(table))
	for i := range out {
		r := map[string]any{
			p + "id": int64(i),
			p + "s":  fmt.Sprintf("s%d", i%6),
			p + "n":  int64(i % 5),
			p + "g":  int32(i % 4),
			p + "d":  float64(i%7) * 1.25,
		}
		switch table {
		case DNOuter:
			nullIf(r, p+"s", i%11 == 4)
			nullIf(r, p+"d", i%13 == 6)
		case DNInner:
			nullIf(r, p+"n", i%7 == 3)
			nullIf(r, p+"d", i%9 == 5)
		case DNWide:
			nullIf(r, p+"s", i%23 == 8)
			nullIf(r, p+"n", i%29 == 11)
		}
		out[i] = r
	}
	return out
}

// dnPins are the entries a defect OTHER than #562 keeps wadjet from
// answering. Same ratchet as the shared-schema arm's.
//
// dn_exists_derived was here under #577 and is gone: #550/#571 declined
// decorrelation over a derived-table inner, so it is correct on both paths
// and gated outright now.
var dnPins = map[string]struct{ issue, reason string }{
	"dn_notin_2key": {"#578", "a CORRELATED NOT IN is lowered to a plain anti join, so it answers " +
		"its NOT EXISTS twin instead of NOT IN's three-valued rule (#507's remainder)"},
}

// DistinctNameCorpus is the arm on which the narrowing actually FIRES.
func DistinctNameCorpus() []Case {
	var out []Case
	add := func(name, sql string, want int64, keys int) {
		c := Case{Name: name, SQL: sql, Want: want, Keys: keys}
		if p, ok := dnPins[name]; ok {
			c.Issue, c.KnownBug = p.issue, p.reason
		}
		out = append(out, c)
	}

	// The #562 shape, now on the firing path.
	add("dn_exists_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`,
		DNOuter, DNInner), 27, 2)
	add("dn_exists_2key_reordered", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.q_n = a.p_n AND b.q_s = a.p_s)`,
		DNOuter, DNInner), 27, 2)
	add("dn_notexists_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`,
		DNOuter, DNInner), 13, 2)
	add("dn_exists_3key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n AND b.q_g = a.p_g)`, DNOuter, DNInner), 19, 3)
	add("dn_notexists_3key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n AND b.q_g = a.p_g)`, DNOuter, DNInner), 21, 3)
	add("dn_exists_dec_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.q_d = a.p_d AND b.q_n = a.p_n)`,
		DNOuter, DNInner), 20, 2)
	add("dn_in_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.p_s IN (SELECT b.q_s FROM %s b WHERE b.q_n = a.p_n)`,
		DNOuter, DNInner), 27, 2)
	add("dn_notin_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.p_s NOT IN (SELECT b.q_s FROM %s b WHERE b.q_n = a.p_n)`,
		DNOuter, DNInner), 9, 2)

	// NULLs, on either side of the pair.
	add("dn_exists_null_probe", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.p_s IS NULL AND EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`, DNOuter, DNInner), 0, 2)
	add("dn_notexists_null_probe", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.p_s IS NULL AND NOT EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`, DNOuter, DNInner), 4, 2)
	add("dn_exists_null_build", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.q_n = a.p_n AND b.q_d = a.p_d)`,
		DNOuter, DNInner), 20, 2)

	// The estimator's swap, and an inner-only predicate.
	add("dn_exists_swap_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.w_s = a.p_s AND b.w_n = a.p_n)`,
		DNOuter, DNWide), 36, 2)
	add("dn_notexists_swap_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.w_s = a.p_s AND b.w_n = a.p_n)`,
		DNOuter, DNWide), 4, 2)
	add("dn_exists_pred_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n AND b.q_id < 12)`, DNOuter, DNInner), 17, 2)

	// A JOIN inside the subquery, both write orders.
	add("dn_exists_joined_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b JOIN %s k ON k.k_k = b.q_g `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`, DNOuter, DNInner, DNDim), 27, 2)
	add("dn_exists_joined_inner_nonlead", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s k JOIN %s b ON k.k_k = b.q_g `+
			`WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`, DNOuter, DNDim, DNInner), 27, 2)
	add("dn_exists_derived", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM `+
			`(SELECT q_s, q_n FROM %s WHERE q_id < 20) b WHERE b.q_s = a.p_s AND b.q_n = a.p_n)`,
		DNOuter, DNInner), 23, 2)

	// --- the QUALIFIED key ------------------------------------------------
	//
	// The subquery's inner SELF-JOINS, so its two arms share every bare name
	// and the join emits one arm's columns qualified (`b1.q_s`). The
	// narrowing's Project aliases each key to its bare name, which RENAMES
	// the key out from under the condition still asking for the qualified
	// one: the build emits `q_s`, the join looks up `b1.q_s`, that resolves
	// to index -1, and the join matches nothing. Semi answered 0 and anti
	// answered 40, silently, on the single-process path only — the DAG's
	// worker resolves the key differently and answered 36, so it was also a
	// two-path split.
	//
	// The last two were WRONG BEFORE #562's fix as well: they are the shapes
	// where the old lexical split happened to attribute both keys.
	add("dn_selfjoin_inner_2key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b1 JOIN %s b2 ON b1.q_g = b2.q_g `+
			`WHERE b1.q_s = a.p_s AND b2.q_n = a.p_n)`, DNOuter, DNInner, DNInner), 36, 2)
	add("dn_selfjoin_inner_2key_swapped", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b1 JOIN %s b2 ON b1.q_g = b2.q_g `+
			`WHERE b2.q_s = a.p_s AND b1.q_n = a.p_n)`, DNOuter, DNInner, DNInner), 36, 2)
	add("dn_selfjoin_inner_notexists", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b1 JOIN %s b2 ON b1.q_g = b2.q_g `+
			`WHERE b1.q_s = a.p_s AND b2.q_n = a.p_n)`, DNOuter, DNInner, DNInner), 4, 2)
	// Both correlations on the SAME outer column through the two arms: the
	// pair of keys strips to one name, which is the collision decline.
	add("dn_selfjoin_inner_samecol", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b1 JOIN %s b2 ON b1.q_g = b2.q_g `+
			`WHERE b1.q_s = a.p_s AND b2.q_s = a.p_s)`, DNOuter, DNInner, DNInner), 36, 2)

	// Single-key controls, and the brackets they put on the two-key answers.
	add("dn_exists_1key_s", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.q_s = a.p_s)`,
		DNOuter, DNInner), 36, 1)
	add("dn_exists_1key_n", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.q_n = a.p_n)`,
		DNOuter, DNInner), 40, 1)

	return out
}

// dnPostgresSetup renders the distinct-name tables for PostgresSetup.
func dnPostgresSetup(b *strings.Builder) {
	for _, tbl := range []string{DNOuter, DNInner, DNWide} {
		p := dnPrefix(tbl)
		fmt.Fprintf(b, "DROP TABLE IF EXISTS %s;\n", tbl)
		fmt.Fprintf(b, "CREATE TABLE %s (%sid bigint, %ss text COLLATE \"C\", %sn bigint, "+
			"%sg integer, %sd numeric(18,4));\n", tbl, p, p, p, p, p)
		for _, r := range DNData(tbl) {
			fmt.Fprintf(b, "INSERT INTO %s VALUES (%d, %s, %s, %s, %s);\n",
				tbl, r[p+"id"].(int64),
				pgLit(r[p+"s"]), pgLit(r[p+"n"]), pgLit(r[p+"g"]), pgLit(r[p+"d"]))
		}
	}
	fmt.Fprintf(b, "DROP TABLE IF EXISTS %s;\n", DNDim)
	fmt.Fprintf(b, "CREATE TABLE %s (k_k integer, k_label text COLLATE \"C\");\n", DNDim)
	for _, r := range DNData(DNDim) {
		fmt.Fprintf(b, "INSERT INTO %s VALUES (%s, %s);\n", DNDim, pgLit(r["k_k"]), pgLit(r["k_label"]))
	}
}
