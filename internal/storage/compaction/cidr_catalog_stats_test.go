package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The CIDR values this fixture cycles through, and the two of them that are
// its extremes in PostgreSQL's inet order (which is NOT the text's: '9.' is
// above '1' as text and below every 10./11./172./192. address as an
// address, and a /8 sorts below a /24 sharing its common bits).
var (
	// The bulk of the rows, all strictly BETWEEN the two extremes below in
	// inet order.
	cidrGateValues = []string{
		"10.0.0.0/24", "11.0.0.0/8", "172.16.0.5/16", "172.16.2.187", "192.168.1.0/24",
	}
	cidrGateInetMin = "9.0.0.0/8"
	cidrGateInetMax = "192.168.188.190/24"
)

// cidrGateRows is the fixture's total row count — gateIngest lays it down as
// gateFiles files of gateRowsPerFile rows.
const cidrGateRows = gateFiles * gateRowsPerFile

func cidrGateSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
	}}
}

// cidrGateData puts the inet-order MINIMUM on the very first row and the
// inet-order MAXIMUM on the very last, so neither extreme is available from
// row group 0 of file 0 alone. That is what makes the per-row-group and
// per-file merges load-bearing: a merge that cannot order two CIDR bounds
// keeps whichever it saw first, and a fixture whose every row group holds
// every value would agree with it by construction.
func cidrGateData(offset, n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		id := offset + i
		v := cidrGateValues[id%len(cidrGateValues)]
		switch id {
		case 0:
			v = cidrGateInetMin
		case cidrGateRows - 1:
			v = cidrGateInetMax
		}
		rows[i] = map[string]any{"id": int64(id), "c_cidr": v}
	}
	return rows
}

// TestCompactionKeepsCidrCatalogStatsAsText sits beside
// TestCompactionIsIdempotentOverTheTypeMatrix rather than inside it: that
// gate deliberately does not read statistics off the footer (see its doc),
// and this is a statement about the STATS the compactor hands the catalog.
//
// Compaction REWRITES a table's catalog stats — extractColumnStatsAt
// re-derives them from what mergeAndWriteFiles wrote and AddNewFiles
// persists them into the manifest — so a defect here is not merely a bad
// value on a new table: it converts every existing table's CIDR stats on
// their next compaction pass, with nothing to undo it.
//
// The two ways it went wrong before #523's follow-up, both silent:
//
//  1. parquet.RowGroupStats hands back a confirmed CIDR bound BOXED
//     (parquet.CidrInetBound) so the prune layer can compare it in inet
//     order. Its Key is a binary string; catalog.FileColumnStats is
//     JSON-tagged and lives in NATS KV, and encoding/json rewrites every
//     byte above 0x7F as U+FFFD with no way back.
//  2. The per-row-group merge orders bounds with parquet.CompareNative,
//     which had no arm for that boxed type and answered 0 for every pair —
//     so a multi-row-group file recorded ROW GROUP 0's bound as the whole
//     file's. The fixture's true extremes are deliberately NOT in the first
//     row group of the first file (see cidrGateData), and the first
//     assertion below runs extractColumnStats over a genuinely
//     multi-row-group input file to pin that merge directly. The COMPACTED
//     output at this fixture's size is a single row group
//     (parquet.DefaultWriterConfig's row-group size is far above 1200 rows),
//     so the generations loop after it pins the unboxing and its survival
//     across passes rather than the merge.
func TestCompactionKeepsCidrCatalogStatsAsText(t *testing.T) {
	g := gateIngest(t, "cidr_stats", cidrGateSchema(), cidrGateData)

	// The MERGE, on an input file that really does span several row groups
	// (gateWriteFile writes at gateRowGroup rows per group): the last input
	// file's inet-order maximum is its very last row, so a merge that cannot
	// order two CIDR bounds keeps row group 0's instead.
	inputs := g.files(t)
	last := extractColumnStats(inputs[len(inputs)-1])["c_cidr"]
	if got, ok := last.MaxValue.(string); !ok || got != cidrGateInetMax {
		t.Errorf("extractColumnStats over the last INPUT file: c_cidr max = %#v, want %q — "+
			"the per-row-group merge is not ordering CIDR bounds in inet order",
			last.MaxValue, cidrGateInetMax)
	}
	if got, ok := last.MinValue.(string); !ok || got != "10.0.0.0/24" {
		t.Errorf("extractColumnStats over the last INPUT file: c_cidr min = %#v, want %q",
			last.MinValue, "10.0.0.0/24")
	}

	for gen := 1; gen <= gateGenerations; gen++ {
		if gen > 1 {
			g.resplit(t)
		}
		g.compact(t)
		assertCidrCatalogStats(t, fmt.Sprintf("generation %d", gen), g)
	}
}

// assertCidrCatalogStats checks every file's persisted c_cidr bounds: valid
// UTF-8 address text, and the fixture's true inet-order extremes.
func assertCidrCatalogStats(t *testing.T, what string, g *gateTable) {
	t.Helper()
	manifest, err := g.cat.GetManifest(context.Background(), g.name)
	if err != nil {
		t.Fatalf("%s: GetManifest: %v", what, err)
	}
	files := 0
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			files++
			cs, ok := f.ColumnStats["c_cidr"]
			if !ok {
				t.Fatalf("%s: %s carries no catalog stats for c_cidr — compaction stopped "+
					"recording them", what, f.Path)
			}
			minStr, okMin := cs.MinValue.(string)
			maxStr, okMax := cs.MaxValue.(string)
			if !okMin || !okMax {
				t.Fatalf("%s: %s catalog c_cidr bounds are %#v / %#v, want the winning rows' "+
					"address TEXT as plain strings", what, f.Path, cs.MinValue, cs.MaxValue)
			}
			if !utf8.ValidString(minStr) || !utf8.ValidString(maxStr) {
				t.Fatalf("%s: %s catalog c_cidr bounds are not valid UTF-8 (%q / %q) — the manifest's "+
					"JSON encoding will replace them with U+FFFD", what, f.Path, minStr, maxStr)
			}
			if minStr != cidrGateInetMin || maxStr != cidrGateInetMax {
				t.Errorf("%s: %s catalog c_cidr bounds = %q / %q, want %q / %q "+
					"(the fixture's true inet-order extremes, which are not row group 0's)",
					what, f.Path, minStr, maxStr, cidrGateInetMin, cidrGateInetMax)
			}
			// The manifest really is persisted as JSON — assert the round
			// trip, since that is the step the binary key did not survive.
			blob, err := json.Marshal(cs)
			if err != nil {
				t.Fatalf("%s: marshalling %s stats: %v", what, f.Path, err)
			}
			var back catalog.FileColumnStats
			if err := json.Unmarshal(blob, &back); err != nil {
				t.Fatalf("%s: unmarshalling %s stats: %v", what, f.Path, err)
			}
			if fmt.Sprint(back.MinValue) != minStr || fmt.Sprint(back.MaxValue) != maxStr {
				t.Errorf("%s: %s c_cidr bounds did not survive a JSON round trip: %q / %q became %q / %q",
					what, f.Path, minStr, maxStr, fmt.Sprint(back.MinValue), fmt.Sprint(back.MaxValue))
			}
		}
	}
	if files == 0 {
		t.Fatalf("%s: the manifest holds no files", what)
	}
}
