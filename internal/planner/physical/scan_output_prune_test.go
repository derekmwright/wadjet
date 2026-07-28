package physical

import (
	"reflect"
	"sort"
	"testing"
)

// The Q13 shape: a filtered scan whose read set includes the pushed
// filter column (o_comment), feeding an exchange that declares only the
// consumed columns. Output must narrow to the declared set; the read
// set must stay whole.
func TestPruneScanOutputColumns(t *testing.T) {
	stages := []Stage{
		{
			ID: "scan-1", Type: StageScan,
			ScanFiles:   []string{"orders/f1.parquet"},
			Columns:     []string{"o_comment", "o_custkey", "o_orderkey"},
			FilterExprs: []string{"o_comment not like '%special%requests%'"},
		},
		{
			ID: "exchange-repartition-3", Type: StageExchangeRepartition,
			Columns:      []string{"c_custkey", "o_orderkey", "o_custkey"},
			Exchange:     &ExchangeStage{Keys: []string{"o_custkey"}, Count: 24},
			Dependencies: []string{"scan-1"},
		},
	}
	pruneScanOutputColumns(stages)
	got := append([]string(nil), stages[0].OutputColumns...)
	sort.Strings(got)
	want := []string{"o_custkey", "o_orderkey"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OutputColumns = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(stages[0].Columns, []string{"o_comment", "o_custkey", "o_orderkey"}) {
		t.Fatalf("read set mutated: %v", stages[0].Columns)
	}

	// Ineligible consumers keep the full output.
	for _, mut := range []func(s *Stage){
		func(s *Stage) { s.Columns = nil },                                        // undeclared exchange columns
		func(s *Stage) { s.Type = StageAggregate },                                // non-exchange consumer
		func(s *Stage) { s.Exchange.ComputedCols = []ComputedCol{{Name: "f"}} },   // computed-col machinery
		func(s *Stage) { s.Exchange.ExtraReadCols = []string{"x"} },               // widened reads
	} {
		st := []Stage{stages[0], stages[1]}
		st[0].OutputColumns = nil
		st[1].Exchange = &ExchangeStage{Keys: []string{"o_custkey"}, Count: 24}
		st[1].Columns = []string{"c_custkey", "o_orderkey", "o_custkey"}
		st[1].Type = StageExchangeRepartition
		mut(&st[1])
		pruneScanOutputColumns(st)
		if st[0].OutputColumns != nil {
			t.Fatalf("ineligible consumer must keep full output, got %v", st[0].OutputColumns)
		}
	}

	// A second consumer widens the union: both consumers' columns survive.
	multi := []Stage{
		stages[0],
		stages[1],
		{
			ID: "exchange-repartition-9", Type: StageExchangeRepartition,
			Columns:      []string{"o_comment", "o_orderkey"},
			Exchange:     &ExchangeStage{Keys: []string{"o_orderkey"}, Count: 24},
			Dependencies: []string{"scan-1"},
		},
	}
	multi[0].OutputColumns = nil
	pruneScanOutputColumns(multi)
	got = append([]string(nil), multi[0].OutputColumns...)
	sort.Strings(got)
	// union covers everything the scan reads → no pruning win → stays nil
	if got != nil {
		t.Fatalf("full-coverage union must not prune, got %v", got)
	}
}
