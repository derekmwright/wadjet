package coordinator

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

func TestWSHFShuffleProjection(t *testing.T) {
	base := func() physical.Stage {
		return physical.Stage{
			ID:      "exchange-repartition-3",
			Type:    physical.StageExchangeRepartition,
			Columns: []string{"c_custkey", "o_orderkey", "o_custkey"},
			Exchange: &physical.ExchangeStage{
				Keys: []string{"o_custkey"},
			},
		}
	}

	t.Run("wshf input gets declared columns plus keys", func(t *testing.T) {
		got := wshfShuffleProjection(base(), "queries/q1/scan-1/chunk_0.wshf", nil)
		want := []string{"c_custkey", "o_orderkey", "o_custkey"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("key not in columns is unioned in", func(t *testing.T) {
		s := base()
		s.Exchange.Keys = []string{"o_orderkey", "l_orderkey"}
		got := wshfShuffleProjection(s, "a.wshf", nil)
		want := []string{"c_custkey", "o_orderkey", "o_custkey", "l_orderkey"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("parquet input ineligible", func(t *testing.T) {
		if got := wshfShuffleProjection(base(), "tables/orders/chunk_0001.parquet", nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("pass-through scan columns own projection", func(t *testing.T) {
		if got := wshfShuffleProjection(base(), "a.wshf", []string{"o_custkey"}); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("no declared columns ineligible", func(t *testing.T) {
		s := base()
		s.Columns = nil
		if got := wshfShuffleProjection(s, "a.wshf", nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("computed cols ineligible", func(t *testing.T) {
		s := base()
		s.Exchange.ComputedCols = []physical.ComputedCol{{Name: "__flag", Expr: "x > 1"}}
		if got := wshfShuffleProjection(s, "a.wshf", nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("extra read cols ineligible", func(t *testing.T) {
		s := base()
		s.Exchange.ExtraReadCols = []string{"x"}
		if got := wshfShuffleProjection(s, "a.wshf", nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("kill switch", func(t *testing.T) {
		wshfShufflePrune = false
		defer func() { wshfShufflePrune = true }()
		if got := wshfShuffleProjection(base(), "a.wshf", nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
}

func TestShuffleTaskColumns(t *testing.T) {
	wshfStage := physical.Stage{
		ScanFiles:     []string{"queries/q1/scan-1/partition=0000/t1.wshf"},
		OutputColumns: []string{"o_custkey", "o_orderkey"},
	}
	t.Run("catalog pruning wins when present", func(t *testing.T) {
		got := shuffleTaskColumns([]string{"a", "b"}, wshfStage)
		if !reflect.DeepEqual(got, []string{"a", "b"}) {
			t.Errorf("got %v, want [a b]", got)
		}
	})
	t.Run("wshf source falls back to declared output", func(t *testing.T) {
		got := shuffleTaskColumns(nil, wshfStage)
		if !reflect.DeepEqual(got, wshfStage.OutputColumns) {
			t.Errorf("got %v, want %v", got, wshfStage.OutputColumns)
		}
	})
	t.Run("parquet source never uses OutputColumns", func(t *testing.T) {
		s := wshfStage
		s.ScanFiles = []string{"tables/orders/chunk_0001.parquet"}
		if got := shuffleTaskColumns(nil, s); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
	t.Run("no declaration passes through nil", func(t *testing.T) {
		s := wshfStage
		s.OutputColumns = nil
		if got := shuffleTaskColumns(nil, s); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
}
