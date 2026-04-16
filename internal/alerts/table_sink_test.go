package alerts

import (
	"context"
	"strings"
	"testing"
	"time"
)

type recordingExecutor struct {
	sqls []string
}

func (r *recordingExecutor) Execute(_ context.Context, sql string) error {
	r.sqls = append(r.sqls, sql)
	return nil
}
func (r *recordingExecutor) Query(context.Context, string, int) ([]map[string]any, []ColumnMeta, int64, bool, error) {
	return nil, nil, 0, false, nil
}

func TestTableSinkDeliver(t *testing.T) {
	ex := &recordingExecutor{}
	s := &TableSink{Executor: ex, Now: func() time.Time { return time.Unix(1700000000, 0).UTC() }}
	fire := AlertFire{
		AlertName:   "a",
		EvaluatedAt: time.Unix(1699999995, 0).UTC(),
		RowCount:    3,
		Rows:        []map[string]any{{"x": 1}},
		Truncated:   false,
	}
	if err := s.Deliver(context.Background(), fire); err != nil {
		t.Fatal(err)
	}
	if len(ex.sqls) != 1 {
		t.Fatalf("want 1 statement, got %d", len(ex.sqls))
	}
	got := ex.sqls[0]
	for _, want := range []string{"INSERT INTO alert_history", "'a'", "partition_date"} {
		if !strings.Contains(got, want) {
			t.Errorf("SQL missing %q: %s", want, got)
		}
	}
}
