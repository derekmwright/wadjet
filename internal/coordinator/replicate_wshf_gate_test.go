package coordinator

import "testing"

func TestAllWSHF(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  bool
	}{
		{"empty list", nil, false},
		{"single wshf", []string{"foo/bar/x.wshf"}, true},
		{"multiple wshf", []string{"a.wshf", "b.wshf", "c.wshf"}, true},
		{"single parquet", []string{"foo/bar/x.parquet"}, false},
		{"mixed wshf+parquet", []string{"a.wshf", "b.parquet"}, false},
		{"wshf with prefix path", []string{"queries/q1/scan-0/a.wshf", "queries/q1/scan-0/b.wshf"}, true},
		{"no extension", []string{"plain"}, false},
		{"wshf-named directory", []string{"foo.wshf/bar"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allWSHF(tt.files); got != tt.want {
				t.Errorf("allWSHF(%v) = %v, want %v", tt.files, got, tt.want)
			}
		})
	}
}
