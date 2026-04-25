package worker

import (
	"strings"
	"testing"
)

func TestClassifyInputFiles(t *testing.T) {
	cases := []struct {
		name    string
		files   []string
		want    inputFileKind
		wantErr bool
	}{
		{
			name:  "partitioned shuffle output",
			files: []string{"queries/q/s1/partition=0000/t.wshf", "queries/q/s1/partition=0001/t.wshf"},
			want:  inputKindPartitioned,
		},
		{
			name:  "flat wshf broadcast",
			files: []string{"queries/q/cache/build-orders.wshf"},
			want:  inputKindShuffleFlat,
		},
		{
			name:  "parquet table scan",
			files: []string{"tables/orders/part-0.parquet", "tables/orders/part-1.parquet"},
			want:  inputKindParquet,
		},
		{
			name:    "empty",
			files:   nil,
			wantErr: true,
		},
		{
			name:    "mixed partitioned and parquet",
			files:   []string{"partition=0000/a.wshf", "tables/b.parquet"},
			wantErr: true,
		},
		{
			name:    "mixed partitioned and flat wshf",
			files:   []string{"partition=0000/a.wshf", "cache/b.wshf"},
			wantErr: true,
		},
		{
			name:    "unknown extension",
			files:   []string{"data/foo.csv"},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := classifyInputFiles(c.files)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if !c.wantErr && got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestClassifyInputFilesErrorMessages(t *testing.T) {
	_, err := classifyInputFiles([]string{"partition=0/a.wshf", "cache/b.wshf"})
	if err == nil || !strings.Contains(err.Error(), "mixed file kinds") {
		t.Errorf("want 'mixed file kinds' in err, got %v", err)
	}
	_, err = classifyInputFiles([]string{"weird.xyz"})
	if err == nil || !strings.Contains(err.Error(), "unrecognized file pattern") {
		t.Errorf("want 'unrecognized file pattern' in err, got %v", err)
	}
}
