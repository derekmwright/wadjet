package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// BenchProfile defines a complete benchmark configuration.
// Profiles are the single source of truth for benchmark runs — both
// tpch-bench (via --config) and Terraform (via yamldecode) read them.
type BenchProfile struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`

	Storage struct {
		Bucket     string `yaml:"bucket"`
		Region     string `yaml:"region"`
		DataPrefix string `yaml:"data_prefix"`
	} `yaml:"storage"`

	Cluster struct {
		Mode                string `yaml:"mode"`
		Workers             int    `yaml:"workers"`
		CoordinatorInstance string `yaml:"coordinator_instance"`
		WorkerInstance      string `yaml:"worker_instance"`
		Arch                string `yaml:"arch"`
		UseSpot             bool   `yaml:"use_spot"`
	} `yaml:"cluster"`

	Benchmark struct {
		ScaleFactor  int    `yaml:"scale_factor"`
		Runs         int    `yaml:"runs"`
		QueryTimeout string `yaml:"query_timeout"`
		SkipLoad     bool   `yaml:"skip_load"`
		SkipQueries  []int  `yaml:"skip_queries"`
		// CatalogSnapshotPrefix enables catalog snapshot/restore under
		// s3://<bucket>/<prefix> (e.g. "catalog/"). When set with
		// skip_load, boot first tries to restore the latest snapshot —
		// skipping the ~5 min discovery + ~10 min ANALYZE entirely — and
		// after a fresh discovery+ANALYZE it writes a new snapshot for the
		// next deploy. Only safe while the bucket's table data is immutable
		// across deploys (true for all pre-staged bench buckets); delete
		// s3://<bucket>/<prefix> after changing the data.
		CatalogSnapshotPrefix string `yaml:"catalog_snapshot_prefix"`
	} `yaml:"benchmark"`
}

func loadProfile(path string) (*BenchProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading profile %s: %w", path, err)
	}
	var p BenchProfile
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parsing profile %s: %w", path, err)
	}
	return &p, nil
}

// s3Endpoint derives the S3 endpoint from a region.
func (p *BenchProfile) s3Endpoint() string {
	if p.Storage.Region == "" {
		return ""
	}
	return "s3." + p.Storage.Region + ".amazonaws.com"
}
