// plan-repro (formerly ./tmpdiag): zero-EC2 SF100 plan repro. Restores
// the production catalog snapshot (ANALYZE stats included) into a MemKV
// and runs the exact coordinator planning path (WorkerCount=3,
// BroadcastBytesThreshold=200MiB) so PlanDistributed reproduces
// production stage shapes locally.
//
// Usage: eval "$(aws configure export-credentials --profile citc --format env)"
//        go run ./cmd/plan-repro -q 5
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

func main() {
	qNum := flag.Int("q", 5, "TPC-H query number")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx := context.Background()
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: "s3.us-east-2.amazonaws.com",
		UseSSL:   true,
		Region:   "us-east-2",
	})
	if err != nil {
		fatal("store: %v", err)
	}
	const bucket = "wadjet-bench-sf100-use2"
	cat := catalog.NewWithCluster(catalog.NewMemKV(), store, bucket, "local")
	ts, err := cat.Restore(ctx, catalog.RestoreOptions{
		SnapshotOptions: catalog.SnapshotOptions{Store: store, Bucket: bucket, Prefix: "catalog/"},
	})
	if err != nil {
		fatal("restore: %v", err)
	}
	fmt.Fprintf(os.Stderr, "catalog restored: %s\n", ts)

	sql := tpch.GetQuery(*qNum, 100).SQL
	parsed, err := plansql.Parse(sql)
	if err != nil {
		fatal("parse: %v", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		fatal("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		fatal("logical: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) {
		physical.NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := physical.NewPlanner(cat)
	planner.WorkerCount = 3
	planner.BroadcastBytesThreshold = 200 << 20
	planner.LateMaterialization = true
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		fatal("physical: %v", err)
	}
	for _, s := range stages {
		fmt.Printf("stage %-28s type=%-22s table=%-10s alias=%-6s est_rows=%-11d deps=%v\n",
			s.ID, s.Type, s.TableName, s.ScanAlias, s.EstimatedRows, s.Dependencies)
		if s.Type == physical.StageHashJoin || s.Type == physical.StageBroadcastJoin {
			fmt.Printf("  join type=%q L=%v R=%v ldep=%s rdep=%s\n",
				s.JoinType, s.JoinLeftKeys, s.JoinRightKeys, s.LeftDepStage, s.RightDepStage)
			for _, c := range s.ChainedJoins {
				fmt.Printf("  chained type=%q L=%v R=%v build=%s\n", c.JoinType, c.JoinLeftKeys, c.JoinRightKeys, c.BuildDepStage)
			}
			for _, f := range s.FusedJoins {
				fmt.Printf("  fused   type=%q L=%v R=%v build=%s\n", f.JoinType, f.JoinLeftKeys, f.JoinRightKeys, f.BuildDepStage)
			}
		}
		if len(s.FilterExprs) > 0 {
			fmt.Printf("  filters=%v\n", s.FilterExprs)
		}
		if s.Exchange != nil {
			fmt.Printf("  fusedExchange keys=%v count=%d\n", s.Exchange.Keys, s.Exchange.Count)
		}
		for _, e := range s.EmitDynamicFilters {
			fmt.Printf("  EMIT   id=%s key=%s atOutput=%t bloomBits=%d\n", e.FilterID, e.KeyColumn, e.AtOutput, e.BloomBits)
		}
		for _, cns := range s.ConsumeDynamicFilters {
			fmt.Printf("  CONSUME id=%s src=%s col=%s\n", cns.FilterID, cns.SourceStageID, cns.TargetColumn)
		}
		if len(s.ScanFiles) > 0 {
			fmt.Printf("  scanFiles=%d cols=%s\n", len(s.ScanFiles), strings.Join(s.Columns, ","))
		}
	}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
