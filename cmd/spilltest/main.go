package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	wadjet "github.com/derekmwright/wadjet/wadjet"
)

func main() {
	ctx := context.Background()
	store := objstore.NewMemStore()

	budget := int64(128 * 1024 * 1024) // 128 MB to force aggressive spilling
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "nolimit":
			budget = 0
		case "256":
			budget = 256 * 1024 * 1024
		case "512":
			budget = 512 * 1024 * 1024
		}
	}

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:        store,
		Bucket:       "tpch",
		MemoryBudget: budget,
	})
	if err != nil {
		log.Fatal(err)
	}

	for tableName, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			log.Fatalf("creating table %s: %v", tableName, err)
		}
	}

	ingesters := make(map[string]*ingest.Ingester)
	for tableName, schema := range tpch.AllTables {
		ingesters[tableName] = db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: 100_000,
			RowGroupSize:  65_536,
		})
	}

	sf := tpch.ScaleFactor(1)
	fmt.Printf("Generating TPC-H SF1 data...\n")
	tpch.GenerateChunked(sf, 100_000, func(table string, rows []map[string]any) error {
		ing := ingesters[table]
		if ing == nil {
			return nil
		}
		return ing.Ingest(ctx, rows)
	})
	for _, ing := range ingesters {
		ing.FlushAll(ctx)
	}

	// Run the failing queries
	for _, qNum := range []int{5, 11, 18} {
		q := tpch.GetQuery(qNum, sf)
		result, err := db.Query(ctx, q.SQL)
		if err != nil {
			fmt.Printf("Q%02d %s: ERROR: %v\n", qNum, q.Name, err)
			continue
		}
		fmt.Printf("Q%02d %s: %d rows (budget=%dMB)\n", qNum, q.Name, len(result.Rows), budget/1024/1024)
	}
}
