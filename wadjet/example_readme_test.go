package wadjet_test

import (
	"context"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// ExampleOpen is the README's "Start embedded, scale distributed" snippet,
// compiled. It has no Output comment on purpose: `go test` builds an example
// without one but never runs it, which is all this needs to be — a compile
// gate so the three lines a reader copies cannot drift from the API the way
// the pre-2026-09-02 README example did (it named a Config field and a
// constructor arity that no longer existed).
func ExampleOpen() {
	ctx := context.Background()

	store, _ := objstore.NewFileStore("/var/lib/wadjet") // or NewMinIOStore(...) for S3
	db, _ := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "analytics"})
	res, _ := db.Query(ctx, "SELECT src_ip, SUM(bytes_in) FROM flow_logs GROUP BY 1")

	_ = res
	defer db.Close()
}
