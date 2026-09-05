package objstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The keys no store may accept. Every one of them was a working write into
// FileStore before ValidateObjectKey existed, and `tables/../../../tmp/x/…` is
// the one `CREATE TABLE "../../../tmp/x"` produced (CodeQL #23/#24/#25).
var refusedObjectKeys = []string{
	"",
	"tables/../../../tmp/x/chunk.parquet",
	"../escape",
	"..",
	".",
	"tables/./x",
	"tables//x",
	"/etc/passwd",
	"tables/x\x00.parquet",
	`tables\x`,
	"tables/../x",
}

var acceptedObjectKeys = []string{
	"tables/lineitem/chunk_abc.parquet",
	"queries/q1/stage-2/part.parquet",
	"single",
	"a/b/c",
	"tables/..hidden/x", // ".." inside a component is not a component
	"tables/x..y/z",     // nor is it a traversal
	"tables/t/.hidden.parquet",
}

func TestNoStoreAcceptsAKeyThatCanLeaveItsBucket(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fs, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	mem := NewMemStore()
	for _, s := range []Store{fs, mem} {
		if err := s.MakeBucket(ctx, "data"); err != nil {
			t.Fatal(err)
		}
	}

	for _, key := range refusedObjectKeys {
		for name, s := range map[string]Store{"FileStore": fs, "MemStore": mem} {
			if _, err := s.Put(ctx, "data", key, bytes.NewReader([]byte("x")), 1, "application/octet-stream"); err == nil {
				t.Errorf("%s accepted the key %q", name, key)
			}
		}
		// FileStore's rule is applied at objectPath, so every operation
		// refuses, not only the write.
		if _, _, err := fs.Get(ctx, "data", key); err == nil {
			t.Errorf("FileStore.Get accepted the key %q", key)
		}
		if _, err := fs.Head(ctx, "data", key); err == nil {
			t.Errorf("FileStore.Head accepted the key %q", key)
		}
		if err := fs.Delete(ctx, "data", key); err == nil {
			t.Errorf("FileStore.Delete accepted the key %q", key)
		}
	}

	for _, key := range acceptedObjectKeys {
		for name, s := range map[string]Store{"FileStore": fs, "MemStore": mem} {
			if _, err := s.Put(ctx, "data", key, bytes.NewReader([]byte("x")), 1, "application/octet-stream"); err != nil {
				t.Errorf("%s refused the ordinary key %q: %v", name, key, err)
			}
		}
	}

	// And nothing landed outside the bucket directory.
	var strays []string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(filepath.Join(root, "data"), p)
		if rerr != nil || strings.HasPrefix(rel, "..") {
			strays = append(strays, p)
		}
		return nil
	})
	if len(strays) > 0 {
		t.Errorf("files were written outside the bucket: %v", strays)
	}
}

// A bucket is one path component too — FileStore turns it into a directory
// directly under the root.
func TestFileStoreRefusesABucketThatIsNotOneComponent(t *testing.T) {
	fs, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, bucket := range []string{"", "..", "a/b", `a\b`, "a\x00b"} {
		if err := fs.MakeBucket(ctx, bucket); err == nil {
			t.Errorf("MakeBucket accepted %q", bucket)
		}
		if _, err := fs.Put(ctx, bucket, "k", bytes.NewReader([]byte("x")), 1, ""); err == nil {
			t.Errorf("Put accepted the bucket %q", bucket)
		}
	}
}

// The direct proof of the escape, on the exact key the SQL door used to
// produce: after the write is refused, the target must not exist.
func TestATableNamedWithATraversalCannotWriteOutsideTheStore(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "..", "s1-escaped.parquet")
	fs, err := NewFileStore(filepath.Join(root, "store"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := fs.MakeBucket(ctx, "data"); err != nil {
		t.Fatal(err)
	}
	// `CREATE TABLE "../../s1-escaped.parquet"` produces this key through
	// partition.TablePrefix.
	key := "tables/../../../s1-escaped.parquet/chunk_0.parquet"
	if _, err := fs.Put(ctx, "data", key, bytes.NewReader([]byte("owned")), 5, ""); err == nil {
		t.Fatal("the escaping key was accepted")
	}
	if _, err := os.Stat(outside); err == nil {
		t.Fatalf("a file was written outside the store root at %s", outside)
	}
}

// The rule is applied on EVERY operation, by every store — not only on the
// write. FileStore validated all six through objectPath; MemStore and the S3
// store validated Put and PutIfMatch only, so `Get("../escape")` was a key
// error on one store and "object not found" on the others. ADR-0012 says the
// rule is applied by all three "alike"; a rule that answers differently per
// store is how a table that works in a test fails in production (round-1
// review P3).
func TestEveryStoreRefusesABadKeyOnEveryOperation(t *testing.T) {
	ctx := context.Background()
	fs, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mem := NewMemStore()
	// The S3 store too, pointed at a dead endpoint. It needs no server: the key
	// rule runs BEFORE the first network call, so a KEY error rather than a
	// dial error is itself the assertion that the guard is in front. Without
	// this arm nothing held the third store to ADR-0012's "every store, every
	// operation" (round-2 review P1).
	s3, err := NewMinIOStore(MinIOConfig{Endpoint: "127.0.0.1:1", AccessKey: "x", SecretKey: "y"})
	if err != nil {
		t.Fatal(err)
	}
	stores := map[string]Store{"FileStore": fs, "MemStore": mem, "S3": s3}
	for name, s := range stores {
		if name == "S3" {
			continue // no server; only the refusals below are asked of it
		}
		if err := s.MakeBucket(ctx, "data"); err != nil {
			t.Fatal(err)
		}
	}

	ops := []struct {
		name string
		run  func(Store, string, string) error
	}{
		{"Put", func(s Store, b, k string) error {
			_, err := s.Put(ctx, b, k, bytes.NewReader([]byte("x")), 1, "")
			return err
		}},
		{"PutIfMatch", func(s Store, b, k string) error {
			_, err := s.PutIfMatch(ctx, b, k, bytes.NewReader([]byte("x")), 1, "", "")
			return err
		}},
		{"Get", func(s Store, b, k string) error {
			rc, _, err := s.Get(ctx, b, k)
			if rc != nil {
				rc.Close()
			}
			return err
		}},
		{"GetReaderAt", func(s Store, b, k string) error {
			ra, ok := s.(interface {
				GetReaderAt(context.Context, string, string) (ReaderAtCloser, int64, error)
			})
			if !ok {
				// Not "skip" and NOT nil: a store that loses GetReaderAt loses
				// a checked operation, and this gate has to say so. Returning
				// nil read as "refused" and hid it (round-2 review N1).
				return fmt.Errorf("store %T has no GetReaderAt, so this gate no longer checks that operation", s)
			}
			rc, _, err := ra.GetReaderAt(ctx, b, k)
			if rc != nil {
				rc.Close()
			}
			return err
		}},
		{"Head", func(s Store, b, k string) error {
			_, err := s.Head(ctx, b, k)
			return err
		}},
		{"Delete", func(s Store, b, k string) error { return s.Delete(ctx, b, k) }},
	}

	for _, key := range refusedObjectKeys {
		for storeName, s := range stores {
			for _, op := range ops {
				err := op.run(s, "data", key)
				// NOT merely "some error". A store where a missing object
				// already errors satisfies `err != nil` with its own "object
				// not found", so this gate passed with three of MemStore's six
				// key checks removed — it could not see the rule disappearing
				// from an entire read path (round-2 review B2). The error has
				// to BE the key refusal.
				if !isKeyRefusal(err) {
					t.Errorf("%s.%s did not REFUSE the key %q: %v", storeName, op.name, key, err)
				}
			}
		}
	}

	// And the BUCKET, on the operations that take one without a key. List and
	// BucketExists reached bucketDir with no check at all, so
	// `List(ctx, "../..")` WALKED outside the store root and returned what it
	// found there (round-1 review P4).
	for _, bucket := range []string{"", "..", "../..", "a/b", `a\b`, "a\x00b"} {
		for storeName, s := range stores {
			if _, err := s.List(ctx, bucket, ListOptions{}); !isBucketRefusal(err) {
				t.Errorf("%s.List did not REFUSE the bucket %q: %v", storeName, bucket, err)
			}
			if _, err := s.BucketExists(ctx, bucket); !isBucketRefusal(err) {
				t.Errorf("%s.BucketExists did not REFUSE the bucket %q: %v", storeName, bucket, err)
			}
			if err := s.MakeBucket(ctx, bucket); !isBucketRefusal(err) {
				t.Errorf("%s.MakeBucket did not REFUSE the bucket %q: %v", storeName, bucket, err)
			}
		}
	}

	// The ordinary key still works on every operation, so the rule refuses
	// what it means to refuse and nothing else.
	const good = "tables/t/chunk_0.parquet"
	for storeName, s := range stores {
		if storeName == "S3" {
			// No server behind this one, so an ordinary key reaches the
			// network and fails there. What this asserts is that it does NOT
			// fail as a key refusal: the guard let it past.
			if err := s.Delete(ctx, "data", good); isKeyRefusal(err) {
				t.Errorf("S3 refused an ordinary key as a key error: %v", err)
			}
			continue
		}
		if _, err := s.Put(ctx, "data", good, bytes.NewReader([]byte("x")), 1, ""); err != nil {
			t.Fatalf("%s.Put refused an ordinary key: %v", storeName, err)
		}
		for _, op := range ops[2:] {
			if err := op.run(s, "data", good); err != nil {
				t.Errorf("%s.%s refused an ordinary key: %v", storeName, op.name, err)
			}
		}
	}
}

// isKeyRefusal reports whether err is ValidateObjectKey's refusal rather than
// some other failure — a missing object, a dial error, a closed file. Matching
// the phrase every one of those messages opens with is what lets the gate tell
// "the RULE refused this" from "something else went wrong", and that difference
// is the whole gate: without it, removing MemStore's key check from Get, Head
// and GetReaderAt left "object not found" behind and the gate still passed.
func isKeyRefusal(err error) bool {
	return err != nil && strings.Contains(err.Error(), "object key")
}

func isBucketRefusal(err error) bool {
	return err != nil && strings.Contains(err.Error(), "bucket name")
}
