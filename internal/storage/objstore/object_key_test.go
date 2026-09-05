package objstore

import (
	"bytes"
	"context"
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
	stores := map[string]Store{"FileStore": fs, "MemStore": mem}
	for _, s := range stores {
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
				// Not "skip": a store that loses GetReaderAt loses a checked
				// operation, and this gate must say so rather than pass.
				return nil
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
				if err := op.run(s, "data", key); err == nil {
					t.Errorf("%s.%s accepted the key %q", storeName, op.name, key)
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
			if _, err := s.List(ctx, bucket, ListOptions{}); err == nil {
				t.Errorf("%s.List accepted the bucket %q", storeName, bucket)
			}
			if _, err := s.BucketExists(ctx, bucket); err == nil {
				t.Errorf("%s.BucketExists accepted the bucket %q", storeName, bucket)
			}
			if err := s.MakeBucket(ctx, bucket); err == nil {
				t.Errorf("%s.MakeBucket accepted the bucket %q", storeName, bucket)
			}
		}
	}

	// The ordinary key still works on every operation, so the rule refuses
	// what it means to refuse and nothing else.
	const good = "tables/t/chunk_0.parquet"
	for storeName, s := range stores {
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
