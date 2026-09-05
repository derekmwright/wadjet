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
