package wadjet

import (
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The out-of-tree embedding surface supports stores, schemas, ingestion and
// queries used by docs/getting-started.md (#805).
// Public types alias their internal types, without wrappers or conversions;
// store constructors return Config.Store's interface without callers naming it.
// Config.MetaKV and Config.AuthProvider still require internal types without
// public constructors; persistent catalog and in-process ABAC remain
// in-repo-only (docs/embedding.md).
// TestTheGuidesProgramBuildsAndRunsOutOfTree builds/runs test/embed's separate
// module importing only github.com/derekmwright/wadjet/wadjet.
// See docs/internals/public-embedding-surface-boundary.md for the design.

// Schema is a table's column list, as CreateTable and NewIngester take it.
type Schema = parquet.Schema

// Column is one column of a Schema.
//
// Precision and Scale carry a DECIMAL's declaration; Dimension a VECTOR's;
// ElementType an ARRAY's element; Fields a ROW's or MAP's children. A column
// of any other type leaves them zero.
type Column = parquet.Column

// ColumnType is a Column's type. The TypeXxx constants below are its values.
type ColumnType = parquet.TypeID

// The 22 column types. These are the values ColumnType takes; a schema cannot
// be written without them, which is why the whole set is here rather than the
// six the guide's program happens to use.
const (
	TypeBool      = parquet.TypeBool
	TypeInt32     = parquet.TypeInt32
	TypeInt64     = parquet.TypeInt64
	TypeFloat32   = parquet.TypeFloat32
	TypeFloat64   = parquet.TypeFloat64
	TypeString    = parquet.TypeString
	TypeBytes     = parquet.TypeBytes
	TypeTimestamp = parquet.TypeTimestamp
	TypeIPv4      = parquet.TypeIPv4
	TypeIPv6      = parquet.TypeIPv6
	TypeCIDR      = parquet.TypeCIDR
	TypeMAC       = parquet.TypeMAC
	TypePort      = parquet.TypePort
	TypeProtocol  = parquet.TypeProtocol
	TypeDuration  = parquet.TypeDuration
	TypeUUID      = parquet.TypeUUID
	TypeDate      = parquet.TypeDate
	TypeDecimal   = parquet.TypeDecimal
	TypeArray     = parquet.TypeArray
	TypeRow       = parquet.TypeRow
	TypeMap       = parquet.TypeMap
	TypeVector    = parquet.TypeVector
)

// IngestConfig is the flush policy NewIngester takes. A zero value means the
// defaults: 128 MB, 1M rows, 60s, 128K rows per row group.
type IngestConfig = ingest.Config

// DefaultIngestConfig returns those defaults spelled out, for a caller that
// wants to change one of them.
func DefaultIngestConfig() IngestConfig { return ingest.DefaultConfig() }

// NewMemStore returns an in-memory object store. Everything written to it dies
// with the process; it is what the test suites in this repository use, and
// what a program should use to try the API out.
func NewMemStore() objstore.Store { return objstore.NewMemStore() }

// NewFileStore returns an object store backed by a directory on local disk.
// The directory is created if it does not exist.
func NewFileStore(dir string) (objstore.Store, error) {
	return objstore.NewFileStore(dir)
}

// S3Config addresses an S3-compatible object store — AWS S3, MinIO,
// Cloudflare R2, or any other implementation of the same API.
//
// AccessKey and SecretKey may both be empty, in which case credentials come
// from the environment, the shared AWS credentials file, or the instance's IAM
// role, in that order.
type S3Config struct {
	Endpoint  string // host:port, no scheme — e.g. "s3.us-east-2.amazonaws.com"
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string
	// MaxConcurrentUploads bounds in-flight PUTs from this store (default 4).
	MaxConcurrentUploads int
}

// NewS3Store returns an object store speaking the S3 API.
func NewS3Store(cfg S3Config) (objstore.Store, error) {
	return objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint:             cfg.Endpoint,
		AccessKey:            cfg.AccessKey,
		SecretKey:            cfg.SecretKey,
		UseSSL:               cfg.UseSSL,
		Region:               cfg.Region,
		MaxConcurrentUploads: cfg.MaxConcurrentUploads,
	})
}
