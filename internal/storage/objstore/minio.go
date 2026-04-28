package objstore

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOConfig holds configuration for connecting to a MinIO/S3 endpoint.
type MinIOConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string

	// MaxConcurrentUploads bounds the number of in-flight PUT operations
	// originating from a single MinIOStore instance. Defaults to 4 when
	// zero. Each PUT for a >16MB object triggers minio-go's multipart
	// uploader which itself opens multiple TCP connections per object
	// (UploadThreads, default 2 here). With multiple worker processes
	// on the same host, the aggregate connection count needs a ceiling
	// so individual uploads aren't starved of bandwidth.
	MaxConcurrentUploads int

	// UploadThreads is the per-PUT multipart concurrency for objects
	// large enough to trigger multipart upload (>16MB). Defaults to 2.
	// Lower values reduce per-upload connection count; higher values
	// speed up individual uploads when bandwidth is plentiful. With 4
	// processes per node and MaxConcurrentUploads=4, this caps host
	// connection count at 4×4×2 = 32.
	UploadThreads int
}

// MinIOStore implements Store using minio-go. Includes a per-instance
// upload semaphore that bounds aggregate PUT concurrency so simultaneous
// uploads from a single process don't fragment available bandwidth.
type MinIOStore struct {
	client        *minio.Client
	uploadSem     chan struct{}
	uploadThreads uint
}

// s3Transport returns an http.Transport tuned for high-throughput S3 access.
// Connection pooling avoids TCP/TLS handshake overhead on repeated requests
// to the same endpoint (typical for scan-heavy query workloads).
func s3Transport(secure bool) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12}, //nolint:gosec // TLS always verified
		ExpectContinueTimeout: 1 * time.Second,
		// ResponseHeaderTimeout fires when the server hasn't sent response
		// headers by the deadline. For PUT, the server only sends headers
		// AFTER the entire body upload completes, so this is effectively a
		// per-upload wall-clock limit. With multiple worker processes per
		// host contending for bandwidth, a 1-2GB shuffle partition can
		// take >2min just to upload. 30min gives enough headroom for
		// SF100-class shuffles under heavy contention; the per-task
		// context still bounds total wall time.
		//
		// Observed in SF10 67062a5 deploy: 12 worker processes uploading
		// shuffle partitions concurrently → 2m36s PUT failed at the
		// previous 120s threshold despite the upload being legitimately
		// in flight.
		ResponseHeaderTimeout: 30 * time.Minute,
		DisableCompression:    true, // Parquet/object data is already compressed
	}
}

// NewMinIOStore creates a new MinIO-backed object store with connection pooling.
// When AccessKey and SecretKey are both empty, credentials are auto-detected
// from environment variables (AWS_ACCESS_KEY_ID) and IAM instance profiles.
func NewMinIOStore(cfg MinIOConfig) (*MinIOStore, error) {
	var creds *credentials.Credentials
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		creds = credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	} else {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{
				Client: &http.Client{Timeout: 10 * time.Second},
			},
		})
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        creds,
		Secure:       cfg.UseSSL,
		Region:       cfg.Region,
		Transport:    s3Transport(cfg.UseSSL),
		BucketLookup: minio.BucketLookupDNS,
	})
	if err != nil {
		return nil, fmt.Errorf("creating minio client: %w", err)
	}
	maxUploads := cfg.MaxConcurrentUploads
	if maxUploads <= 0 {
		maxUploads = 4
	}
	uploadThreads := cfg.UploadThreads
	if uploadThreads <= 0 {
		uploadThreads = 2
	}
	return &MinIOStore{
		client:        client,
		uploadSem:     make(chan struct{}, maxUploads),
		uploadThreads: uint(uploadThreads),
	}, nil
}

// acquireUpload blocks until an upload slot is available or ctx is
// cancelled. Returns a release func that the caller MUST defer to
// return the slot. Bounds aggregate PUT concurrency from this MinIOStore
// instance so simultaneous uploads don't all fight for the same EC2
// outbound bandwidth at low individual throughput.
func (s *MinIOStore) acquireUpload(ctx context.Context) (func(), error) {
	if s.uploadSem == nil {
		return func() {}, nil
	}
	select {
	case s.uploadSem <- struct{}{}:
		return func() { <-s.uploadSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *MinIOStore) MakeBucket(ctx context.Context, bucket string) error {
	exists, err := s.client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("checking bucket: %w", err)
	}
	if exists {
		return nil
	}
	return s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
}

func (s *MinIOStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return s.client.BucketExists(ctx, bucket)
}

func (s *MinIOStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	release, err := s.acquireUpload(ctx)
	if err != nil {
		return "", fmt.Errorf("acquiring upload slot: %w", err)
	}
	defer release()
	opts := minio.PutObjectOptions{
		ContentType: contentType,
		NumThreads:  s.uploadThreads,
	}
	info, err := s.client.PutObject(ctx, bucket, key, r, size, opts)
	if err != nil {
		return "", fmt.Errorf("putting object: %w", err)
	}
	return info.ETag, nil
}

func (s *MinIOStore) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	release, err := s.acquireUpload(ctx)
	if err != nil {
		return "", fmt.Errorf("acquiring upload slot: %w", err)
	}
	defer release()
	opts := minio.PutObjectOptions{
		ContentType: contentType,
		NumThreads:  s.uploadThreads,
	}
	if expectedETag == "" {
		// Create-only: use If-None-Match: *
		opts.Internal = minio.AdvancedPutOptions{
			SourceETag: "",
		}
	}
	// Note: MinIO doesn't natively support conditional PUTs with ETags in all cases.
	// We implement optimistic concurrency by checking first, then writing.
	// This has a race window but is acceptable for catalog updates at our scale.
	if expectedETag != "" {
		info, err := s.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if err != nil {
			return "", ErrPreconditionFailed
		}
		if info.ETag != expectedETag {
			return "", ErrPreconditionFailed
		}
	} else {
		_, err := s.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if err == nil {
			return "", ErrPreconditionFailed
		}
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode != http.StatusNotFound {
			return "", fmt.Errorf("checking object existence: %w", err)
		}
	}

	result, err := s.client.PutObject(ctx, bucket, key, r, size, opts)
	if err != nil {
		return "", fmt.Errorf("putting object: %w", err)
	}
	return result.ETag, nil
}

func (s *MinIOStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, ObjectInfo{}, fmt.Errorf("getting object: %w", err)
	}

	info, err := obj.Stat()
	if err != nil {
		obj.Close()
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return nil, ObjectInfo{}, ErrNotFound
		}
		return nil, ObjectInfo{}, fmt.Errorf("stat object: %w", err)
	}

	return obj, ObjectInfo{
		Key:          info.Key,
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		ContentType:  info.ContentType,
	}, nil
}

// minioReaderAt wraps a *minio.Object as ReaderAtCloser.
// minio.Object already implements io.ReaderAt via range requests.
type minioReaderAt struct {
	obj *minio.Object
}

func (m *minioReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return m.obj.ReadAt(p, off)
}

func (m *minioReaderAt) Close() error {
	return m.obj.Close()
}

func (s *MinIOStore) GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("getting object: %w", err)
	}

	info, err := obj.Stat()
	if err != nil {
		obj.Close()
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("stat object: %w", err)
	}

	return &minioReaderAt{obj: obj}, info.Size, nil
}

func (s *MinIOStore) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	info, err := s.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return ObjectInfo{}, ErrNotFound
		}
		return ObjectInfo{}, fmt.Errorf("stat object: %w", err)
	}

	return ObjectInfo{
		Key:          info.Key,
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		ContentType:  info.ContentType,
	}, nil
}

func (s *MinIOStore) List(ctx context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	listOpts := minio.ListObjectsOptions{
		Prefix:    opts.Prefix,
		Recursive: opts.Delimiter == "",
	}

	var result []ObjectInfo
	for obj := range s.client.ListObjects(ctx, bucket, listOpts) {
		if obj.Err != nil {
			return nil, fmt.Errorf("listing objects: %w", obj.Err)
		}
		result = append(result, ObjectInfo{
			Key:          obj.Key,
			Size:         obj.Size,
			ETag:         obj.ETag,
			LastModified: obj.LastModified,
			ContentType:  obj.ContentType,
		})
		if opts.MaxKeys > 0 && len(result) >= opts.MaxKeys {
			break
		}
	}

	return result, nil
}

func (s *MinIOStore) Delete(ctx context.Context, bucket, key string) error {
	err := s.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("deleting object: %w", err)
	}
	return nil
}
