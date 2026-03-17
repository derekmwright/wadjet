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
}

// MinIOStore implements Store using minio-go.
type MinIOStore struct {
	client *minio.Client
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
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    true, // Parquet/object data is already compressed
	}
}

// NewMinIOStore creates a new MinIO-backed object store with connection pooling.
func NewMinIOStore(cfg MinIOConfig) (*MinIOStore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:    cfg.UseSSL,
		Region:    cfg.Region,
		Transport: s3Transport(cfg.UseSSL),
	})
	if err != nil {
		return nil, fmt.Errorf("creating minio client: %w", err)
	}
	return &MinIOStore{client: client}, nil
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
	opts := minio.PutObjectOptions{ContentType: contentType}
	info, err := s.client.PutObject(ctx, bucket, key, r, size, opts)
	if err != nil {
		return "", fmt.Errorf("putting object: %w", err)
	}
	return info.ETag, nil
}

func (s *MinIOStore) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	opts := minio.PutObjectOptions{ContentType: contentType}
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
