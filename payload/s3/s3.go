// Package s3 provides a content-addressed payload store for S3-compatible
// services including AWS S3, MinIO, and Cloudflare R2.
package s3

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/vestavision/trail/payload"
)

type Config struct {
	Endpoint, AccessKey, SecretKey, SessionToken string
	Bucket, Region, Prefix, Name                 string
	Secure, PathStyle                            bool
	TLS                                          *tls.Config
}
type Store struct {
	client               *minio.Client
	bucket, prefix, name string
}

func Open(cfg Config) (*Store, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("trail/payload/s3: endpoint and bucket are required")
	}
	transport, err := minio.DefaultTransport(true)
	if err != nil {
		return nil, err
	}
	if cfg.TLS != nil {
		transport.TLSClientConfig = cfg.TLS.Clone()
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken), Secure: cfg.Secure, Region: cfg.Region, Transport: transport, BucketLookup: bucketLookup(cfg.PathStyle)})
	if err != nil {
		return nil, err
	}
	return New(client, cfg.Bucket, cfg.Prefix, cfg.Name)
}
func New(client *minio.Client, bucket, prefix, name string) (*Store, error) {
	if client == nil || bucket == "" {
		return nil, errors.New("trail/payload/s3: client and bucket are required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix != "" {
		prefix += "/"
	}
	if name == "" {
		name = "s3"
	}
	return &Store{client: client, bucket: bucket, prefix: prefix, name: name}, nil
}
func (s *Store) EnsureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
}
func (s *Store) Put(ctx context.Context, reader io.Reader, opt payload.PutOptions) (payload.Ref, error) {
	if reader == nil {
		return payload.Ref{}, errors.New("trail/payload/s3: nil reader")
	}
	if opt.Compression != payload.CompressionNone && opt.Compression != payload.CompressionGZIP {
		return payload.Ref{}, fmt.Errorf("trail/payload/s3: unsupported compression %q", opt.Compression)
	}
	tmp, err := os.CreateTemp("", "trail-s3-payload-*")
	if err != nil {
		return payload.Ref{}, err
	}
	name := tmp.Name()
	defer func() { _ = tmp.Close(); _ = os.Remove(name) }()
	hash := sha256.New()
	source := io.TeeReader(&contextReader{ctx: ctx, r: reader}, hash)
	var size int64
	if opt.Compression == payload.CompressionGZIP {
		writer := gzip.NewWriter(tmp)
		size, err = io.Copy(writer, source)
		closeErr := writer.Close()
		if err == nil {
			err = closeErr
		}
	} else {
		size, err = io.Copy(tmp, source)
	}
	if err != nil {
		return payload.Ref{}, err
	}
	stored, err := tmp.Seek(0, io.SeekEnd)
	if err != nil {
		return payload.Ref{}, err
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return payload.Ref{}, err
	}
	var sum [32]byte
	copy(sum[:], hash.Sum(nil))
	key := s.prefix + fmt.Sprintf("%x", sum)
	if opt.Compression == payload.CompressionGZIP {
		key += ".gz"
	}
	_, err = s.client.PutObject(ctx, s.bucket, key, tmp, stored, minio.PutObjectOptions{ContentType: opt.ContentType, UserMetadata: map[string]string{"trail-sha256": fmt.Sprintf("%x", sum), "trail-size": fmt.Sprint(size), "trail-compression": string(opt.Compression)}})
	if err != nil {
		return payload.Ref{}, err
	}
	return payload.Ref{Store: s.name, Key: key, ContentType: opt.ContentType, Size: size, StoredSize: stored, SHA256: sum, Compression: opt.Compression, RetainUntil: opt.RetainUntil, RetentionClass: opt.RetentionClass}, nil
}
func (s *Store) Open(ctx context.Context, ref payload.Ref) (io.ReadCloser, error) {
	if err := s.validate(ref); err != nil {
		return nil, err
	}
	object, err := s.client.GetObject(ctx, s.bucket, ref.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapError(err)
	}
	if _, err = object.Stat(); err != nil {
		_ = object.Close()
		return nil, mapError(err)
	}
	return object, nil
}
func (s *Store) Delete(ctx context.Context, ref payload.Ref) error {
	if err := s.validate(ref); err != nil {
		return err
	}
	return mapError(s.client.RemoveObject(ctx, s.bucket, ref.Key, minio.RemoveObjectOptions{}))
}
func (s *Store) validate(ref payload.Ref) error {
	if ref.Store != s.name || ref.Key == "" || !strings.HasPrefix(ref.Key, s.prefix) || strings.Contains(ref.Key, "..") || strings.ContainsAny(strings.TrimPrefix(ref.Key, s.prefix), `\\`) {
		return payload.ErrInvalidRef
	}
	return nil
}
func mapError(err error) error {
	if err == nil {
		return nil
	}
	response := minio.ToErrorResponse(err)
	if response.StatusCode == http.StatusNotFound || response.Code == "NoSuchKey" || response.Code == "NoSuchBucket" {
		return payload.ErrNotFound
	}
	return err
}
func bucketLookup(path bool) minio.BucketLookupType {
	if path {
		return minio.BucketLookupPath
	}
	return minio.BucketLookupAuto
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

var _ payload.Store = (*Store)(nil)
