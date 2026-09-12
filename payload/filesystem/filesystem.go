// Package filesystem provides a content-addressed development payload store.
package filesystem

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/vestavision/trail/payload"
)

type Store struct {
	root string
	name string
}

func New(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("trail/payload/filesystem: root is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absolute, 0o750); err != nil {
		return nil, err
	}
	return &Store{root: absolute, name: "filesystem"}, nil
}

func (s *Store) Put(ctx context.Context, reader io.Reader, options payload.PutOptions) (payload.Ref, error) {
	if reader == nil {
		return payload.Ref{}, errors.New("trail/payload/filesystem: nil reader")
	}
	if options.Compression != payload.CompressionNone && options.Compression != payload.CompressionGZIP {
		return payload.Ref{}, fmt.Errorf("trail/payload/filesystem: unsupported compression %q", options.Compression)
	}
	temporary, err := os.CreateTemp(s.root, ".trail-payload-*")
	if err != nil {
		return payload.Ref{}, err
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()

	hash := sha256.New()
	source := io.TeeReader(&contextReader{ctx: ctx, reader: reader}, hash)
	var size int64
	if options.Compression == payload.CompressionGZIP {
		compressed := gzip.NewWriter(temporary)
		size, err = io.Copy(compressed, source)
		closeErr := compressed.Close()
		if err == nil {
			err = closeErr
		}
	} else {
		size, err = io.Copy(temporary, source)
	}
	if err != nil {
		return payload.Ref{}, err
	}
	if err := temporary.Sync(); err != nil {
		return payload.Ref{}, err
	}
	info, err := temporary.Stat()
	if err != nil {
		return payload.Ref{}, err
	}
	if err := temporary.Close(); err != nil {
		return payload.Ref{}, err
	}

	var checksum [32]byte
	copy(checksum[:], hash.Sum(nil))
	key := fmt.Sprintf("%x", checksum)
	if options.Compression == payload.CompressionGZIP {
		key += ".gz"
	}
	destination := filepath.Join(s.root, key)
	if err := os.Rename(temporaryName, destination); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return payload.Ref{}, err
		}
	}
	keep = true
	return payload.Ref{
		Store: s.name, Key: key, ContentType: options.ContentType, Size: size, StoredSize: info.Size(),
		SHA256: checksum, Compression: options.Compression, RetainUntil: options.RetainUntil, RetentionClass: options.RetentionClass,
	}, nil
}

func (s *Store) Open(ctx context.Context, ref payload.Ref) (io.ReadCloser, error) {
	path, err := s.path(ref)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, payload.ErrNotFound
	}
	return file, err
}

func (s *Store) Delete(ctx context.Context, ref payload.Ref) error {
	path, err := s.path(ref)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return payload.ErrNotFound
	}
	return err
}

func (s *Store) path(ref payload.Ref) (string, error) {
	if ref.Store != s.name || ref.Key == "" || filepath.Base(ref.Key) != ref.Key || strings.ContainsAny(ref.Key, `/\\`) {
		return "", payload.ErrInvalidRef
	}
	return filepath.Join(s.root, ref.Key), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}
