package filesystem

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/vestavision/trail/payload"
)

func TestStoreRoundTrip(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("a payload large enough to compress")
	ref, err := store.Put(context.Background(), bytes.NewReader(content), payload.PutOptions{ContentType: "text/plain", Compression: payload.CompressionGZIP, RetentionClass: "short"})
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(content)
	if ref.SHA256 != wantHash || ref.Size != int64(len(content)) || ref.StoredSize == 0 || ref.RetentionClass != "short" {
		t.Fatalf("ref=%+v", ref)
	}
	reader, err := store.Open(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(decoder)
	if err != nil || !bytes.Equal(decoded, content) {
		t.Fatalf("decoded=%q err=%v", decoded, err)
	}
	if len(ref.Fields("request")) < 6 {
		t.Fatal("payload fields missing")
	}
	if err := store.Delete(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(context.Background(), ref); !errors.Is(err, payload.ErrNotFound) {
		t.Fatalf("open deleted=%v", err)
	}
}

func TestRejectsInvalidReferenceAndCancellation(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(context.Background(), payload.Ref{Store: "filesystem", Key: "../secret"}); !errors.Is(err, payload.ErrInvalidRef) {
		t.Fatalf("path error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Put(ctx, bytes.NewReader([]byte("data")), payload.PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}
