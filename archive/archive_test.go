package archive

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/payload/filesystem"
	"github.com/vestavision/trail/storage"
)

type restoreStore struct {
	events map[trail.EventID]storage.EventRecord
}

func (s *restoreStore) RestoreArchiveEvents(_ context.Context, _ string, events []storage.EventRecord) (int, error) {
	if s.events == nil {
		s.events = map[trail.EventID]storage.EventRecord{}
	}
	for _, event := range events {
		if _, exists := s.events[event.ID]; !exists {
			s.events[event.ID] = event
		}
	}
	return len(events), nil
}

func TestBundleRoundTripAndIdempotentRestore(t *testing.T) {
	ctx := context.Background()
	source, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	archiveStore, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := source.Put(ctx, strings.NewReader("large response"), payload.PutOptions{ContentType: "application/xml", Compression: payload.CompressionGZIP, RetentionClass: "http"})
	if err != nil {
		t.Fatal(err)
	}
	id1, _ := trail.ParseEventID("00000000000000000000000001")
	id2, _ := trail.ParseEventID("00000000000000000000000002")
	events := []storage.EventRecord{
		{ID: id1, Timestamp: time.Unix(1, 0).UTC(), Kind: "provider.http", Payloads: []storage.PayloadLink{{Role: "response", Ref: ref}}},
		{ID: id2, Timestamp: time.Unix(2, 0).UTC(), Kind: "payment.completed"},
	}
	w, err := NewWriter(archiveStore, map[string]payload.Store{"filesystem": source})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := w.Write(ctx, events)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := ReadManifest(ctx, archiveStore, receipt.ManifestRef)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.EventCount != 2 || len(manifest.Payloads) != 1 {
		t.Fatalf("manifest=%+v", manifest)
	}
	page, complete, err := ReadEvents(ctx, archiveStore, manifest, 0, 1)
	if err != nil || complete || len(page) != 1 || page[0].ID != id1 {
		t.Fatalf("page=%+v complete=%v err=%v", page, complete, err)
	}
	restoredPayloads, _ := filesystem.New(t.TempDir())
	target := &restoreStore{}
	r1, err := Restore(ctx, archiveStore, map[string]payload.Store{"filesystem": restoredPayloads}, target, receipt, 0, 1)
	if err != nil || r1.Complete || r1.Next != 1 {
		t.Fatalf("first restore=%+v err=%v", r1, err)
	}
	r2, err := Restore(ctx, archiveStore, map[string]payload.Store{"filesystem": restoredPayloads}, target, receipt, r1.Next, 10)
	if err != nil || !r2.Complete || len(target.events) != 2 {
		t.Fatalf("second restore=%+v events=%d err=%v", r2, len(target.events), err)
	}
	if _, err = Restore(ctx, archiveStore, map[string]payload.Store{"filesystem": restoredPayloads}, target, receipt, 0, 10); err != nil || len(target.events) != 2 {
		t.Fatalf("idempotent restore events=%d err=%v", len(target.events), err)
	}
	opened, err := restoredPayloads.Open(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_ = opened.Close()
}

type failingStore struct{ err error }

func (s failingStore) Put(context.Context, io.Reader, payload.PutOptions) (payload.Ref, error) {
	return payload.Ref{}, s.err
}
func (s failingStore) Open(context.Context, payload.Ref) (io.ReadCloser, error) {
	return nil, s.err
}
func (s failingStore) Delete(context.Context, payload.Ref) error { return s.err }

func TestArchiveWriteFailureHasNoReceipt(t *testing.T) {
	w, err := NewWriter(failingStore{err: errors.New("archive unavailable")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := trail.ParseEventID("00000000000000000000000001")
	if receipt, err := w.Write(context.Background(), []storage.EventRecord{{ID: id}}); err == nil || receipt.Manifest.ID != "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}
