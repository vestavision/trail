package retention

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/archive"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/payload/filesystem"
	"github.com/vestavision/trail/storage"
)

type fakeStore struct {
	events     []Event
	candidates []Candidate
	deleted    int
	live       map[string]bool
	failDelete bool
	failRecord bool
	lastLimit  int
}

func (*fakeStore) ReconcileRetention(context.Context) error { return nil }
func (*fakeStore) LoadArchiveEvents(_ context.Context, events []Event) ([]storage.EventRecord, error) {
	out := make([]storage.EventRecord, len(events))
	for i, event := range events {
		raw, err := hex.DecodeString(strings.Repeat("0", 32-len(event.ID)) + event.ID)
		if err != nil {
			return nil, err
		}
		copy(out[i].ID[:], raw)
	}
	return out, nil
}
func (s *fakeStore) RecordArchive(context.Context, archive.Receipt) error {
	if s.failRecord {
		return errors.New("archive catalog unavailable")
	}
	return nil
}

type fakeArchiver struct{}

func (fakeArchiver) Write(_ context.Context, events []storage.EventRecord) (archive.Receipt, error) {
	return archive.Receipt{Manifest: archive.Manifest{ID: "archive", EventCount: len(events)}}, nil
}
func withArchiver(c Config) Config { c.Archiver = fakeArchiver{}; return c }

type failingArchiver struct{}

func (failingArchiver) Write(context.Context, []storage.EventRecord) (archive.Receipt, error) {
	return archive.Receipt{}, errors.New("archive unavailable")
}

func (s *fakeStore) ScanRetentionEvents(_ context.Context, r ScanRequest) (EventPage, error) {
	s.lastLimit = r.Limit
	if r.Cursor != "" {
		return EventPage{}, nil
	}
	n := len(s.events)
	more := false
	if n > r.Limit {
		n = r.Limit
		more = true
	}
	return EventPage{Events: s.events[:n], HasMore: more, NextCursor: "next"}, nil
}
func (s *fakeStore) DeleteRetentionEvents(_ context.Context, r DeleteRequest) (DeleteResult, error) {
	if s.failDelete {
		return DeleteResult{}, errors.New("delete failed")
	}
	if r.DryRun {
		return DeleteResult{}, nil
	}
	s.deleted += len(r.Events)
	seen := map[string]bool{}
	for _, e := range r.Events {
		for _, p := range e.Payloads {
			k := payloadKey(p.Ref)
			if at, ok := r.PayloadEligibleAt[k]; ok && !seen[k] {
				s.candidates = append(s.candidates, Candidate{Ref: p.Ref, ArchiveID: r.ArchiveID, EligibleAt: at, CandidateAt: time.Unix(1, 0)})
				seen[k] = true
			}
		}
	}
	return DeleteResult{EventsDeleted: uint64(len(r.Events)), PayloadCandidates: uint64(len(seen))}, nil
}
func (s *fakeStore) ListPayloadCandidates(_ context.Context, _ time.Time, n int) (CandidatePage, error) {
	if len(s.candidates) > n {
		return CandidatePage{Candidates: s.candidates[:n]}, nil
	}
	return CandidatePage{Candidates: append([]Candidate(nil), s.candidates...)}, nil
}
func (s *fakeStore) ReferencedPayloads(_ context.Context, refs []payload.Ref) (map[string]bool, error) {
	out := map[string]bool{}
	for _, r := range refs {
		out[payloadKey(r)] = s.live[payloadKey(r)]
	}
	return out, nil
}
func (s *fakeStore) CompletePayloadCandidate(_ context.Context, r payload.Ref, failure string) error {
	if failure == "" || failure == "referenced" {
		k := payloadKey(r)
		for i := range s.candidates {
			if payloadKey(s.candidates[i].Ref) == k {
				s.candidates = append(s.candidates[:i], s.candidates[i+1:]...)
				break
			}
		}
	}
	return nil
}
func TestPolicyPrecedenceAndDryRun(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()
	warn := trail.LevelWarn
	store := &fakeStore{events: []Event{{ID: "1", Timestamp: now.Add(-48 * time.Hour), Kind: "provider.http", Level: warn, RetentionClass: "audit"}, {ID: "2", Timestamp: now.Add(-48 * time.Hour), Kind: "provider.http", Level: warn}}}
	cfg := Config{Now: func() time.Time { return now }, EventClasses: map[string]time.Duration{"audit": 365 * 24 * time.Hour}, EventPolicies: []EventPolicy{{Name: "http", Match: EventMatch{Kind: "provider.http"}, TTL: 24 * time.Hour}}, PayloadGracePeriod: time.Hour}
	engine, err := New(withArchiver(cfg), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := engine.Run(context.Background(), RunOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.EventsScanned != 2 || r.EventsEligible != 1 || r.EventsDeleted != 0 || store.deleted != 0 {
		t.Fatalf("result=%+v deleted=%d", r, store.deleted)
	}
}

func TestUnknownClassesRetainAndOrderedPolicyWins(t *testing.T) {
	now := time.Unix(1_500_000, 0).UTC()
	e := &Engine{cfg: Config{EventClasses: map[string]time.Duration{"audit": time.Hour}, EventPolicies: []EventPolicy{{Name: "first", Match: EventMatch{Kind: "x"}, TTL: time.Hour}, {Name: "second", Match: EventMatch{Kind: "x"}, TTL: 2 * time.Hour}}, DefaultEventTTL: 3 * time.Hour}}
	if _, ok := e.eventExpiry(Event{Timestamp: now, Kind: "x", RetentionClass: "unknown"}); ok {
		t.Fatal("unknown class must retain")
	}
	expiry, ok := e.eventExpiry(Event{Timestamp: now, Kind: "x"})
	if !ok || !expiry.Equal(now.Add(time.Hour)) {
		t.Fatalf("expiry=%v ok=%v", expiry, ok)
	}
	ref := payload.Ref{RetentionClass: "unknown"}
	if _, ok = e.payloadExpiry(Event{Timestamp: now}, Payload{Ref: ref}); ok {
		t.Fatal("unknown payload class must retain")
	}
}

func TestExplicitPayloadExpiryAndSharedReference(t *testing.T) {
	now := time.Unix(2_000_000, 0).UTC()
	ref := payload.Ref{Store: "memory", Key: "abc", StoredSize: 42, RetainUntil: now.Add(-time.Hour)}
	store := &fakeStore{events: []Event{{ID: "1", Timestamp: now.Add(-48 * time.Hour), Payloads: []Payload{{Role: "response", Ref: ref}}}}, live: map[string]bool{payloadKey(ref): true}}
	ps := &memoryPayload{}
	engine, err := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: 24 * time.Hour, PayloadGracePeriod: time.Hour}), store, map[string]payload.Store{"memory": ps})
	if err != nil {
		t.Fatal(err)
	}
	r, err := engine.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.EventsDeleted != 1 || r.PayloadsReferenced != 1 || ps.deleted != 0 {
		t.Fatalf("result=%+v deleted=%d", r, ps.deleted)
	}
}

func TestDryRunReportsPayloadCandidateWithoutStaging(t *testing.T) {
	now := time.Unix(2_500_000, 0).UTC()
	ref := payload.Ref{Store: "filesystem", Key: "candidate", RetainUntil: now.Add(-time.Hour)}
	store := &fakeStore{events: []Event{{ID: "1", Timestamp: now.Add(-48 * time.Hour), Payloads: []Payload{{Role: "response", Ref: ref}}}}}
	engine, err := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Hour}), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := engine.Run(context.Background(), RunOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.PayloadCandidates != 1 || len(store.candidates) != 0 || r.EventsDeleted != 0 {
		t.Fatalf("result=%+v candidates=%d", r, len(store.candidates))
	}
}

func TestDeleteFailureDoesNotTouchPayload(t *testing.T) {
	now := time.Unix(3_000_000, 0).UTC()
	store := &fakeStore{failDelete: true, events: []Event{{ID: "1", Timestamp: now.Add(-48 * time.Hour)}}}
	engine, _ := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Hour}), store, nil)
	r, err := engine.Run(context.Background(), RunOptions{})
	if err == nil || r.EventsDeleted != 0 || len(store.candidates) != 0 {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}

func TestArchiveFailureAndCatalogFailurePreventDeletion(t *testing.T) {
	now := time.Unix(3_100_000, 0).UTC()
	for _, tc := range []struct {
		name       string
		archiver   archive.Writer
		failRecord bool
	}{{"object write", failingArchiver{}, false}, {"catalog commit", fakeArchiver{}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{failRecord: tc.failRecord, events: []Event{{ID: "1", Timestamp: now.Add(-48 * time.Hour)}}}
			cfg := Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Hour, Archiver: tc.archiver}
			engine, err := New(cfg, store, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := engine.Run(context.Background(), RunOptions{})
			if err == nil || store.deleted != 0 || result.EventsDeleted != 0 || result.ArchiveFailures != 1 {
				t.Fatalf("result=%+v deleted=%d err=%v", result, store.deleted, err)
			}
		})
	}
}

func TestArchiveIsMandatory(t *testing.T) {
	_, err := New(Config{DefaultEventTTL: time.Hour}, &fakeStore{}, nil)
	if err == nil || !strings.Contains(err.Error(), "archiver is required") {
		t.Fatalf("err=%v", err)
	}
}

func TestRunHonorsEventLimitAndCancellation(t *testing.T) {
	now := time.Unix(3_500_000, 0).UTC()
	store := &fakeStore{events: []Event{
		{ID: "1", Timestamp: now.Add(-time.Hour)},
		{ID: "2", Timestamp: now.Add(-time.Hour)},
		{ID: "3", Timestamp: now.Add(-time.Hour)},
	}}
	engine, err := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Minute, EventBatchSize: 10, MaxEvents: 2}), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := engine.Run(context.Background(), RunOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.EventsScanned != 2 || store.lastLimit != 2 {
		t.Fatalf("result=%+v limit=%d", r, store.lastLimit)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	store.deleted = 0
	_, err = engine.Run(cancelled, RunOptions{})
	if !errors.Is(err, context.Canceled) || store.deleted != 0 {
		t.Fatalf("cancel err=%v deleted=%d", err, store.deleted)
	}
}

func TestFilesystemPayloadCleanup(t *testing.T) {
	now := time.Unix(4_000_000, 0).UTC()
	fs, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := fs.Put(context.Background(), strings.NewReader("expired payload"), payload.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{candidates: []Candidate{{Ref: ref, ArchiveID: "archive", EligibleAt: now.Add(-2 * time.Hour), CandidateAt: now.Add(-2 * time.Hour)}}, live: map[string]bool{}}
	engine, err := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Hour, PayloadGracePeriod: time.Hour}), store, map[string]payload.Store{"filesystem": fs})
	if err != nil {
		t.Fatal(err)
	}
	r, err := engine.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.PayloadsDeleted != 1 || r.PayloadBytesReclaimed == 0 {
		t.Fatalf("result=%+v", r)
	}
	if _, err = fs.Open(context.Background(), ref); !errors.Is(err, payload.ErrNotFound) {
		t.Fatalf("open deleted payload: %v", err)
	}
}

func TestPayloadDeleteFailureRemainsRetryable(t *testing.T) {
	now := time.Unix(4_500_000, 0).UTC()
	var sum [32]byte
	sum[0] = 1
	key := hex.EncodeToString(sum[:])
	ref := payload.Ref{Store: "memory", Key: key, SHA256: sum, StoredSize: 9}
	store := &fakeStore{candidates: []Candidate{{Ref: ref, ArchiveID: "archive", EligibleAt: now.Add(-2 * time.Hour), CandidateAt: now.Add(-2 * time.Hour)}}, live: map[string]bool{}}
	ps := &memoryPayload{err: errors.New("object store unavailable")}
	engine, err := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Hour, PayloadGracePeriod: time.Hour}), store, map[string]payload.Store{"memory": ps})
	if err != nil {
		t.Fatal(err)
	}
	r, err := engine.Run(context.Background(), RunOptions{})
	if err == nil || r.Errors != 1 || r.PayloadsDeleted != 0 || len(store.candidates) != 1 {
		t.Fatalf("result=%+v err=%v candidates=%d", r, err, len(store.candidates))
	}
}

func TestPayloadCandidateWithoutArchiveIsNeverDeleted(t *testing.T) {
	now := time.Unix(4_700_000, 0).UTC()
	ref := payload.Ref{Store: "memory", Key: "legacy", StoredSize: 9}
	store := &fakeStore{candidates: []Candidate{{Ref: ref, EligibleAt: now.Add(-2 * time.Hour), CandidateAt: now.Add(-2 * time.Hour)}}}
	ps := &memoryPayload{}
	engine, err := New(withArchiver(Config{Now: func() time.Time { return now }, DefaultEventTTL: time.Hour, PayloadGracePeriod: time.Hour}), store, map[string]payload.Store{"memory": ps})
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Run(context.Background(), RunOptions{})
	if err != nil || ps.deleted != 0 || result.PayloadsUnknown != 1 {
		t.Fatalf("result=%+v deleted=%d err=%v", result, ps.deleted, err)
	}
}

type memoryPayload struct {
	deleted int
	err     error
}

func (*memoryPayload) Put(context.Context, io.Reader, payload.PutOptions) (payload.Ref, error) {
	panic("unused")
}
func (*memoryPayload) Open(context.Context, payload.Ref) (io.ReadCloser, error) {
	return nil, payload.ErrNotFound
}
func (p *memoryPayload) Delete(context.Context, payload.Ref) error { p.deleted++; return p.err }
