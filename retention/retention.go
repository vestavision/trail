// Package retention provides an optional, bounded lifecycle engine for stored
// Trail events and payloads. It is deliberately independent from Trail's
// logging runtime and never starts background work.
package retention

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/archive"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/storage"
)

type EventMatch struct {
	Kind, Service, Environment  string
	Level                       *trail.Level
	FlowStatus, ExecutionStatus string
	HTTPStatusClass             int
	HTTPSuccess                 *bool
}

type EventPolicy struct {
	Name  string
	Match EventMatch
	TTL   time.Duration
}

type PayloadMatch struct {
	EventMatch
	Role, ContentType, RetentionClass string
}

type PayloadPolicy struct {
	Name  string
	Match PayloadMatch
	TTL   time.Duration
}

type Config struct {
	EventPolicies      []EventPolicy
	EventClasses       map[string]time.Duration
	DefaultEventTTL    time.Duration
	PayloadPolicies    []PayloadPolicy
	PayloadClasses     map[string]time.Duration
	DefaultPayloadTTL  time.Duration
	EventBatchSize     int
	PayloadBatchSize   int
	PayloadConcurrency int
	MaxEvents          int
	MaxPayloads        int
	MaxRuntime         time.Duration
	PayloadGracePeriod time.Duration
	Now                func() time.Time
	Archiver           archive.Writer
}

type Event struct {
	ID, Cursor                  string
	FlowID, ExecutionID         string
	Timestamp                   time.Time
	Kind, Service, Environment  string
	Level                       trail.Level
	FlowStatus, ExecutionStatus string
	RetentionClass              string
	HTTPStatusCode              int
	HTTPSuccess                 bool
	Payloads                    []Payload
}

type Payload struct {
	Role string
	Ref  payload.Ref
}

type ScanRequest struct {
	Cursor string
	Limit  int
}
type EventPage struct {
	Events     []Event
	NextCursor string
	HasMore    bool
}
type DeleteRequest struct {
	Events            []Event
	PayloadEligibleAt map[string]time.Time
	ArchiveID         string
	DryRun            bool
}
type DeleteResult struct{ EventsDeleted, PayloadCandidates uint64 }

type Candidate struct {
	Ref         payload.Ref
	ArchiveID   string
	EligibleAt  time.Time
	CandidateAt time.Time
	Attempts    uint32
	LastError   string
}
type CandidatePage struct{ Candidates []Candidate }

// Store is intentionally separate from storage.IngestStore and
// storage.ExplorerStore. Implementations may use database-native deletion.
type Store interface {
	ReconcileRetention(context.Context) error
	LoadArchiveEvents(context.Context, []Event) ([]storage.EventRecord, error)
	RecordArchive(context.Context, archive.Receipt) error
	ScanRetentionEvents(context.Context, ScanRequest) (EventPage, error)
	DeleteRetentionEvents(context.Context, DeleteRequest) (DeleteResult, error)
	ListPayloadCandidates(context.Context, time.Time, int) (CandidatePage, error)
	ReferencedPayloads(context.Context, []payload.Ref) (map[string]bool, error)
	CompletePayloadCandidate(context.Context, payload.Ref, string) error
}

type RunOptions struct{ DryRun bool }
type Result struct {
	StartedAt             time.Time     `json:"started_at"`
	FinishedAt            time.Time     `json:"finished_at"`
	Duration              time.Duration `json:"duration_ns"`
	EventsScanned         uint64        `json:"events_scanned"`
	EventsMatched         uint64        `json:"events_matched"`
	EventsEligible        uint64        `json:"events_eligible"`
	EventsDeleted         uint64        `json:"events_deleted"`
	PayloadCandidates     uint64        `json:"payload_candidates"`
	PayloadsReferenced    uint64        `json:"payloads_referenced"`
	PayloadsOrphaned      uint64        `json:"payloads_orphaned"`
	PayloadsUnknown       uint64        `json:"payloads_unknown"`
	PayloadsDeleted       uint64        `json:"payloads_deleted"`
	PayloadBytesReclaimed uint64        `json:"payload_bytes_reclaimed"`
	Errors                uint64        `json:"errors"`
	ArchivesCreated       uint64        `json:"archives_created"`
	EventsArchived        uint64        `json:"events_archived"`
	PayloadsArchived      uint64        `json:"payloads_archived"`
	ArchiveBytesWritten   uint64        `json:"archive_bytes_written"`
	ArchiveFailures       uint64        `json:"archive_failures"`
}

type Engine struct {
	cfg      Config
	store    Store
	payloads map[string]payload.Store
}

func New(cfg Config, store Store, payloads map[string]payload.Store) (*Engine, error) {
	if store == nil {
		return nil, errors.New("trail/retention: store is required")
	}
	if cfg.Archiver == nil {
		return nil, errors.New("trail/retention: archiver is required")
	}
	if cfg.EventBatchSize <= 0 {
		cfg.EventBatchSize = 1000
	}
	if cfg.PayloadBatchSize <= 0 {
		cfg.PayloadBatchSize = 100
	}
	if cfg.PayloadConcurrency <= 0 {
		cfg.PayloadConcurrency = 1
	}
	if cfg.PayloadConcurrency > 32 {
		return nil, errors.New("trail/retention: payload concurrency exceeds 32")
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = 100000
	}
	if cfg.MaxPayloads <= 0 {
		cfg.MaxPayloads = 10000
	}
	if cfg.MaxRuntime <= 0 {
		cfg.MaxRuntime = 10 * time.Minute
	}
	if cfg.PayloadGracePeriod <= 0 {
		cfg.PayloadGracePeriod = 24 * time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.EventBatchSize > 100000 || cfg.PayloadBatchSize > 10000 || cfg.MaxEvents > 10000000 || cfg.MaxPayloads > 1000000 || cfg.MaxRuntime > 24*time.Hour {
		return nil, errors.New("trail/retention: configured work bound exceeds safety limit")
	}
	if err := validate(cfg); err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg, store: store, payloads: payloads}, nil
}

func validate(c Config) error {
	for _, p := range c.EventPolicies {
		if p.TTL <= 0 {
			return fmt.Errorf("trail/retention: event policy %q needs positive TTL", p.Name)
		}
		if p.Match.HTTPStatusClass < 0 || p.Match.HTTPStatusClass > 5 {
			return fmt.Errorf("trail/retention: event policy %q has invalid HTTP status class", p.Name)
		}
	}
	for _, p := range c.PayloadPolicies {
		if p.TTL <= 0 {
			return fmt.Errorf("trail/retention: payload policy %q needs positive TTL", p.Name)
		}
		if p.Match.HTTPStatusClass < 0 || p.Match.HTTPStatusClass > 5 {
			return fmt.Errorf("trail/retention: payload policy %q has invalid HTTP status class", p.Name)
		}
	}
	for n, ttl := range c.EventClasses {
		if strings.TrimSpace(n) == "" || ttl <= 0 {
			return errors.New("trail/retention: invalid event class")
		}
	}
	for n, ttl := range c.PayloadClasses {
		if strings.TrimSpace(n) == "" || ttl <= 0 {
			return errors.New("trail/retention: invalid payload class")
		}
	}
	if c.DefaultEventTTL < 0 || c.DefaultPayloadTTL < 0 {
		return errors.New("trail/retention: default TTL cannot be negative")
	}
	return nil
}

func (e *Engine) Run(ctx context.Context, opt RunOptions) (Result, error) {
	now := e.cfg.Now().UTC()
	result := Result{StartedAt: now}
	ctx, cancel := context.WithTimeout(ctx, e.cfg.MaxRuntime)
	defer cancel()
	if !opt.DryRun {
		if err := e.store.ReconcileRetention(ctx); err != nil {
			result.Errors++
			return finish(result, e.cfg.Now()), err
		}
	}
	cursor := ""
	dryPayloads := map[string]bool{}
	for result.EventsScanned < uint64(e.cfg.MaxEvents) && ctx.Err() == nil {
		limit := e.cfg.EventBatchSize
		remain := e.cfg.MaxEvents - int(result.EventsScanned)
		if limit > remain {
			limit = remain
		}
		page, err := e.store.ScanRetentionEvents(ctx, ScanRequest{Cursor: cursor, Limit: limit})
		if err != nil {
			result.Errors++
			return finish(result, e.cfg.Now()), err
		}
		if len(page.Events) == 0 {
			break
		}
		eligible := make([]Event, 0, len(page.Events))
		times := map[string]time.Time{}
		for _, event := range page.Events {
			result.EventsScanned++
			expiry, matched := e.eventExpiry(event)
			if matched {
				result.EventsMatched++
			}
			if matched && !expiry.After(now) {
				eligible = append(eligible, event)
				result.EventsEligible++
				for _, p := range event.Payloads {
					if at, ok := e.payloadExpiry(event, p); ok {
						k := payloadKey(p.Ref)
						if old := times[k]; at.After(old) {
							times[k] = at
						}
					}
				}
			}
		}
		if len(eligible) > 0 {
			if opt.DryRun {
				for key := range times {
					dryPayloads[key] = true
				}
				result.PayloadCandidates = uint64(len(dryPayloads))
			}
			archiveID := ""
			if !opt.DryRun {
				records, err := e.store.LoadArchiveEvents(ctx, eligible)
				if err != nil {
					result.Errors++
					result.ArchiveFailures++
					return finish(result, e.cfg.Now()), err
				}
				if !sameArchiveEvents(records, eligible) {
					result.Errors++
					result.ArchiveFailures++
					return finish(result, e.cfg.Now()), errors.New("trail/retention: incomplete archive export")
				}
				receipt, err := e.cfg.Archiver.Write(ctx, records)
				if err != nil {
					result.Errors++
					result.ArchiveFailures++
					return finish(result, e.cfg.Now()), err
				}
				if err = e.store.RecordArchive(ctx, receipt); err != nil {
					result.Errors++
					result.ArchiveFailures++
					return finish(result, e.cfg.Now()), err
				}
				archiveID = receipt.Manifest.ID
				result.ArchivesCreated++
				result.EventsArchived += uint64(receipt.Manifest.EventCount)
				result.PayloadsArchived += uint64(len(receipt.Manifest.Payloads))
				result.ArchiveBytesWritten += uint64(receipt.Manifest.Events.StoredSize + receipt.ManifestRef.StoredSize)
				for _, p := range receipt.Manifest.Payloads {
					if p.Archive.StoredSize > 0 {
						result.ArchiveBytesWritten += uint64(p.Archive.StoredSize)
					}
				}
			}
			dr, err := e.store.DeleteRetentionEvents(ctx, DeleteRequest{Events: eligible, PayloadEligibleAt: times, ArchiveID: archiveID, DryRun: opt.DryRun})
			if !opt.DryRun {
				result.EventsDeleted += dr.EventsDeleted
				result.PayloadCandidates += dr.PayloadCandidates
			}
			if err != nil {
				result.Errors++
				return finish(result, e.cfg.Now()), err
			}
		}
		cursor = page.NextCursor
		if !page.HasMore {
			break
		}
	}
	if !opt.DryRun {
		previousErrors := result.Errors
		if err := e.collectPayloads(ctx, now, &result); err != nil {
			if result.Errors == previousErrors {
				result.Errors++
			}
			return finish(result, e.cfg.Now()), err
		}
	}
	return finish(result, e.cfg.Now()), ctx.Err()
}

func sameArchiveEvents(records []storage.EventRecord, eligible []Event) bool {
	if len(records) != len(eligible) {
		return false
	}
	want := make(map[string]bool, len(eligible))
	for _, event := range eligible {
		id := event.ID
		if len(id) < 32 {
			id = strings.Repeat("0", 32-len(id)) + id
		}
		want[id] = true
	}
	for _, event := range records {
		if !want[hex.EncodeToString(event.ID[:])] {
			return false
		}
		delete(want, hex.EncodeToString(event.ID[:]))
	}
	return len(want) == 0
}

func finish(r Result, now time.Time) Result {
	r.FinishedAt = now.UTC()
	r.Duration = r.FinishedAt.Sub(r.StartedAt)
	return r
}

func (e *Engine) eventExpiry(v Event) (time.Time, bool) {
	if v.RetentionClass != "" {
		ttl, ok := e.cfg.EventClasses[v.RetentionClass]
		if !ok {
			return time.Time{}, false
		}
		return v.Timestamp.Add(ttl), true
	}
	for _, p := range e.cfg.EventPolicies {
		if matchEvent(p.Match, v) {
			return v.Timestamp.Add(p.TTL), true
		}
	}
	if e.cfg.DefaultEventTTL > 0 {
		return v.Timestamp.Add(e.cfg.DefaultEventTTL), true
	}
	return time.Time{}, false
}
func (e *Engine) payloadExpiry(v Event, p Payload) (time.Time, bool) {
	if !p.Ref.RetainUntil.IsZero() {
		return p.Ref.RetainUntil, true
	}
	if p.Ref.RetentionClass != "" {
		ttl, ok := e.cfg.PayloadClasses[p.Ref.RetentionClass]
		if !ok {
			return time.Time{}, false
		}
		return v.Timestamp.Add(ttl), true
	}
	for _, x := range e.cfg.PayloadPolicies {
		if matchEvent(x.Match.EventMatch, v) && (x.Match.Role == "" || x.Match.Role == p.Role) && (x.Match.ContentType == "" || strings.HasPrefix(p.Ref.ContentType, x.Match.ContentType)) {
			return v.Timestamp.Add(x.TTL), true
		}
	}
	if e.cfg.DefaultPayloadTTL > 0 {
		return v.Timestamp.Add(e.cfg.DefaultPayloadTTL), true
	}
	return time.Time{}, false
}
func matchEvent(m EventMatch, v Event) bool {
	return (m.Kind == "" || m.Kind == v.Kind) && (m.Service == "" || m.Service == v.Service) && (m.Environment == "" || m.Environment == v.Environment) && (m.Level == nil || *m.Level == v.Level) && (m.FlowStatus == "" || m.FlowStatus == v.FlowStatus) && (m.ExecutionStatus == "" || m.ExecutionStatus == v.ExecutionStatus) && (m.HTTPStatusClass == 0 || v.HTTPStatusCode/100 == m.HTTPStatusClass) && (m.HTTPSuccess == nil || *m.HTTPSuccess == v.HTTPSuccess)
}
func payloadKey(r payload.Ref) string { return r.Store + "\x00" + r.Key }

func (e *Engine) collectPayloads(ctx context.Context, now time.Time, r *Result) error {
	remaining := e.cfg.MaxPayloads
	for remaining > 0 && ctx.Err() == nil {
		limit := e.cfg.PayloadBatchSize
		if limit > remaining {
			limit = remaining
		}
		page, err := e.store.ListPayloadCandidates(ctx, now.Add(-e.cfg.PayloadGracePeriod), limit)
		if err != nil {
			return err
		}
		if len(page.Candidates) == 0 {
			return nil
		}
		verified := page.Candidates[:0]
		for _, candidate := range page.Candidates {
			if candidate.ArchiveID == "" {
				r.PayloadsUnknown++
				continue
			}
			verified = append(verified, candidate)
		}
		page.Candidates = verified
		if len(page.Candidates) == 0 {
			return nil
		}
		refs := make([]payload.Ref, len(page.Candidates))
		for i := range page.Candidates {
			refs[i] = page.Candidates[i].Ref
		}
		live, err := e.store.ReferencedPayloads(ctx, refs)
		if err != nil {
			return err
		}
		remaining -= len(page.Candidates)
		jobs := make(chan Candidate)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error
		worker := func() {
			defer wg.Done()
			for c := range jobs {
				referenced, orphaned, unknown, deleted, bytesReclaimed, failures, err := e.processCandidate(ctx, c, live[payloadKey(c.Ref)])
				mu.Lock()
				if referenced {
					r.PayloadsReferenced++
				}
				if orphaned {
					r.PayloadsOrphaned++
				}
				if unknown {
					r.PayloadsUnknown++
				}
				if deleted {
					r.PayloadsDeleted++
				}
				r.PayloadBytesReclaimed += bytesReclaimed
				r.Errors += failures
				if firstErr == nil && err != nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}
		workers := e.cfg.PayloadConcurrency
		if workers > len(page.Candidates) {
			workers = len(page.Candidates)
		}
		wg.Add(workers)
		for range workers {
			go worker()
		}
		for _, c := range page.Candidates {
			jobs <- c
		}
		close(jobs)
		wg.Wait()
		if firstErr != nil {
			return firstErr
		}
		if len(page.Candidates) < limit {
			return nil
		}
	}
	return ctx.Err()
}

func (e *Engine) processCandidate(ctx context.Context, c Candidate, live bool) (bool, bool, bool, bool, uint64, uint64, error) {
	if live {
		return true, false, false, false, 0, 0, e.store.CompletePayloadCandidate(ctx, c.Ref, "referenced")
	}
	ps := e.payloads[c.Ref.Store]
	if ps == nil {
		failure := fmt.Errorf("trail/retention: payload store %q is not configured", c.Ref.Store)
		if err := e.store.CompletePayloadCandidate(ctx, c.Ref, failure.Error()); err != nil {
			return false, false, true, false, 0, 1, err
		}
		return false, false, true, false, 0, 1, failure
	}
	if !validContentAddressedRef(c.Ref) {
		failure := errors.New("trail/retention: invalid content-addressed payload reference")
		if err := e.store.CompletePayloadCandidate(ctx, c.Ref, failure.Error()); err != nil {
			return false, false, true, false, 0, 1, err
		}
		return false, false, true, false, 0, 1, failure
	}
	err := ps.Delete(ctx, c.Ref)
	if errors.Is(err, payload.ErrNotFound) {
		err = nil
	}
	if err != nil {
		if recordErr := e.store.CompletePayloadCandidate(ctx, c.Ref, err.Error()); recordErr != nil {
			return false, true, false, false, 0, 1, recordErr
		}
		return false, true, false, false, 0, 1, err
	}
	if err = e.store.CompletePayloadCandidate(ctx, c.Ref, ""); err != nil {
		return false, true, false, true, 0, 0, err
	}
	var reclaimed uint64
	if c.Ref.StoredSize > 0 {
		reclaimed = uint64(c.Ref.StoredSize)
	}
	return false, true, false, true, reclaimed, 0, nil
}

func validContentAddressedRef(ref payload.Ref) bool {
	var zero [32]byte
	if ref.Store == "" || ref.Key == "" || ref.SHA256 == zero {
		return false
	}
	sum := hex.EncodeToString(ref.SHA256[:])
	return strings.HasSuffix(ref.Key, sum) || strings.HasSuffix(ref.Key, sum+".gz")
}
