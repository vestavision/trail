// Package archive writes immutable, verifiable Trail retention bundles.
package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/storage"
)

const Version = "trail.archive/v1"

type PayloadEntry struct {
	Source  payload.Ref `json:"source"`
	Archive payload.Ref `json:"archive"`
}
type Manifest struct {
	Version    string         `json:"version"`
	ID         string         `json:"id"`
	CreatedAt  time.Time      `json:"created_at"`
	EventCount int            `json:"event_count"`
	EventIDs   []string       `json:"event_ids"`
	Events     payload.Ref    `json:"events"`
	Payloads   []PayloadEntry `json:"payloads,omitempty"`
}
type Receipt struct {
	Manifest    Manifest    `json:"manifest"`
	ManifestRef payload.Ref `json:"manifest_ref"`
}

type Page struct {
	Items   []Receipt `json:"items"`
	HasMore bool      `json:"has_more"`
}

// Catalog is the small database-backed archive discovery boundary. Object
// store keys are never accepted as archive identities.
type Catalog interface {
	ListArchives(context.Context, int, int) (Page, error)
	GetArchive(context.Context, string) (Receipt, error)
}

type Writer interface {
	Write(context.Context, []storage.EventRecord) (Receipt, error)
}

// RestoreStore accepts a bounded archive chunk. Implementations must make
// repeated writes idempotent by event ID.
type RestoreStore interface {
	RestoreArchiveEvents(context.Context, string, []storage.EventRecord) (int, error)
}

type RestoreResult struct {
	ArchiveID string `json:"archive_id"`
	Restored  int    `json:"restored"`
	Next      int    `json:"next_offset,omitempty"`
	Complete  bool   `json:"complete"`
}

type BundleWriter struct {
	archive payload.Store
	sources map[string]payload.Store
	now     func() time.Time
}

func NewWriter(store payload.Store, sources map[string]payload.Store) (*BundleWriter, error) {
	if store == nil {
		return nil, errors.New("trail/archive: archive store is required")
	}
	return &BundleWriter{archive: store, sources: sources, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (w *BundleWriter) Write(ctx context.Context, events []storage.EventRecord) (Receipt, error) {
	if len(events) == 0 {
		return Receipt{}, errors.New("trail/archive: empty event batch")
	}
	var eventData bytes.Buffer
	h := sha256.New()
	mw := io.MultiWriter(&eventData, h)
	bw := bufio.NewWriter(mw)
	enc := json.NewEncoder(bw)
	for i := range events {
		if err := enc.Encode(events[i]); err != nil {
			return Receipt{}, err
		}
	}
	if err := bw.Flush(); err != nil {
		return Receipt{}, err
	}
	eventRef, err := w.archive.Put(ctx, bytes.NewReader(eventData.Bytes()), payload.PutOptions{ContentType: "application/x-ndjson", Compression: payload.CompressionGZIP})
	if err != nil {
		return Receipt{}, err
	}
	id := hex.EncodeToString(h.Sum(nil))
	manifest := Manifest{Version: Version, ID: id, CreatedAt: w.now(), EventCount: len(events), Events: eventRef, EventIDs: make([]string, len(events))}
	for i := range events {
		manifest.EventIDs[i] = events[i].ID.String()
	}
	seen := map[string]bool{}
	for _, event := range events {
		for _, link := range event.Payloads {
			k := link.Ref.Store + "\x00" + link.Ref.Key
			if seen[k] {
				continue
			}
			seen[k] = true
			source := w.sources[link.Ref.Store]
			if source == nil {
				return Receipt{}, errors.New("trail/archive: source payload store is not configured")
			}
			reader, err := source.Open(ctx, link.Ref)
			if err != nil {
				return Receipt{}, err
			}
			logical := io.Reader(reader)
			var gz *gzip.Reader
			if link.Ref.Compression == payload.CompressionGZIP {
				gz, err = gzip.NewReader(reader)
				if err != nil {
					_ = reader.Close()
					return Receipt{}, err
				}
				logical = gz
			}
			archived, err := w.archive.Put(ctx, logical, payload.PutOptions{ContentType: link.Ref.ContentType, Compression: link.Ref.Compression, RetainUntil: link.Ref.RetainUntil, RetentionClass: link.Ref.RetentionClass})
			if gz != nil {
				_ = gz.Close()
			}
			_ = reader.Close()
			if err != nil {
				return Receipt{}, err
			}
			if archived.SHA256 != link.Ref.SHA256 || archived.Size != link.Ref.Size {
				return Receipt{}, errors.New("trail/archive: payload verification failed")
			}
			manifest.Payloads = append(manifest.Payloads, PayloadEntry{Source: link.Ref, Archive: archived})
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return Receipt{}, err
	}
	manifestRef, err := w.archive.Put(ctx, bytes.NewReader(data), payload.PutOptions{ContentType: "application/vnd.trail.archive-manifest+json"})
	if err != nil {
		return Receipt{}, err
	}
	opened, err := w.archive.Open(ctx, manifestRef)
	if err != nil {
		return Receipt{}, err
	}
	verified, err := io.ReadAll(io.LimitReader(opened, int64(len(data))+1))
	_ = opened.Close()
	if err != nil || !bytes.Equal(verified, data) {
		return Receipt{}, errors.New("trail/archive: manifest verification failed")
	}
	return Receipt{Manifest: manifest, ManifestRef: manifestRef}, nil
}

// ReadManifest reads and verifies a manifest through a previously trusted
// reference. Callers should obtain the reference from the retention catalog,
// never from untrusted request input.
func ReadManifest(ctx context.Context, store payload.Store, ref payload.Ref) (Manifest, error) {
	if store == nil {
		return Manifest{}, errors.New("trail/archive: archive store is required")
	}
	r, err := store.Open(ctx, ref)
	if err != nil {
		return Manifest{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, 16<<20))
	if err != nil {
		return Manifest{}, err
	}
	if sum := sha256.Sum256(data); sum != ref.SHA256 {
		return Manifest{}, errors.New("trail/archive: manifest checksum verification failed")
	}
	var manifest Manifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Version != Version || manifest.ID == "" || manifest.EventCount < 1 || len(manifest.EventIDs) != manifest.EventCount {
		return Manifest{}, errors.New("trail/archive: invalid manifest")
	}
	return manifest, nil
}

// ReadEvents returns at most limit events beginning at offset. It never loads
// an unbounded archive into memory.
func ReadEvents(ctx context.Context, store payload.Store, manifest Manifest, offset, limit int) ([]storage.EventRecord, bool, error) {
	if store == nil || offset < 0 || limit < 1 || limit > 10_000 {
		return nil, false, errors.New("trail/archive: invalid bounded read")
	}
	r, err := store.Open(ctx, manifest.Events)
	if err != nil {
		return nil, false, err
	}
	defer r.Close()
	logical := io.Reader(r)
	var gz *gzip.Reader
	if manifest.Events.Compression == payload.CompressionGZIP {
		gz, err = gzip.NewReader(r)
		if err != nil {
			return nil, false, err
		}
		defer gz.Close()
		logical = gz
	}
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(logical, hash))
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	index := 0
	result := make([]storage.EventRecord, 0, limit)
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			return nil, false, err
		}
		if index >= offset && len(result) < limit {
			var event storage.EventRecord
			if err = json.Unmarshal(scanner.Bytes(), &event); err != nil {
				return nil, false, err
			}
			result = append(result, event)
		}
		index++
	}
	if err = scanner.Err(); err != nil {
		return nil, false, err
	}
	if index != manifest.EventCount {
		return nil, false, errors.New("trail/archive: event count verification failed")
	}
	var sum [32]byte
	copy(sum[:], hash.Sum(nil))
	if sum != manifest.Events.SHA256 {
		return nil, false, errors.New("trail/archive: event checksum verification failed")
	}
	return result, offset+len(result) >= manifest.EventCount, nil
}

// Restore copies referenced payloads first and then idempotently merges a
// bounded event chunk into the active event store.
func Restore(ctx context.Context, archiveStore payload.Store, activeStores map[string]payload.Store, target RestoreStore, receipt Receipt, offset, limit int) (RestoreResult, error) {
	result := RestoreResult{ArchiveID: receipt.Manifest.ID}
	if target == nil {
		return result, errors.New("trail/archive: restore store is required")
	}
	manifest, err := ReadManifest(ctx, archiveStore, receipt.ManifestRef)
	if err != nil {
		return result, err
	}
	if manifest.ID != receipt.Manifest.ID {
		return result, errors.New("trail/archive: manifest identity mismatch")
	}
	events, complete, err := ReadEvents(ctx, archiveStore, manifest, offset, limit)
	if err != nil {
		return result, err
	}
	needed := map[string]bool{}
	for _, event := range events {
		for _, link := range event.Payloads {
			needed[link.Ref.Store+"\x00"+link.Ref.Key] = true
		}
	}
	for _, entry := range manifest.Payloads {
		if !needed[entry.Source.Store+"\x00"+entry.Source.Key] {
			continue
		}
		destination := activeStores[entry.Source.Store]
		if destination == nil {
			return result, errors.New("trail/archive: destination payload store is not configured")
		}
		reader, er := archiveStore.Open(ctx, entry.Archive)
		if er != nil {
			return result, er
		}
		logical := io.Reader(reader)
		var gz *gzip.Reader
		if entry.Archive.Compression == payload.CompressionGZIP {
			gz, er = gzip.NewReader(reader)
			if er != nil {
				_ = reader.Close()
				return result, er
			}
			logical = gz
		}
		restored, er := destination.Put(ctx, logical, payload.PutOptions{ContentType: entry.Source.ContentType, Compression: entry.Source.Compression, RetainUntil: entry.Source.RetainUntil, RetentionClass: entry.Source.RetentionClass})
		if gz != nil {
			_ = gz.Close()
		}
		_ = reader.Close()
		if er != nil {
			return result, er
		}
		if restored.SHA256 != entry.Source.SHA256 || restored.Size != entry.Source.Size {
			return result, errors.New("trail/archive: restored payload verification failed")
		}
	}
	result.Restored, err = target.RestoreArchiveEvents(ctx, manifest.ID, events)
	if err != nil {
		return result, err
	}
	result.Complete = complete
	if !complete {
		result.Next = offset + len(events)
	}
	return result, nil
}
