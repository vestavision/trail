// Package generator creates deterministic, coherent Trail business stories.
package generator

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/convention"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/wire"
)

type Config struct {
	Events             uint64
	Seed               uint64
	Start              time.Time
	BatchSize          int
	PublishConcurrency int
	PayloadStore       payload.Store
	PayloadEvery       uint64
}

type Manifest struct {
	Version int       `json:"version"`
	Events  uint64    `json:"events"`
	Batches uint64    `json:"batches"`
	Stories uint64    `json:"stories"`
	Seed    uint64    `json:"seed"`
	Start   time.Time `json:"start"`
}

type PublishFunc func(context.Context, wire.Envelope) error

func Generate(ctx context.Context, cfg Config, publish PublishFunc) (Manifest, error) {
	if cfg.Events == 0 || publish == nil {
		return Manifest{}, errors.New("trail/generator: events and publisher are required")
	}
	if cfg.Start.IsZero() {
		cfg.Start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.BatchSize > 1024 {
		return Manifest{}, errors.New("trail/generator: batch size exceeds 1024")
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	if cfg.PublishConcurrency <= 0 {
		cfg.PublishConcurrency = 1
	}
	manifest := Manifest{Version: 1, Seed: cfg.Seed, Start: cfg.Start}
	publishCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan wire.Envelope, cfg.PublishConcurrency*2)
	errCh := make(chan error, 1)
	var workers sync.WaitGroup
	for range cfg.PublishConcurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for envelope := range jobs {
				if err := publish(publishCtx, envelope); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}
	var finishOnce sync.Once
	var finishErr error
	finish := func() error {
		finishOnce.Do(func() {
			close(jobs)
			workers.Wait()
			select {
			case finishErr = <-errCh:
			default:
			}
		})
		return finishErr
	}
	defer finish()
	buffers := map[string][]wire.Event{}
	emit := func(service string, events []wire.Event) error {
		copyEvents := append([]wire.Event(nil), events...)
		envelope := wire.Envelope{SchemaVersion: wire.Version, BatchID: copyEvents[0].EventID, Metadata: trail.Metadata{Service: service, Environment: "demo", Version: "generator-v1"}, Events: copyEvents}
		select {
		case jobs <- envelope:
			manifest.Batches++
			return nil
		case <-publishCtx.Done():
			if err := finish(); err != nil {
				return err
			}
			return publishCtx.Err()
		}
	}
	for manifest.Events < cfg.Events {
		if err := publishCtx.Err(); err != nil {
			if publishErr := finish(); publishErr != nil {
				return manifest, publishErr
			}
			return manifest, err
		}
		story := buildStory(cfg, manifest.Stories, rng)
		if cfg.PayloadStore != nil && cfg.PayloadEvery > 0 && manifest.Stories%cfg.PayloadEvery == 0 {
			if err := attachPayload(ctx, cfg, manifest.Stories, &story); err != nil {
				return manifest, err
			}
		}
		manifest.Stories++
		remaining := cfg.Events - manifest.Events
		if uint64(len(story.events)) > remaining {
			story.events = story.events[:remaining]
		}
		manifest.Events += uint64(len(story.events))
		buffers[story.service] = append(buffers[story.service], story.events...)
		for len(buffers[story.service]) >= cfg.BatchSize {
			if err := emit(story.service, buffers[story.service][:cfg.BatchSize]); err != nil {
				return manifest, err
			}
			buffers[story.service] = buffers[story.service][cfg.BatchSize:]
		}
	}
	for _, service := range []string{"order-api", "order-worker", "catalog", "webhooks", "notifications"} {
		if len(buffers[service]) > 0 {
			if err := emit(service, buffers[service]); err != nil {
				return manifest, err
			}
		}
	}
	return manifest, finish()
}

func attachPayload(ctx context.Context, cfg Config, index uint64, story *story) error {
	content, contentType := demoPayload(cfg.Seed, index)
	ref, err := cfg.PayloadStore.Put(ctx, strings.NewReader(content), payload.PutOptions{ContentType: contentType, Compression: payload.CompressionGZIP, RetentionClass: "demo"})
	if err != nil {
		return err
	}
	for i := range story.events {
		if story.events[i].Kind == "provider.http" {
			story.events[i].Fields = append(story.events[i].Fields, payloadFields("response", ref)...)
			story.events[i].Fields = append(story.events[i].Fields,
				numField("http.response_size", "int64", uint64(len(content))),
				textField("http.response_preview", preview(content, 320)),
			)
			return nil
		}
	}
	return nil
}

func demoPayload(seed, index uint64) (string, string) {
	if index%10 != 0 {
		return fmt.Sprintf(`{"generator_seed":%d,"story":%d,"provider_response":{"reference":"demo-%d","matched":true}}`, seed, index, index), "application/json"
	}
	var b strings.Builder
	b.Grow(256 << 10)
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><CatalogResponse seed="%d" story="%d"><Warehouse id="warehouse-a">`, seed, index)
	for i := 0; i < 1800; i++ {
		fmt.Fprintf(&b, `<Item sku="SKU-%d-%04d"><UpdatedAt>2026-01-%02dT%02d:%02d:00Z</UpdatedAt><Reference>TRAIL-DEMO-%d-%04d</Reference><Description>Generic catalog inventory record</Description><AvailableUnits>%d</AvailableUnits><State>%s</State></Item>`, index, i, i%28+1, i%24, i%60, index, i, (i%900)+1, []string{"available", "reserved"}[i%2])
	}
	b.WriteString(`</Warehouse></CatalogResponse>`)
	return b.String(), "application/xml"
}

func preview(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}
func payloadFields(role string, ref payload.Ref) []wire.Field {
	prefix := "payload." + role + "."
	out := []wire.Field{textField(prefix+"store", ref.Store), textField(prefix+"key", ref.Key), textField(prefix+"content_type", ref.ContentType), numField(prefix+"size", "int64", uint64(ref.Size)), numField(prefix+"stored_size", "int64", uint64(ref.StoredSize)), textField(prefix+"sha256", fmt.Sprintf("%x", ref.SHA256)), textField(prefix+"compression", string(ref.Compression)), textField(prefix+"retention_class", ref.RetentionClass), numField(prefix+"original_size", "int64", uint64(ref.Size)), numField(prefix+"captured_size", "int64", uint64(ref.Size)), boolField(prefix+"truncated", false)}
	return out
}

type story struct {
	service string
	events  []wire.Event
}

func buildStory(cfg Config, index uint64, rng *rand.Rand) story {
	services := []string{"order-api", "order-worker", "catalog", "webhooks", "notifications"}
	service := services[index%uint64(len(services))]
	entityIndex := index / 3
	entityID := "order_" + strconv.FormatUint(entityIndex, 10)
	attempt := uint32(index%3 + 1)
	executionID := deterministicID(cfg.Seed, "execution", index)
	base := cfg.Start.Add(time.Duration(index)*3*time.Second + time.Duration(rng.Uint64N(uint64(time.Second))))
	failure := index%7 == 0 || index%11 == 0
	executionStatus := convention.StatusSucceeded
	if failure {
		executionStatus = convention.StatusFailed
	}
	events := make([]wire.Event, 0, 24)
	sequence := uint64(0)
	newEvent := func(kind string, at time.Time, flowID, parentID string, fields ...wire.Field) wire.Event {
		id := deterministicID(cfg.Seed, "event", index<<16|sequence)
		sequence++
		return wire.Event{
			EventID: id, TimestampUnixNano: at.UnixNano(), Kind: kind, Level: "info", FlowID: flowID,
			ExecutionID: executionID, ExecutionAttempt: attempt, ExecutionSource: sourceFor(index),
			EntityType: "order", EntityID: entityID, ParentID: parentID, Fields: fields,
		}
	}
	start := newEvent("catalog.sync.started", base, "", "",
		textField(convention.FieldExecutionKind, "catalog.sync"),
		textField(convention.FieldExecutionStatus, string(convention.StatusRunning)),
	)
	if attempt > 1 {
		start.RetryOfExecutionID = deterministicID(cfg.Seed, "execution", index-1)
	}
	if index%9 == 0 && index > 0 {
		start.ParentExecutionID = deterministicID(cfg.Seed, "execution", index-1)
	}
	events = append(events, start)
	for flowNumber := uint64(0); flowNumber < 2+index%4; flowNumber++ {
		flowID := deterministicID(cfg.Seed, "flow", index<<8|flowNumber)
		at := base.Add(time.Duration(flowNumber+1) * 10 * time.Millisecond)
		flowStart := newEvent("order.fulfillment.started", at, flowID, start.EventID, textField(convention.FieldFlowStatus, string(convention.StatusRunning)))
		candidate := newEvent("order.fulfillment.inventory_reserved", at.Add(2*time.Millisecond), flowID, flowStart.EventID,
			textField("warehouse", warehouseFor(index+flowNumber)), numField("available_units", "int", 70+rng.Uint64N(30)))
		statusCode := uint64(200)
		flowFailed := failure && flowNumber == 0
		if flowFailed {
			if index%2 == 0 {
				statusCode = 503
			} else {
				statusCode = 408
			}
		}
		httpEvent := newEvent("provider.http", at.Add(time.Duration(5+rng.Uint64N(40))*time.Millisecond), flowID, candidate.EventID,
			textField("http.method", "POST"), textField("http.scheme", "https"), textField("http.host", "api.example"),
			textField("http.path", "/inventory/reservations"), numField("http.status_code", "int", statusCode),
			numField("http.duration", "duration", uint64((20+time.Duration(rng.Uint64N(200)))*time.Millisecond)),
			boolField("http.success", statusCode >= 200 && statusCode < 400))
		terminalKind := "order.fulfillment.completed"
		flowStatus := convention.StatusSucceeded
		if flowFailed {
			terminalKind, flowStatus, httpEvent.Level = "order.fulfillment.rejected", convention.StatusFailed, "warn"
		}
		terminal := newEvent(terminalKind, at.Add(250*time.Millisecond), flowID, httpEvent.EventID, textField(convention.FieldFlowStatus, string(flowStatus)))
		if flowFailed {
			terminal.Level = "error"
		}
		events = append(events, flowStart, candidate, httpEvent, terminal)
	}
	finished := newEvent("catalog.sync.completed", base.Add(2*time.Second), "", events[len(events)-1].EventID,
		textField(convention.FieldExecutionStatus, string(executionStatus)))
	if failure {
		finished.Kind, finished.Level = "catalog.sync.failed", "error"
	}
	events = append(events, finished)
	return story{service: service, events: events}
}

func deterministicID(seed uint64, namespace string, sequence uint64) string {
	var input [16]byte
	binary.BigEndian.PutUint64(input[:8], seed)
	binary.BigEndian.PutUint64(input[8:], sequence)
	hash := sha256.New()
	_, _ = hash.Write([]byte("trail-generator-v1:" + namespace + ":"))
	_, _ = hash.Write(input[:])
	sum := hash.Sum(nil)
	var id trail.EventID
	copy(id[:], sum[:16])
	return id.String()
}

func sourceFor(index uint64) string {
	sources := []trail.ExecutionSource{trail.SourceCron, trail.SourceAPI, trail.SourceQueue, trail.SourceWebhook, trail.SourceManual}
	return string(sources[index%uint64(len(sources))])
}

func warehouseFor(index uint64) string {
	providers := []string{"warehouse-a", "warehouse-b", "warehouse-c", "warehouse-d"}
	return providers[index%uint64(len(providers))]
}

func textField(key, value string) wire.Field {
	return wire.Field{Key: key, Type: "string", Text: value}
}
func numField(key, kind string, value uint64) wire.Field {
	return wire.Field{Key: key, Type: kind, Num: value}
}
func boolField(key string, value bool) wire.Field {
	var number uint64
	if value {
		number = 1
	}
	return wire.Field{Key: key, Type: "bool", Num: number}
}

func ValidateManifest(manifest Manifest) error {
	if manifest.Version != 1 {
		return fmt.Errorf("trail/generator: unsupported manifest version %d", manifest.Version)
	}
	return nil
}
