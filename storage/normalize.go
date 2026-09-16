package storage

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/convention"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/wire"
)

func Normalize(envelope wire.Envelope, delivery Delivery) (Batch, error) {
	if err := wire.Validate(envelope); err != nil {
		return Batch{}, err
	}
	batchID, _ := trail.ParseEventID(envelope.BatchID)
	batch := Batch{BatchID: batchID, Metadata: envelope.Metadata, Delivery: delivery, Events: make([]EventRecord, len(envelope.Events))}
	for i := range envelope.Events {
		record, err := normalizeEvent(envelope.Events[i], envelope.Metadata, batchID, delivery)
		if err != nil {
			return Batch{}, fmt.Errorf("event %d: %w", i, err)
		}
		batch.Events[i] = record
	}
	return batch, nil
}

func normalizeEvent(event wire.Event, metadata trail.Metadata, batchID trail.EventID, delivery Delivery) (EventRecord, error) {
	id, _ := trail.ParseEventID(event.EventID)
	record := EventRecord{
		ID: id, Timestamp: time.Unix(0, event.TimestampUnixNano).UTC(), Kind: event.Kind,
		ExecutionAttempt: event.ExecutionAttempt, ExecutionSource: trail.ExecutionSource(event.ExecutionSource),
		Scope:      trail.Scope{Type: event.ScopeType, ID: event.ScopeID},
		EntityType: event.EntityType, EntityID: event.EntityID, Fields: append([]wire.Field(nil), event.Fields...),
		BatchID: batchID, Metadata: metadata, Delivery: delivery,
	}
	record.Level = parseLevel(event.Level)
	if event.FlowID != "" {
		record.FlowID, _ = trail.ParseFlowID(event.FlowID)
	}
	if event.ExecutionID != "" {
		record.ExecutionID, _ = trail.ParseExecutionID(event.ExecutionID)
	}
	if event.ParentExecutionID != "" {
		record.ParentExecutionID, _ = trail.ParseExecutionID(event.ParentExecutionID)
	}
	if event.RetryOfExecutionID != "" {
		record.RetryOfExecutionID, _ = trail.ParseExecutionID(event.RetryOfExecutionID)
	}
	if event.ParentID != "" {
		record.ParentID, _ = trail.ParseEventID(event.ParentID)
	}

	payloads := make(map[string]*PayloadLink)
	for _, field := range event.Fields {
		switch field.Key {
		case convention.FieldFlowStatus:
			record.FlowStatus = field.Text
		case convention.FieldExecutionStatus:
			record.ExecutionStatus = field.Text
		case convention.FieldExecutionKind:
			record.ExecutionKind = field.Text
		case convention.FieldRetentionClass:
			record.RetentionClass = field.Text
		case "provider":
			record.Provider = field.Text
		case "http.method":
			record.HTTP.Method = field.Text
		case "http.scheme":
			record.HTTP.Scheme = field.Text
		case "http.host":
			record.HTTP.Host = field.Text
		case "http.path":
			record.HTTP.Path = field.Text
		case "http.status_code":
			record.HTTP.StatusCode = int(int64(field.Num))
		case "http.duration":
			record.HTTP.Duration = time.Duration(field.Num)
		case "http.success":
			record.HTTP.Success = field.Num != 0
		case "http.request_size":
			record.HTTP.RequestSize = int64(field.Num)
		case "http.response_size":
			record.HTTP.ResponseSize = int64(field.Num)
		case "http.request_preview":
			record.HTTP.RequestPreview = field.Text
		case "http.response_preview":
			record.HTTP.ResponsePreview = field.Text
		default:
			if field.Type == "error" {
				record.HasError = true
			}
			if strings.HasPrefix(field.Key, "payload.") {
				applyPayloadField(payloads, field)
			}
		}
	}
	roles := make([]string, 0, len(payloads))
	for role := range payloads {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		link := payloads[role]
		if link.Ref.Store != "" || link.Ref.Key != "" || link.Error != "" {
			record.Payloads = append(record.Payloads, *link)
		}
	}
	return record, nil
}

func parseLevel(level string) trail.Level {
	switch level {
	case "debug":
		return trail.LevelDebug
	case "warn":
		return trail.LevelWarn
	case "error":
		return trail.LevelError
	default:
		return trail.LevelInfo
	}
}

func applyPayloadField(links map[string]*PayloadLink, field wire.Field) {
	parts := strings.SplitN(strings.TrimPrefix(field.Key, "payload."), ".", 2)
	if len(parts) != 2 || parts[0] == "" {
		return
	}
	link := links[parts[0]]
	if link == nil {
		link = &PayloadLink{Role: parts[0]}
		links[parts[0]] = link
	}
	switch parts[1] {
	case "store":
		link.Ref.Store = field.Text
	case "key":
		link.Ref.Key = field.Text
	case "content_type":
		link.Ref.ContentType = field.Text
	case "size":
		link.Ref.Size = int64(field.Num)
	case "stored_size":
		link.Ref.StoredSize = int64(field.Num)
	case "sha256":
		decoded, err := hex.DecodeString(field.Text)
		if err == nil && len(decoded) == len(link.Ref.SHA256) {
			copy(link.Ref.SHA256[:], decoded)
		}
	case "compression":
		link.Ref.Compression = payload.Compression(field.Text)
	case "retain_until_unix_nano":
		link.Ref.RetainUntil = time.Unix(0, int64(field.Num)).UTC()
	case "retention_class":
		link.Ref.RetentionClass = field.Text
	case "original_size":
		link.OriginalSize = int64(field.Num)
	case "captured_size":
		link.CapturedSize = int64(field.Num)
	case "truncated":
		link.Truncated = field.Num != 0
	case "error":
		link.Error = field.Text
	}
}

// Float64Value decodes the lossless numeric bits of a float wire field.
func Float64Value(field wire.Field) float64 { return math.Float64frombits(field.Num) }
