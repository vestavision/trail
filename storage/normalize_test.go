package storage

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/convention"
	"github.com/vestavision/trail/wire"
)

func TestNormalizePromotesPortableFields(t *testing.T) {
	eventID := trail.EventID(trail.NewFlow()).String()
	flowID := trail.NewFlow().String()
	executionID := trail.NewExecution().String()
	checksum := make([]byte, 32)
	for i := range checksum {
		checksum[i] = byte(i)
	}
	envelope := wire.Envelope{
		SchemaVersion: wire.Version, BatchID: eventID, Metadata: trail.Metadata{Service: "matcher", Environment: "test"},
		Events: []wire.Event{{
			EventID: eventID, TimestampUnixNano: time.Unix(10, 20).UnixNano(), Kind: "provider.http", Level: "warn",
			FlowID: flowID, ExecutionID: executionID,
			Fields: []wire.Field{
				{Key: convention.FieldFlowStatus, Type: "string", Text: "awaiting_review"},
				{Key: convention.FieldExecutionStatus, Type: "string", Text: string(convention.StatusFailed)},
				{Key: convention.FieldRetentionClass, Type: "string", Text: "audit"},
				{Key: "http.status_code", Type: "int", Num: 503},
				{Key: "http.success", Type: "bool"},
				{Key: "payload.response.store", Type: "string", Text: "filesystem"},
				{Key: "payload.response.key", Type: "string", Text: "object"},
				{Key: "payload.response.sha256", Type: "string", Text: hex.EncodeToString(checksum)},
				{Key: "payload.response.truncated", Type: "bool", Num: 1},
			},
		}},
	}
	delivery := Delivery{Stream: "TRAIL_EVENTS", StreamSequence: 7, ReceivedAt: time.Now()}
	batch, err := Normalize(envelope, delivery)
	if err != nil {
		t.Fatal(err)
	}
	record := batch.Events[0]
	if record.FlowStatus != "awaiting_review" || record.ExecutionStatus != "failed" || record.RetentionClass != "audit" || record.HTTP.StatusCode != 503 || record.HTTP.Success {
		t.Fatalf("record=%+v", record)
	}
	if len(record.Payloads) != 1 || record.Payloads[0].Role != "response" || !record.Payloads[0].Truncated || record.Payloads[0].Ref.SHA256 != [32]byte(checksum) {
		t.Fatalf("payloads=%+v", record.Payloads)
	}
	if record.Delivery.StreamSequence != 7 || record.Metadata.Service != "matcher" {
		t.Fatalf("delivery/metadata=%+v %+v", record.Delivery, record.Metadata)
	}
}

func TestNormalizeRejectsInvalidEnvelope(t *testing.T) {
	if _, err := Normalize(wire.Envelope{}, Delivery{}); err == nil {
		t.Fatal("invalid envelope normalized")
	}
}
