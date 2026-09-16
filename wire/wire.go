// Package wire defines Trail's versioned transport envelope. It is separate
// from the core event model so transports can evolve without making JSON part
// of Trail's logging API.
package wire

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vestavision/trail"
)

const Version = 1

var (
	ErrInvalidEnvelope    = errors.New("trail/wire: invalid envelope")
	ErrUnsupportedVersion = errors.New("trail/wire: unsupported schema version")
)

type Envelope struct {
	SchemaVersion int            `json:"schema_version"`
	BatchID       string         `json:"batch_id"`
	Metadata      trail.Metadata `json:"metadata"`
	Events        []Event        `json:"events"`
}

type Event struct {
	EventID            string  `json:"event_id"`
	TimestampUnixNano  int64   `json:"timestamp_unix_nano"`
	Kind               string  `json:"kind"`
	Level              string  `json:"level"`
	FlowID             string  `json:"flow_id,omitempty"`
	ExecutionID        string  `json:"execution_id,omitempty"`
	ParentExecutionID  string  `json:"parent_execution_id,omitempty"`
	RetryOfExecutionID string  `json:"retry_of_execution_id,omitempty"`
	ExecutionAttempt   uint32  `json:"execution_attempt,omitempty"`
	ExecutionSource    string  `json:"execution_source,omitempty"`
	ScopeType          string  `json:"scope_type,omitempty"`
	ScopeID            string  `json:"scope_id,omitempty"`
	EntityType         string  `json:"entity_type,omitempty"`
	EntityID           string  `json:"entity_id,omitempty"`
	ParentID           string  `json:"parent_id,omitempty"`
	Fields             []Field `json:"fields,omitempty"`
}

// Field stores numeric values using the same lossless uint64 representation as
// trail.Field. Signed integers use two's-complement bits and floats use IEEE-754
// bits. Type determines how Num is interpreted, including when it is zero.
type Field struct {
	Key  string `json:"key"`
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	Num  uint64 `json:"num,omitempty"`
}

// MarshalBatch encodes one non-empty Trail batch. The first event ID is the
// stable batch ID, which also makes a suitable JetStream deduplication key.
func MarshalBatch(batch trail.Batch) ([]byte, error) {
	if len(batch.Events) == 0 {
		return nil, fmt.Errorf("%w: empty batch", ErrInvalidEnvelope)
	}
	envelope := Envelope{
		SchemaVersion: Version,
		BatchID:       batch.Events[0].ID.String(),
		Metadata:      batch.Metadata,
		Events:        make([]Event, len(batch.Events)),
	}
	for i := range batch.Events {
		envelope.Events[i] = fromEvent(batch.Events[i])
	}
	return Marshal(envelope)
}

// Marshal validates and encodes a constructed envelope. It is intended for
// deterministic generators and transport tooling; normal Trail sinks should
// continue to use MarshalBatch.
func Marshal(envelope Envelope) ([]byte, error) {
	if err := Validate(envelope); err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

// Validate checks the complete v1 envelope contract.
func Validate(envelope Envelope) error {
	if envelope.SchemaVersion != Version {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, envelope.SchemaVersion)
	}
	if envelope.BatchID == "" || len(envelope.Events) == 0 || envelope.Metadata.Service == "" {
		return ErrInvalidEnvelope
	}
	if _, err := trail.ParseEventID(envelope.BatchID); err != nil {
		return fmt.Errorf("%w: batch_id", ErrInvalidEnvelope)
	}
	for i := range envelope.Events {
		if err := validateEvent(envelope.Events[i]); err != nil {
			return fmt.Errorf("%w: event %d: %v", ErrInvalidEnvelope, i, err)
		}
	}
	return nil
}

// Unmarshal validates and decodes a wire envelope. It returns the transport
// representation deliberately; consumers should not need access to Field's
// private in-memory layout.
func Unmarshal(data []byte) (Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	if err := Validate(envelope); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func fromEvent(event trail.Event) Event {
	encoded := Event{
		EventID: event.ID.String(), TimestampUnixNano: event.Timestamp, Kind: event.Kind,
		Level: event.Level.String(), ScopeType: event.ScopeType, ScopeID: event.ScopeID,
		EntityType: event.EntityType, EntityID: event.EntityID,
	}
	if !event.FlowID.IsZero() {
		encoded.FlowID = event.FlowID.String()
	}
	if !event.ExecutionID.IsZero() {
		encoded.ExecutionID = event.ExecutionID.String()
	}
	if !event.ParentExecutionID.IsZero() {
		encoded.ParentExecutionID = event.ParentExecutionID.String()
	}
	if !event.RetryOfExecutionID.IsZero() {
		encoded.RetryOfExecutionID = event.RetryOfExecutionID.String()
	}
	encoded.ExecutionAttempt = event.ExecutionAttempt
	encoded.ExecutionSource = string(event.ExecutionSource)
	if !event.ParentID.IsZero() {
		encoded.ParentID = event.ParentID.String()
	}
	if len(event.Fields) != 0 {
		encoded.Fields = make([]Field, len(event.Fields))
		for i := range event.Fields {
			field := event.Fields[i]
			encoded.Fields[i] = Field{Key: field.Key(), Type: fieldType(field.Kind())}
			switch field.Kind() {
			case trail.FieldString, trail.FieldError:
				encoded.Fields[i].Text = field.Text()
			default:
				encoded.Fields[i].Num = uint64(field.Int64())
			}
		}
	}
	return encoded
}

func fieldType(kind trail.FieldKind) string {
	switch kind {
	case trail.FieldString:
		return "string"
	case trail.FieldInt:
		return "int"
	case trail.FieldInt64:
		return "int64"
	case trail.FieldBool:
		return "bool"
	case trail.FieldFloat64:
		return "float64"
	case trail.FieldDuration:
		return "duration"
	case trail.FieldError:
		return "error"
	default:
		return "unknown"
	}
}

func validateEvent(event Event) error {
	if event.EventID == "" || event.TimestampUnixNano == 0 || event.Kind == "" {
		return errors.New("missing required value")
	}
	if _, err := trail.ParseEventID(event.EventID); err != nil {
		return errors.New("invalid event_id")
	}
	if event.FlowID != "" {
		if _, err := trail.ParseFlowID(event.FlowID); err != nil {
			return errors.New("invalid flow_id")
		}
	}
	if event.ExecutionID != "" {
		if _, err := trail.ParseExecutionID(event.ExecutionID); err != nil {
			return errors.New("invalid execution_id")
		}
	}
	if event.ParentExecutionID != "" {
		if _, err := trail.ParseExecutionID(event.ParentExecutionID); err != nil {
			return errors.New("invalid parent_execution_id")
		}
	}
	if event.RetryOfExecutionID != "" {
		if _, err := trail.ParseExecutionID(event.RetryOfExecutionID); err != nil {
			return errors.New("invalid retry_of_execution_id")
		}
	}
	if event.ParentID != "" {
		if _, err := trail.ParseEventID(event.ParentID); err != nil {
			return errors.New("invalid parent_id")
		}
	}
	if (event.ScopeType == "") != (event.ScopeID == "") {
		return errors.New("scope_type and scope_id must be provided together")
	}
	switch event.Level {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("invalid level")
	}
	for _, field := range event.Fields {
		if field.Key == "" {
			return errors.New("empty field key")
		}
		switch field.Type {
		case "string", "int", "int64", "bool", "float64", "duration", "error":
		default:
			return errors.New("invalid field type")
		}
	}
	return nil
}
