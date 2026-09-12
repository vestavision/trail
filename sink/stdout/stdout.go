// Package stdout provides a newline-delimited JSON Sink.
package stdout

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/vestavision/trail"
)

var (
	ErrClosed    = errors.New("trail/stdout: sink closed")
	ErrNilWriter = errors.New("trail/stdout: nil writer")
)

type Sink struct {
	mu     sync.Mutex
	writer io.Writer
	closed bool
}

func New(writer io.Writer) *Sink { return &Sink{writer: writer} }

type encodedEvent struct {
	EventID            string         `json:"event_id"`
	Timestamp          string         `json:"timestamp"`
	Kind               string         `json:"kind"`
	Level              string         `json:"level"`
	FlowID             string         `json:"flow_id,omitempty"`
	ExecutionID        string         `json:"execution_id,omitempty"`
	ParentExecutionID  string         `json:"parent_execution_id,omitempty"`
	RetryOfExecutionID string         `json:"retry_of_execution_id,omitempty"`
	ExecutionAttempt   uint32         `json:"execution_attempt,omitempty"`
	ExecutionSource    string         `json:"execution_source,omitempty"`
	EntityType         string         `json:"entity_type,omitempty"`
	EntityID           string         `json:"entity_id,omitempty"`
	ParentID           string         `json:"parent_id,omitempty"`
	Service            string         `json:"service"`
	Environment        string         `json:"environment,omitempty"`
	Version            string         `json:"version,omitempty"`
	Fields             map[string]any `json:"fields,omitempty"`
}

func (s *Sink) WriteBatch(batch trail.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.writer == nil {
		return ErrNilWriter
	}
	encoder := json.NewEncoder(s.writer)
	for _, event := range batch.Events {
		encoded := encodedEvent{
			EventID: event.ID.String(), Timestamp: time.Unix(0, event.Timestamp).UTC().Format(time.RFC3339Nano),
			Kind: event.Kind, Level: event.Level.String(), EntityType: event.EntityType, EntityID: event.EntityID,
			Service: batch.Metadata.Service, Environment: batch.Metadata.Environment, Version: batch.Metadata.Version,
			ExecutionAttempt: event.ExecutionAttempt, ExecutionSource: string(event.ExecutionSource),
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
		if !event.ParentID.IsZero() {
			encoded.ParentID = event.ParentID.String()
		}
		if len(event.Fields) != 0 {
			encoded.Fields = make(map[string]any, len(event.Fields))
			for _, field := range event.Fields {
				encoded.Fields[field.Key()] = fieldValue(field)
			}
		}
		if err := encoder.Encode(encoded); err != nil {
			return err
		}
	}
	return nil
}

func fieldValue(field trail.Field) any {
	switch field.Kind() {
	case trail.FieldString, trail.FieldError:
		return field.Text()
	case trail.FieldInt, trail.FieldInt64:
		return field.Int64()
	case trail.FieldBool:
		return field.Bool()
	case trail.FieldFloat64:
		return field.Float64()
	case trail.FieldDuration:
		return field.Duration().String()
	default:
		return nil
	}
}

// Close prevents later writes. It deliberately does not close the io.Writer,
// whose ownership remains with the caller.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
