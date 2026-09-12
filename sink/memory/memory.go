// Package memory provides a concurrency-safe Sink for tests and local use.
package memory

import (
	"errors"
	"sync"

	"github.com/vestavision/trail"
)

var ErrClosed = errors.New("trail/memory: sink closed")

type Sink struct {
	mu       sync.RWMutex
	events   []trail.Event
	metadata trail.Metadata
	closed   bool
}

func New() *Sink { return &Sink{} }

func (s *Sink) WriteBatch(batch trail.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.metadata = batch.Metadata
	for i := range batch.Events {
		s.events = append(s.events, cloneEvent(batch.Events[i]))
	}
	return nil
}

// Events returns a deep snapshot of all events received by the sink.
func (s *Sink) Events() []trail.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]trail.Event, len(s.events))
	for i := range s.events {
		out[i] = cloneEvent(s.events[i])
	}
	return out
}

func (s *Sink) Metadata() trail.Metadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metadata
}

func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func cloneEvent(event trail.Event) trail.Event {
	copy := event
	copy.Fields = append([]trail.Field(nil), event.Fields...)
	return copy
}
