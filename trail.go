// Package trail provides lightweight structured operational event journaling.
package trail

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var global struct {
	mu      sync.Mutex
	current atomic.Pointer[writer]
}

type writer struct {
	metadata Metadata
	sink     Sink
	policy   FullPolicy
	queue    chan Event
	stop     chan struct{}
	done     chan struct{}
	wake     chan struct{}

	accepting  atomic.Bool
	inflight   atomic.Int64
	batchSize  int
	interval   time.Duration
	publishErr error
	closeErr   error
}

// Init installs the process-global Trail writer. A successful call transfers
// ownership of cfg.Sink to Trail.
func Init(cfg Config) error {
	global.mu.Lock()
	defer global.mu.Unlock()
	if global.current.Load() != nil {
		return ErrAlreadyInitialized
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return err
	}
	w := &writer{
		metadata:  Metadata{Service: normalized.Service, Environment: normalized.Environment, Version: normalized.Version},
		sink:      normalized.Sink,
		policy:    normalized.FullPolicy,
		queue:     make(chan Event, normalized.BufferCapacity),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		wake:      make(chan struct{}, 1),
		batchSize: normalized.BatchSize,
		interval:  normalized.FlushInterval,
	}
	w.accepting.Store(true)
	global.current.Store(w)
	go w.run()
	return nil
}

// Log emits an event to the global writer. It is always fire-and-forget.
func Log(kind string, options ...Option) {
	w := global.current.Load()
	if w == nil || !w.accepting.Load() {
		counters.dropped.Add(1)
		return
	}

	event := Event{ID: newEventID(), Timestamp: time.Now().UnixNano(), Kind: kind, Level: LevelInfo}
	fieldCount := 0
	for i := range options {
		if options[i].kind == optionField {
			fieldCount++
		}
	}
	if fieldCount != 0 {
		event.Fields = make([]Field, 0, fieldCount)
	}
	for i := range options {
		option := &options[i]
		switch option.kind {
		case optionField:
			event.Fields = append(event.Fields, option.field)
		case optionFlow:
			event.FlowID = FlowID(option.id)
		case optionExecution:
			event.ExecutionID = ExecutionID(option.id)
		case optionScope:
			event.ScopeType, event.ScopeID = option.entityType, option.entityID
		case optionEntity:
			event.EntityType, event.EntityID = option.entityType, option.entityID
		case optionParent:
			event.ParentID = EventID(option.id)
		case optionLevel:
			event.Level = option.level
		case optionParentExecution:
			event.ParentExecutionID = ExecutionID(option.id)
		case optionRetryOfExecution:
			event.RetryOfExecutionID = ExecutionID(option.id)
		case optionExecutionAttempt:
			event.ExecutionAttempt = option.attempt
		case optionExecutionSource:
			event.ExecutionSource = option.source
		}
	}
	w.enqueue(event)
}

func (w *writer) enqueue(event Event) {
	w.inflight.Add(1)
	defer func() {
		w.inflight.Add(-1)
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}()
	if !w.accepting.Load() {
		counters.dropped.Add(1)
		return
	}

	counters.buffered.Add(1)
	if w.policy == Block {
		select {
		case w.queue <- event:
		case <-w.stop:
			counters.buffered.Add(^uint64(0))
			counters.dropped.Add(1)
		}
		return
	}
	select {
	case w.queue <- event:
	default:
		counters.buffered.Add(^uint64(0))
		counters.dropped.Add(1)
	}
}

// Close stops the active writer, drains accepted events, and closes its Sink.
// Calling Close without an active writer is safe and returns nil.
func Close() error {
	global.mu.Lock()
	defer global.mu.Unlock()
	w := global.current.Swap(nil)
	if w == nil {
		return nil
	}
	w.accepting.Store(false)
	close(w.stop)
	<-w.done
	return errors.Join(w.publishErr, w.closeErr)
}

func (w *writer) run() {
	defer close(w.done)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	batch := make([]Event, 0, w.batchSize)

	for {
		select {
		case event := <-w.queue:
			batch = append(batch, event)
			if len(batch) == w.batchSize {
				w.publish(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) != 0 {
				w.publish(batch)
				batch = batch[:0]
			}
		case <-w.stop:
			w.waitForProducers()
			for {
				select {
				case event := <-w.queue:
					batch = append(batch, event)
					if len(batch) == w.batchSize {
						w.publish(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) != 0 {
						w.publish(batch)
					}
					if err := w.sink.Close(); err != nil {
						w.closeErr = err
					}
					return
				}
			}
		}
	}
}

func (w *writer) waitForProducers() {
	for w.inflight.Load() != 0 {
		<-w.wake
	}
}

func (w *writer) publish(events []Event) {
	err := w.sink.WriteBatch(Batch{Metadata: w.metadata, Events: events})
	count := uint64(len(events))
	if err != nil {
		counters.publishErrors.Add(1)
		counters.dropped.Add(count)
		if w.publishErr == nil {
			w.publishErr = err
		}
	} else {
		counters.written.Add(count)
	}
	counters.buffered.Add(^uint64(count - 1))
}
