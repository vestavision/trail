// Package ingestor moves bounded Trail envelopes from JetStream into a storage
// adapter and acknowledges them only after a durable write.
package ingestor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vestavision/trail/storage"
	"github.com/vestavision/trail/wire"
)

type Config struct {
	MaxMessages   int
	MaxEvents     int
	MaxBytes      int
	FetchWait     time.Duration
	WriteTimeout  time.Duration
	AckTimeout    time.Duration
	MaxDeliveries uint64
	NakDelay      time.Duration
	DLQSubject    string
}

type DLQPublisher interface {
	PublishDLQ(context.Context, string, []byte, gonats.Header) error
}

type Statistics struct {
	MessagesFetched uint64 `json:"messages_fetched"`
	MessagesDecoded uint64 `json:"messages_decoded"`
	MessagesAcked   uint64 `json:"messages_acked"`
	MessagesDLQ     uint64 `json:"messages_dlq"`
	Redeliveries    uint64 `json:"redeliveries"`
	EventsWritten   uint64 `json:"events_written"`
	BytesFetched    uint64 `json:"bytes_fetched"`
	DecodeErrors    uint64 `json:"decode_errors"`
	WriteErrors     uint64 `json:"write_errors"`
	AckErrors       uint64 `json:"ack_errors"`
}

type counters struct {
	messagesFetched atomic.Uint64
	messagesDecoded atomic.Uint64
	messagesAcked   atomic.Uint64
	messagesDLQ     atomic.Uint64
	redeliveries    atomic.Uint64
	eventsWritten   atomic.Uint64
	bytesFetched    atomic.Uint64
	decodeErrors    atomic.Uint64
	writeErrors     atomic.Uint64
	ackErrors       atomic.Uint64
}

type Engine struct {
	cfg      Config
	consumer jetstream.Consumer
	store    storage.IngestStore
	dlq      DLQPublisher
	counters counters
}

func New(cfg Config, consumer jetstream.Consumer, store storage.IngestStore, dlq DLQPublisher) (*Engine, error) {
	if consumer == nil || store == nil || dlq == nil {
		return nil, errors.New("trail/ingestor: consumer, store, and DLQ publisher are required")
	}
	if cfg.MaxMessages <= 0 {
		cfg.MaxMessages = 64
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = 4096
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 8 << 20
	}
	if cfg.FetchWait <= 0 {
		cfg.FetchWait = time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 2 * time.Second
	}
	if cfg.MaxDeliveries == 0 {
		cfg.MaxDeliveries = 5
	}
	if cfg.NakDelay <= 0 {
		cfg.NakDelay = time.Second
	}
	if cfg.DLQSubject == "" {
		cfg.DLQSubject = "trail.events.dlq.v1"
	}
	return &Engine{cfg: cfg, consumer: consumer, store: store, dlq: dlq}, nil
}

type pending struct {
	message jetstream.Msg
	batch   storage.Batch
	bytes   int
}

func (e *Engine) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		result, err := e.consumer.Fetch(e.cfg.MaxMessages, jetstream.FetchMaxWait(e.cfg.FetchWait))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		group := make([]pending, 0, e.cfg.MaxMessages)
		events, bytes := 0, 0
		for message := range result.Messages() {
			item, ok := e.decode(ctx, message)
			if !ok {
				continue
			}
			if len(group) != 0 && (events+len(item.batch.Events) > e.cfg.MaxEvents || bytes+item.bytes > e.cfg.MaxBytes) {
				if err := e.write(ctx, group); err != nil {
					e.reject(group, err)
					group = group[:0]
					events, bytes = 0, 0
				} else {
					group = group[:0]
					events, bytes = 0, 0
				}
			}
			group = append(group, item)
			events += len(item.batch.Events)
			bytes += item.bytes
		}
		if len(group) != 0 {
			if err := e.write(ctx, group); err != nil {
				e.reject(group, err)
			}
		}
		if err := result.Error(); err != nil && !errors.Is(err, gonats.ErrTimeout) && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

func (e *Engine) decode(ctx context.Context, message jetstream.Msg) (pending, bool) {
	data := message.Data()
	e.counters.messagesFetched.Add(1)
	e.counters.bytesFetched.Add(uint64(len(data)))
	metadata, err := message.Metadata()
	if err != nil {
		e.counters.decodeErrors.Add(1)
		e.deadLetter(ctx, message, "metadata", err)
		return pending{}, false
	}
	if metadata.NumDelivered > 1 {
		e.counters.redeliveries.Add(1)
	}
	if len(data) > e.cfg.MaxBytes {
		e.counters.decodeErrors.Add(1)
		e.deadLetter(ctx, message, "oversized", fmt.Errorf("message is %d bytes", len(data)))
		return pending{}, false
	}
	envelope, err := wire.Unmarshal(data)
	if err != nil {
		e.counters.decodeErrors.Add(1)
		e.deadLetter(ctx, message, "decode", err)
		return pending{}, false
	}
	batch, err := storage.Normalize(envelope, storage.Delivery{
		Stream: metadata.Stream, Consumer: metadata.Consumer, StreamSequence: metadata.Sequence.Stream,
		ConsumerSequence: metadata.Sequence.Consumer, Delivered: metadata.NumDelivered, ReceivedAt: time.Now().UTC(),
	})
	if err != nil {
		e.counters.decodeErrors.Add(1)
		e.deadLetter(ctx, message, "normalize", err)
		return pending{}, false
	}
	e.counters.messagesDecoded.Add(1)
	return pending{message: message, batch: batch, bytes: len(data)}, true
}

func (e *Engine) write(ctx context.Context, group []pending) error {
	batches := make([]storage.Batch, len(group))
	eventCount := 0
	for i := range group {
		batches[i] = group[i].batch
		eventCount += len(group[i].batch.Events)
	}
	writeCtx, cancel := context.WithTimeout(ctx, e.cfg.WriteTimeout)
	err := e.store.WriteBatches(writeCtx, batches)
	cancel()
	if err != nil {
		e.counters.writeErrors.Add(1)
		return err
	}
	e.counters.eventsWritten.Add(uint64(eventCount))
	for _, item := range group {
		ackCtx, cancel := context.WithTimeout(ctx, e.cfg.AckTimeout)
		err := item.message.DoubleAck(ackCtx)
		cancel()
		if err != nil {
			e.counters.ackErrors.Add(1)
			continue
		}
		e.counters.messagesAcked.Add(1)
	}
	return nil
}

func (e *Engine) reject(group []pending, cause error) {
	for _, item := range group {
		metadata, err := item.message.Metadata()
		if err == nil && metadata.NumDelivered >= e.cfg.MaxDeliveries {
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.AckTimeout)
			e.deadLetter(ctx, item.message, "max-deliveries", cause)
			cancel()
			continue
		}
		_ = item.message.NakWithDelay(e.cfg.NakDelay)
	}
}

func (e *Engine) deadLetter(ctx context.Context, message jetstream.Msg, reason string, cause error) {
	header := gonats.Header{}
	header.Set("Trail-DLQ-Reason", reason)
	header.Set("Trail-DLQ-Error", boundedError(cause))
	header.Set("Trail-Original-Subject", message.Subject())
	if err := e.dlq.PublishDLQ(ctx, e.cfg.DLQSubject, message.Data(), header); err != nil {
		_ = message.NakWithDelay(e.cfg.NakDelay)
		return
	}
	if err := message.Ack(); err != nil {
		e.counters.ackErrors.Add(1)
		return
	}
	e.counters.messagesDLQ.Add(1)
	e.counters.messagesAcked.Add(1)
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

func (e *Engine) Stats() Statistics {
	return Statistics{
		MessagesFetched: e.counters.messagesFetched.Load(), MessagesDecoded: e.counters.messagesDecoded.Load(),
		MessagesAcked: e.counters.messagesAcked.Load(), MessagesDLQ: e.counters.messagesDLQ.Load(),
		Redeliveries: e.counters.redeliveries.Load(), EventsWritten: e.counters.eventsWritten.Load(),
		BytesFetched: e.counters.bytesFetched.Load(), DecodeErrors: e.counters.decodeErrors.Load(),
		WriteErrors: e.counters.writeErrors.Load(), AckErrors: e.counters.ackErrors.Load(),
	}
}
