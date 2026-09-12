// Package nats publishes versioned Trail batches to NATS or JetStream.
package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vestavision/trail"
	"github.com/vestavision/trail/wire"
)

const (
	defaultSubject         = "trail.events.v1"
	defaultPublishTimeout  = 2 * time.Second
	defaultMaxMessageBytes = 1024 * 1024
)

var (
	ErrClosed           = errors.New("trail/nats: sink closed")
	ErrInvalidConfig    = errors.New("trail/nats: invalid config")
	ErrMessageTooLarge  = errors.New("trail/nats: message too large")
	ErrConnectionClosed = errors.New("trail/nats: connection closed")
)

type Mode uint8

const (
	Core Mode = iota
	JetStream
)

type Config struct {
	Subject         string
	Mode            Mode
	PublishTimeout  time.Duration
	MaxMessageBytes int
}

type Statistics struct {
	Messages uint64
	Events   uint64
	Bytes    uint64
	Errors   uint64
}

type Sink struct {
	mu       sync.RWMutex
	conn     *gonats.Conn
	js       jetstream.JetStream
	cfg      Config
	owned    bool
	closed   bool
	messages atomic.Uint64
	events   atomic.Uint64
	bytes    atomic.Uint64
	errors   atomic.Uint64
}

// New creates a sink that borrows conn. Closing the sink never closes or drains
// a caller-owned connection.
func New(conn *gonats.Conn, cfg Config) (*Sink, error) {
	return newSink(conn, cfg, false)
}

// Connect creates a sink and an owned NATS connection. Closing the sink closes
// that connection exactly once.
func Connect(url string, cfg Config, options ...gonats.Option) (*Sink, error) {
	normalized, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := gonats.Connect(url, options...)
	if err != nil {
		return nil, err
	}
	sink, err := newSink(conn, normalized, true)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return sink, nil
}

func newSink(conn *gonats.Conn, cfg Config, owned bool) (*Sink, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: nil connection", ErrInvalidConfig)
	}
	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}
	normalized, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	sink := &Sink{conn: conn, cfg: normalized, owned: owned}
	if normalized.Mode == JetStream {
		sink.js, err = jetstream.New(conn)
		if err != nil {
			return nil, err
		}
	}
	return sink, nil
}

func normalize(cfg Config) (Config, error) {
	if cfg.Mode != Core && cfg.Mode != JetStream {
		return Config{}, fmt.Errorf("%w: unknown mode", ErrInvalidConfig)
	}
	if cfg.PublishTimeout < 0 || cfg.MaxMessageBytes < 0 {
		return Config{}, fmt.Errorf("%w: negative limit", ErrInvalidConfig)
	}
	if cfg.Subject == "" {
		cfg.Subject = defaultSubject
	}
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = defaultPublishTimeout
	}
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = defaultMaxMessageBytes
	}
	if !validPublishSubject(cfg.Subject) {
		return Config{}, fmt.Errorf("%w: invalid subject", ErrInvalidConfig)
	}
	return cfg, nil
}

func validPublishSubject(subject string) bool {
	if subject == "" || strings.ContainsAny(subject, " \t\r\n*>") {
		return false
	}
	for _, token := range strings.Split(subject, ".") {
		if token == "" {
			return false
		}
	}
	return true
}

func (s *Sink) WriteBatch(batch trail.Batch) error {
	data, err := wire.MarshalBatch(batch)
	if err != nil {
		s.errors.Add(1)
		return err
	}
	return s.publish(context.Background(), data, batch.Events[0].ID.String(), len(batch.Events))
}

// PublishEnvelope publishes an already constructed and validated envelope.
// It exists for deterministic generators and transport tooling; application
// logging should normally use Trail's Sink path.
func (s *Sink) PublishEnvelope(ctx context.Context, envelope wire.Envelope) error {
	data, err := wire.Marshal(envelope)
	if err != nil {
		s.errors.Add(1)
		return err
	}
	return s.publish(ctx, data, envelope.BatchID, len(envelope.Events))
}

func (s *Sink) publish(ctx context.Context, data []byte, batchID string, eventCount int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	if s.conn.IsClosed() {
		s.errors.Add(1)
		return ErrConnectionClosed
	}
	s.bytes.Add(uint64(len(data)))
	if len(data) > s.cfg.MaxMessageBytes {
		s.errors.Add(1)
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(data), s.cfg.MaxMessageBytes)
	}
	var err error

	msg := &gonats.Msg{Subject: s.cfg.Subject, Data: data}
	msg.Header = gonats.Header{}
	msg.Header.Set("Content-Type", "application/vnd.vestavision.trail.batch+json")
	msg.Header.Set("Trail-Schema-Version", "1")
	if s.cfg.Mode == JetStream {
		ctx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
		_, err = s.js.PublishMsg(ctx, msg, jetstream.WithMsgID(batchID))
		cancel()
	} else {
		err = s.conn.PublishMsg(msg)
		if err == nil {
			err = s.conn.FlushTimeout(s.cfg.PublishTimeout)
		}
	}
	if err != nil {
		s.errors.Add(1)
		return err
	}
	s.messages.Add(1)
	s.events.Add(uint64(eventCount))
	return nil
}

func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.owned {
		s.conn.Close()
		return nil
	}
	if s.conn.IsClosed() {
		return nil
	}
	return s.conn.FlushTimeout(s.cfg.PublishTimeout)
}

func (s *Sink) Stats() Statistics {
	return Statistics{
		Messages: s.messages.Load(), Events: s.events.Load(),
		Bytes: s.bytes.Load(), Errors: s.errors.Load(),
	}
}
