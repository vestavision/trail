package ingestor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vestavision/trail"
	"github.com/vestavision/trail/storage"
	"github.com/vestavision/trail/wire"
)

type testStore struct {
	fail    atomic.Int32
	mu      sync.Mutex
	batches []storage.Batch
}

func (s *testStore) WriteBatches(_ context.Context, b []storage.Batch) error {
	if s.fail.Add(-1) >= 0 {
		return errors.New("unavailable")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, b...)
	return nil
}
func (*testStore) Close() error { return nil }

type testDLQ struct{ calls atomic.Int32 }

func (d *testDLQ) PublishDLQ(context.Context, string, []byte, gonats.Header) error {
	d.calls.Add(1)
	return nil
}

func TestRedeliveryIsWrittenBeforeAck(t *testing.T) {
	server := startServer(t)
	conn, err := gonats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, _ := jetstream.New(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "TRAIL", Subjects: []string{"trail.events.v1"}})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := js.CreateConsumer(ctx, "TRAIL", jetstream.ConsumerConfig{Durable: "INGEST", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 100 * time.Millisecond, MaxDeliver: 5})
	if err != nil {
		t.Fatal(err)
	}
	id := trail.EventID(trail.NewFlow())
	envelope := wire.Envelope{SchemaVersion: wire.Version, BatchID: id.String(), Metadata: trail.Metadata{Service: "test"}, Events: []wire.Event{{EventID: id.String(), TimestampUnixNano: time.Now().UnixNano(), Kind: "test", Level: "info"}}}
	data, _ := wire.Marshal(envelope)
	if _, err = js.Publish(ctx, "trail.events.v1", data); err != nil {
		t.Fatal(err)
	}
	store := &testStore{}
	store.fail.Store(1)
	dlq := &testDLQ{}
	engine, err := New(Config{MaxMessages: 1, FetchWait: 50 * time.Millisecond, NakDelay: 10 * time.Millisecond, WriteTimeout: time.Second, AckTimeout: time.Second}, consumer, store, dlq)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- engine.Run(runCtx) }()
	deadline := time.Now().Add(5 * time.Second)
	for engine.Stats().MessagesAcked < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("engine did not stop")
	}
	stats := engine.Stats()
	if stats.WriteErrors != 1 || stats.Redeliveries < 1 || stats.EventsWritten != 1 || stats.MessagesAcked != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if dlq.calls.Load() != 0 {
		t.Fatal("unexpected DLQ")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.batches) != 1 || len(store.batches[0].Events) != 1 {
		t.Fatalf("batches=%d", len(store.batches))
	}
}

func startServer(t *testing.T) *natsserver.Server {
	t.Helper()
	options := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()}
	server, err := natsserver.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS did not start")
	}
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	return server
}
