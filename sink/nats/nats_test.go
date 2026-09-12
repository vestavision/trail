package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vestavision/trail"
	"github.com/vestavision/trail/wire"
)

func runServer(t *testing.T, enableJetStream bool) *natsserver.Server {
	t.Helper()
	options := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: enableJetStream}
	if enableJetStream {
		options.StoreDir = t.TempDir()
	}
	server, err := natsserver.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		server.Shutdown()
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	return server
}

func testBatch(events int) trail.Batch {
	batch := trail.Batch{Metadata: trail.Metadata{Service: "test"}, Events: make([]trail.Event, events)}
	for i := range batch.Events {
		batch.Events[i] = trail.Event{
			ID: trail.EventID(trail.NewFlow()), Timestamp: time.Now().UnixNano(), Kind: "test.event", Level: trail.LevelInfo,
		}
	}
	return batch
}

func TestCorePublishesEnvelopeAndBorrowsConnection(t *testing.T) {
	server := runServer(t, false)
	conn, err := gonats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	sub, err := conn.SubscribeSync("trail.events.v1")
	if err != nil {
		t.Fatal(err)
	}
	sink, err := New(conn, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteBatch(testBatch(2)); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Header.Get("Trail-Schema-Version") != "1" {
		t.Fatalf("headers = %v", msg.Header)
	}
	envelope, err := wire.Unmarshal(msg.Data)
	if err != nil || len(envelope.Events) != 2 {
		t.Fatalf("envelope=%+v err=%v", envelope, err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.IsClosed() {
		t.Fatal("borrowed connection was closed")
	}
	if stats := sink.Stats(); stats.Messages != 1 || stats.Events != 2 || stats.Bytes == 0 || stats.Errors != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := sink.WriteBatch(testBatch(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close = %v", err)
	}
}

func TestJetStreamPublishesWithAcknowledgement(t *testing.T) {
	server := runServer(t, true)
	conn, err := gonats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TRAIL_EVENTS", Subjects: []string{"trail.events.v1"}}); err != nil {
		t.Fatal(err)
	}
	sink, err := New(conn, Config{Mode: JetStream})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteBatch(testBatch(1)); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "TRAIL_EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("stream info=%+v err=%v", info, err)
	}
}

func TestFailureAndLimits(t *testing.T) {
	server := runServer(t, false)
	conn, err := gonats.Connect(server.ClientURL(), gonats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	sink, err := New(conn, Config{MaxMessageBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteBatch(testBatch(1)); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversize error = %v", err)
	}
	server.Shutdown()
	server.WaitForShutdown()
	deadline := time.Now().Add(time.Second)
	for !conn.IsClosed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := sink.WriteBatch(testBatch(1)); err == nil {
		t.Fatal("write to unavailable NATS succeeded")
	}
	if sink.Stats().Errors != 2 {
		t.Fatalf("stats = %+v", sink.Stats())
	}
}

func TestConnectOwnsConnection(t *testing.T) {
	server := runServer(t, false)
	sink, err := Connect(server.ClientURL(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn := sink.conn
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if !conn.IsClosed() {
		t.Fatal("owned connection remains open")
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(nil, Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil connection error = %v", err)
	}
	server := runServer(t, false)
	conn, err := gonats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	for _, cfg := range []Config{{Mode: 99}, {Subject: "bad.*"}, {PublishTimeout: -1}, {MaxMessageBytes: -1}} {
		if _, err := New(conn, cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("config %+v error = %v", cfg, err)
		}
	}
}
