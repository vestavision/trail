package main

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEnsureJetStreamTopologyCreatesMissingResources(t *testing.T) {
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS did not become ready")
	}
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })

	conn, err := gonats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ensureJetStreamTopology(ctx, js, "TRAIL_EVENTS", "TRAIL_INGESTOR", "trail.events.v1", 5, 64); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "TRAIL_EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Consumer(ctx, "TRAIL_INGESTOR"); err != nil {
		t.Fatal(err)
	}
	if err := ensureJetStreamTopology(ctx, js, "TRAIL_EVENTS", "TRAIL_INGESTOR", "trail.events.v1", 5, 64); err != nil {
		t.Fatal(err)
	}
}
