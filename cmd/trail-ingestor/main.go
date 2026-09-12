package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vestavision/trail/ingestor"
	"github.com/vestavision/trail/storage"
	trailch "github.com/vestavision/trail/storage/clickhouse"
	trailpg "github.com/vestavision/trail/storage/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintln(os.Stderr, "trail-ingestor:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	store, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	conn, err := gonats.Connect(env("TRAIL_NATS_URL", gonats.DefaultURL), gonats.Name("trail-ingestor"))
	if err != nil {
		return err
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}
	streamName := env("TRAIL_NATS_STREAM", "TRAIL_EVENTS")
	consumerName := env("TRAIL_NATS_CONSUMER", "TRAIL_INGESTOR")
	subject := env("TRAIL_NATS_SUBJECT", "trail.events.v1")
	if envBool("TRAIL_MANAGE_STREAM", false) {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: streamName, Subjects: []string{subject}, Storage: jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, MaxAge: 30 * 24 * time.Hour,
		}); err != nil {
			return err
		}
		if _, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable: consumerName, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second,
			MaxDeliver: envInt("TRAIL_INGEST_MAX_DELIVERIES", 5), MaxAckPending: envInt("TRAIL_INGEST_MAX_MESSAGES", 64) * 2,
		}); err != nil {
			return err
		}
	}
	consumer, err := js.Consumer(ctx, streamName, consumerName)
	if err != nil {
		return err
	}
	engine, err := ingestor.New(ingestor.Config{
		MaxMessages: envInt("TRAIL_INGEST_MAX_MESSAGES", 64), MaxEvents: envInt("TRAIL_INGEST_MAX_EVENTS", 4096),
		MaxBytes: envInt("TRAIL_INGEST_MAX_BYTES", 8<<20), MaxDeliveries: uint64(envInt("TRAIL_INGEST_MAX_DELIVERIES", 5)),
		DLQSubject: env("TRAIL_INGEST_DLQ_SUBJECT", "trail.events.dlq.v1"),
	}, consumer, store, dlqPublisher{js: js})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: env("TRAIL_INGEST_HTTP_ADDR", ":8081"), Handler: healthHandler(engine), ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = server.ListenAndServe() }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return engine.Run(ctx)
}

func openStore(ctx context.Context) (storage.IngestStore, error) {
	switch env("TRAIL_STORE", "clickhouse") {
	case "clickhouse":
		store, err := trailch.Open(trailch.Config{
			Addresses: []string{env("TRAIL_CLICKHOUSE_ADDR", "127.0.0.1:9000")}, Database: env("TRAIL_CLICKHOUSE_DATABASE", "default"),
			Username: env("TRAIL_CLICKHOUSE_USERNAME", "default"), Password: os.Getenv("TRAIL_CLICKHOUSE_PASSWORD"),
		})
		if err != nil {
			return nil, err
		}
		if err := store.Migrate(ctx); err != nil {
			_ = store.Close()
			return nil, err
		}
		return store, nil
	case "postgres":
		store, err := trailpg.Open(ctx, trailpg.Config{
			URL:            env("TRAIL_POSTGRES_URL", "postgres://trail:trail@127.0.0.1:5432/trail?sslmode=disable"),
			MaxConnections: int32(envInt("TRAIL_POSTGRES_MAX_CONNECTIONS", 8)),
		})
		if err != nil {
			return nil, err
		}
		if err := store.Migrate(ctx); err != nil {
			_ = store.Close()
			return nil, err
		}
		return store, nil
	default:
		return nil, fmt.Errorf("trail-ingestor: unknown TRAIL_STORE")
	}
}

type dlqPublisher struct{ js jetstream.JetStream }

func (p dlqPublisher) PublishDLQ(ctx context.Context, subject string, data []byte, header gonats.Header) error {
	_, err := p.js.PublishMsg(ctx, &gonats.Msg{Subject: subject, Data: data, Header: header})
	return err
}

func healthHandler(engine *ingestor.Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /stats", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(engine.Stats())
	})
	return mux
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
