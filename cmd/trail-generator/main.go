package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vestavision/trail/generator"
	trails3 "github.com/vestavision/trail/payload/s3"
	trailnats "github.com/vestavision/trail/sink/nats"
	"github.com/vestavision/trail/wire"
)

func main() {
	var cfg generator.Config
	var output, natsURL, subject, startText, s3Endpoint, s3Bucket, s3Access, s3Secret string
	flag.Uint64Var(&cfg.Events, "events", 100000, "exact number of events to generate")
	flag.Uint64Var(&cfg.Seed, "seed", 42, "deterministic generator seed")
	flag.IntVar(&cfg.BatchSize, "batch-size", 64, "events per wire envelope")
	flag.IntVar(&cfg.PublishConcurrency, "publish-concurrency", 16, "bounded concurrent publishers")
	flag.StringVar(&startText, "start", "2026-01-01T00:00:00Z", "logical start time in RFC3339")
	flag.StringVar(&output, "output", "nats", "output mode: nats or ndjson")
	flag.StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS URL")
	flag.StringVar(&subject, "subject", "trail.events.v1", "NATS subject")
	flag.Uint64Var(&cfg.PayloadEvery, "payload-every", 0, "persist one payload every N stories (0 disables)")
	flag.StringVar(&s3Endpoint, "payload-s3-endpoint", "", "S3-compatible endpoint without scheme")
	flag.StringVar(&s3Bucket, "payload-s3-bucket", "trail-payloads", "S3 payload bucket")
	flag.StringVar(&s3Access, "payload-s3-access-key", "", "S3 access key")
	flag.StringVar(&s3Secret, "payload-s3-secret-key", "", "S3 secret key")
	flag.Parse()
	start, err := time.Parse(time.RFC3339Nano, startText)
	if err != nil {
		fatal(err)
	}
	cfg.Start = start
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if s3Endpoint != "" {
		store, err := trails3.Open(trails3.Config{Endpoint: s3Endpoint, AccessKey: s3Access, SecretKey: s3Secret, Bucket: s3Bucket, PathStyle: true, Name: "s3"})
		if err != nil {
			fatal(err)
		}
		if err = store.EnsureBucket(ctx); err != nil {
			fatal(err)
		}
		cfg.PayloadStore = store
		if cfg.PayloadEvery == 0 {
			cfg.PayloadEvery = 1000
		}
	}

	var publish generator.PublishFunc
	var closeOutput func() error
	switch output {
	case "ndjson":
		encoder := json.NewEncoder(os.Stdout)
		publish = func(_ context.Context, envelope wire.Envelope) error { return encoder.Encode(envelope) }
		closeOutput = func() error { return nil }
	case "nats":
		sink, err := trailnats.Connect(natsURL, trailnats.Config{Mode: trailnats.JetStream, Subject: subject})
		if err != nil {
			fatal(err)
		}
		publish = sink.PublishEnvelope
		closeOutput = sink.Close
	default:
		fatal(fmt.Errorf("unknown output %q", output))
	}
	manifest, err := generator.Generate(ctx, cfg, publish)
	closeErr := closeOutput()
	if err != nil {
		fatal(err)
	}
	if closeErr != nil {
		fatal(closeErr)
	}
	if err := json.NewEncoder(os.Stderr).Encode(manifest); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "trail-generator:", err)
	os.Exit(1)
}
