package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vestavision/trail/explorerapi"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/payload/filesystem"
	trails3 "github.com/vestavision/trail/payload/s3"
	"github.com/vestavision/trail/storage"
	trailch "github.com/vestavision/trail/storage/clickhouse"
	trailpg "github.com/vestavision/trail/storage/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintln(os.Stderr, "trail-explorer-api:", err)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	store, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	payloads := map[string]payload.Store{}
	if root := os.Getenv("TRAIL_PAYLOAD_FILESYSTEM_ROOT"); root != "" {
		fs, err := filesystem.New(root)
		if err != nil {
			return err
		}
		payloads["filesystem"] = fs
	}
	if endpoint := os.Getenv("TRAIL_PAYLOAD_S3_ENDPOINT"); endpoint != "" {
		s3, err := trails3.Open(trails3.Config{Endpoint: endpoint, AccessKey: os.Getenv("TRAIL_PAYLOAD_S3_ACCESS_KEY"), SecretKey: os.Getenv("TRAIL_PAYLOAD_S3_SECRET_KEY"), Bucket: env("TRAIL_PAYLOAD_S3_BUCKET", "trail-payloads"), Region: os.Getenv("TRAIL_PAYLOAD_S3_REGION"), Secure: envBool("TRAIL_PAYLOAD_S3_SECURE", false), PathStyle: envBool("TRAIL_PAYLOAD_S3_PATH_STYLE", true), Name: "s3"})
		if err != nil {
			return err
		}
		if envBool("TRAIL_PAYLOAD_S3_ENSURE_BUCKET", false) {
			if err := s3.EnsureBucket(ctx); err != nil {
				return err
			}
		}
		payloads["s3"] = s3
	}
	handler, err := explorerapi.New(explorerapi.Config{Store: store, PayloadStores: payloads, MaxPayloadBytes: int64(envInt("TRAIL_EXPLORER_MAX_PAYLOAD_BYTES", 1<<20)), AllowedOrigins: split(os.Getenv("TRAIL_EXPLORER_ALLOWED_ORIGINS"))})
	if err != nil {
		return err
	}
	server := &http.Server{Addr: env("TRAIL_EXPLORER_ADDR", ":8080"), Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func openStore(ctx context.Context) (storage.ExplorerStore, error) {
	switch env("TRAIL_STORE", "clickhouse") {
	case "clickhouse":
		s, e := trailch.Open(trailch.Config{Addresses: []string{env("TRAIL_CLICKHOUSE_ADDR", "127.0.0.1:9000")}, Database: env("TRAIL_CLICKHOUSE_DATABASE", "default"), Username: env("TRAIL_CLICKHOUSE_USERNAME", "default"), Password: os.Getenv("TRAIL_CLICKHOUSE_PASSWORD")})
		if e != nil {
			return nil, e
		}
		if e = s.Migrate(ctx); e != nil {
			_ = s.Close()
			return nil, e
		}
		return s, nil
	case "postgres":
		s, e := trailpg.Open(ctx, trailpg.Config{URL: env("TRAIL_POSTGRES_URL", "postgres://trail:trail@127.0.0.1:5432/trail?sslmode=disable"), MaxConnections: int32(envInt("TRAIL_POSTGRES_MAX_CONNECTIONS", 8))})
		if e != nil {
			return nil, e
		}
		if e = s.Migrate(ctx); e != nil {
			_ = s.Close()
			return nil, e
		}
		return s, nil
	default:
		return nil, fmt.Errorf("unknown TRAIL_STORE")
	}
}
func env(k, v string) string {
	if x := os.Getenv(k); x != "" {
		return x
	}
	return v
}
func envInt(k string, v int) int {
	x, e := strconv.Atoi(os.Getenv(k))
	if e != nil || x <= 0 {
		return v
	}
	return x
}
func envBool(k string, v bool) bool {
	x := os.Getenv(k)
	if x == "" {
		return v
	}
	parsed, e := strconv.ParseBool(x)
	if e != nil {
		return v
	}
	return parsed
}
func split(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}
