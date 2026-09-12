package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/archive"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/payload/filesystem"
	trails3 "github.com/vestavision/trail/payload/s3"
	"github.com/vestavision/trail/retention"
	trailch "github.com/vestavision/trail/storage/clickhouse"
	trailpg "github.com/vestavision/trail/storage/postgres"
)

type duration time.Duration

func (d *duration) UnmarshalJSON(data []byte) error {
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return errors.New("durations must be strings such as \"168h\"")
	}
	x, err := time.ParseDuration(v)
	*d = duration(x)
	return err
}

type matchFile struct {
	Kind            string `json:"kind,omitempty"`
	Service         string `json:"service,omitempty"`
	Environment     string `json:"environment,omitempty"`
	Level           string `json:"level,omitempty"`
	FlowStatus      string `json:"flow_status,omitempty"`
	ExecutionStatus string `json:"execution_status,omitempty"`
	HTTPStatusClass int    `json:"http_status_class,omitempty"`
	HTTPSuccess     *bool  `json:"http_success,omitempty"`
}
type eventPolicyFile struct {
	Name  string    `json:"name"`
	Match matchFile `json:"match"`
	TTL   duration  `json:"ttl"`
}
type payloadMatchFile struct {
	matchFile
	Role           string `json:"role,omitempty"`
	ContentType    string `json:"content_type,omitempty"`
	RetentionClass string `json:"retention_class,omitempty"`
}
type payloadPolicyFile struct {
	Name  string           `json:"name"`
	Match payloadMatchFile `json:"match"`
	TTL   duration         `json:"ttl"`
}
type configFile struct {
	EventPolicies      []eventPolicyFile   `json:"event_policies"`
	EventClasses       map[string]duration `json:"event_classes"`
	DefaultEventTTL    duration            `json:"default_event_ttl"`
	PayloadPolicies    []payloadPolicyFile `json:"payload_policies"`
	PayloadClasses     map[string]duration `json:"payload_classes"`
	DefaultPayloadTTL  duration            `json:"default_payload_ttl"`
	EventBatchSize     int                 `json:"event_batch_size"`
	PayloadBatchSize   int                 `json:"payload_batch_size"`
	PayloadConcurrency int                 `json:"payload_concurrency"`
	MaxEvents          int                 `json:"max_events"`
	MaxPayloads        int                 `json:"max_payloads"`
	MaxRuntime         duration            `json:"max_runtime"`
	PayloadGracePeriod duration            `json:"payload_grace_period"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "trail-retention:", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: trail-retention <run|archives|inspect|restore>")
	}
	if args[0] != "run" {
		return runArchiveCommand(ctx, args)
	}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	path := fs.String("config", env("TRAIL_RETENTION_CONFIG", ""), "retention policy JSON")
	dry := fs.Bool("dry-run", false, "report without deleting")
	maxEvents := fs.Int("max-events", 0, "override maximum events examined")
	maxPayloads := fs.Int("max-payloads", 0, "override maximum payload candidates")
	maxRuntime := fs.Duration("max-runtime", 0, "override maximum run duration")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("--config or TRAIL_RETENTION_CONFIG is required")
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if *maxEvents > 0 {
		cfg.MaxEvents = *maxEvents
	}
	if *maxPayloads > 0 {
		cfg.MaxPayloads = *maxPayloads
	}
	if *maxRuntime > 0 {
		cfg.MaxRuntime = *maxRuntime
	}
	store, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	payloads, err := openPayloads()
	if err != nil {
		return err
	}
	archiveStore, err := openArchiveStore()
	if err != nil {
		return err
	}
	archiver, err := archive.NewWriter(archiveStore, payloads)
	if err != nil {
		return err
	}
	cfg.Archiver = archiver
	engine, err := retention.New(cfg, store, payloads)
	if err != nil {
		return err
	}
	result, err := engine.Run(ctx, retention.RunOptions{DryRun: *dry})
	_ = json.NewEncoder(os.Stdout).Encode(result)
	return err
}

func runArchiveCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	id := fs.String("archive-id", "", "catalog archive ID")
	offset := fs.Int("offset", 0, "event offset")
	limit := fs.Int("limit", 100, "bounded event limit (1-10000)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	store, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	switch args[0] {
	case "archives":
		page, err := store.ListArchives(ctx, *offset, min(*limit, 500))
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(page)
		}
		return err
	case "inspect":
		if *id == "" {
			return errors.New("--archive-id is required")
		}
		receipt, err := store.GetArchive(ctx, *id)
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(receipt)
		}
		return err
	case "restore":
		if *id == "" {
			return errors.New("--archive-id is required")
		}
		archiveStore, err := openArchiveStore()
		if err != nil {
			return err
		}
		payloads, err := openPayloads()
		if err != nil {
			return err
		}
		receipt, err := store.GetArchive(ctx, *id)
		if err != nil {
			return err
		}
		result, err := archive.Restore(ctx, archiveStore, payloads, store, receipt, *offset, *limit)
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return err
	default:
		return errors.New("usage: trail-retention <run|archives|inspect|restore>")
	}
}

func openArchiveStore() (payload.Store, error) {
	switch env("TRAIL_ARCHIVE_STORE", "filesystem") {
	case "filesystem":
		root := os.Getenv("TRAIL_ARCHIVE_FILESYSTEM_ROOT")
		if root == "" {
			return nil, errors.New("TRAIL_ARCHIVE_FILESYSTEM_ROOT is required")
		}
		archiveRoot, err := filepath.Abs(root)
		if err != nil {
			return nil, err
		}
		if active := os.Getenv("TRAIL_PAYLOAD_FILESYSTEM_ROOT"); active != "" {
			activeRoot, err := filepath.Abs(active)
			if err != nil {
				return nil, err
			}
			if archiveRoot == activeRoot {
				return nil, errors.New("archive and active filesystem roots must be different")
			}
		}
		return filesystem.New(archiveRoot)
	case "s3":
		endpoint := os.Getenv("TRAIL_ARCHIVE_S3_ENDPOINT")
		if endpoint == "" {
			return nil, errors.New("TRAIL_ARCHIVE_S3_ENDPOINT is required")
		}
		archiveBucket := env("TRAIL_ARCHIVE_S3_BUCKET", "trail-archives")
		archivePrefix := env("TRAIL_ARCHIVE_S3_PREFIX", "archives")
		if endpoint == os.Getenv("TRAIL_PAYLOAD_S3_ENDPOINT") && archiveBucket == env("TRAIL_PAYLOAD_S3_BUCKET", "trail-payloads") && prefixesOverlap(archivePrefix, os.Getenv("TRAIL_PAYLOAD_S3_PREFIX")) {
			return nil, errors.New("archive and active S3 prefixes must not overlap")
		}
		return trails3.Open(trails3.Config{Endpoint: endpoint, AccessKey: os.Getenv("TRAIL_ARCHIVE_S3_ACCESS_KEY"), SecretKey: os.Getenv("TRAIL_ARCHIVE_S3_SECRET_KEY"), Bucket: archiveBucket, Region: os.Getenv("TRAIL_ARCHIVE_S3_REGION"), Prefix: archivePrefix, Secure: envBool("TRAIL_ARCHIVE_S3_SECURE", false), PathStyle: envBool("TRAIL_ARCHIVE_S3_PATH_STYLE", true), Name: "archive-s3"})
	default:
		return nil, errors.New("unknown TRAIL_ARCHIVE_STORE")
	}
}
func loadConfig(path string) (retention.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return retention.Config{}, err
	}
	var f configFile
	if err = json.Unmarshal(data, &f); err != nil {
		return retention.Config{}, err
	}
	c := retention.Config{DefaultEventTTL: time.Duration(f.DefaultEventTTL), DefaultPayloadTTL: time.Duration(f.DefaultPayloadTTL), EventBatchSize: f.EventBatchSize, PayloadBatchSize: f.PayloadBatchSize, PayloadConcurrency: f.PayloadConcurrency, MaxEvents: f.MaxEvents, MaxPayloads: f.MaxPayloads, MaxRuntime: time.Duration(f.MaxRuntime), PayloadGracePeriod: time.Duration(f.PayloadGracePeriod), EventClasses: map[string]time.Duration{}, PayloadClasses: map[string]time.Duration{}}
	for k, v := range f.EventClasses {
		c.EventClasses[k] = time.Duration(v)
	}
	for k, v := range f.PayloadClasses {
		c.PayloadClasses[k] = time.Duration(v)
	}
	for _, p := range f.EventPolicies {
		m, err := eventMatch(p.Match)
		if err != nil {
			return c, err
		}
		c.EventPolicies = append(c.EventPolicies, retention.EventPolicy{Name: p.Name, Match: m, TTL: time.Duration(p.TTL)})
	}
	for _, p := range f.PayloadPolicies {
		m, err := eventMatch(p.Match.matchFile)
		if err != nil {
			return c, err
		}
		c.PayloadPolicies = append(c.PayloadPolicies, retention.PayloadPolicy{Name: p.Name, TTL: time.Duration(p.TTL), Match: retention.PayloadMatch{EventMatch: m, Role: p.Match.Role, ContentType: p.Match.ContentType, RetentionClass: p.Match.RetentionClass}})
	}
	if len(c.EventPolicies) == 0 && len(c.EventClasses) == 0 && c.DefaultEventTTL == 0 {
		return c, errors.New("at least one event retention policy, class, or default TTL is required")
	}
	return c, nil
}
func eventMatch(f matchFile) (retention.EventMatch, error) {
	m := retention.EventMatch{Kind: f.Kind, Service: f.Service, Environment: f.Environment, FlowStatus: f.FlowStatus, ExecutionStatus: f.ExecutionStatus, HTTPStatusClass: f.HTTPStatusClass, HTTPSuccess: f.HTTPSuccess}
	if f.Level != "" {
		var l trail.Level
		switch f.Level {
		case "debug":
			l = trail.LevelDebug
		case "info":
			l = trail.LevelInfo
		case "warn":
			l = trail.LevelWarn
		case "error":
			l = trail.LevelError
		default:
			return m, fmt.Errorf("invalid level %q", f.Level)
		}
		m.Level = &l
	}
	return m, nil
}
func openStore(ctx context.Context) (interface {
	retention.Store
	archive.Catalog
	archive.RestoreStore
	Close() error
}, error) {
	switch env("TRAIL_STORE", "clickhouse") {
	case "clickhouse":
		s, e := trailch.Open(trailch.Config{Addresses: []string{env("TRAIL_CLICKHOUSE_ADDR", "127.0.0.1:9000")}, Database: env("TRAIL_CLICKHOUSE_DATABASE", "default"), Username: env("TRAIL_CLICKHOUSE_USERNAME", "default"), Password: os.Getenv("TRAIL_CLICKHOUSE_PASSWORD")})
		if e == nil {
			e = s.Migrate(ctx)
		}
		return s, e
	case "postgres":
		s, e := trailpg.Open(ctx, trailpg.Config{URL: env("TRAIL_POSTGRES_URL", "postgres://trail:trail@127.0.0.1:5432/trail?sslmode=disable"), MaxConnections: int32(envInt("TRAIL_POSTGRES_MAX_CONNECTIONS", 4))})
		if e == nil {
			e = s.Migrate(ctx)
		}
		return s, e
	default:
		return nil, errors.New("unknown TRAIL_STORE")
	}
}
func openPayloads() (map[string]payload.Store, error) {
	out := map[string]payload.Store{}
	if root := os.Getenv("TRAIL_PAYLOAD_FILESYSTEM_ROOT"); root != "" {
		s, e := filesystem.New(root)
		if e != nil {
			return nil, e
		}
		out["filesystem"] = s
	}
	if endpoint := os.Getenv("TRAIL_PAYLOAD_S3_ENDPOINT"); endpoint != "" {
		s, e := trails3.Open(trails3.Config{Endpoint: endpoint, AccessKey: os.Getenv("TRAIL_PAYLOAD_S3_ACCESS_KEY"), SecretKey: os.Getenv("TRAIL_PAYLOAD_S3_SECRET_KEY"), Bucket: env("TRAIL_PAYLOAD_S3_BUCKET", "trail-payloads"), Region: os.Getenv("TRAIL_PAYLOAD_S3_REGION"), Prefix: os.Getenv("TRAIL_PAYLOAD_S3_PREFIX"), Secure: envBool("TRAIL_PAYLOAD_S3_SECURE", false), PathStyle: envBool("TRAIL_PAYLOAD_S3_PATH_STYLE", true), Name: "s3"})
		if e != nil {
			return nil, e
		}
		out["s3"] = s
	}
	return out, nil
}

func prefixesOverlap(a, b string) bool {
	a = strings.Trim(strings.TrimSpace(a), "/")
	b = strings.Trim(strings.TrimSpace(b), "/")
	return a == b || a == "" || b == "" || strings.HasPrefix(a+"/", b+"/") || strings.HasPrefix(b+"/", a+"/")
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
	b, e := strconv.ParseBool(x)
	if e != nil {
		return v
	}
	return b
}
