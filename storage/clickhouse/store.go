// Package clickhouse implements Trail storage using ClickHouse.
package clickhouse

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/vestavision/trail/storage"
)

type Config struct {
	Addresses   []string
	Database    string
	Username    string
	Password    string
	TLS         *tls.Config
	DialTimeout time.Duration
}

type Store struct{ conn ch.Conn }

const insertEvents = `INSERT INTO trail_events (
    event_time, event_id, batch_id,
    jetstream_stream, jetstream_stream_seq, jetstream_consumer, jetstream_consumer_seq, jetstream_delivered, received_at,
    service, environment, version, kind, level,
    flow_id, execution_id, parent_execution_id, retry_of_execution_id, execution_attempt, execution_source,
    scope_type, scope_id, entity_type, entity_id, parent_event_id, flow_status, execution_status, execution_kind,
    provider, has_error,
    field_keys, field_types, field_text, field_num,
    http_method, http_scheme, http_host, http_path, http_status_code, http_duration_ns, http_success,
    http_request_size, http_response_size, http_request_preview, http_response_preview, payload_refs_json
)`

func Open(cfg Config) (*Store, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("trail/clickhouse: address is required")
	}
	if cfg.Database == "" {
		cfg.Database = "default"
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	conn, err := ch.Open(&ch.Options{
		Addr: cfg.Addresses,
		Auth: ch.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		TLS:  cfg.TLS, DialTimeout: cfg.DialTimeout, Compression: &ch.Compression{Method: ch.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}
	return &Store{conn: conn}, nil
}

func New(conn ch.Conn) *Store { return &Store{conn: conn} }

func (s *Store) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	for i, statement := range migrationStatements {
		if err := s.conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("clickhouse migration %d: %w", i+1, err)
		}
	}
	return nil
}

func (s *Store) Close() error { return s.conn.Close() }

func (s *Store) WriteBatches(ctx context.Context, batches []storage.Batch) error {
	if len(batches) == 0 {
		return nil
	}
	token := deduplicationToken(batches)
	ctx = ch.Context(ctx, ch.WithSettings(ch.Settings{"insert_deduplication_token": token}))
	batch, err := s.conn.PrepareBatch(ctx, insertEvents)
	if err != nil {
		return err
	}
	for _, input := range batches {
		for i := range input.Events {
			if err := appendEvent(batch, input.Events[i]); err != nil {
				_ = batch.Abort()
				return err
			}
		}
	}
	return batch.Send()
}

func appendEvent(batch driver.Batch, event storage.EventRecord) error {
	keys := make([]string, len(event.Fields))
	types := make([]string, len(event.Fields))
	texts := make([]string, len(event.Fields))
	nums := make([]uint64, len(event.Fields))
	for i, field := range event.Fields {
		keys[i], types[i], texts[i], nums[i] = field.Key, field.Type, field.Text, field.Num
	}
	payloadJSON, err := json.Marshal(event.Payloads)
	if err != nil {
		return err
	}
	return batch.Append(
		event.Timestamp, idBytes(event.ID), idBytes(event.BatchID),
		event.Delivery.Stream, event.Delivery.StreamSequence, event.Delivery.Consumer, event.Delivery.ConsumerSequence,
		event.Delivery.Delivered, event.Delivery.ReceivedAt,
		event.Metadata.Service, event.Metadata.Environment, event.Metadata.Version, event.Kind, event.Level.String(),
		idBytes(event.FlowID), idBytes(event.ExecutionID), idBytes(event.ParentExecutionID), idBytes(event.RetryOfExecutionID),
		event.ExecutionAttempt, string(event.ExecutionSource), event.Scope.Type, event.Scope.ID,
		event.EntityType, event.EntityID, idBytes(event.ParentID),
		event.FlowStatus, event.ExecutionStatus, event.ExecutionKind, event.Provider, boolByte(event.HasError),
		keys, types, texts, nums,
		event.HTTP.Method, event.HTTP.Scheme, event.HTTP.Host, event.HTTP.Path, uint16(event.HTTP.StatusCode),
		uint64(event.HTTP.Duration), boolByte(event.HTTP.Success), event.HTTP.RequestSize, event.HTTP.ResponseSize,
		event.HTTP.RequestPreview, event.HTTP.ResponsePreview, string(payloadJSON),
	)
}

func deduplicationToken(batches []storage.Batch) string {
	identities := make([]string, len(batches))
	for i, batch := range batches {
		identities[i] = fmt.Sprintf("%s:%d:%s", batch.Delivery.Stream, batch.Delivery.StreamSequence, batch.BatchID.String())
	}
	sort.Strings(identities)
	sum := sha256.Sum256([]byte(strings.Join(identities, "|")))
	return hex.EncodeToString(sum[:])
}

func idBytes[T ~[16]byte](id T) string { return string(id[:]) }
func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

var _ storage.IngestStore = (*Store)(nil)
