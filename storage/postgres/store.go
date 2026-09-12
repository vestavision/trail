// Package postgres implements moderate-volume Trail storage using PostgreSQL.
package postgres

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vestavision/trail/storage"
)

type Config struct {
	URL            string
	TLS            *tls.Config
	MaxConnections int32
	ConnectTimeout time.Duration
}

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("trail/postgres: URL is required")
	}
	parsed, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("trail/postgres: parse config: %w", err)
	}
	if cfg.TLS != nil {
		parsed.ConnConfig.TLSConfig = cfg.TLS
	}
	if cfg.MaxConnections > 0 {
		parsed.MaxConns = cfg.MaxConnections
	}
	if cfg.ConnectTimeout > 0 {
		parsed.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func New(pool *pgxpool.Pool) *Store             { return &Store{pool: pool} }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close() error                   { s.pool.Close(); return nil }

func (s *Store) Migrate(ctx context.Context) error {
	for i, statement := range migrationStatements {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("postgres migration %d: %w", i+1, err)
		}
	}
	return nil
}

func (s *Store) WriteBatches(ctx context.Context, batches []storage.Batch) error {
	if len(batches) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	batch := &pgx.Batch{}
	for _, input := range batches {
		for i := range input.Events {
			e := input.Events[i]
			fields, err := json.Marshal(e.Fields)
			if err != nil {
				return err
			}
			payloads, err := json.Marshal(e.Payloads)
			if err != nil {
				return err
			}
			batch.Queue(insertEventSQL, eventValues(e, fields, payloads)...)
		}
	}
	results := tx.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	if err := results.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

var insertEventSQL = `INSERT INTO trail_events (` +
	`event_time,event_id,batch_id,jetstream_stream,jetstream_stream_seq,jetstream_consumer,jetstream_consumer_seq,jetstream_delivered,received_at,service,environment,version,kind,level,flow_id,execution_id,parent_execution_id,retry_of_execution_id,execution_attempt,execution_source,entity_type,entity_id,parent_event_id,flow_status,execution_status,execution_kind,fields,http_method,http_scheme,http_host,http_path,http_status_code,http_duration_ns,http_success,http_request_size,http_response_size,http_request_preview,http_response_preview,payload_refs` +
	`) VALUES (` + placeholders(39) + `) ON CONFLICT (event_id) DO NOTHING`

func eventValues(e storage.EventRecord, fields, payloads []byte) []any {
	return []any{e.Timestamp, idBytes(e.ID), idBytes(e.BatchID), e.Delivery.Stream, e.Delivery.StreamSequence, e.Delivery.Consumer, e.Delivery.ConsumerSequence, e.Delivery.Delivered, e.Delivery.ReceivedAt, e.Metadata.Service, e.Metadata.Environment, e.Metadata.Version, e.Kind, e.Level.String(), nullableID(e.FlowID), nullableID(e.ExecutionID), nullableID(e.ParentExecutionID), nullableID(e.RetryOfExecutionID), e.ExecutionAttempt, string(e.ExecutionSource), e.EntityType, e.EntityID, nullableID(e.ParentID), e.FlowStatus, e.ExecutionStatus, e.ExecutionKind, fields, e.HTTP.Method, e.HTTP.Scheme, e.HTTP.Host, e.HTTP.Path, e.HTTP.StatusCode, e.HTTP.Duration.Nanoseconds(), e.HTTP.Success, e.HTTP.RequestSize, e.HTTP.ResponseSize, e.HTTP.RequestPreview, e.HTTP.ResponsePreview, payloads}
}
func idBytes[T ~[16]byte](id T) []byte { b := make([]byte, 16); copy(b, id[:]); return b }
func nullableID[T ~[16]byte](id T) any {
	var z T
	if id == z {
		return nil
	}
	return idBytes(id)
}
func placeholders(n int) string {
	out := ""
	for i := 1; i <= n; i++ {
		if i > 1 {
			out += ","
		}
		out += fmt.Sprintf("$%d", i)
	}
	return out
}

var _ storage.IngestStore = (*Store)(nil)
