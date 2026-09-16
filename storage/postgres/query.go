package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/vestavision/trail"
	"github.com/vestavision/trail/storage"
)

const flowSummarySelect = `SELECT flow_id, service, environment,
 COALESCE((array_agg(scope_type ORDER BY event_time DESC,event_id DESC) FILTER(WHERE scope_type<>''))[1],''),
 COALESCE((array_agg(scope_id ORDER BY event_time DESC,event_id DESC) FILTER(WHERE scope_id<>''))[1],''),
 COALESCE((array_agg(entity_type ORDER BY event_time DESC,event_id DESC) FILTER(WHERE entity_type<>''))[1],''),
 COALESCE((array_agg(entity_id ORDER BY event_time DESC,event_id DESC) FILTER(WHERE entity_id<>''))[1],''),
 COALESCE((array_agg(execution_id ORDER BY event_time DESC,event_id DESC) FILTER(WHERE execution_id IS NOT NULL))[1],decode(repeat('00',16),'hex')),
 COALESCE((array_agg(flow_status ORDER BY event_time DESC,event_id DESC) FILTER(WHERE flow_status<>''))[1],''),
 min(event_time),max(event_time),count(DISTINCT event_id),
 (array_agg(kind ORDER BY event_time,event_id))[1],(array_agg(kind ORDER BY event_time DESC,event_id DESC))[1]
 FROM trail_events WHERE flow_id IS NOT NULL`

func (s *Store) ListFlows(ctx context.Context, f storage.FlowFilter, p storage.PageRequest) (storage.Page[storage.FlowSummary], error) {
	b := newWhere(1)
	applyCommon(b, f.Time, f.Service, f.Environment)
	applyScope(b, f.Scope)
	b.add("flow_id IS NOT NULL")
	if f.EntityType != "" {
		b.eq("entity_type", f.EntityType)
	}
	if f.EntityID != "" {
		b.eq("entity_id", f.EntityID)
	}
	if !f.ExecutionID.IsZero() {
		b.eq("execution_id", idBytes(f.ExecutionID))
	}
	q := flowSummarySelect + b.sql() + ` GROUP BY flow_id,service,environment`
	args := b.args
	if f.Status != "" {
		args = append(args, f.Status)
		q += fmt.Sprintf(` HAVING COALESCE((array_agg(flow_status ORDER BY event_time DESC,event_id DESC) FILTER(WHERE flow_status<>''))[1],'')=$%d`, len(args))
	}
	q, args = paginateGrouped(q, args, p, "min(event_time)", "flow_id")
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return storage.Page[storage.FlowSummary]{}, err
	}
	defer rows.Close()
	items := make([]storage.FlowSummary, 0, p.Limit+1)
	for rows.Next() {
		v, _, _, err := scanFlow(rows)
		if err != nil {
			return storage.Page[storage.FlowSummary]{}, err
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		return storage.Page[storage.FlowSummary]{}, err
	}
	return makePage(items, p, func(v storage.FlowSummary) storage.Cursor {
		return storage.Cursor{Timestamp: v.StartedAt, ID: v.ID.String()}
	})
}
func (s *Store) GetFlow(ctx context.Context, id trail.FlowID) (storage.FlowDetail, error) {
	row := s.pool.QueryRow(ctx, flowSummarySelect+` AND flow_id=$1 GROUP BY flow_id,service,environment`, idBytes(id))
	summary, first, last, err := scanFlow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.FlowDetail{}, storage.ErrNotFound
	}
	return storage.FlowDetail{FlowSummary: summary, FirstKind: first, LastKind: last}, err
}
func (s *Store) ListExecutionFlows(ctx context.Context, id trail.ExecutionID, f storage.FlowFilter, p storage.PageRequest) (storage.Page[storage.FlowSummary], error) {
	f.ExecutionID = id
	return s.ListFlows(ctx, f, p)
}
func (s *Store) ListEntityFlows(ctx context.Context, key storage.EntityKey, f storage.FlowFilter, p storage.PageRequest) (storage.Page[storage.FlowSummary], error) {
	f.EntityType = key.Type
	f.EntityID = key.ID
	return s.ListFlows(ctx, f, p)
}

const executionSummarySelect = `SELECT execution_id,service,environment,
 COALESCE((array_agg(scope_type ORDER BY event_time DESC,event_id DESC) FILTER(WHERE scope_type<>''))[1],''),
 COALESCE((array_agg(scope_id ORDER BY event_time DESC,event_id DESC) FILTER(WHERE scope_id<>''))[1],''),
 COALESCE((array_agg(execution_kind ORDER BY event_time DESC,event_id DESC) FILTER(WHERE execution_kind<>''))[1],''),
 COALESCE((array_agg(execution_source ORDER BY event_time DESC,event_id DESC) FILTER(WHERE execution_source<>''))[1],''),
 COALESCE((array_agg(execution_status ORDER BY event_time DESC,event_id DESC) FILTER(WHERE execution_status<>''))[1],''),max(execution_attempt),
 COALESCE((array_agg(parent_execution_id ORDER BY event_time DESC,event_id DESC) FILTER(WHERE parent_execution_id IS NOT NULL))[1],decode(repeat('00',16),'hex')),
 COALESCE((array_agg(retry_of_execution_id ORDER BY event_time DESC,event_id DESC) FILTER(WHERE retry_of_execution_id IS NOT NULL))[1],decode(repeat('00',16),'hex')),
 min(event_time),max(event_time),count(DISTINCT event_id),count(DISTINCT flow_id) FILTER(WHERE flow_id IS NOT NULL)
 FROM trail_events WHERE execution_id IS NOT NULL`

func (s *Store) ListExecutions(ctx context.Context, f storage.ExecutionFilter, p storage.PageRequest) (storage.Page[storage.ExecutionSummary], error) {
	b := newWhere(1)
	applyCommon(b, f.Time, f.Service, f.Environment)
	applyScope(b, f.Scope)
	b.add("execution_id IS NOT NULL")
	if f.Source != "" {
		b.eq("execution_source", string(f.Source))
	}
	if f.Kind != "" {
		b.eq("execution_kind", f.Kind)
	}
	q := executionSummarySelect + b.sql() + ` GROUP BY execution_id,service,environment`
	args := b.args
	if f.Status != "" {
		args = append(args, f.Status)
		q += fmt.Sprintf(` HAVING COALESCE((array_agg(execution_status ORDER BY event_time DESC,event_id DESC) FILTER(WHERE execution_status<>''))[1],'')=$%d`, len(args))
	}
	q, args = paginateGrouped(q, args, p, "min(event_time)", "execution_id")
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return storage.Page[storage.ExecutionSummary]{}, err
	}
	defer rows.Close()
	items := make([]storage.ExecutionSummary, 0, p.Limit+1)
	for rows.Next() {
		v, e := scanExecution(rows)
		if e != nil {
			return storage.Page[storage.ExecutionSummary]{}, e
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		return storage.Page[storage.ExecutionSummary]{}, err
	}
	return makePage(items, p, func(v storage.ExecutionSummary) storage.Cursor {
		return storage.Cursor{Timestamp: v.StartedAt, ID: v.ID.String()}
	})
}
func (s *Store) GetExecution(ctx context.Context, id trail.ExecutionID) (storage.ExecutionDetail, error) {
	v, e := scanExecution(s.pool.QueryRow(ctx, executionSummarySelect+` AND execution_id=$1 GROUP BY execution_id,service,environment`, idBytes(id)))
	if errors.Is(e, pgx.ErrNoRows) {
		return storage.ExecutionDetail{}, storage.ErrNotFound
	}
	return storage.ExecutionDetail{ExecutionSummary: v}, e
}
func (s *Store) GetRetryChain(ctx context.Context, id trail.ExecutionID) (storage.RetryChain, error) {
	detail, e := s.GetExecution(ctx, id)
	if e != nil {
		return storage.RetryChain{}, e
	}
	root := detail.RetryOfExecutionID
	if root.IsZero() {
		root = id
	}
	rows, e := s.pool.Query(ctx, executionSummarySelect+` AND (execution_id=$1 OR retry_of_execution_id=$1) GROUP BY execution_id,service,environment ORDER BY max(execution_attempt),min(event_time)`, idBytes(root))
	if e != nil {
		return storage.RetryChain{}, e
	}
	defer rows.Close()
	var out storage.RetryChain
	for rows.Next() {
		v, e := scanExecution(rows)
		if e != nil {
			return out, e
		}
		out.Executions = append(out.Executions, v)
	}
	return out, rows.Err()
}

func (s *Store) SearchEntities(ctx context.Context, f storage.EntityFilter, p storage.PageRequest) (storage.Page[storage.EntitySummary], error) {
	b := newWhere(0)
	applyCommon(b, f.Time, f.Service, f.Environment)
	applyScope(b, f.Scope)
	b.add("entity_type<>'' AND entity_id<>''")
	if f.EntityType != "" {
		b.eq("entity_type", f.EntityType)
	}
	if f.IDPrefix != "" {
		b.args = append(b.args, f.IDPrefix+"%")
		b.add(fmt.Sprintf("entity_id LIKE $%d", len(b.args)))
	}
	q := `SELECT scope_type,scope_id,entity_type,entity_id,min(event_time),max(event_time),count(DISTINCT flow_id) FILTER(WHERE flow_id IS NOT NULL) FROM trail_events` + b.sql() + ` GROUP BY scope_type,scope_id,entity_type,entity_id`
	args := b.args
	if p.Cursor.ID != "" {
		op := "<"
		if !p.Desc {
			op = ">"
		}
		args = append(args, p.Cursor.Timestamp, p.Cursor.ID)
		q += fmt.Sprintf(" HAVING (min(event_time),entity_id) %s ($%d,$%d)", op, len(args)-1, len(args))
	}
	order := "DESC"
	if !p.Desc {
		order = "ASC"
	}
	args = append(args, p.Limit+1)
	q += fmt.Sprintf(" ORDER BY min(event_time) %s,entity_id %s LIMIT $%d", order, order, len(args))
	rows, e := s.pool.Query(ctx, q, args...)
	if e != nil {
		return storage.Page[storage.EntitySummary]{}, e
	}
	defer rows.Close()
	items := make([]storage.EntitySummary, 0, p.Limit+1)
	for rows.Next() {
		var v storage.EntitySummary
		if e = rows.Scan(&v.Scope.Type, &v.Scope.ID, &v.Type, &v.ID, &v.FirstSeen, &v.LastSeen, &v.FlowCount); e != nil {
			return storage.Page[storage.EntitySummary]{}, e
		}
		v.FirstSeen = v.FirstSeen.UTC()
		v.LastSeen = v.LastSeen.UTC()
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		return storage.Page[storage.EntitySummary]{}, err
	}
	return makePage(items, p, func(v storage.EntitySummary) storage.Cursor { return storage.Cursor{Timestamp: v.FirstSeen, ID: v.ID} })
}
func (s *Store) GetEntity(ctx context.Context, key storage.EntityKey) (storage.EntityDetail, error) {
	var v storage.EntitySummary
	v.EntityKey = key
	b := newWhere(0)
	b.eq("entity_type", key.Type)
	b.eq("entity_id", key.ID)
	applyScope(b, key.Scope)
	e := s.pool.QueryRow(ctx, `SELECT min(event_time),max(event_time),count(DISTINCT flow_id) FILTER(WHERE flow_id IS NOT NULL) FROM trail_events`+b.sql()+` GROUP BY entity_type,entity_id`, b.args...).Scan(&v.FirstSeen, &v.LastSeen, &v.FlowCount)
	if errors.Is(e, pgx.ErrNoRows) {
		return storage.EntityDetail{}, storage.ErrNotFound
	}
	v.FirstSeen = v.FirstSeen.UTC()
	v.LastSeen = v.LastSeen.UTC()
	return storage.EntityDetail{EntitySummary: v}, e
}

const eventSelect = `SELECT event_time,event_id,batch_id,jetstream_stream,jetstream_stream_seq,jetstream_consumer,jetstream_consumer_seq,jetstream_delivered,received_at,service,environment,version,kind,level,flow_id,execution_id,parent_execution_id,retry_of_execution_id,execution_attempt,execution_source,scope_type,scope_id,entity_type,entity_id,parent_event_id,flow_status,execution_status,execution_kind,provider,has_error,fields,http_method,http_scheme,http_host,http_path,http_status_code,http_duration_ns,http_success,http_request_size,http_response_size,http_request_preview,http_response_preview,payload_refs FROM trail_events`

func (s *Store) ListEvents(ctx context.Context, f storage.EventFilter, p storage.PageRequest) (storage.Page[storage.EventRecord], error) {
	return s.listEvents(ctx, f, p, "", nil)
}
func (s *Store) ListFlowEvents(ctx context.Context, id trail.FlowID, f storage.EventFilter, p storage.PageRequest) (storage.Page[storage.EventRecord], error) {
	return s.listEvents(ctx, f, p, "flow_id", idBytes(id))
}
func (s *Store) listEvents(ctx context.Context, f storage.EventFilter, p storage.PageRequest, extraColumn string, extraValue any) (storage.Page[storage.EventRecord], error) {
	b := newWhere(0)
	applyCommon(b, f.Time, f.Service, f.Environment)
	applyScope(b, f.Scope)
	if f.Kind != "" {
		b.eq("kind", f.Kind)
	}
	if f.Level != nil {
		b.eq("level", f.Level.String())
	}
	if f.HTTPStatusClass > 0 {
		b.args = append(b.args, f.HTTPStatusClass*100, (f.HTTPStatusClass+1)*100)
		b.add(fmt.Sprintf("http_status_code >= $%d AND http_status_code < $%d", len(b.args)-1, len(b.args)))
	}
	if f.HTTPMethod != "" {
		b.eq("http_method", f.HTTPMethod)
	}
	if f.HTTPStatus > 0 {
		b.eq("http_status_code", f.HTTPStatus)
	}
	if f.Provider != "" {
		b.eq("provider", f.Provider)
	}
	if f.HasError != nil {
		b.eq("has_error", *f.HasError)
	}
	if !f.FlowID.IsZero() {
		b.eq("flow_id", idBytes(f.FlowID))
	}
	if !f.ExecutionID.IsZero() {
		b.eq("execution_id", idBytes(f.ExecutionID))
	}
	if f.EntityType != "" {
		b.eq("entity_type", f.EntityType)
	}
	if f.EntityID != "" {
		b.eq("entity_id", f.EntityID)
	}
	if extraColumn != "" {
		b.eq(extraColumn, extraValue)
	}
	applyCursor(b, p, "event_time", "event_id")
	order := "DESC"
	if !p.Desc {
		order = "ASC"
	}
	b.args = append(b.args, p.Limit+1)
	q := eventSelect + b.sql() + ` ORDER BY event_time ` + order + `,event_id ` + order + fmt.Sprintf(" LIMIT $%d", len(b.args))
	rows, e := s.pool.Query(ctx, q, b.args...)
	if e != nil {
		return storage.Page[storage.EventRecord]{}, e
	}
	defer rows.Close()
	items := make([]storage.EventRecord, 0, p.Limit+1)
	for rows.Next() {
		v, e := scanEvent(rows)
		if e != nil {
			return storage.Page[storage.EventRecord]{}, e
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		return storage.Page[storage.EventRecord]{}, err
	}
	return makePage(items, p, func(v storage.EventRecord) storage.Cursor {
		return storage.Cursor{Timestamp: v.Timestamp, ID: v.ID.String()}
	})
}
func (s *Store) GetEvent(ctx context.Context, id trail.EventID) (storage.EventRecord, error) {
	v, e := scanEvent(s.pool.QueryRow(ctx, eventSelect+` WHERE event_id=$1`, idBytes(id)))
	if errors.Is(e, pgx.ErrNoRows) {
		return storage.EventRecord{}, storage.ErrNotFound
	}
	return v, e
}

func (s *Store) Overview(ctx context.Context, f storage.OverviewFilter) (storage.Overview, error) {
	b := newWhere(0)
	applyCommon(b, f.Time, f.Service, f.Environment)
	applyScope(b, f.Scope)
	q := `SELECT COALESCE(min(event_time),now()),COALESCE(max(event_time),now()),count(*),count(DISTINCT flow_id) FILTER(WHERE flow_id IS NOT NULL),count(DISTINCT execution_id) FILTER(WHERE execution_id IS NOT NULL),count(DISTINCT (entity_type,entity_id)) FILTER(WHERE entity_type<>'' AND entity_id<>''),count(DISTINCT flow_id) FILTER(WHERE flow_status='failed'),count(DISTINCT execution_id) FILTER(WHERE execution_status='failed'),count(*) FILTER(WHERE http_status_code>=500) FROM trail_events` + b.sql()
	v := storage.Overview{AsOf: time.Now().UTC()}
	e := s.pool.QueryRow(ctx, q, b.args...).Scan(&v.FirstEvent, &v.LastEvent, &v.Events, &v.Flows, &v.Executions, &v.Entities, &v.FailedFlows, &v.FailedExecutions, &v.HTTP5xx)
	return v, e
}

func (s *Store) OverviewActivity(ctx context.Context, f storage.OverviewActivityFilter) (storage.OverviewActivity, error) {
	out := storage.OverviewActivity{From: f.Time.From, To: f.Time.To, Interval: f.Interval, Buckets: []storage.ActivityBucket{}, FlowStatuses: []storage.CountByName{}, ExecutionSources: []storage.CountByName{}}
	b := newWhere(0)
	applyCommon(b, f.Time, f.Service, f.Environment)
	applyScope(b, f.Scope)
	unit := "hour"
	if f.Interval == storage.ActivityDay {
		unit = "day"
	}
	rows, err := s.pool.Query(ctx, `SELECT date_trunc('`+unit+`',event_time) bucket,count(*),count(*) FILTER(WHERE level='error'),count(*) FILTER(WHERE http_status_code>=500) FROM trail_events`+b.sql()+` GROUP BY bucket ORDER BY bucket`, b.args...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var v storage.ActivityBucket
		if err = rows.Scan(&v.Timestamp, &v.Events, &v.Errors, &v.HTTP5xx); err != nil {
			rows.Close()
			return out, err
		}
		v.Timestamp = v.Timestamp.UTC()
		out.Buckets = append(out.Buckets, v)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	flows := newWhere(0)
	applyCommon(flows, f.Time, f.Service, f.Environment)
	applyScope(flows, f.Scope)
	flows.add("flow_id IS NOT NULL AND flow_status<>''")
	rows, err = s.pool.Query(ctx, `SELECT status,count(*) FROM (SELECT DISTINCT ON (flow_id) flow_id,flow_status status FROM trail_events`+flows.sql()+` ORDER BY flow_id,event_time DESC,event_id DESC) flows GROUP BY status ORDER BY count(*) DESC`, flows.args...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var v storage.CountByName
		if err = rows.Scan(&v.Name, &v.Count); err != nil {
			rows.Close()
			return out, err
		}
		out.FlowStatuses = append(out.FlowStatuses, v)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	executions := newWhere(0)
	applyCommon(executions, f.Time, f.Service, f.Environment)
	applyScope(executions, f.Scope)
	executions.add("execution_id IS NOT NULL AND execution_source<>''")
	rows, err = s.pool.Query(ctx, `SELECT source,count(*) FROM (SELECT DISTINCT ON (execution_id) execution_id,execution_source source FROM trail_events`+executions.sql()+` ORDER BY execution_id,event_time DESC,event_id DESC) executions GROUP BY source ORDER BY count(*) DESC`, executions.args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var v storage.CountByName
		if err = rows.Scan(&v.Name, &v.Count); err != nil {
			return out, err
		}
		out.ExecutionSources = append(out.ExecutionSources, v)
	}
	return out, rows.Err()
}

type scanner interface{ Scan(...any) error }

func scanFlow(row scanner) (storage.FlowSummary, string, string, error) {
	var v storage.FlowSummary
	var id, exec []byte
	var first, last string
	e := row.Scan(&id, &v.Service, &v.Environment, &v.Scope.Type, &v.Scope.ID, &v.EntityType, &v.EntityID, &exec, &v.Status, &v.StartedAt, &v.FinishedAt, &v.EventCount, &first, &last)
	copy(v.ID[:], id)
	copy(v.ExecutionID[:], exec)
	v.StartedAt = v.StartedAt.UTC()
	v.FinishedAt = v.FinishedAt.UTC()
	return v, first, last, e
}
func scanExecution(row scanner) (storage.ExecutionSummary, error) {
	var v storage.ExecutionSummary
	var id, parent, retry []byte
	var source string
	e := row.Scan(&id, &v.Service, &v.Environment, &v.Scope.Type, &v.Scope.ID, &v.Kind, &source, &v.Status, &v.Attempt, &parent, &retry, &v.StartedAt, &v.FinishedAt, &v.EventCount, &v.FlowCount)
	copy(v.ID[:], id)
	copy(v.ParentExecutionID[:], parent)
	copy(v.RetryOfExecutionID[:], retry)
	v.Source = trail.ExecutionSource(source)
	v.StartedAt = v.StartedAt.UTC()
	v.FinishedAt = v.FinishedAt.UTC()
	return v, e
}
func scanEvent(row scanner) (storage.EventRecord, error) {
	var v storage.EventRecord
	var id, batch, flow, exec, parentExec, retry, parent []byte
	var level, source string
	var fields, payloads []byte
	var duration int64
	e := row.Scan(&v.Timestamp, &id, &batch, &v.Delivery.Stream, &v.Delivery.StreamSequence, &v.Delivery.Consumer, &v.Delivery.ConsumerSequence, &v.Delivery.Delivered, &v.Delivery.ReceivedAt, &v.Metadata.Service, &v.Metadata.Environment, &v.Metadata.Version, &v.Kind, &level, &flow, &exec, &parentExec, &retry, &v.ExecutionAttempt, &source, &v.Scope.Type, &v.Scope.ID, &v.EntityType, &v.EntityID, &parent, &v.FlowStatus, &v.ExecutionStatus, &v.ExecutionKind, &v.Provider, &v.HasError, &fields, &v.HTTP.Method, &v.HTTP.Scheme, &v.HTTP.Host, &v.HTTP.Path, &v.HTTP.StatusCode, &duration, &v.HTTP.Success, &v.HTTP.RequestSize, &v.HTTP.ResponseSize, &v.HTTP.RequestPreview, &v.HTTP.ResponsePreview, &payloads)
	copy(v.ID[:], id)
	copy(v.BatchID[:], batch)
	copy(v.FlowID[:], flow)
	copy(v.ExecutionID[:], exec)
	copy(v.ParentExecutionID[:], parentExec)
	copy(v.RetryOfExecutionID[:], retry)
	copy(v.ParentID[:], parent)
	v.HTTP.Duration = time.Duration(duration)
	v.ExecutionSource = trail.ExecutionSource(source)
	v.Timestamp = v.Timestamp.UTC()
	v.Delivery.ReceivedAt = v.Delivery.ReceivedAt.UTC()
	v.Level = parseLevel(level)
	if e == nil {
		e = json.Unmarshal(fields, &v.Fields)
	}
	if e == nil {
		e = json.Unmarshal(payloads, &v.Payloads)
	}
	return v, e
}
func parseLevel(s string) trail.Level {
	switch s {
	case "debug":
		return trail.LevelDebug
	case "warn":
		return trail.LevelWarn
	case "error":
		return trail.LevelError
	default:
		return trail.LevelInfo
	}
}

type where struct {
	parts []string
	args  []any
	base  bool
}

func newWhere(existingWhere int) *where { return &where{base: existingWhere > 0} }
func (b *where) add(s string)           { b.parts = append(b.parts, s) }
func (b *where) eq(column string, value any) {
	b.args = append(b.args, value)
	b.add(fmt.Sprintf("%s=$%d", column, len(b.args)))
}
func (b *where) sql() string {
	if len(b.parts) == 0 {
		return ""
	}
	prefix := " WHERE "
	if b.base {
		prefix = " AND "
	}
	return prefix + strings.Join(b.parts, " AND ")
}
func applyCommon(b *where, t storage.TimeRange, service, environment string) {
	if !t.From.IsZero() {
		b.args = append(b.args, t.From)
		b.add(fmt.Sprintf("event_time >= $%d", len(b.args)))
	}
	if !t.To.IsZero() {
		b.args = append(b.args, t.To)
		b.add(fmt.Sprintf("event_time < $%d", len(b.args)))
	}
	if service != "" {
		b.eq("service", service)
	}
	if environment != "" {
		b.eq("environment", environment)
	}
}

func applyScope(b *where, scope trail.Scope) {
	if scope.IsZero() {
		return
	}
	b.eq("scope_type", scope.Type)
	b.eq("scope_id", scope.ID)
}
func applyCursor(b *where, p storage.PageRequest, timeCol, idCol string) {
	if p.Cursor.ID == "" {
		return
	}
	raw, err := trail.ParseEventID(p.Cursor.ID)
	if err != nil {
		return
	}
	op := "<"
	if !p.Desc {
		op = ">"
	}
	b.args = append(b.args, p.Cursor.Timestamp, idBytes(raw))
	b.add(fmt.Sprintf("(%s,%s) %s ($%d,$%d)", timeCol, idCol, op, len(b.args)-1, len(b.args)))
}
func paginateGrouped(q string, args []any, p storage.PageRequest, timeCol, idCol string) (string, []any) {
	if p.Cursor.ID != "" {
		raw, _ := trail.ParseEventID(p.Cursor.ID)
		op := "<"
		if !p.Desc {
			op = ">"
		}
		args = append(args, p.Cursor.Timestamp, idBytes(raw))
		q += fmt.Sprintf(" HAVING (%s,%s) %s ($%d,$%d)", timeCol, idCol, op, len(args)-1, len(args))
	}
	order := "DESC"
	if !p.Desc {
		order = "ASC"
	}
	args = append(args, p.Limit+1)
	q += fmt.Sprintf(" ORDER BY %s %s,%s %s LIMIT $%d", timeCol, order, idCol, order, len(args))
	return q, args
}
func makePage[T any](items []T, p storage.PageRequest, cursor func(T) storage.Cursor) (storage.Page[T], error) {
	out := storage.Page[T]{Items: items}
	if len(items) > p.Limit {
		out.HasMore = true
		out.Items = items[:p.Limit]
		out.NextCursor = cursor(out.Items[len(out.Items)-1])
	}
	return out, nil
}

var _ storage.ExplorerStore = (*Store)(nil)
var _ storage.OverviewActivityReader = (*Store)(nil)
