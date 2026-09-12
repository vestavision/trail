package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/storage"
	"github.com/vestavision/trail/wire"
)

const flowRows = `SELECT flow_id,service,environment,entity_type,entity_id,execution_id,status,started_at,finished_at,event_count,first_kind,last_kind FROM (SELECT flow_id,service,environment,argMaxMerge(entity_type) entity_type,argMaxMerge(entity_id) entity_id,argMaxMerge(execution_id) execution_id,argMaxIfMerge(status) status,minMerge(started_at) started_at,maxMerge(finished_at) finished_at,uniqExactMerge(event_count) event_count,argMinMerge(first_kind) first_kind,argMaxMerge(last_kind) last_kind FROM trail_flow_summaries GROUP BY flow_id,service,environment)`
const executionRows = `SELECT execution_id,service,environment,kind,source,status,attempt,parent_execution_id,retry_of_execution_id,started_at,finished_at,event_count,flow_count FROM (SELECT execution_id,service,environment,argMaxIfMerge(kind) kind,argMaxIfMerge(source) source,argMaxIfMerge(status) status,maxMerge(attempt) attempt,argMaxMerge(parent_execution_id) parent_execution_id,argMaxMerge(retry_of_execution_id) retry_of_execution_id,minMerge(started_at) started_at,maxMerge(finished_at) finished_at,uniqExactMerge(event_count) event_count,uniqExactMerge(flow_count) flow_count FROM trail_execution_summaries GROUP BY execution_id,service,environment)`

func (s *Store) ListFlows(ctx context.Context, f storage.FlowFilter, p storage.PageRequest) (storage.Page[storage.FlowSummary], error) {
	b := chWhere{}
	commonCH(&b, f.Time, f.Service, f.Environment)
	if f.Status != "" {
		b.eq("status", f.Status)
	}
	if f.EntityType != "" {
		b.eq("entity_type", f.EntityType)
	}
	if f.EntityID != "" {
		b.eq("entity_id", f.EntityID)
	}
	if !f.ExecutionID.IsZero() {
		b.eq("execution_id", idBytes(f.ExecutionID))
	}
	cursorCH(&b, p, "event_time", "flow_id", true)
	q, args := limitCH(`SELECT flow_id,event_time FROM trail_flow_terminals`+b.sql(), b.args, p, "event_time", "flow_id")
	rows, e := s.conn.Query(ctx, q, args...)
	if e != nil {
		return storage.Page[storage.FlowSummary]{}, e
	}
	defer rows.Close()
	ids := make([]string, 0, p.Limit+1)
	times := make([]time.Time, 0, p.Limit+1)
	for rows.Next() {
		var id string
		var at time.Time
		if e := rows.Scan(&id, &at); e != nil {
			return storage.Page[storage.FlowSummary]{}, e
		}
		ids = append(ids, id)
		times = append(times, at)
	}
	if e = rows.Err(); e != nil {
		return storage.Page[storage.FlowSummary]{}, e
	}
	if len(ids) == 0 {
		return storage.Page[storage.FlowSummary]{Items: []storage.FlowSummary{}}, nil
	}
	hasMore := len(ids) > p.Limit
	if hasMore {
		ids = ids[:p.Limit]
		times = times[:p.Limit]
	}
	summaryRows, e := s.conn.Query(ctx, flowRows+` WHERE flow_id IN ?`, ids)
	if e != nil {
		return storage.Page[storage.FlowSummary]{}, e
	}
	defer summaryRows.Close()
	byID := make(map[string]storage.FlowSummary, len(ids))
	for summaryRows.Next() {
		v, _, _, e := scanCHFlow(summaryRows)
		if e != nil {
			return storage.Page[storage.FlowSummary]{}, e
		}
		byID[string(v.ID[:])] = v
	}
	if e = summaryRows.Err(); e != nil {
		return storage.Page[storage.FlowSummary]{}, e
	}
	items := make([]storage.FlowSummary, 0, len(ids))
	for _, id := range ids {
		if v, ok := byID[id]; ok {
			items = append(items, v)
		}
	}
	out := storage.Page[storage.FlowSummary]{Items: items, HasMore: hasMore}
	if hasMore && len(items) > 0 {
		out.NextCursor = storage.Cursor{Timestamp: times[len(times)-1], ID: items[len(items)-1].ID.String()}
	}
	return out, nil
}
func (s *Store) GetFlow(ctx context.Context, id trail.FlowID) (storage.FlowDetail, error) {
	v, first, last, e := scanCHFlow(s.conn.QueryRow(ctx, flowRows+` WHERE flow_id=?`, idBytes(id)))
	if errors.Is(e, sql.ErrNoRows) {
		return storage.FlowDetail{}, storage.ErrNotFound
	}
	return storage.FlowDetail{FlowSummary: v, FirstKind: first, LastKind: last}, e
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

func (s *Store) ListExecutions(ctx context.Context, f storage.ExecutionFilter, p storage.PageRequest) (storage.Page[storage.ExecutionSummary], error) {
	b := chWhere{}
	summaryCH(&b, f.Time, f.Service, f.Environment)
	if f.Status != "" {
		b.eq("status", f.Status)
	}
	if f.Kind != "" {
		b.eq("kind", f.Kind)
	}
	if f.Source != "" {
		b.eq("source", string(f.Source))
	}
	cursorCH(&b, p, "started_at", "execution_id", true)
	q, args := limitCH(executionRows+b.sql(), b.args, p, "started_at", "execution_id")
	rows, e := s.conn.Query(ctx, q, args...)
	if e != nil {
		return storage.Page[storage.ExecutionSummary]{}, e
	}
	defer rows.Close()
	items := make([]storage.ExecutionSummary, 0, p.Limit+1)
	for rows.Next() {
		v, e := scanCHExecution(rows)
		if e != nil {
			return storage.Page[storage.ExecutionSummary]{}, e
		}
		items = append(items, v)
	}
	if e = rows.Err(); e != nil {
		return storage.Page[storage.ExecutionSummary]{}, e
	}
	return pageCH(items, p, func(v storage.ExecutionSummary) storage.Cursor {
		return storage.Cursor{Timestamp: v.StartedAt, ID: v.ID.String()}
	}), nil
}
func (s *Store) GetExecution(ctx context.Context, id trail.ExecutionID) (storage.ExecutionDetail, error) {
	v, e := scanCHExecution(s.conn.QueryRow(ctx, executionRows+` WHERE execution_id=?`, idBytes(id)))
	if errors.Is(e, sql.ErrNoRows) {
		return storage.ExecutionDetail{}, storage.ErrNotFound
	}
	return storage.ExecutionDetail{ExecutionSummary: v}, e
}
func (s *Store) GetRetryChain(ctx context.Context, id trail.ExecutionID) (storage.RetryChain, error) {
	d, e := s.GetExecution(ctx, id)
	if e != nil {
		return storage.RetryChain{}, e
	}
	root := d.RetryOfExecutionID
	if root.IsZero() {
		root = id
	}
	rows, e := s.conn.Query(ctx, executionRows+` WHERE execution_id=? OR retry_of_execution_id=? ORDER BY attempt,started_at`, idBytes(root), idBytes(root))
	if e != nil {
		return storage.RetryChain{}, e
	}
	defer rows.Close()
	var out storage.RetryChain
	for rows.Next() {
		v, e := scanCHExecution(rows)
		if e != nil {
			return out, e
		}
		out.Executions = append(out.Executions, v)
	}
	return out, rows.Err()
}

func (s *Store) SearchEntities(ctx context.Context, f storage.EntityFilter, p storage.PageRequest) (storage.Page[storage.EntitySummary], error) {
	b := chWhere{}
	commonCH(&b, f.Time, f.Service, f.Environment)
	b.add("entity_type != '' AND entity_id != ''")
	if f.EntityType != "" {
		b.eq("entity_type", f.EntityType)
	}
	if f.IDPrefix != "" {
		b.add("startsWith(entity_id, ?)", f.IDPrefix)
	}
	q := `SELECT entity_type,entity_id,min(event_time),max(event_time),uniqExactIf(flow_id,flow_id!=unhex('00000000000000000000000000000000')) FROM trail_events` + b.sql() + ` GROUP BY entity_type,entity_id`
	if p.Cursor.ID != "" {
		op := "<"
		if !p.Desc {
			op = ">"
		}
		q += ` HAVING (min(event_time),entity_id) ` + op + ` (?,?)`
		b.args = append(b.args, p.Cursor.Timestamp, p.Cursor.ID)
	}
	q, args := limitCH(q, b.args, p, "min(event_time)", "entity_id")
	rows, e := s.conn.Query(ctx, q, args...)
	if e != nil {
		return storage.Page[storage.EntitySummary]{}, e
	}
	defer rows.Close()
	items := make([]storage.EntitySummary, 0, p.Limit+1)
	for rows.Next() {
		var v storage.EntitySummary
		if e = rows.Scan(&v.Type, &v.ID, &v.FirstSeen, &v.LastSeen, &v.FlowCount); e != nil {
			return storage.Page[storage.EntitySummary]{}, e
		}
		items = append(items, v)
	}
	if e = rows.Err(); e != nil {
		return storage.Page[storage.EntitySummary]{}, e
	}
	return pageCH(items, p, func(v storage.EntitySummary) storage.Cursor { return storage.Cursor{Timestamp: v.FirstSeen, ID: v.ID} }), nil
}
func (s *Store) GetEntity(ctx context.Context, key storage.EntityKey) (storage.EntityDetail, error) {
	v := storage.EntitySummary{EntityKey: key}
	e := s.conn.QueryRow(ctx, `SELECT min(event_time),max(event_time),uniqExactIf(flow_id,flow_id!=unhex('00000000000000000000000000000000')) FROM trail_events WHERE entity_type=? AND entity_id=? GROUP BY entity_type,entity_id`, key.Type, key.ID).Scan(&v.FirstSeen, &v.LastSeen, &v.FlowCount)
	if errors.Is(e, sql.ErrNoRows) {
		return storage.EntityDetail{}, storage.ErrNotFound
	}
	return storage.EntityDetail{EntitySummary: v}, e
}

// FINAL makes event detail/list semantics idempotent during the interval before
// ReplacingMergeTree has merged a JetStream redelivery.
const eventRows = `SELECT event_time,event_id,batch_id,jetstream_stream,jetstream_stream_seq,jetstream_consumer,jetstream_consumer_seq,jetstream_delivered,received_at,service,environment,version,kind,toString(level),flow_id,execution_id,parent_execution_id,retry_of_execution_id,execution_attempt,execution_source,entity_type,entity_id,parent_event_id,flow_status,execution_status,execution_kind,field_keys,field_types,field_text,field_num,http_method,http_scheme,http_host,http_path,http_status_code,http_duration_ns,http_success,http_request_size,http_response_size,http_request_preview,http_response_preview,payload_refs_json FROM trail_events`

func (s *Store) ListEvents(ctx context.Context, f storage.EventFilter, p storage.PageRequest) (storage.Page[storage.EventRecord], error) {
	return s.listCHEvents(ctx, f, p, "", nil)
}
func (s *Store) ListFlowEvents(ctx context.Context, id trail.FlowID, f storage.EventFilter, p storage.PageRequest) (storage.Page[storage.EventRecord], error) {
	return s.listCHEvents(ctx, f, p, "flow_id", idBytes(id))
}
func (s *Store) listCHEvents(ctx context.Context, f storage.EventFilter, p storage.PageRequest, column string, value any) (storage.Page[storage.EventRecord], error) {
	b := chWhere{}
	commonCH(&b, f.Time, f.Service, f.Environment)
	if column != "" {
		b.eq(column, value)
	}
	if f.Kind != "" {
		b.eq("kind", f.Kind)
	}
	if f.Level != nil {
		b.eq("level", f.Level.String())
	}
	if f.HTTPStatusClass > 0 {
		b.add("http_status_code>=? AND http_status_code<?", f.HTTPStatusClass*100, (f.HTTPStatusClass+1)*100)
	}
	cursorCH(&b, p, "event_time", "event_id", true)
	q, args := limitCH(eventRows+b.sql(), b.args, p, "event_time", "event_id")
	rows, e := s.conn.Query(ctx, q, args...)
	if e != nil {
		return storage.Page[storage.EventRecord]{}, e
	}
	defer rows.Close()
	items := make([]storage.EventRecord, 0, p.Limit+1)
	for rows.Next() {
		v, e := scanCHEvent(rows)
		if e != nil {
			return storage.Page[storage.EventRecord]{}, e
		}
		items = append(items, v)
	}
	if e = rows.Err(); e != nil {
		return storage.Page[storage.EventRecord]{}, e
	}
	return pageCH(items, p, func(v storage.EventRecord) storage.Cursor {
		return storage.Cursor{Timestamp: v.Timestamp, ID: v.ID.String()}
	}), nil
}
func (s *Store) GetEvent(ctx context.Context, id trail.EventID) (storage.EventRecord, error) {
	v, e := scanCHEvent(s.conn.QueryRow(ctx, eventRows+` WHERE event_id=?`, idBytes(id)))
	if errors.Is(e, sql.ErrNoRows) {
		return storage.EventRecord{}, storage.ErrNotFound
	}
	return v, e
}
func (s *Store) Overview(ctx context.Context, f storage.OverviewFilter) (storage.Overview, error) {
	b := chWhere{}
	commonCH(&b, f.Time, f.Service, f.Environment)
	v := storage.Overview{AsOf: time.Now().UTC()}
	e := s.conn.QueryRow(ctx, `SELECT min(event_time),max(event_time),uniqExact(event_id),uniqExactIf(flow_id,flow_id!=unhex('00000000000000000000000000000000')),uniqExactIf(execution_id,execution_id!=unhex('00000000000000000000000000000000')),uniqExactIf(tuple(entity_type,entity_id),entity_type!='' AND entity_id!=''),uniqExactIf(flow_id,flow_status='failed'),uniqExactIf(execution_id,execution_status='failed'),uniqExactIf(event_id,http_status_code>=500) FROM trail_events`+b.sql(), b.args...).Scan(&v.FirstEvent, &v.LastEvent, &v.Events, &v.Flows, &v.Executions, &v.Entities, &v.FailedFlows, &v.FailedExecutions, &v.HTTP5xx)
	return v, e
}

func (s *Store) OverviewActivity(ctx context.Context, f storage.OverviewActivityFilter) (storage.OverviewActivity, error) {
	out := storage.OverviewActivity{From: f.Time.From, To: f.Time.To, Interval: f.Interval, Buckets: []storage.ActivityBucket{}, FlowStatuses: []storage.CountByName{}, ExecutionSources: []storage.CountByName{}}
	b := chWhere{}
	commonCH(&b, f.Time, f.Service, f.Environment)
	bucket := "toStartOfHour(event_time)"
	if f.Interval == storage.ActivityDay {
		bucket = "toStartOfDay(event_time)"
	}
	rows, err := s.conn.Query(ctx, `SELECT `+bucket+` bucket,count(),countIf(level='error'),countIf(http_status_code>=500) FROM trail_events`+b.sql()+` GROUP BY bucket ORDER BY bucket`, b.args...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var v storage.ActivityBucket
		if err = rows.Scan(&v.Timestamp, &v.Events, &v.Errors, &v.HTTP5xx); err != nil {
			rows.Close()
			return out, err
		}
		out.Buckets = append(out.Buckets, v)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()
	statusWhere := chWhere{}
	commonCH(&statusWhere, f.Time, f.Service, f.Environment)
	rows, err = s.conn.Query(ctx, `SELECT status,count() FROM trail_flow_terminals FINAL`+statusWhere.sql()+` GROUP BY status ORDER BY count() DESC`, statusWhere.args...)
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
	summaryWhere := chWhere{}
	summaryCH(&summaryWhere, f.Time, f.Service, f.Environment)
	rows, err = s.conn.Query(ctx, `SELECT source,count() FROM (`+executionRows+`)`+summaryWhere.sql()+` GROUP BY source ORDER BY count() DESC`, summaryWhere.args...)
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

type rowScanner interface{ Scan(...any) error }

func scanCHFlow(r rowScanner) (storage.FlowSummary, string, string, error) {
	var v storage.FlowSummary
	var id, execution string
	var first, last string
	e := r.Scan(&id, &v.Service, &v.Environment, &v.EntityType, &v.EntityID, &execution, &v.Status, &v.StartedAt, &v.FinishedAt, &v.EventCount, &first, &last)
	copy(v.ID[:], id)
	copy(v.ExecutionID[:], execution)
	return v, first, last, e
}
func scanCHExecution(r rowScanner) (storage.ExecutionSummary, error) {
	var v storage.ExecutionSummary
	var id, parent, retry, source string
	e := r.Scan(&id, &v.Service, &v.Environment, &v.Kind, &source, &v.Status, &v.Attempt, &parent, &retry, &v.StartedAt, &v.FinishedAt, &v.EventCount, &v.FlowCount)
	copy(v.ID[:], id)
	copy(v.ParentExecutionID[:], parent)
	copy(v.RetryOfExecutionID[:], retry)
	v.Source = trail.ExecutionSource(source)
	return v, e
}
func scanCHEvent(r rowScanner) (storage.EventRecord, error) {
	var v storage.EventRecord
	var id, batch, flow, execution, parentExecution, retry, parent string
	var level, source string
	var keys, types, texts []string
	var nums []uint64
	var payloadJSON string
	var duration uint64
	var status uint16
	var success uint8
	e := r.Scan(&v.Timestamp, &id, &batch, &v.Delivery.Stream, &v.Delivery.StreamSequence, &v.Delivery.Consumer, &v.Delivery.ConsumerSequence, &v.Delivery.Delivered, &v.Delivery.ReceivedAt, &v.Metadata.Service, &v.Metadata.Environment, &v.Metadata.Version, &v.Kind, &level, &flow, &execution, &parentExecution, &retry, &v.ExecutionAttempt, &source, &v.EntityType, &v.EntityID, &parent, &v.FlowStatus, &v.ExecutionStatus, &v.ExecutionKind, &keys, &types, &texts, &nums, &v.HTTP.Method, &v.HTTP.Scheme, &v.HTTP.Host, &v.HTTP.Path, &status, &duration, &success, &v.HTTP.RequestSize, &v.HTTP.ResponseSize, &v.HTTP.RequestPreview, &v.HTTP.ResponsePreview, &payloadJSON)
	if e != nil {
		return v, e
	}
	copy(v.ID[:], id)
	copy(v.BatchID[:], batch)
	copy(v.FlowID[:], flow)
	copy(v.ExecutionID[:], execution)
	copy(v.ParentExecutionID[:], parentExecution)
	copy(v.RetryOfExecutionID[:], retry)
	copy(v.ParentID[:], parent)
	v.Level = levelCH(level)
	v.ExecutionSource = trail.ExecutionSource(source)
	v.HTTP.Duration = time.Duration(duration)
	v.HTTP.StatusCode = int(status)
	v.HTTP.Success = success != 0
	v.Fields = make([]wire.Field, len(keys))
	for i := range keys {
		v.Fields[i] = wire.Field{Key: keys[i], Type: types[i], Text: texts[i], Num: nums[i]}
	}
	e = json.Unmarshal([]byte(payloadJSON), &v.Payloads)
	return v, e
}
func levelCH(v string) trail.Level {
	switch v {
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

type chWhere struct {
	parts []string
	args  []any
}

func (b *chWhere) add(q string, args ...any) {
	b.parts = append(b.parts, q)
	b.args = append(b.args, args...)
}
func (b *chWhere) eq(k string, v any) { b.add(k+" = ?", v) }
func (b *chWhere) sql() string {
	if len(b.parts) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(b.parts, " AND ")
}
func commonCH(b *chWhere, t storage.TimeRange, service, environment string) {
	if !t.From.IsZero() {
		b.add("event_time >= ?", t.From)
	}
	if !t.To.IsZero() {
		b.add("event_time < ?", t.To)
	}
	if service != "" {
		b.eq("service", service)
	}
	if environment != "" {
		b.eq("environment", environment)
	}
}
func summaryCH(b *chWhere, t storage.TimeRange, service, environment string) {
	if !t.From.IsZero() {
		b.add("started_at >= ?", t.From)
	}
	if !t.To.IsZero() {
		b.add("started_at < ?", t.To)
	}
	if service != "" {
		b.eq("service", service)
	}
	if environment != "" {
		b.eq("environment", environment)
	}
}
func cursorCH(b *chWhere, p storage.PageRequest, timeCol, idCol string, binary bool) {
	if p.Cursor.ID == "" {
		return
	}
	op := "<"
	if !p.Desc {
		op = ">"
	}
	var id any = p.Cursor.ID
	if binary {
		parsed, e := trail.ParseEventID(p.Cursor.ID)
		if e != nil {
			return
		}
		id = idBytes(parsed)
	}
	b.add(fmt.Sprintf("(%s,%s) %s (?,?)", timeCol, idCol, op), p.Cursor.Timestamp, id)
}
func limitCH(q string, args []any, p storage.PageRequest, timeCol, idCol string) (string, []any) {
	order := "DESC"
	if !p.Desc {
		order = "ASC"
	}
	return q + fmt.Sprintf(" ORDER BY %s %s,%s %s LIMIT ?", timeCol, order, idCol, order), append(args, p.Limit+1)
}
func pageCH[T any](items []T, p storage.PageRequest, cursor func(T) storage.Cursor) storage.Page[T] {
	out := storage.Page[T]{Items: items}
	if len(items) > p.Limit {
		out.HasMore = true
		out.Items = items[:p.Limit]
		out.NextCursor = cursor(out.Items[len(out.Items)-1])
	}
	return out
}

var _ storage.ExplorerStore = (*Store)(nil)
var _ storage.OverviewActivityReader = (*Store)(nil)
