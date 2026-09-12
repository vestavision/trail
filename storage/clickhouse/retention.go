package clickhouse

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/archive"
	"github.com/vestavision/trail/convention"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/retention"
	"github.com/vestavision/trail/storage"
	"github.com/vestavision/trail/wire"
)

func (s *Store) ScanRetentionEvents(ctx context.Context, req retention.ScanRequest) (retention.EventPage, error) {
	if req.Limit <= 0 {
		return retention.EventPage{}, nil
	}
	q := `SELECT event_time,event_id,flow_id,execution_id,kind,toString(level),service,environment,flow_status,execution_status,http_status_code,http_success,field_keys,field_types,field_text,field_num,payload_refs_json FROM trail_events FINAL`
	args := []any{}
	if req.Cursor != "" {
		ts, id, err := parseCHRetentionCursor(req.Cursor)
		if err != nil {
			return retention.EventPage{}, err
		}
		q += ` WHERE (event_time,event_id)>(?,?)`
		args = []any{ts, string(id)}
	}
	q += ` ORDER BY event_time,event_id LIMIT ?`
	args = append(args, req.Limit+1)
	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return retention.EventPage{}, err
	}
	defer rows.Close()
	var out retention.EventPage
	for rows.Next() {
		var v retention.Event
		var id, flow, execution, level, payloadJSON string
		var keys, types, texts []string
		var nums []uint64
		var status uint16
		var success uint8
		if err = rows.Scan(&v.Timestamp, &id, &flow, &execution, &v.Kind, &level, &v.Service, &v.Environment, &v.FlowStatus, &v.ExecutionStatus, &status, &success, &keys, &types, &texts, &nums, &payloadJSON); err != nil {
			return out, err
		}
		v.ID = hex.EncodeToString([]byte(id))
		v.FlowID = hex.EncodeToString([]byte(flow))
		v.ExecutionID = hex.EncodeToString([]byte(execution))
		v.Cursor = chRetentionCursor(v.Timestamp, []byte(id))
		v.Level = chRetentionLevel(level)
		v.HTTPStatusCode = int(status)
		v.HTTPSuccess = success != 0
		for i, k := range keys {
			if k == convention.FieldRetentionClass && i < len(texts) {
				v.RetentionClass = texts[i]
				break
			}
		}
		var links []storage.PayloadLink
		if err = json.Unmarshal([]byte(payloadJSON), &links); err != nil {
			return out, err
		}
		for _, p := range links {
			v.Payloads = append(v.Payloads, retention.Payload{Role: p.Role, Ref: p.Ref})
		}
		out.Events = append(out.Events, v)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Events) > req.Limit {
		out.Events = out.Events[:req.Limit]
		out.HasMore = true
	}
	if len(out.Events) > 0 {
		out.NextCursor = out.Events[len(out.Events)-1].Cursor
	}
	return out, nil
}

func (s *Store) DeleteRetentionEvents(ctx context.Context, req retention.DeleteRequest) (retention.DeleteResult, error) {
	var out retention.DeleteResult
	if len(req.Events) == 0 {
		return out, nil
	}
	if req.DryRun {
		return out, nil
	}
	if req.ArchiveID == "" {
		return out, fmt.Errorf("trail/clickhouse: archive receipt is required")
	}
	ids := make([]string, 0, len(req.Events))
	for _, e := range req.Events {
		id, er := hex.DecodeString(e.ID)
		if er != nil {
			return out, er
		}
		ids = append(ids, string(id))
	}
	var archived uint64
	if err := s.conn.QueryRow(ctx, `SELECT count() FROM trail_event_archives FINAL WHERE archive_id=? AND event_id IN ?`, req.ArchiveID, ids).Scan(&archived); err != nil {
		return out, err
	}
	if archived != uint64(len(ids)) {
		return out, fmt.Errorf("trail/clickhouse: archive receipt does not cover deletion batch")
	}
	now := time.Now().UTC()
	seen := map[string]bool{}
	type staged struct {
		ref                 payload.Ref
		eligible, candidate time.Time
	}
	var stagedItems []staged
	for _, e := range req.Events {
		for _, p := range e.Payloads {
			k := p.Ref.Store + "\x00" + p.Ref.Key
			at, ok := req.PayloadEligibleAt[k]
			if !ok || seen[k] || p.Ref.Store == "" || p.Ref.Key == "" {
				continue
			}
			seen[k] = true
			candidateAt := now
			var count uint64
			var oldEligible, oldCandidate time.Time
			if er := s.conn.QueryRow(ctx, `SELECT count(),max(eligible_at),min(candidate_at) FROM trail_payload_gc_candidates FINAL WHERE store=? AND object_key=?`, p.Ref.Store, p.Ref.Key).Scan(&count, &oldEligible, &oldCandidate); er != nil {
				return out, er
			}
			if count > 0 {
				if oldEligible.After(at) {
					at = oldEligible
				}
				if oldCandidate.Before(candidateAt) {
					candidateAt = oldCandidate
				}
			}
			stagedItems = append(stagedItems, staged{ref: p.Ref, eligible: at, candidate: candidateAt})
		}
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO trail_payload_gc_candidates (store,object_key,ref_json,eligible_at,candidate_at,retry_after,attempts,last_error,archive_id,version)`)
	if err != nil {
		return out, err
	}
	for _, item := range stagedItems {
		raw, er := json.Marshal(item.ref)
		if er != nil {
			_ = batch.Abort()
			return out, er
		}
		if er = batch.Append(item.ref.Store, item.ref.Key, string(raw), item.eligible, item.candidate, now, uint32(0), "", req.ArchiveID, now); er != nil {
			_ = batch.Abort()
			return out, er
		}
		out.PayloadCandidates++
	}
	if out.PayloadCandidates > 0 {
		if err = batch.Send(); err != nil {
			return retention.DeleteResult{}, err
		}
	} else {
		_ = batch.Abort()
	}
	if err = s.stageRetentionSummaryRebuilds(ctx, req.Events); err != nil {
		return out, err
	}
	if err = s.conn.Exec(ctx, `ALTER TABLE trail_events DELETE WHERE event_id IN ? SETTINGS mutations_sync=2`, ids); err != nil {
		return out, err
	}
	out.EventsDeleted = uint64(len(ids))
	if err = s.ReconcileRetention(ctx); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) LoadArchiveEvents(ctx context.Context, events []retention.Event) ([]storage.EventRecord, error) {
	ids := make([]string, 0, len(events))
	for _, e := range events {
		id, err := hex.DecodeString(e.ID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, string(id))
	}
	rows, err := s.conn.Query(ctx, eventRows+` FINAL WHERE event_id IN ? ORDER BY event_time,event_id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]storage.EventRecord, 0, len(events))
	for rows.Next() {
		v, err := scanCHEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) RecordArchive(ctx context.Context, receipt archive.Receipt) error {
	manifest, err := json.Marshal(receipt.Manifest)
	if err != nil {
		return err
	}
	ref, err := json.Marshal(receipt.ManifestRef)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err = s.conn.Exec(ctx, `INSERT INTO trail_archives VALUES(?,?,?,?,?)`, receipt.Manifest.ID, string(manifest), string(ref), receipt.Manifest.CreatedAt, now); err != nil {
		return err
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO trail_event_archives`)
	if err != nil {
		return err
	}
	for _, id := range receipt.Manifest.EventIDs {
		parsed, er := trail.ParseEventID(id)
		if er != nil {
			_ = batch.Abort()
			return er
		}
		if er = batch.Append(string(parsed[:]), receipt.Manifest.ID, now, now); er != nil {
			_ = batch.Abort()
			return er
		}
	}
	return batch.Send()
}

func (s *Store) ListArchives(ctx context.Context, offset, limit int) (archive.Page, error) {
	if offset < 0 || limit < 1 || limit > 500 {
		return archive.Page{}, storage.ErrInvalidCursor
	}
	rows, err := s.conn.Query(ctx, `SELECT manifest_json,manifest_ref_json FROM trail_archives FINAL ORDER BY created_at DESC,archive_id LIMIT ? OFFSET ?`, limit+1, offset)
	if err != nil {
		return archive.Page{}, err
	}
	defer rows.Close()
	var out archive.Page
	for rows.Next() {
		var manifestJSON, refJSON string
		if err = rows.Scan(&manifestJSON, &refJSON); err != nil {
			return out, err
		}
		var receipt archive.Receipt
		if err = json.Unmarshal([]byte(manifestJSON), &receipt.Manifest); err != nil {
			return out, err
		}
		if err = json.Unmarshal([]byte(refJSON), &receipt.ManifestRef); err != nil {
			return out, err
		}
		out.Items = append(out.Items, receipt)
	}
	if len(out.Items) > limit {
		out.Items = out.Items[:limit]
		out.HasMore = true
	}
	return out, rows.Err()
}

func (s *Store) GetArchive(ctx context.Context, id string) (archive.Receipt, error) {
	var manifestJSON, refJSON string
	var count uint64
	if err := s.conn.QueryRow(ctx, `SELECT count(),any(manifest_json),any(manifest_ref_json) FROM trail_archives FINAL WHERE archive_id=?`, id).Scan(&count, &manifestJSON, &refJSON); err != nil {
		return archive.Receipt{}, err
	}
	if count == 0 {
		return archive.Receipt{}, storage.ErrNotFound
	}
	var out archive.Receipt
	if err := json.Unmarshal([]byte(manifestJSON), &out.Manifest); err != nil {
		return out, err
	}
	if err := json.Unmarshal([]byte(refJSON), &out.ManifestRef); err != nil {
		return out, err
	}
	return out, nil
}

// RestoreArchiveEvents uses an archive-specific insertion identity. The
// ReplacingMergeTree event_id key makes repeated restores idempotent after
// merges, while the token makes immediate retries safe.
func (s *Store) RestoreArchiveEvents(ctx context.Context, archiveID string, events []storage.EventRecord) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	raw, err := hex.DecodeString(archiveID)
	if err != nil || len(raw) < 16 {
		return 0, fmt.Errorf("trail/clickhouse: invalid archive ID")
	}
	var batchID trail.EventID
	copy(batchID[:], raw[:16])
	for i := range events {
		events[i].BatchID = batchID
		events[i].Delivery.Stream = "trail-archive-restore"
	}
	if err = s.WriteBatches(ctx, []storage.Batch{{BatchID: batchID, Delivery: storage.Delivery{Stream: "trail-archive-restore"}, Events: events}}); err != nil {
		return 0, err
	}
	return len(events), nil
}

func (s *Store) stageRetentionSummaryRebuilds(ctx context.Context, events []retention.Event) error {
	now := time.Now().UTC()
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO trail_retention_summary_rebuilds`)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	count := 0
	for _, e := range events {
		for _, v := range []struct{ kind, id string }{{"flow", e.FlowID}, {"execution", e.ExecutionID}} {
			if v.id == "" || v.id == strings.Repeat("0", 32) || seen[v.kind+v.id] {
				continue
			}
			id, er := hex.DecodeString(v.id)
			if er != nil {
				_ = batch.Abort()
				return er
			}
			if er = batch.Append(v.kind, string(id), now); er != nil {
				_ = batch.Abort()
				return er
			}
			seen[v.kind+v.id] = true
			count++
		}
	}
	if count == 0 {
		_ = batch.Abort()
		return nil
	}
	return batch.Send()
}

func (s *Store) ReconcileRetention(ctx context.Context) error {
	rows, err := s.conn.Query(ctx, `SELECT kind,identity FROM trail_retention_summary_rebuilds FINAL ORDER BY kind,identity LIMIT 1000`)
	if err != nil {
		return err
	}
	var events []retention.Event
	var taskIDs []string
	for rows.Next() {
		var kind, id string
		if err = rows.Scan(&kind, &id); err != nil {
			rows.Close()
			return err
		}
		hexID := hex.EncodeToString([]byte(id))
		e := retention.Event{}
		if kind == "flow" {
			e.FlowID = hexID
		} else {
			e.ExecutionID = hexID
		}
		events = append(events, e)
		taskIDs = append(taskIDs, id)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(events) == 0 {
		return nil
	}
	if err = s.rebuildRetentionSummaries(ctx, events); err != nil {
		return err
	}
	return s.conn.Exec(ctx, `ALTER TABLE trail_retention_summary_rebuilds DELETE WHERE identity IN ? SETTINGS mutations_sync=2`, taskIDs)
}

func (s *Store) rebuildRetentionSummaries(ctx context.Context, events []retention.Event) error {
	zero := strings.Repeat("0", 32)
	flowSet, executionSet := map[string]bool{}, map[string]bool{}
	for _, e := range events {
		if e.FlowID != "" && e.FlowID != zero {
			b, err := hex.DecodeString(e.FlowID)
			if err != nil {
				return err
			}
			flowSet[string(b)] = true
		}
		if e.ExecutionID != "" && e.ExecutionID != zero {
			b, err := hex.DecodeString(e.ExecutionID)
			if err != nil {
				return err
			}
			executionSet[string(b)] = true
		}
	}
	flows := make([]string, 0, len(flowSet))
	for id := range flowSet {
		flows = append(flows, id)
	}
	executions := make([]string, 0, len(executionSet))
	for id := range executionSet {
		executions = append(executions, id)
	}
	if len(flows) > 0 {
		if err := s.conn.Exec(ctx, `ALTER TABLE trail_flow_summaries DELETE WHERE flow_id IN ? SETTINGS mutations_sync=2`, flows); err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, `INSERT INTO trail_flow_summaries SELECT flow_id,service,environment,minState(event_time),maxState(event_time),uniqExactState(event_id),argMaxState(entity_type,tuple(event_time,event_id)),argMaxState(entity_id,tuple(event_time,event_id)),argMaxState(execution_id,tuple(event_time,event_id)),argMaxIfState(flow_status,tuple(event_time,event_id),notEmpty(flow_status)),argMinState(kind,tuple(event_time,event_id)),argMaxState(kind,tuple(event_time,event_id)) FROM trail_events WHERE flow_id IN ? GROUP BY flow_id,service,environment`, flows); err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, `ALTER TABLE trail_flow_terminals DELETE WHERE flow_id IN ? SETTINGS mutations_sync=2`, flows); err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, `INSERT INTO trail_flow_terminals SELECT flow_id,event_time,service,environment,entity_type,entity_id,execution_id,flow_status FROM trail_events WHERE flow_id IN ? AND flow_status IN ('succeeded','failed','cancelled')`, flows); err != nil {
			return err
		}
	}
	if len(executions) > 0 {
		if err := s.conn.Exec(ctx, `ALTER TABLE trail_execution_summaries DELETE WHERE execution_id IN ? SETTINGS mutations_sync=2`, executions); err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, `INSERT INTO trail_execution_summaries SELECT execution_id,service,environment,minState(event_time),maxState(event_time),uniqExactState(event_id),uniqExactStateIf(flow_id,flow_id!=unhex('00000000000000000000000000000000')),argMaxIfState(execution_kind,tuple(event_time,event_id),notEmpty(execution_kind)),argMaxIfState(execution_source,tuple(event_time,event_id),notEmpty(execution_source)),argMaxIfState(execution_status,tuple(event_time,event_id),notEmpty(execution_status)),maxState(execution_attempt),argMaxState(parent_execution_id,tuple(event_time,event_id)),argMaxState(retry_of_execution_id,tuple(event_time,event_id)) FROM trail_events WHERE execution_id IN ? GROUP BY execution_id,service,environment`, executions); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListPayloadCandidates(ctx context.Context, before time.Time, limit int) (retention.CandidatePage, error) {
	rows, err := s.conn.Query(ctx, `SELECT ref_json,archive_id,eligible_at,candidate_at,attempts,last_error FROM trail_payload_gc_candidates FINAL WHERE archive_id!='' AND eligible_at<=now64(9) AND candidate_at<=? AND retry_after<=now64(9) ORDER BY candidate_at,store,object_key LIMIT ?`, before, limit)
	if err != nil {
		return retention.CandidatePage{}, err
	}
	defer rows.Close()
	var out retention.CandidatePage
	for rows.Next() {
		var raw string
		var c retention.Candidate
		if err = rows.Scan(&raw, &c.ArchiveID, &c.EligibleAt, &c.CandidateAt, &c.Attempts, &c.LastError); err != nil {
			return out, err
		}
		if err = json.Unmarshal([]byte(raw), &c.Ref); err != nil {
			return out, err
		}
		out.Candidates = append(out.Candidates, c)
	}
	return out, rows.Err()
}
func (s *Store) ReferencedPayloads(ctx context.Context, refs []payload.Ref) (map[string]bool, error) {
	out := map[string]bool{}
	for _, ref := range refs {
		rows, err := s.conn.Query(ctx, `SELECT payload_refs_json FROM trail_events WHERE position(payload_refs_json,?)>0`, ref.Key)
		if err != nil {
			return nil, err
		}
		found := false
		for rows.Next() {
			var raw string
			if err = rows.Scan(&raw); err != nil {
				rows.Close()
				return nil, err
			}
			var links []storage.PayloadLink
			if json.Unmarshal([]byte(raw), &links) == nil {
				for _, p := range links {
					if p.Ref.Store == ref.Store && p.Ref.Key == ref.Key {
						found = true
						break
					}
				}
			}
			if found {
				break
			}
		}
		rows.Close()
		out[ref.Store+"\x00"+ref.Key] = found
	}
	return out, nil
}
func (s *Store) CompletePayloadCandidate(ctx context.Context, ref payload.Ref, failure string) error {
	if failure == "" || failure == "referenced" {
		return s.conn.Exec(ctx, `ALTER TABLE trail_payload_gc_candidates DELETE WHERE store=? AND object_key=? SETTINGS mutations_sync=2`, ref.Store, ref.Key)
	}
	var raw string
	var archiveID string
	var eligible, candidate time.Time
	var attempts uint32
	err := s.conn.QueryRow(ctx, `SELECT ref_json,archive_id,eligible_at,candidate_at,attempts FROM trail_payload_gc_candidates FINAL WHERE store=? AND object_key=?`, ref.Store, ref.Key).Scan(&raw, &archiveID, &eligible, &candidate, &attempts)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return s.conn.Exec(ctx, `INSERT INTO trail_payload_gc_candidates (store,object_key,ref_json,eligible_at,candidate_at,retry_after,attempts,last_error,archive_id,version) VALUES(?,?,?,?,?,?,?,?,?,?)`, ref.Store, ref.Key, raw, eligible, candidate, now.Add(time.Hour), attempts+1, failure, archiveID, now)
}
func chRetentionCursor(t time.Time, id []byte) string {
	return strconv.FormatInt(t.UnixNano(), 10) + ":" + hex.EncodeToString(id)
}
func parseCHRetentionCursor(v string) (time.Time, []byte, error) {
	a, b, ok := strings.Cut(v, ":")
	if !ok {
		return time.Time{}, nil, fmt.Errorf("invalid retention cursor")
	}
	n, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return time.Time{}, nil, err
	}
	id, err := hex.DecodeString(b)
	return time.Unix(0, n).UTC(), id, err
}
func chRetentionLevel(v string) trail.Level {
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

var _ retention.Store = (*Store)(nil)
var _ archive.Catalog = (*Store)(nil)
var _ archive.RestoreStore = (*Store)(nil)
var _ = wire.Field{}
