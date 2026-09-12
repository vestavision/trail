package postgres

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
	args := []any{}
	where := ""
	if req.Cursor != "" {
		ts, id, err := parseRetentionCursor(req.Cursor)
		if err != nil {
			return retention.EventPage{}, err
		}
		args = []any{ts, id}
		where = " WHERE (event_time,event_id)>($1,$2)"
	}
	args = append(args, req.Limit+1)
	q := `SELECT event_time,event_id,flow_id,execution_id,kind,level,service,environment,flow_status,execution_status,http_status_code,http_success,fields,payload_refs FROM trail_events` + where + fmt.Sprintf(` ORDER BY event_time,event_id LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return retention.EventPage{}, err
	}
	defer rows.Close()
	var out retention.EventPage
	for rows.Next() {
		var v retention.Event
		var id, flow, execution, fields, payloads []byte
		var level string
		if err = rows.Scan(&v.Timestamp, &id, &flow, &execution, &v.Kind, &level, &v.Service, &v.Environment, &v.FlowStatus, &v.ExecutionStatus, &v.HTTPStatusCode, &v.HTTPSuccess, &fields, &payloads); err != nil {
			return out, err
		}
		v.ID = hex.EncodeToString(id)
		v.FlowID = hex.EncodeToString(flow)
		v.ExecutionID = hex.EncodeToString(execution)
		v.Cursor = retentionCursor(v.Timestamp, id)
		v.Level = retentionLevel(level)
		var links []storage.PayloadLink
		if err = json.Unmarshal(payloads, &links); err != nil {
			return out, err
		}
		for _, p := range links {
			v.Payloads = append(v.Payloads, retention.Payload{Role: p.Role, Ref: p.Ref})
		}
		var fs []wire.Field
		if json.Unmarshal(fields, &fs) == nil {
			for _, f := range fs {
				if f.Key == convention.FieldRetentionClass {
					v.RetentionClass = f.Text
					break
				}
			}
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
		return out, fmt.Errorf("trail/postgres: archive receipt is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	seen := map[string]bool{}
	for _, e := range req.Events {
		for _, p := range e.Payloads {
			k := p.Ref.Store + "\x00" + p.Ref.Key
			at, ok := req.PayloadEligibleAt[k]
			if !ok || seen[k] || p.Ref.Store == "" || p.Ref.Key == "" {
				continue
			}
			seen[k] = true
			b, er := json.Marshal(p.Ref)
			if er != nil {
				return out, er
			}
			_, er = tx.Exec(ctx, `INSERT INTO trail_payload_gc_candidates(store,object_key,ref,eligible_at,archive_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(store,object_key) DO UPDATE SET eligible_at=GREATEST(trail_payload_gc_candidates.eligible_at,EXCLUDED.eligible_at),ref=EXCLUDED.ref,archive_id=EXCLUDED.archive_id`, p.Ref.Store, p.Ref.Key, b, at, req.ArchiveID)
			if er != nil {
				return out, er
			}
			out.PayloadCandidates++
		}
	}
	ids := make([][]byte, 0, len(req.Events))
	for _, e := range req.Events {
		id, er := hex.DecodeString(e.ID)
		if er != nil {
			return out, er
		}
		ids = append(ids, id)
	}
	var archived int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM trail_event_archives WHERE archive_id=$1 AND event_id=ANY($2)`, req.ArchiveID, ids).Scan(&archived); err != nil {
		return out, err
	}
	if archived != len(ids) {
		return out, fmt.Errorf("trail/postgres: archive receipt does not cover deletion batch")
	}
	ct, err := tx.Exec(ctx, `DELETE FROM trail_events WHERE event_id=ANY($1)`, ids)
	if err != nil {
		return out, err
	}
	out.EventsDeleted = uint64(ct.RowsAffected())
	if err = tx.Commit(ctx); err != nil {
		return retention.DeleteResult{}, err
	}
	return out, nil
}

func (s *Store) LoadArchiveEvents(ctx context.Context, events []retention.Event) ([]storage.EventRecord, error) {
	ids := make([][]byte, 0, len(events))
	for _, e := range events {
		id, err := hex.DecodeString(e.ID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	rows, err := s.pool.Query(ctx, eventSelect+` WHERE event_id=ANY($1) ORDER BY event_time,event_id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]storage.EventRecord, 0, len(events))
	for rows.Next() {
		v, err := scanEvent(rows)
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO trail_archives(archive_id,manifest,manifest_ref,created_at) VALUES($1,$2,$3,$4) ON CONFLICT(archive_id) DO NOTHING`, receipt.Manifest.ID, manifest, ref, receipt.Manifest.CreatedAt); err != nil {
		return err
	}
	for _, id := range receipt.Manifest.EventIDs {
		parsed, er := trail.ParseEventID(id)
		if er != nil {
			return er
		}
		raw := parsed[:]
		if _, er = tx.Exec(ctx, `INSERT INTO trail_event_archives(event_id,archive_id) VALUES($1,$2) ON CONFLICT(event_id) DO UPDATE SET archive_id=EXCLUDED.archive_id`, raw, receipt.Manifest.ID); er != nil {
			return er
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) ListArchives(ctx context.Context, offset, limit int) (archive.Page, error) {
	if offset < 0 || limit < 1 || limit > 500 {
		return archive.Page{}, storage.ErrInvalidCursor
	}
	rows, err := s.pool.Query(ctx, `SELECT manifest,manifest_ref FROM trail_archives ORDER BY created_at DESC,archive_id LIMIT $1 OFFSET $2`, limit+1, offset)
	if err != nil {
		return archive.Page{}, err
	}
	defer rows.Close()
	var out archive.Page
	for rows.Next() {
		var manifestJSON, refJSON []byte
		if err = rows.Scan(&manifestJSON, &refJSON); err != nil {
			return out, err
		}
		var receipt archive.Receipt
		if err = json.Unmarshal(manifestJSON, &receipt.Manifest); err != nil {
			return out, err
		}
		if err = json.Unmarshal(refJSON, &receipt.ManifestRef); err != nil {
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
	var manifestJSON, refJSON []byte
	if err := s.pool.QueryRow(ctx, `SELECT manifest,manifest_ref FROM trail_archives WHERE archive_id=$1`, id).Scan(&manifestJSON, &refJSON); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return archive.Receipt{}, storage.ErrNotFound
		}
		return archive.Receipt{}, err
	}
	var out archive.Receipt
	if err := json.Unmarshal(manifestJSON, &out.Manifest); err != nil {
		return out, err
	}
	if err := json.Unmarshal(refJSON, &out.ManifestRef); err != nil {
		return out, err
	}
	return out, nil
}

// RestoreArchiveEvents performs an idempotent merge using the event_id primary
// key. Existing live events are intentionally preserved.
func (s *Store) RestoreArchiveEvents(ctx context.Context, _ string, events []storage.EventRecord) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	if err := s.WriteBatches(ctx, []storage.Batch{{Events: events}}); err != nil {
		return 0, err
	}
	return len(events), nil
}

func (s *Store) ListPayloadCandidates(ctx context.Context, before time.Time, limit int) (retention.CandidatePage, error) {
	rows, err := s.pool.Query(ctx, `SELECT ref,archive_id,eligible_at,candidate_at,attempts,last_error FROM trail_payload_gc_candidates WHERE archive_id<>'' AND eligible_at<=now() AND candidate_at<=$1 AND retry_after<=now() ORDER BY candidate_at,store,object_key LIMIT $2`, before, limit)
	if err != nil {
		return retention.CandidatePage{}, err
	}
	defer rows.Close()
	var out retention.CandidatePage
	for rows.Next() {
		var raw []byte
		var c retention.Candidate
		if err = rows.Scan(&raw, &c.ArchiveID, &c.EligibleAt, &c.CandidateAt, &c.Attempts, &c.LastError); err != nil {
			return out, err
		}
		if err = json.Unmarshal(raw, &c.Ref); err != nil {
			return out, err
		}
		out.Candidates = append(out.Candidates, c)
	}
	return out, rows.Err()
}
func (s *Store) ReferencedPayloads(ctx context.Context, refs []payload.Ref) (map[string]bool, error) {
	out := map[string]bool{}
	for _, ref := range refs {
		var yes bool
		err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM trail_events e CROSS JOIN LATERAL jsonb_array_elements(e.payload_refs) p WHERE p->'Ref'->>'store'=$1 AND p->'Ref'->>'key'=$2 LIMIT 1)`, ref.Store, ref.Key).Scan(&yes)
		if err != nil {
			return nil, err
		}
		out[ref.Store+"\x00"+ref.Key] = yes
	}
	return out, nil
}
func (s *Store) CompletePayloadCandidate(ctx context.Context, ref payload.Ref, failure string) error {
	if failure == "" || failure == "referenced" {
		_, err := s.pool.Exec(ctx, `DELETE FROM trail_payload_gc_candidates WHERE store=$1 AND object_key=$2`, ref.Store, ref.Key)
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE trail_payload_gc_candidates SET attempts=attempts+1,last_error=$3,retry_after=now()+interval '1 hour' WHERE store=$1 AND object_key=$2`, ref.Store, ref.Key, failure)
	return err
}

func retentionCursor(t time.Time, id []byte) string {
	return strconv.FormatInt(t.UnixNano(), 10) + ":" + hex.EncodeToString(id)
}
func parseRetentionCursor(v string) (time.Time, []byte, error) {
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
func retentionLevel(v string) trail.Level {
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

func (s *Store) ReconcileRetention(context.Context) error { return nil }
