// Package explorerapi exposes Trail's storage-neutral Explorer HTTP API.
package explorerapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/archive"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/storage"
)

type Config struct {
	Store           storage.ExplorerStore
	PayloadStores   map[string]payload.Store
	MaxPayloadBytes int64
	AllowedOrigins  []string
	ArchiveCatalog  archive.Catalog
	ArchiveStore    payload.Store
	ArchiveRestore  archive.RestoreStore
	EnableRestore   bool
}

type Server struct {
	cfg     Config
	handler http.Handler
}

func New(cfg Config) (*Server, error) {
	if cfg.Store == nil {
		return nil, errors.New("trail/explorerapi: store is required")
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = 1 << 20
	}
	s := &Server{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/overview", s.overview)
	mux.HandleFunc("GET /api/v1/overview/activity", s.overviewActivity)
	mux.HandleFunc("GET /api/v1/flows", s.flows)
	mux.HandleFunc("GET /api/v1/flows/{id}", s.flow)
	mux.HandleFunc("GET /api/v1/flows/{id}/events", s.flowEvents)
	mux.HandleFunc("GET /api/v1/executions", s.executions)
	mux.HandleFunc("GET /api/v1/executions/{id}", s.execution)
	mux.HandleFunc("GET /api/v1/executions/{id}/flows", s.executionFlows)
	mux.HandleFunc("GET /api/v1/executions/{id}/retries", s.retries)
	mux.HandleFunc("GET /api/v1/entities", s.entities)
	mux.HandleFunc("GET /api/v1/entities/{type}/{id}", s.entity)
	mux.HandleFunc("GET /api/v1/entities/{type}/{id}/flows", s.entityFlows)
	mux.HandleFunc("GET /api/v1/events", s.events)
	mux.HandleFunc("GET /api/v1/events/{id}", s.event)
	mux.HandleFunc("GET /api/v1/events/{event}/payloads/{role}", s.eventPayload)
	mux.HandleFunc("GET /api/v1/archives", s.archives)
	mux.HandleFunc("GET /api/v1/archives/{id}", s.archiveDetail)
	mux.HandleFunc("GET /api/v1/archives/{id}/events", s.archiveEvents)
	mux.HandleFunc("GET /api/v1/archives/{id}/payloads/{index}", s.archivePayload)
	mux.HandleFunc("POST /api/v1/archives/{id}/restore", s.restoreArchive)
	s.handler = recoverMiddleware(cors(cfg.AllowedOrigins, mux))
	return s, nil
}

func (s *Server) archives(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ArchiveCatalog == nil {
		respondError(w, http.StatusNotImplemented, "capability_unavailable", "archive catalog is not configured")
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, _ = strconv.Atoi(raw)
	}
	if offset < 0 || limit < 1 || limit > 500 {
		bad(w, errors.New("offset and limit are invalid"))
		return
	}
	v, err := s.cfg.ArchiveCatalog.ListArchives(r.Context(), offset, limit)
	respond(w, v, err)
}

func (s *Server) archiveDetail(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ArchiveCatalog == nil {
		respondError(w, http.StatusNotImplemented, "capability_unavailable", "archive catalog is not configured")
		return
	}
	v, err := s.cfg.ArchiveCatalog.GetArchive(r.Context(), r.PathValue("id"))
	respond(w, v, err)
}

func (s *Server) archiveEvents(w http.ResponseWriter, r *http.Request) {
	receipt, offset, limit, ok := s.archiveRequest(w, r)
	if !ok {
		return
	}
	events, complete, err := archive.ReadEvents(r.Context(), s.cfg.ArchiveStore, receipt.Manifest, offset, limit)
	respond(w, map[string]any{"items": events, "next_offset": offset + len(events), "complete": complete}, err)
}

func (s *Server) archivePayload(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ArchiveCatalog == nil || s.cfg.ArchiveStore == nil {
		respondError(w, http.StatusNotImplemented, "capability_unavailable", "archive access is not configured")
		return
	}
	receipt, err := s.cfg.ArchiveCatalog.GetArchive(r.Context(), r.PathValue("id"))
	if err != nil {
		respond(w, nil, err)
		return
	}
	index, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || index < 0 || index >= len(receipt.Manifest.Payloads) {
		bad(w, errors.New("invalid payload index"))
		return
	}
	ref := receipt.Manifest.Payloads[index].Archive
	if ref.Size > s.cfg.MaxPayloadBytes {
		respondError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "payload exceeds Explorer read limit")
		return
	}
	reader, err := s.cfg.ArchiveStore.Open(r.Context(), ref)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", safeContentType(ref.ContentType))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if ref.Compression == payload.CompressionGZIP {
		w.Header().Set("Content-Encoding", "gzip")
	}
	_, _ = io.Copy(w, io.LimitReader(reader, s.cfg.MaxPayloadBytes))
}

func (s *Server) restoreArchive(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableRestore || s.cfg.ArchiveRestore == nil {
		respondError(w, http.StatusNotFound, "not_found", "archive restore is not enabled")
		return
	}
	receipt, offset, limit, ok := s.archiveRequest(w, r)
	if !ok {
		return
	}
	v, err := archive.Restore(r.Context(), s.cfg.ArchiveStore, s.cfg.PayloadStores, s.cfg.ArchiveRestore, receipt, offset, limit)
	respond(w, v, err)
}

func (s *Server) archiveRequest(w http.ResponseWriter, r *http.Request) (archive.Receipt, int, int, bool) {
	if s.cfg.ArchiveCatalog == nil || s.cfg.ArchiveStore == nil {
		respondError(w, http.StatusNotImplemented, "capability_unavailable", "archive access is not configured")
		return archive.Receipt{}, 0, 0, false
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, _ = strconv.Atoi(raw)
	}
	if offset < 0 || limit < 1 || limit > 10_000 {
		bad(w, errors.New("offset and limit are invalid"))
		return archive.Receipt{}, 0, 0, false
	}
	receipt, err := s.cfg.ArchiveCatalog.GetArchive(r.Context(), r.PathValue("id"))
	if err != nil {
		respond(w, nil, err)
		return archive.Receipt{}, 0, 0, false
	}
	return receipt, offset, limit, true
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	v, e := s.cfg.Store.Overview(r.Context(), overviewFilter(r))
	respond(w, v, e)
}
func (s *Server) overviewActivity(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.cfg.Store.(storage.OverviewActivityReader)
	if !ok {
		respondError(w, http.StatusNotImplemented, "capability_unavailable", "overview activity is not supported by this storage adapter")
		return
	}
	f, err := overviewActivityFilter(r)
	if err != nil {
		bad(w, err)
		return
	}
	v, err := reader.OverviewActivity(r.Context(), f)
	respond(w, v, err)
}
func (s *Server) flows(w http.ResponseWriter, r *http.Request) {
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.ListFlows(r.Context(), flowFilter(r), p)
	respondPage(w, v, e)
}
func (s *Server) flow(w http.ResponseWriter, r *http.Request) {
	id, e := trail.ParseFlowID(r.PathValue("id"))
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.GetFlow(r.Context(), id)
	respond(w, v, e)
}
func (s *Server) flowEvents(w http.ResponseWriter, r *http.Request) {
	id, e := trail.ParseFlowID(r.PathValue("id"))
	if e != nil {
		bad(w, e)
		return
	}
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.ListFlowEvents(r.Context(), id, eventFilter(r), p)
	respondPage(w, v, e)
}
func (s *Server) executions(w http.ResponseWriter, r *http.Request) {
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.ListExecutions(r.Context(), executionFilter(r), p)
	respondPage(w, v, e)
}
func (s *Server) execution(w http.ResponseWriter, r *http.Request) {
	id, e := trail.ParseExecutionID(r.PathValue("id"))
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.GetExecution(r.Context(), id)
	respond(w, v, e)
}
func (s *Server) executionFlows(w http.ResponseWriter, r *http.Request) {
	id, e := trail.ParseExecutionID(r.PathValue("id"))
	if e != nil {
		bad(w, e)
		return
	}
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	f := flowFilter(r)
	f.ExecutionID = id
	v, e := s.cfg.Store.ListExecutionFlows(r.Context(), id, f, p)
	respondPage(w, v, e)
}
func (s *Server) retries(w http.ResponseWriter, r *http.Request) {
	id, e := trail.ParseExecutionID(r.PathValue("id"))
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.GetRetryChain(r.Context(), id)
	respond(w, v, e)
}
func (s *Server) entities(w http.ResponseWriter, r *http.Request) {
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.SearchEntities(r.Context(), entityFilter(r), p)
	respondPage(w, v, e)
}
func (s *Server) entity(w http.ResponseWriter, r *http.Request) {
	key := storage.EntityKey{Type: r.PathValue("type"), ID: r.PathValue("id")}
	if key.Type == "" || key.ID == "" {
		bad(w, errors.New("entity type and ID are required"))
		return
	}
	v, e := s.cfg.Store.GetEntity(r.Context(), key)
	respond(w, v, e)
}
func (s *Server) entityFlows(w http.ResponseWriter, r *http.Request) {
	key := storage.EntityKey{Type: r.PathValue("type"), ID: r.PathValue("id")}
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.ListEntityFlows(r.Context(), key, flowFilter(r), p)
	respondPage(w, v, e)
}
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	p, e := page(r)
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.ListEvents(r.Context(), eventFilter(r), p)
	respondPage(w, v, e)
}
func (s *Server) event(w http.ResponseWriter, r *http.Request) {
	id, e := trail.ParseEventID(r.PathValue("id"))
	if e != nil {
		bad(w, e)
		return
	}
	v, e := s.cfg.Store.GetEvent(r.Context(), id)
	respond(w, v, e)
}

func (s *Server) eventPayload(w http.ResponseWriter, r *http.Request) {
	id, err := trail.ParseEventID(r.PathValue("event"))
	if err != nil {
		bad(w, err)
		return
	}
	event, err := s.cfg.Store.GetEvent(r.Context(), id)
	if err != nil {
		respond(w, nil, err)
		return
	}
	var link *storage.PayloadLink
	for i := range event.Payloads {
		if event.Payloads[i].Role == r.PathValue("role") {
			link = &event.Payloads[i]
			break
		}
	}
	if link == nil {
		respond(w, nil, storage.ErrNotFound)
		return
	}
	store := s.cfg.PayloadStores[link.Ref.Store]
	if store == nil {
		respondError(w, http.StatusNotFound, "payload_store_unavailable", "payload store is not configured")
		return
	}
	if link.Ref.Size > s.cfg.MaxPayloadBytes {
		respondError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "payload exceeds Explorer read limit")
		return
	}
	reader, err := store.Open(r.Context(), link.Ref)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", safeContentType(link.Ref.ContentType))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", "inline")
	if link.Ref.Compression == payload.CompressionGZIP {
		w.Header().Set("Content-Encoding", "gzip")
	}
	_, _ = io.Copy(w, io.LimitReader(reader, s.cfg.MaxPayloadBytes))
}

func page(r *http.Request) (storage.PageRequest, error) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 500 {
			return storage.PageRequest{}, errors.New("limit must be between 1 and 500")
		}
		limit = n
	}
	c, e := decodeCursor(r.URL.Query().Get("cursor"))
	return storage.PageRequest{Limit: limit, Cursor: c, Desc: r.URL.Query().Get("order") != "asc"}, e
}
func decodeCursor(raw string) (storage.Cursor, error) {
	if raw == "" {
		return storage.Cursor{}, nil
	}
	data, e := base64.RawURLEncoding.DecodeString(raw)
	if e != nil {
		return storage.Cursor{}, storage.ErrInvalidCursor
	}
	var c storage.Cursor
	if e = json.Unmarshal(data, &c); e != nil {
		return storage.Cursor{}, storage.ErrInvalidCursor
	}
	return c, nil
}
func encodeCursor(c storage.Cursor) string {
	if c.ID == "" {
		return ""
	}
	data, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(data)
}
func timeRange(r *http.Request) storage.TimeRange {
	q := r.URL.Query()
	var x storage.TimeRange
	x.From, _ = time.Parse(time.RFC3339Nano, q.Get("from"))
	x.To, _ = time.Parse(time.RFC3339Nano, q.Get("to"))
	return x
}
func overviewFilter(r *http.Request) storage.OverviewFilter {
	q := r.URL.Query()
	return storage.OverviewFilter{Time: timeRange(r), Service: q.Get("service"), Environment: q.Get("environment")}
}
func overviewActivityFilter(r *http.Request) (storage.OverviewActivityFilter, error) {
	base := overviewFilter(r)
	if !base.Time.From.IsZero() && !base.Time.To.IsZero() && !base.Time.From.Before(base.Time.To) {
		return storage.OverviewActivityFilter{}, errors.New("from must be before to")
	}
	interval := storage.ActivityInterval(r.URL.Query().Get("interval"))
	if interval == "" {
		interval = storage.ActivityDay
	}
	if interval != storage.ActivityHour && interval != storage.ActivityDay {
		return storage.OverviewActivityFilter{}, errors.New("interval must be hour or day")
	}
	return storage.OverviewActivityFilter{OverviewFilter: base, Interval: interval}, nil
}
func flowFilter(r *http.Request) storage.FlowFilter {
	q := r.URL.Query()
	f := storage.FlowFilter{Time: timeRange(r), Service: q.Get("service"), Environment: q.Get("environment"), Status: q.Get("status"), EntityType: q.Get("entity_type"), EntityID: q.Get("entity_id")}
	if x := q.Get("execution_id"); x != "" {
		f.ExecutionID, _ = trail.ParseExecutionID(x)
	}
	return f
}
func executionFilter(r *http.Request) storage.ExecutionFilter {
	q := r.URL.Query()
	return storage.ExecutionFilter{Time: timeRange(r), Service: q.Get("service"), Environment: q.Get("environment"), Status: q.Get("status"), Source: trail.ExecutionSource(q.Get("source")), Kind: q.Get("kind")}
}
func entityFilter(r *http.Request) storage.EntityFilter {
	q := r.URL.Query()
	return storage.EntityFilter{Time: timeRange(r), Service: q.Get("service"), Environment: q.Get("environment"), EntityType: q.Get("entity_type"), IDPrefix: q.Get("q")}
}
func eventFilter(r *http.Request) storage.EventFilter {
	q := r.URL.Query()
	f := storage.EventFilter{Time: timeRange(r), Service: q.Get("service"), Environment: q.Get("environment"), Kind: q.Get("kind")}
	if raw := q.Get("http_status_class"); raw != "" {
		f.HTTPStatusClass, _ = strconv.Atoi(raw)
	}
	if raw := q.Get("level"); raw != "" {
		l := parseLevel(raw)
		f.Level = &l
	}
	return f
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

func respondPage[T any](w http.ResponseWriter, p storage.Page[T], err error) {
	if err != nil {
		respond(w, nil, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": p.Items, "next_cursor": encodeCursor(p.NextCursor), "has_more": p.HasMore})
}
func respond(w http.ResponseWriter, v any, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, v)
		return
	}
	if errors.Is(err, storage.ErrNotFound) || errors.Is(err, payload.ErrNotFound) {
		respondError(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	if errors.Is(err, storage.ErrInvalidCursor) {
		bad(w, err)
		return
	}
	log.Printf("trail/explorerapi: request failed: %v", err)
	respondError(w, http.StatusInternalServerError, "internal_error", "request failed")
}
func bad(w http.ResponseWriter, err error) {
	respondError(w, http.StatusBadRequest, "invalid_request", err.Error())
}
func respondError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func safeContentType(value string) string {
	value = strings.TrimSpace(strings.Split(value, ";")[0])
	if strings.HasPrefix(value, "text/") || value == "application/json" || value == "application/xml" {
		return value
	}
	return "application/octet-stream"
}
func cors(origins []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		for _, allowed := range origins {
			if origin == allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				break
			}
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recover() != nil {
				respondError(w, http.StatusInternalServerError, "internal_error", "request failed")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
