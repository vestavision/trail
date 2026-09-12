// Package trailhttp journals completed outgoing HTTP requests without changing
// Trail's context-free core correlation model.
package trailhttp

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/vestavision/trail"
)

const maxPreviewBytes = 64 << 10

type Correlation struct {
	FlowID      trail.FlowID
	ExecutionID trail.ExecutionID
	EntityType  string
	EntityID    string
}

type PreviewRedactor func(contentType string, preview []byte) []byte
type SuccessClassifier func(statusCode int) bool
type Option func(*config)

type config struct {
	kind            string
	previewBytes    int
	includeQuery    bool
	headerAllow     map[string]struct{}
	headerSensitive map[string]struct{}
	querySensitive  map[string]struct{}
	redactor        PreviewRedactor
	success         SuccessClassifier
	now             func() time.Time
	spooler         *PayloadSpooler
	payloadBytes    int
}

func EventKind(kind string) Option {
	return func(c *config) {
		if kind != "" {
			c.kind = kind
		}
	}
}
func BodyPreview(limit int) Option {
	return func(c *config) {
		if limit < 0 {
			limit = 0
		}
		if limit > maxPreviewBytes {
			limit = maxPreviewBytes
		}
		c.previewBytes = limit
	}
}
func IncludeQuery(include bool) Option { return func(c *config) { c.includeQuery = include } }
func CaptureHeaders(names ...string) Option {
	return func(c *config) {
		for _, name := range names {
			if name != "" {
				c.headerAllow[http.CanonicalHeaderKey(name)] = struct{}{}
			}
		}
	}
}
func SensitiveHeaders(names ...string) Option {
	return func(c *config) {
		for _, name := range names {
			if name != "" {
				c.headerSensitive[http.CanonicalHeaderKey(name)] = struct{}{}
			}
		}
	}
}
func SensitiveQueryParameters(names ...string) Option {
	return func(c *config) {
		for _, name := range names {
			if name != "" {
				c.querySensitive[strings.ToLower(name)] = struct{}{}
			}
		}
	}
}
func RedactPreview(redactor PreviewRedactor) Option { return func(c *config) { c.redactor = redactor } }
func ClassifySuccess(classifier SuccessClassifier) Option {
	return func(c *config) {
		if classifier != nil {
			c.success = classifier
		}
	}
}

type transport struct {
	base http.RoundTripper
	cfg  config
}

// Wrap returns a concurrency-safe RoundTripper. A nil base uses
// http.DefaultTransport.
func Wrap(base http.RoundTripper, options ...Option) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	cfg := config{
		kind: "provider.http", now: time.Now,
		headerAllow: make(map[string]struct{}),
		headerSensitive: map[string]struct{}{
			"Authorization": {}, "Proxy-Authorization": {}, "Cookie": {}, "Set-Cookie": {},
			"X-Api-Key": {}, "X-Auth-Token": {},
		},
		querySensitive: map[string]struct{}{
			"access_token": {}, "api_key": {}, "key": {}, "password": {}, "secret": {}, "signature": {}, "token": {},
		},
		success: func(code int) bool { return code >= 200 && code < 400 },
	}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return &transport{base: base, cfg: cfg}
}

type correlationKey struct{}

func WithCorrelation(request *http.Request, correlation Correlation) *http.Request {
	if request == nil {
		return nil
	}
	return request.WithContext(context.WithValue(request.Context(), correlationKey{}, correlation))
}

func WithFlow(request *http.Request, id trail.FlowID) *http.Request {
	correlation := correlationFrom(request)
	correlation.FlowID = id
	return WithCorrelation(request, correlation)
}

func WithExecution(request *http.Request, id trail.ExecutionID) *http.Request {
	correlation := correlationFrom(request)
	correlation.ExecutionID = id
	return WithCorrelation(request, correlation)
}

func WithEntity(request *http.Request, entityType, entityID string) *http.Request {
	correlation := correlationFrom(request)
	correlation.EntityType, correlation.EntityID = entityType, entityID
	return WithCorrelation(request, correlation)
}

func correlationFrom(request *http.Request) Correlation {
	if request == nil {
		return Correlation{}
	}
	correlation, _ := request.Context().Value(correlationKey{}).(Correlation)
	return correlation
}

func (t *transport) RoundTrip(request *http.Request) (*http.Response, error) {
	started := t.cfg.now()
	correlation := correlationFrom(request)
	requestCapture := newCapture(t.cfg.previewBytes, t.cfg.payloadBytes)
	outgoing := request.Clone(request.Context())
	if outgoing.Body != nil {
		outgoing.Body = &capturingBody{ReadCloser: outgoing.Body, capture: requestCapture}
	}
	response, err := t.base.RoundTrip(outgoing)
	if err != nil {
		t.log(outgoing, nil, correlation, started, requestCapture, nil, err)
		return response, err
	}
	if response.Body == nil {
		t.log(outgoing, response, correlation, started, requestCapture, nil, nil)
		return response, nil
	}
	responseCapture := newCapture(t.cfg.previewBytes, t.cfg.payloadBytes)
	response.Body = &responseBody{
		ReadCloser: response.Body,
		capture:    responseCapture,
		finish:     func() { t.log(outgoing, response, correlation, started, requestCapture, responseCapture, nil) },
	}
	return response, nil
}

type capture struct {
	limit        int
	payloadLimit int
	n            int64
	data         []byte
	payload      []byte
}

func newCapture(limit int, payloadLimit ...int) *capture {
	c := &capture{limit: limit}
	if len(payloadLimit) > 0 {
		c.payloadLimit = payloadLimit[0]
	}
	if limit > 0 {
		c.data = make([]byte, 0, limit)
	}
	if c.payloadLimit > 0 {
		c.payload = make([]byte, 0, min(c.payloadLimit, 32<<10))
	}
	return c
}

func (c *capture) add(data []byte) {
	c.n += int64(len(data))
	if len(c.payload) < c.payloadLimit {
		remaining := c.payloadLimit - len(c.payload)
		chunk := data
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		c.payload = append(c.payload, chunk...)
	}
	if len(c.data) < c.limit {
		remaining := c.limit - len(c.data)
		chunk := data
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		c.data = append(c.data, chunk...)
	}
}

type capturingBody struct {
	io.ReadCloser
	capture *capture
}

func (b *capturingBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if n > 0 {
		b.capture.add(data[:n])
	}
	return n, err
}

type responseBody struct {
	io.ReadCloser
	capture *capture
	finish  func()
	once    sync.Once
}

func (b *responseBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if n > 0 {
		b.capture.add(data[:n])
	}
	if err == io.EOF {
		b.once.Do(b.finish)
	}
	return n, err
}

func (b *responseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.finish)
	return err
}

func (t *transport) log(request *http.Request, response *http.Response, correlation Correlation, started time.Time, requestCapture, responseCapture *capture, transportErr error) {
	requestSize := request.ContentLength
	if requestCapture != nil && requestCapture.n != 0 {
		requestSize = requestCapture.n
	}
	options := []trail.Option{
		trail.String("http.method", request.Method),
		trail.String("http.scheme", request.URL.Scheme),
		trail.String("http.host", request.URL.Host),
		trail.String("http.path", t.requestPath(request.URL)),
		trail.Duration("http.duration", t.cfg.now().Sub(started)),
		trail.Int64("http.request_size", requestSize),
	}
	if !correlation.FlowID.IsZero() {
		options = append(options, trail.Flow(correlation.FlowID))
	}
	if !correlation.ExecutionID.IsZero() {
		options = append(options, trail.Execution(correlation.ExecutionID))
	}
	if correlation.EntityType != "" || correlation.EntityID != "" {
		options = append(options, trail.Entity(correlation.EntityType, correlation.EntityID))
	}
	if transportErr != nil {
		options = append(options, trail.Bool("http.success", false), trail.Error(transportErr), trail.WithLevel(trail.LevelError))
	} else if response != nil {
		success := t.cfg.success(response.StatusCode)
		responseSize := response.ContentLength
		if responseCapture != nil && responseCapture.n != 0 {
			responseSize = responseCapture.n
		}
		options = append(options,
			trail.Int("http.status_code", response.StatusCode), trail.Bool("http.success", success),
			trail.Int64("http.response_size", responseSize),
		)
		if !success {
			options = append(options, trail.WithLevel(trail.LevelWarn))
		}
	}
	options = append(options, t.headerFields("http.request.header.", request.Header)...)
	if response != nil {
		options = append(options, t.headerFields("http.response.header.", response.Header)...)
	}
	if requestCapture != nil && len(requestCapture.data) != 0 {
		preview := t.redact(request.Header.Get("Content-Type"), requestCapture.data)
		options = append(options, trail.String("http.request_preview", string(preview)))
	}
	if response != nil && responseCapture != nil && len(responseCapture.data) != 0 {
		preview := t.redact(response.Header.Get("Content-Type"), responseCapture.data)
		options = append(options, trail.String("http.response_preview", string(preview)))
	}
	if t.cfg.spooler != nil && (len(requestCapture.payload) != 0 || responseCapture != nil && len(responseCapture.payload) != 0) {
		job := spoolJob{kind: t.cfg.kind, options: options}
		job.request = t.spoolBody("request", request.Header.Get("Content-Type"), requestCapture)
		if responseCapture != nil {
			job.response = t.spoolBody("response", response.Header.Get("Content-Type"), responseCapture)
		}
		if t.cfg.spooler.enqueue(job) {
			return
		}
		options = append(options, trail.String("payload.error", "spool queue full"))
	}
	trail.Log(t.cfg.kind, options...)
}

func (t *transport) spoolBody(role, contentType string, capture *capture) spoolBody {
	if capture == nil || len(capture.payload) == 0 {
		return spoolBody{}
	}
	data := capture.payload
	if t.cfg.redactor != nil {
		data = t.cfg.redactor(contentType, append([]byte(nil), data...))
		if len(data) > t.cfg.payloadBytes {
			data = data[:t.cfg.payloadBytes]
		}
	}
	return spoolBody{role: role, contentType: contentType, data: data, original: capture.n, truncated: capture.n > int64(len(capture.payload))}
}

func (t *transport) redact(contentType string, preview []byte) []byte {
	if t.cfg.redactor == nil {
		return preview
	}
	redacted := t.cfg.redactor(contentType, append([]byte(nil), preview...))
	if len(redacted) > maxPreviewBytes {
		redacted = redacted[:maxPreviewBytes]
	}
	return redacted
}

func (t *transport) headerFields(prefix string, header http.Header) []trail.Option {
	fields := make([]trail.Option, 0, len(t.cfg.headerAllow))
	for name := range t.cfg.headerAllow {
		values, exists := header[name]
		if !exists {
			continue
		}
		value := strings.Join(values, ", ")
		if _, sensitive := t.cfg.headerSensitive[name]; sensitive {
			value = "[REDACTED]"
		}
		fields = append(fields, trail.String(prefix+strings.ToLower(name), value))
	}
	return fields
}

func (t *transport) requestPath(value *url.URL) string {
	if value == nil {
		return ""
	}
	if t.cfg.includeQuery && value.RawQuery != "" {
		query := value.Query()
		for key := range query {
			if _, sensitive := t.cfg.querySensitive[strings.ToLower(key)]; sensitive {
				query[key] = []string{"[REDACTED]"}
			}
		}
		return value.EscapedPath() + "?" + query.Encode()
	}
	return value.EscapedPath()
}
