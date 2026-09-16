package trailhttp

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/sink/memory"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

func setupTrail(t *testing.T) *memory.Sink {
	t.Helper()
	_ = trail.Close()
	sink := memory.New()
	if err := trail.Init(trail.Config{Service: "test", Sink: sink, BatchSize: 1, FlushInterval: time.Hour, FullPolicy: trail.Block}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trail.Close() })
	return sink
}

func field(event trail.Event, key string) (trail.Field, bool) {
	for _, candidate := range event.Fields {
		if candidate.Key() == key {
			return candidate, true
		}
	}
	return trail.Field{}, false
}

func TestCompletedResponsePreservesBodyAndCorrelation(t *testing.T) {
	sink := setupTrail(t)
	base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil || string(body) != "request-secret" {
			t.Fatalf("request body=%q err=%v", body, err)
		}
		return &http.Response{
			StatusCode: 201, ContentLength: 13, Header: http.Header{"Content-Type": {"application/json"}, "X-Api-Key": {"secret"}},
			Body: io.NopCloser(strings.NewReader("response-body")), Request: request,
		}, nil
	})
	client := &http.Client{Transport: Wrap(base, BodyPreview(8), CaptureHeaders("X-Api-Key"), Fields(trail.String("provider", "inventory")))}
	request, _ := http.NewRequest(http.MethodPost, "https://api.example/orders?token=secret", strings.NewReader("request-secret"))
	flow, execution := trail.NewFlow(), trail.NewExecution()
	request = WithFlow(request, flow)
	request = WithExecution(request, execution)
	request = WithScope(request, "tenant", "tenant_1")
	request = WithEntity(request, "order", "order_1")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "response-body" {
		t.Fatalf("response body=%q err=%v", body, err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := trail.Close(); err != nil {
		t.Fatal(err)
	}
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	event := events[0]
	if event.FlowID != flow || event.ExecutionID != execution || event.ScopeType != "tenant" || event.ScopeID != "tenant_1" || event.EntityType != "order" || event.EntityID != "order_1" {
		t.Fatalf("correlation=%+v", event)
	}
	path, _ := field(event, "http.path")
	status, _ := field(event, "http.status_code")
	requestPreview, _ := field(event, "http.request_preview")
	responsePreview, _ := field(event, "http.response_preview")
	header, _ := field(event, "http.response.header.x-api-key")
	provider, _ := field(event, "provider")
	if path.Text() != "/orders" || status.Int64() != 201 || requestPreview.Text() != "request-" || responsePreview.Text() != "response" || header.Text() != "[REDACTED]" || provider.Text() != "inventory" {
		t.Fatalf("event fields=%+v", event.Fields)
	}
}

func TestTransportErrorEmitsOnce(t *testing.T) {
	sink := setupTrail(t)
	want := errors.New("offline")
	client := &http.Client{Transport: Wrap(roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, want }))}
	request, _ := http.NewRequest(http.MethodGet, "https://api.example/status", nil)
	if _, err := client.Do(request); !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if err := trail.Close(); err != nil {
		t.Fatal(err)
	}
	events := sink.Events()
	if len(events) != 1 || events[0].Level != trail.LevelError {
		t.Fatalf("events=%+v", events)
	}
	success, _ := field(events[0], "http.success")
	if success.Bool() {
		t.Fatal("failed transport marked successful")
	}
}

func TestResponseEOFAndCloseEmitOnce(t *testing.T) {
	sink := setupTrail(t)
	client := &http.Client{Transport: Wrap(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: request}, nil
	}))}
	request, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	_ = response.Body.Close()
	if err := trail.Close(); err != nil {
		t.Fatal(err)
	}
	if len(sink.Events()) != 1 {
		t.Fatalf("events=%d", len(sink.Events()))
	}
}

func TestNilRequestCorrelation(t *testing.T) {
	if WithCorrelation(nil, Correlation{}) != nil || WithFlow(nil, trail.FlowID{}) != nil || WithScope(nil, "tenant", "tenant_1") != nil {
		t.Fatal("nil request was not preserved")
	}
}

func TestIncludedQueryRedactsSensitiveValues(t *testing.T) {
	sink := setupTrail(t)
	client := &http.Client{Transport: Wrap(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: request}, nil
	}), IncludeQuery(true))}
	request, _ := http.NewRequest(http.MethodGet, "https://example.test/path?token=secret&visible=yes", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if err := trail.Close(); err != nil {
		t.Fatal(err)
	}
	path, _ := field(sink.Events()[0], "http.path")
	if path.Text() != "/path?token=%5BREDACTED%5D&visible=yes" {
		t.Fatalf("path=%q", path.Text())
	}
}
