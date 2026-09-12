package trailhttp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/payload"
)

type payloadMemory struct {
	mu   sync.Mutex
	data []byte
}

func (p *payloadMemory) Put(_ context.Context, r io.Reader, o payload.PutOptions) (payload.Ref, error) {
	data, e := io.ReadAll(r)
	p.mu.Lock()
	p.data = append([]byte(nil), data...)
	p.mu.Unlock()
	return payload.Ref{Store: "test", Key: "object", ContentType: o.ContentType, Size: int64(len(data)), StoredSize: int64(len(data))}, e
}
func (*payloadMemory) Open(context.Context, payload.Ref) (io.ReadCloser, error) {
	return nil, payload.ErrNotFound
}
func (*payloadMemory) Delete(context.Context, payload.Ref) error { return nil }

func TestPayloadSpoolingIsBoundedAndPreservesResponse(t *testing.T) {
	sink := setupTrail(t)
	store := &payloadMemory{}
	spooler, err := NewPayloadSpooler(store, PayloadSpoolConfig{MaxCaptureBytes: 4, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_, _ = io.ReadAll(r.Body)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/plain"}}, Body: io.NopCloser(strings.NewReader("response")), Request: r}, nil
	})
	client := &http.Client{Transport: Wrap(base, BodyPreview(3), PayloadSpooling(spooler))}
	request, _ := http.NewRequest(http.MethodPost, "https://provider.example/x", strings.NewReader("request"))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if string(body) != "response" {
		t.Fatalf("response changed: %q", body)
	}
	spooler.Close()
	if err := trail.Close(); err != nil {
		t.Fatal(err)
	}
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	found := false
	for _, field := range events[0].Fields {
		if field.Key() == "payload.response.truncated" && field.Bool() {
			found = true
		}
	}
	if !found {
		t.Fatal("missing truncated payload metadata")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) > 4 {
		t.Fatalf("stored %d bytes", len(store.data))
	}
}
