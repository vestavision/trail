package stdout

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/vestavision/trail"
)

func TestSinkEncodesNDJSON(t *testing.T) {
	var buffer bytes.Buffer
	sink := New(&buffer)
	flow := trail.NewFlow()
	event := trail.Event{ID: trail.EventID(flow), Timestamp: time.Unix(1, 2).UnixNano(), Kind: "matched", Level: trail.LevelInfo, FlowID: flow}
	if err := sink.WriteBatch(trail.Batch{Metadata: trail.Metadata{Service: "svc", Environment: "test"}, Events: []trail.Event{event}}); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON %q: %v", buffer.String(), err)
	}
	if decoded["service"] != "svc" || decoded["flow_id"] != flow.String() || decoded["timestamp"] != "1970-01-01T00:00:01.000000002Z" {
		t.Fatalf("unexpected document: %#v", decoded)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteBatch(trail.Batch{}); err != ErrClosed {
		t.Fatalf("write after close = %v", err)
	}
}

func TestNilWriter(t *testing.T) {
	if err := New(nil).WriteBatch(trail.Batch{}); err != ErrNilWriter {
		t.Fatalf("nil writer error = %v", err)
	}
}
