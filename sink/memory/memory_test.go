package memory

import (
	"testing"

	"github.com/vestavision/trail"
)

func TestSinkCopiesEvents(t *testing.T) {
	sink := New()
	event := trail.Event{Kind: "original"}
	if err := sink.WriteBatch(trail.Batch{Metadata: trail.Metadata{Service: "svc"}, Events: []trail.Event{event}}); err != nil {
		t.Fatal(err)
	}
	first := sink.Events()
	first[0].Kind = "changed"
	if sink.Events()[0].Kind != "original" {
		t.Fatal("Events returned internal storage")
	}
	if sink.Metadata().Service != "svc" {
		t.Fatal("metadata not retained")
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteBatch(trail.Batch{}); err != ErrClosed {
		t.Fatalf("write after close = %v", err)
	}
}
