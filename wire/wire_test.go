package wire

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/vestavision/trail"
)

func TestBatchRoundTrip(t *testing.T) {
	flow, execution := trail.NewFlow(), trail.NewExecution()
	event := trail.Event{
		ID: trail.EventID(trail.NewFlow()), Timestamp: time.Now().UnixNano(), Kind: "provider.http", Level: trail.LevelWarn,
		FlowID: flow, ExecutionID: execution,
	}
	options := []trail.Option{trail.String("provider", "bank"), trail.Int("score", -3), trail.Float64("ratio", math.Pi), trail.Bool("ok", true)}
	// Build fields through the public logging API and a retaining sink so the
	// test does not depend on Field's private representation.
	sink := &captureSink{}
	_ = trail.Close()
	if err := trail.Init(trail.Config{Service: "svc", Sink: sink, BatchSize: 1}); err != nil {
		t.Fatal(err)
	}
	trail.Log(event.Kind, append([]trail.Option{trail.Flow(flow), trail.Execution(execution), trail.WithLevel(trail.LevelWarn)}, options...)...)
	if err := trail.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := MarshalBatch(sink.batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != Version || decoded.Metadata.Service != "svc" || len(decoded.Events) != 1 || len(decoded.Events[0].Fields) != 4 {
		t.Fatalf("decoded envelope = %+v", decoded)
	}
	if decoded.Events[0].Fields[1].Num != ^uint64(2) || decoded.Events[0].Fields[2].Num != math.Float64bits(math.Pi) {
		t.Fatalf("numeric fields = %+v", decoded.Events[0].Fields)
	}
}

func TestRejectsInvalidEnvelope(t *testing.T) {
	if _, err := MarshalBatch(trail.Batch{}); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("empty batch error = %v", err)
	}
	data, _ := json.Marshal(Envelope{SchemaVersion: 2, BatchID: "bad", Events: []Event{{}}})
	if _, err := Unmarshal(data); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("version error = %v", err)
	}
}

type captureSink struct{ batch trail.Batch }

func (s *captureSink) WriteBatch(batch trail.Batch) error {
	s.batch = batch
	s.batch.Events = append([]trail.Event(nil), batch.Events...)
	for i := range s.batch.Events {
		s.batch.Events[i].Fields = append([]trail.Field(nil), batch.Events[i].Fields...)
	}
	return nil
}
func (*captureSink) Close() error { return nil }
