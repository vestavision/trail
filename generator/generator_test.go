package generator

import (
	"context"
	"reflect"
	"testing"

	"github.com/vestavision/trail/wire"
)

func TestDeterministicStories(t *testing.T) {
	cfg := Config{Events: 1000, Seed: 42, BatchSize: 64}
	var first, second []wire.Envelope
	manifest1, err := Generate(context.Background(), cfg, func(_ context.Context, envelope wire.Envelope) error {
		first = append(first, envelope)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest2, err := Generate(context.Background(), cfg, func(_ context.Context, envelope wire.Envelope) error {
		second = append(second, envelope)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || manifest1 != manifest2 {
		t.Fatal("same seed produced different logical data")
	}
	if manifest1.Events != cfg.Events || len(first) == 0 {
		t.Fatalf("manifest=%+v batches=%d", manifest1, len(first))
	}
	for _, envelope := range first {
		if err := wire.Validate(envelope); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDifferentSeedsProduceDifferentData(t *testing.T) {
	var ids [2]string
	for i, seed := range []uint64{1, 2} {
		_, err := Generate(context.Background(), Config{Events: 1, Seed: seed}, func(_ context.Context, envelope wire.Envelope) error {
			ids[i] = envelope.Events[0].EventID
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("different seeds produced same first ID")
	}
}

func TestDemoPayloadIncludesLargeXMLExample(t *testing.T) {
	content, contentType := demoPayload(42, 10)
	if contentType != "application/xml" || len(content) < 250_000 {
		t.Fatalf("content_type=%q size=%d", contentType, len(content))
	}
	if got := preview(content, 320); len(got) <= 320 {
		t.Fatalf("expected a visibly truncated preview, got %d bytes", len(got))
	}
}
