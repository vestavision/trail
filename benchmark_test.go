package trail

import (
	"testing"
	"time"
)

type discardSink struct{}

func (discardSink) WriteBatch(Batch) error { return nil }
func (discardSink) Close() error           { return nil }

func benchmarkWriter(b *testing.B) {
	_ = Close()
	if err := Init(Config{Service: "bench", Sink: discardSink{}, BufferCapacity: 65536, BatchSize: 256, FlushInterval: time.Hour, FullPolicy: Block}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = Close() })
	b.ReportAllocs()
}

func BenchmarkLogNoFields(b *testing.B) {
	benchmarkWriter(b)
	b.ResetTimer()
	for range b.N {
		Log("order.fulfillment.started")
	}
}

func BenchmarkLogThreeFields(b *testing.B) {
	benchmarkWriter(b)
	b.ResetTimer()
	for range b.N {
		Log("order.fulfillment.inventory_reserved", String("warehouse", "warehouse-a"), Int("available_units", 97), Bool("reserved", true))
	}
}

func BenchmarkNewFlow(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = NewFlow()
	}
}

func BenchmarkNewExecution(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = NewExecution()
	}
}

func BenchmarkConcurrentLog(b *testing.B) {
	benchmarkWriter(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Log("concurrent")
		}
	})
}
