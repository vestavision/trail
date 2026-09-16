package trail

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingSink struct {
	mu       sync.Mutex
	batches  []Batch
	closed   int
	fail     error
	closeErr error
	entered  chan struct{}
	release  chan struct{}
	enterOne sync.Once
}

func (s *recordingSink) WriteBatch(batch Batch) error {
	if s.entered != nil {
		s.enterOne.Do(func() { close(s.entered) })
		<-s.release
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copyBatch := Batch{Metadata: batch.Metadata, Events: make([]Event, len(batch.Events))}
	for i := range batch.Events {
		copyBatch.Events[i] = batch.Events[i]
		copyBatch.Events[i].Fields = append([]Field(nil), batch.Events[i].Fields...)
	}
	s.batches = append(s.batches, copyBatch)
	return s.fail
}

func (s *recordingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return s.closeErr
}

func (s *recordingSink) snapshot() ([]Batch, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Batch(nil), s.batches...), s.closed
}

func initTestWriter(t *testing.T, cfg Config) {
	t.Helper()
	_ = Close()
	if err := Init(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
}

func TestLogBuildsEventAndCloseDrains(t *testing.T) {
	sink := &recordingSink{}
	initTestWriter(t, Config{Service: "order-worker", Environment: "test", Version: "v1", Sink: sink, BatchSize: 64, FlushInterval: time.Hour})
	flow, execution := NewFlow(), NewExecution()
	parent := newEventID()
	Log("order.fulfillment", Flow(flow), Execution(execution), WithScope("tenant", "tenant_1"), Entity("order", "order_1"), Parent(parent), WithLevel(LevelWarn), String("warehouse", "warehouse-a"), Int("available_units", 97))
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	batches, closed := sink.snapshot()
	if closed != 1 || len(batches) != 1 || len(batches[0].Events) != 1 {
		t.Fatalf("batches=%d closed=%d", len(batches), closed)
	}
	event := batches[0].Events[0]
	if event.ID.IsZero() || event.Timestamp == 0 || event.Kind != "order.fulfillment" || event.Level != LevelWarn {
		t.Fatalf("bad core event: %+v", event)
	}
	if event.FlowID != flow || event.ExecutionID != execution || event.ParentID != parent || event.ScopeType != "tenant" || event.ScopeID != "tenant_1" || event.EntityType != "order" || event.EntityID != "order_1" {
		t.Fatalf("bad correlation: %+v", event)
	}
	if len(event.Fields) != 2 || batches[0].Metadata.Service != "order-worker" || batches[0].Metadata.Environment != "test" {
		t.Fatalf("bad fields or metadata: %+v %+v", event.Fields, batches[0].Metadata)
	}
	if Stats().Buffered != 0 {
		t.Fatalf("buffered after close = %d", Stats().Buffered)
	}
	if err := Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, closed = sink.snapshot()
	if closed != 1 {
		t.Fatalf("sink closed %d times", closed)
	}
}

func TestOptionLastWinsAndDefaultLevel(t *testing.T) {
	sink := &recordingSink{}
	initTestWriter(t, Config{Service: "svc", Sink: sink, FlushInterval: time.Hour})
	first, second := NewFlow(), NewFlow()
	Log("kind", Flow(first), WithLevel(LevelError), Flow(second), WithLevel(LevelDebug), String("x", "1"), String("x", "2"))
	_ = Close()
	batches, _ := sink.snapshot()
	event := batches[0].Events[0]
	if event.FlowID != second || event.Level != LevelDebug || len(event.Fields) != 2 {
		t.Fatalf("unexpected option result: %+v", event)
	}
}

func TestExecutionSemanticsAreStatelessOptions(t *testing.T) {
	sink := &recordingSink{}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BatchSize: 1})
	parent, previous, execution := NewExecution(), NewExecution(), NewExecution()
	Log("job.started", Execution(execution), ParentExecution(parent), RetryOfExecution(previous), ExecutionAttempt(3), WithExecutionSource(SourceRetry))
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	batches, _ := sink.snapshot()
	event := batches[0].Events[0]
	if event.ExecutionID != execution || event.ParentExecutionID != parent || event.RetryOfExecutionID != previous || event.ExecutionAttempt != 3 || event.ExecutionSource != SourceRetry {
		t.Fatalf("execution metadata = %+v", event)
	}
}

func TestInitValidationAndLifecycle(t *testing.T) {
	_ = Close()
	if err := Init(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", err)
	}
	first, rejected := &recordingSink{}, &recordingSink{}
	if err := Init(Config{Service: "svc", Sink: first}); err != nil {
		t.Fatal(err)
	}
	if err := Init(Config{Service: "other", Sink: rejected}); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("repeated Init error = %v", err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	_, rejectedClosed := rejected.snapshot()
	if rejectedClosed != 0 {
		t.Fatal("Trail took ownership of rejected sink")
	}
	if err := Init(Config{Service: "again", Sink: rejected}); err != nil {
		t.Fatalf("reinitialize: %v", err)
	}
	_ = Close()
}

func TestInactiveLogDrops(t *testing.T) {
	_ = Close()
	before := Stats().Dropped
	Log("inactive")
	if Stats().Dropped != before+1 {
		t.Fatalf("dropped delta = %d", Stats().Dropped-before)
	}
}

func TestTimedPartialBatch(t *testing.T) {
	sink := &recordingSink{}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BatchSize: 8, FlushInterval: 5 * time.Millisecond})
	Log("one")
	deadline := time.Now().Add(time.Second)
	for {
		batches, _ := sink.snapshot()
		if len(batches) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partial batch was not flushed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchSize(t *testing.T) {
	sink := &recordingSink{}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BatchSize: 2, FlushInterval: time.Hour})
	for range 5 {
		Log("event")
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	batches, _ := sink.snapshot()
	if len(batches) != 3 || len(batches[0].Events) != 2 || len(batches[1].Events) != 2 || len(batches[2].Events) != 1 {
		t.Fatalf("unexpected batches: %#v", batches)
	}
}

func TestDropWhenFull(t *testing.T) {
	sink := &recordingSink{entered: make(chan struct{}), release: make(chan struct{})}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BufferCapacity: 1, BatchSize: 1, FlushInterval: time.Hour, FullPolicy: Drop})
	before := Stats().Dropped
	Log("blocks-sink")
	<-sink.entered
	Log("fills-queue")
	Log("dropped")
	if Stats().Dropped != before+1 {
		t.Fatalf("dropped delta = %d", Stats().Dropped-before)
	}
	close(sink.release)
}

func TestBlockReleasedByClose(t *testing.T) {
	sink := &recordingSink{entered: make(chan struct{}), release: make(chan struct{})}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BufferCapacity: 1, BatchSize: 1, FlushInterval: time.Hour, FullPolicy: Block})
	Log("blocks-sink")
	<-sink.entered
	Log("fills-queue")
	logDone := make(chan struct{})
	go func() {
		Log("blocked-producer")
		close(logDone)
	}()
	select {
	case <-logDone:
		t.Fatal("Block policy returned while queue was full")
	case <-time.After(10 * time.Millisecond):
	}
	closeDone := make(chan struct{})
	go func() { _ = Close(); close(closeDone) }()
	select {
	case <-logDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not release blocked producer")
	}
	close(sink.release)
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestSinkFailureUpdatesStatsAndCloseError(t *testing.T) {
	publishErr := errors.New("publish failed")
	sink := &recordingSink{fail: publishErr}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BatchSize: 1})
	before := Stats()
	Log("failed")
	deadline := time.Now().Add(time.Second)
	for Stats().PublishErrors == before.PublishErrors {
		if time.Now().After(deadline) {
			t.Fatal("publish failure was not observed")
		}
		time.Sleep(time.Millisecond)
	}
	if err := Close(); !errors.Is(err, publishErr) {
		t.Fatalf("Close error = %v", err)
	}
	after := Stats()
	if after.Dropped != before.Dropped+1 || after.Written != before.Written || after.Buffered != 0 {
		t.Fatalf("stats before=%+v after=%+v", before, after)
	}
}

func TestCloseReturnsSinkCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	sink := &recordingSink{closeErr: closeErr}
	initTestWriter(t, Config{Service: "svc", Sink: sink})
	if err := Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v", err)
	}
}

func TestPublishAndCloseErrorsAreJoined(t *testing.T) {
	publishErr := errors.New("publish failed")
	closeErr := errors.New("close failed")
	sink := &recordingSink{fail: publishErr, closeErr: closeErr}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BatchSize: 1})
	Log("failed")
	err := Close()
	if !errors.Is(err, publishErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v", err)
	}
}

func TestRejectedInitDoesNotDisturbActiveWriter(t *testing.T) {
	_ = Close()
	active, rejected := &recordingSink{}, &recordingSink{}
	if err := Init(Config{Service: "active", Sink: active, BatchSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := Init(Config{Service: "rejected", Sink: rejected}); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("Init error = %v", err)
	}
	Log("still-active")
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	batches, activeClosed := active.snapshot()
	_, rejectedClosed := rejected.snapshot()
	if len(batches) != 1 || batches[0].Metadata.Service != "active" || activeClosed != 1 || rejectedClosed != 0 {
		t.Fatalf("active batches=%d activeClosed=%d rejectedClosed=%d", len(batches), activeClosed, rejectedClosed)
	}
}

func TestRepeatedLifecycle(t *testing.T) {
	_ = Close()
	before := Stats()
	const cycles = 50
	for i := 0; i < cycles; i++ {
		sink := &recordingSink{}
		if err := Init(Config{Service: "svc", Sink: sink, BatchSize: 4, FlushInterval: time.Hour, FullPolicy: Block}); err != nil {
			t.Fatalf("cycle %d Init: %v", i, err)
		}
		Log("one", Int("cycle", i))
		if err := Close(); err != nil {
			t.Fatalf("cycle %d Close: %v", i, err)
		}
		_, closed := sink.snapshot()
		if closed != 1 {
			t.Fatalf("cycle %d sink closed %d times", i, closed)
		}
	}
	after := Stats()
	if after.Written-before.Written != cycles || after.Buffered != 0 {
		t.Fatalf("stats before=%+v after=%+v", before, after)
	}
}

func TestConcurrentLogAndClose(t *testing.T) {
	sink := &recordingSink{}
	initTestWriter(t, Config{Service: "svc", Sink: sink, BufferCapacity: 128, BatchSize: 16})
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				Log("concurrent", Int("n", 1))
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	stop.Store(true)
	wg.Wait()
	if Stats().Buffered != 0 {
		t.Fatalf("buffered after concurrent Close = %d", Stats().Buffered)
	}
}
