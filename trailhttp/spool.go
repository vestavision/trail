package trailhttp

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/payload"
)

const maxPayloadCaptureBytes = 16 << 20

type PayloadSpoolConfig struct {
	MaxCaptureBytes int
	QueueCapacity   int
	PutTimeout      time.Duration
	Compression     payload.Compression
}

// PayloadSpooler persists bounded body captures away from the request goroutine.
// Close flushes accepted captures. The caller owns both the spooler and store.
type PayloadSpooler struct {
	store     payload.Store
	cfg       PayloadSpoolConfig
	jobs      chan spoolJob
	closeOnce sync.Once
	workers   sync.WaitGroup
	mu        sync.RWMutex
	closed    bool
}
type spoolJob struct {
	kind              string
	options           []trail.Option
	request, response spoolBody
}
type spoolBody struct {
	role, contentType string
	data              []byte
	original          int64
	truncated         bool
}

func NewPayloadSpooler(store payload.Store, cfg PayloadSpoolConfig) (*PayloadSpooler, error) {
	if store == nil {
		return nil, errors.New("trailhttp: payload store is required")
	}
	if cfg.MaxCaptureBytes <= 0 {
		return nil, errors.New("trailhttp: positive payload capture limit is required")
	}
	if cfg.MaxCaptureBytes > maxPayloadCaptureBytes {
		cfg.MaxCaptureBytes = maxPayloadCaptureBytes
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 64
	}
	if cfg.PutTimeout <= 0 {
		cfg.PutTimeout = 15 * time.Second
	}
	s := &PayloadSpooler{store: store, cfg: cfg, jobs: make(chan spoolJob, cfg.QueueCapacity)}
	s.workers.Add(1)
	go s.run()
	return s, nil
}
func (s *PayloadSpooler) Close() {
	s.closeOnce.Do(func() { s.mu.Lock(); s.closed = true; close(s.jobs); s.mu.Unlock(); s.workers.Wait() })
}
func (s *PayloadSpooler) enqueue(job spoolJob) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false
	}
	select {
	case s.jobs <- job:
		return true
	default:
		return false
	}
}
func (s *PayloadSpooler) run() {
	defer s.workers.Done()
	for job := range s.jobs {
		options := job.options
		for _, body := range []spoolBody{job.request, job.response} {
			if len(body.data) == 0 {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PutTimeout)
			ref, err := s.store.Put(ctx, bytes.NewReader(body.data), payload.PutOptions{ContentType: body.contentType, Compression: s.cfg.Compression})
			cancel()
			prefix := "payload." + body.role + "."
			if err != nil {
				options = append(options, trail.String(prefix+"error", err.Error()))
				continue
			}
			options = append(options, ref.Fields(body.role)...)
			options = append(options, trail.Int64(prefix+"original_size", body.original), trail.Int64(prefix+"captured_size", int64(len(body.data))), trail.Bool(prefix+"truncated", body.truncated))
		}
		trail.Log(job.kind, options...)
	}
}

func PayloadSpooling(spooler *PayloadSpooler) Option {
	return func(c *config) {
		c.spooler = spooler
		if spooler != nil {
			c.payloadBytes = spooler.cfg.MaxCaptureBytes
		}
	}
}
