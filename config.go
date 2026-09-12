package trail

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultBufferCapacity = 4096
	defaultBatchSize      = 64
	defaultFlushInterval  = 100 * time.Millisecond
)

var (
	ErrAlreadyInitialized = errors.New("trail: already initialized")
	ErrInvalidConfig      = errors.New("trail: invalid config")
)

// FullPolicy controls behavior when the bounded event queue is full.
type FullPolicy uint8

const (
	Drop FullPolicy = iota
	Block
)

type Config struct {
	Service        string
	Environment    string
	Version        string
	Sink           Sink
	BufferCapacity int
	BatchSize      int
	FlushInterval  time.Duration
	FullPolicy     FullPolicy
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Service == "" {
		return Config{}, fmt.Errorf("%w: Service is required", ErrInvalidConfig)
	}
	if cfg.Sink == nil {
		return Config{}, fmt.Errorf("%w: Sink is required", ErrInvalidConfig)
	}
	if cfg.BufferCapacity < 0 || cfg.BatchSize < 0 || cfg.FlushInterval < 0 {
		return Config{}, fmt.Errorf("%w: writer sizes and interval cannot be negative", ErrInvalidConfig)
	}
	if cfg.FullPolicy != Drop && cfg.FullPolicy != Block {
		return Config{}, fmt.Errorf("%w: unknown full-buffer policy", ErrInvalidConfig)
	}
	if cfg.BufferCapacity == 0 {
		cfg.BufferCapacity = defaultBufferCapacity
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	return cfg, nil
}
