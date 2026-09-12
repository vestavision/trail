// Package payload defines storage for large data referenced by Trail events.
// Payload persistence is deliberately separate from the event hot path.
package payload

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/vestavision/trail"
)

var (
	ErrInvalidRef = errors.New("trail/payload: invalid reference")
	ErrNotFound   = errors.New("trail/payload: not found")
)

type Compression string

const (
	CompressionNone Compression = ""
	CompressionGZIP Compression = "gzip"
)

type Ref struct {
	Store          string      `json:"store"`
	Key            string      `json:"key"`
	ContentType    string      `json:"content_type,omitempty"`
	Size           int64       `json:"size"`
	StoredSize     int64       `json:"stored_size"`
	SHA256         [32]byte    `json:"sha256"`
	Compression    Compression `json:"compression,omitempty"`
	RetainUntil    time.Time   `json:"retain_until,omitempty"`
	RetentionClass string      `json:"retention_class,omitempty"`
}

type PutOptions struct {
	ContentType    string
	Compression    Compression
	RetainUntil    time.Time
	RetentionClass string
}

type Store interface {
	Put(context.Context, io.Reader, PutOptions) (Ref, error)
	Open(context.Context, Ref) (io.ReadCloser, error)
	Delete(context.Context, Ref) error
}

// Fields returns the reserved typed fields used to attach a payload reference
// to an event. Role should be a stable value such as "request", "response", or
// "business". Payload-bearing events are intentionally a slower path.
func (r Ref) Fields(role string) []trail.Option {
	prefix := "payload." + role + "."
	fields := []trail.Option{
		trail.String(prefix+"store", r.Store),
		trail.String(prefix+"key", r.Key),
		trail.String(prefix+"content_type", r.ContentType),
		trail.Int64(prefix+"size", r.Size),
		trail.Int64(prefix+"stored_size", r.StoredSize),
		trail.String(prefix+"sha256", checksumText(r.SHA256)),
	}
	if r.Compression != CompressionNone {
		fields = append(fields, trail.String(prefix+"compression", string(r.Compression)))
	}
	if !r.RetainUntil.IsZero() {
		fields = append(fields, trail.Int64(prefix+"retain_until_unix_nano", r.RetainUntil.UnixNano()))
	}
	if r.RetentionClass != "" {
		fields = append(fields, trail.String(prefix+"retention_class", r.RetentionClass))
	}
	return fields
}

func checksumText(sum [32]byte) string {
	const digits = "0123456789abcdef"
	var text [64]byte
	for i, value := range sum {
		text[i*2] = digits[value>>4]
		text[i*2+1] = digits[value&15]
	}
	return string(text[:])
}
