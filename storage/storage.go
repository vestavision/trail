// Package storage defines Trail's database-neutral ingestion and Explorer
// contracts. It deliberately exposes domain operations rather than SQL or a
// generic query language.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/vestavision/trail"
	"github.com/vestavision/trail/payload"
	"github.com/vestavision/trail/wire"
)

var (
	ErrNotFound      = errors.New("trail/storage: not found")
	ErrInvalidCursor = errors.New("trail/storage: invalid cursor")
)

type Delivery struct {
	Stream           string
	Consumer         string
	StreamSequence   uint64
	ConsumerSequence uint64
	Delivered        uint64
	ReceivedAt       time.Time
}

type Batch struct {
	BatchID  trail.EventID
	Metadata trail.Metadata
	Delivery Delivery
	Events   []EventRecord
}

type PayloadLink struct {
	Role         string
	Ref          payload.Ref
	OriginalSize int64
	CapturedSize int64
	Truncated    bool
	Error        string
}

type HTTPRecord struct {
	Method          string
	Scheme          string
	Host            string
	Path            string
	StatusCode      int
	Duration        time.Duration
	Success         bool
	RequestSize     int64
	ResponseSize    int64
	RequestPreview  string
	ResponsePreview string
}

type EventRecord struct {
	ID                 trail.EventID
	Timestamp          time.Time
	Kind               string
	Level              trail.Level
	FlowID             trail.FlowID
	ExecutionID        trail.ExecutionID
	ParentExecutionID  trail.ExecutionID
	RetryOfExecutionID trail.ExecutionID
	ExecutionAttempt   uint32
	ExecutionSource    trail.ExecutionSource
	EntityType         string
	EntityID           string
	ParentID           trail.EventID
	Fields             []wire.Field
	FlowStatus         string
	ExecutionStatus    string
	ExecutionKind      string
	RetentionClass     string
	HTTP               HTTPRecord
	Payloads           []PayloadLink
	BatchID            trail.EventID
	Metadata           trail.Metadata
	Delivery           Delivery
}

type IngestStore interface {
	WriteBatches(context.Context, []Batch) error
	Close() error
}

type Cursor struct {
	Timestamp time.Time
	ID        string
}

type PageRequest struct {
	Limit  int
	Cursor Cursor
	Desc   bool
}

type Page[T any] struct {
	Items      []T
	NextCursor Cursor
	HasMore    bool
}

type TimeRange struct {
	From time.Time
	To   time.Time
}

type FlowFilter struct {
	Time        TimeRange
	Service     string
	Environment string
	Status      string
	EntityType  string
	EntityID    string
	ExecutionID trail.ExecutionID
}

type ExecutionFilter struct {
	Time        TimeRange
	Service     string
	Environment string
	Status      string
	Source      trail.ExecutionSource
	Kind        string
}

type EntityFilter struct {
	Time        TimeRange
	Service     string
	Environment string
	EntityType  string
	IDPrefix    string
}

type EventFilter struct {
	Time            TimeRange
	Service         string
	Environment     string
	Kind            string
	Level           *trail.Level
	HTTPStatusClass int
}

type OverviewFilter struct {
	Time        TimeRange
	Service     string
	Environment string
}

type ActivityInterval string

const (
	ActivityHour ActivityInterval = "hour"
	ActivityDay  ActivityInterval = "day"
)

type OverviewActivityFilter struct {
	OverviewFilter
	Interval ActivityInterval
}

type FlowSummary struct {
	ID          trail.FlowID
	Service     string
	Environment string
	EntityType  string
	EntityID    string
	ExecutionID trail.ExecutionID
	Status      string
	StartedAt   time.Time
	FinishedAt  time.Time
	EventCount  uint64
}

type FlowDetail struct {
	FlowSummary
	FirstKind string
	LastKind  string
}

type ExecutionSummary struct {
	ID                 trail.ExecutionID
	Service            string
	Environment        string
	Kind               string
	Source             trail.ExecutionSource
	Status             string
	Attempt            uint32
	ParentExecutionID  trail.ExecutionID
	RetryOfExecutionID trail.ExecutionID
	StartedAt          time.Time
	FinishedAt         time.Time
	EventCount         uint64
	FlowCount          uint64
}

type ExecutionDetail struct{ ExecutionSummary }

type RetryChain struct {
	Executions []ExecutionSummary
}

type EntityKey struct {
	Type string
	ID   string
}

type EntitySummary struct {
	EntityKey
	FirstSeen time.Time
	LastSeen  time.Time
	FlowCount uint64
}

type EntityDetail struct{ EntitySummary }

type Overview struct {
	AsOf             time.Time
	Estimated        bool
	FirstEvent       time.Time
	LastEvent        time.Time
	Events           uint64
	Flows            uint64
	Executions       uint64
	Entities         uint64
	FailedFlows      uint64
	FailedExecutions uint64
	HTTP5xx          uint64
}

type ActivityBucket struct {
	Timestamp time.Time
	Events    uint64
	Errors    uint64
	HTTP5xx   uint64
}

type CountByName struct {
	Name  string
	Count uint64
}

type OverviewActivity struct {
	From             time.Time
	To               time.Time
	Interval         ActivityInterval
	Buckets          []ActivityBucket
	FlowStatuses     []CountByName
	ExecutionSources []CountByName
}

type FlowReader interface {
	ListFlows(context.Context, FlowFilter, PageRequest) (Page[FlowSummary], error)
	GetFlow(context.Context, trail.FlowID) (FlowDetail, error)
	ListFlowEvents(context.Context, trail.FlowID, EventFilter, PageRequest) (Page[EventRecord], error)
}

type ExecutionReader interface {
	ListExecutions(context.Context, ExecutionFilter, PageRequest) (Page[ExecutionSummary], error)
	GetExecution(context.Context, trail.ExecutionID) (ExecutionDetail, error)
	ListExecutionFlows(context.Context, trail.ExecutionID, FlowFilter, PageRequest) (Page[FlowSummary], error)
	GetRetryChain(context.Context, trail.ExecutionID) (RetryChain, error)
}

type EntityReader interface {
	SearchEntities(context.Context, EntityFilter, PageRequest) (Page[EntitySummary], error)
	GetEntity(context.Context, EntityKey) (EntityDetail, error)
	ListEntityFlows(context.Context, EntityKey, FlowFilter, PageRequest) (Page[FlowSummary], error)
}

type EventReader interface {
	ListEvents(context.Context, EventFilter, PageRequest) (Page[EventRecord], error)
	GetEvent(context.Context, trail.EventID) (EventRecord, error)
}

type OverviewReader interface {
	Overview(context.Context, OverviewFilter) (Overview, error)
}

// OverviewActivityReader is an optional Explorer capability. Keeping it
// separate means existing custom ExplorerStore implementations do not break
// when dashboard analytics are added.
type OverviewActivityReader interface {
	OverviewActivity(context.Context, OverviewActivityFilter) (OverviewActivity, error)
}

type ExplorerStore interface {
	FlowReader
	ExecutionReader
	EntityReader
	EventReader
	OverviewReader
	Close() error
}
