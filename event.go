package trail

// Event is the compact record delivered to a Sink. Timestamp is Unix
// nanoseconds. Sinks must treat Events and their Fields as immutable.
type Event struct {
	ID                 EventID
	Timestamp          int64
	Kind               string
	Level              Level
	FlowID             FlowID
	ExecutionID        ExecutionID
	ParentExecutionID  ExecutionID
	RetryOfExecutionID ExecutionID
	ExecutionAttempt   uint32
	ExecutionSource    ExecutionSource
	ScopeType          string
	ScopeID            string
	EntityType         string
	EntityID           string
	ParentID           EventID
	Fields             []Field
}

// Metadata is configured once and attached to batches rather than copied into
// every Event.
type Metadata struct {
	Service     string
	Environment string
	Version     string
}

// Batch is a synchronous delivery unit. A Sink must copy it before retaining
// any Events or Fields after WriteBatch returns.
type Batch struct {
	Metadata Metadata
	Events   []Event
}
