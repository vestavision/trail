package trail

import (
	"math"
	"time"
)

// FieldKind identifies the value stored in a Field.
type FieldKind uint8

const (
	FieldString FieldKind = iota + 1
	FieldInt
	FieldInt64
	FieldBool
	FieldFloat64
	FieldDuration
	FieldError
)

// Field is a compact typed key/value pair. Its value is exposed through typed
// accessors so the hot path does not require an interface value.
type Field struct {
	key  string
	kind FieldKind
	text string
	num  uint64
}

func (f Field) Key() string             { return f.key }
func (f Field) Kind() FieldKind         { return f.kind }
func (f Field) Text() string            { return f.text }
func (f Field) Int64() int64            { return int64(f.num) }
func (f Field) Bool() bool              { return f.num != 0 }
func (f Field) Float64() float64        { return math.Float64frombits(f.num) }
func (f Field) Duration() time.Duration { return time.Duration(f.num) }

type optionKind uint8

const (
	optionField optionKind = iota + 1
	optionFlow
	optionExecution
	optionEntity
	optionParent
	optionLevel
	optionParentExecution
	optionRetryOfExecution
	optionExecutionAttempt
	optionExecutionSource
)

// Option adds correlation or a typed field to an event.
type Option struct {
	kind       optionKind
	field      Field
	id         [16]byte
	entityType string
	entityID   string
	level      Level
	attempt    uint32
	source     ExecutionSource
}

func String(key, value string) Option {
	return Option{kind: optionField, field: Field{key: key, kind: FieldString, text: value}}
}

func Int(key string, value int) Option {
	return Option{kind: optionField, field: Field{key: key, kind: FieldInt, num: uint64(int64(value))}}
}

func Int64(key string, value int64) Option {
	return Option{kind: optionField, field: Field{key: key, kind: FieldInt64, num: uint64(value)}}
}

func Bool(key string, value bool) Option {
	var n uint64
	if value {
		n = 1
	}
	return Option{kind: optionField, field: Field{key: key, kind: FieldBool, num: n}}
}

func Float64(key string, value float64) Option {
	return Option{kind: optionField, field: Field{key: key, kind: FieldFloat64, num: math.Float64bits(value)}}
}

func Duration(key string, value time.Duration) Option {
	return Option{kind: optionField, field: Field{key: key, kind: FieldDuration, num: uint64(value)}}
}

// Error records err under the conventional "error" key.
func Error(err error) Option {
	text := "<nil>"
	if err != nil {
		text = err.Error()
	}
	return Option{kind: optionField, field: Field{key: "error", kind: FieldError, text: text}}
}

func Flow(id FlowID) Option           { return Option{kind: optionFlow, id: [16]byte(id)} }
func Execution(id ExecutionID) Option { return Option{kind: optionExecution, id: [16]byte(id)} }
func Parent(id EventID) Option        { return Option{kind: optionParent, id: [16]byte(id)} }

func Entity(entityType, entityID string) Option {
	return Option{kind: optionEntity, entityType: entityType, entityID: entityID}
}

func WithLevel(level Level) Option { return Option{kind: optionLevel, level: level} }

func ParentExecution(id ExecutionID) Option {
	return Option{kind: optionParentExecution, id: [16]byte(id)}
}

func RetryOfExecution(id ExecutionID) Option {
	return Option{kind: optionRetryOfExecution, id: [16]byte(id)}
}

// ExecutionAttempt uses one-based attempt numbers. Zero means unspecified.
func ExecutionAttempt(attempt uint32) Option {
	return Option{kind: optionExecutionAttempt, attempt: attempt}
}

func WithExecutionSource(source ExecutionSource) Option {
	return Option{kind: optionExecutionSource, source: source}
}
