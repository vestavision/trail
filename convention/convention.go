// Package convention defines optional, storage-portable Trail field
// conventions. Applications may use custom values; the constants only provide
// shared semantics for the Explorer.
package convention

import "github.com/vestavision/trail"

const (
	FieldFlowStatus      = "trail.flow.status"
	FieldExecutionStatus = "trail.execution.status"
	FieldExecutionKind   = "trail.execution.kind"
	FieldRetentionClass  = "trail.retention.class"
)

type Status string

const (
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func FlowStatus(status Status) trail.Option {
	return trail.String(FieldFlowStatus, string(status))
}

func ExecutionStatus(status Status) trail.Option {
	return trail.String(FieldExecutionStatus, string(status))
}

func ExecutionKind(kind string) trail.Option {
	return trail.String(FieldExecutionKind, kind)
}

// RetentionClass assigns an optional lifecycle class to an event. It has no
// effect unless an external retention engine is configured for that class.
func RetentionClass(class string) trail.Option {
	return trail.String(FieldRetentionClass, class)
}

func IsCanonicalStatus(status string) bool {
	switch Status(status) {
	case StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}
