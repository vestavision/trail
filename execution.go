package trail

// ExecutionSource describes what initiated an execution. The constants are
// conventions, not a closed set; applications may use their own stable values.
type ExecutionSource string

const (
	SourceCron      ExecutionSource = "cron"
	SourceManual    ExecutionSource = "manual"
	SourceQueue     ExecutionSource = "queue"
	SourceAPI       ExecutionSource = "api"
	SourceRetry     ExecutionSource = "retry"
	SourceWebhook   ExecutionSource = "webhook"
	SourceMigration ExecutionSource = "migration"
)
