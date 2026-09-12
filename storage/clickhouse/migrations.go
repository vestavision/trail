package clickhouse

const createEventsTable = `
CREATE TABLE IF NOT EXISTS trail_events
(
    event_time DateTime64(9, 'UTC'),
    event_id FixedString(16),
    batch_id FixedString(16),
    jetstream_stream LowCardinality(String),
    jetstream_stream_seq UInt64,
    jetstream_consumer LowCardinality(String),
    jetstream_consumer_seq UInt64,
    jetstream_delivered UInt64,
    received_at DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6),
    service LowCardinality(String),
    environment LowCardinality(String),
    version LowCardinality(String),
    kind LowCardinality(String),
    level Enum8('debug' = 1, 'info' = 2, 'warn' = 3, 'error' = 4),
    flow_id FixedString(16),
    execution_id FixedString(16),
    parent_execution_id FixedString(16),
    retry_of_execution_id FixedString(16),
    execution_attempt UInt32,
    execution_source LowCardinality(String),
    entity_type LowCardinality(String),
    entity_id String,
    parent_event_id FixedString(16),
    flow_status LowCardinality(String),
    execution_status LowCardinality(String),
    execution_kind LowCardinality(String),
    field_keys Array(String),
    field_types Array(String),
    field_text Array(String),
    field_num Array(UInt64),
    http_method LowCardinality(String),
    http_scheme LowCardinality(String),
    http_host LowCardinality(String),
    http_path String,
    http_status_code UInt16,
    http_duration_ns UInt64,
    http_success UInt8,
    http_request_size Int64,
    http_response_size Int64,
    http_request_preview String,
    http_response_preview String,
    payload_refs_json String,
    PROJECTION by_time (SELECT _part_offset ORDER BY (service, event_time, event_id)),
    PROJECTION by_global_time (SELECT _part_offset ORDER BY (event_time, event_id)),
    PROJECTION by_flow (SELECT _part_offset ORDER BY (flow_id, event_time, event_id)),
    PROJECTION by_execution (SELECT _part_offset ORDER BY (execution_id, event_time, event_id)),
    PROJECTION by_entity (SELECT _part_offset ORDER BY (entity_type, entity_id, event_time, event_id)),
    PROJECTION by_kind (SELECT _part_offset ORDER BY (kind, event_time, event_id)),
    PROJECTION by_http_status (SELECT _part_offset ORDER BY (http_status_code, event_time, event_id))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY event_id
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
`

const createFlowSummaries = `
CREATE TABLE IF NOT EXISTS trail_flow_summaries
(
    flow_id FixedString(16),
    service LowCardinality(String),
    environment LowCardinality(String),
    started_at AggregateFunction(min, DateTime64(9, 'UTC')),
    finished_at AggregateFunction(max, DateTime64(9, 'UTC')),
    event_count AggregateFunction(uniqExact, FixedString(16)),
    entity_type AggregateFunction(argMax, String, Tuple(DateTime64(9, 'UTC'), FixedString(16))),
    entity_id AggregateFunction(argMax, String, Tuple(DateTime64(9, 'UTC'), FixedString(16))),
    execution_id AggregateFunction(argMax, FixedString(16), Tuple(DateTime64(9, 'UTC'), FixedString(16))),
    status AggregateFunction(argMaxIf, String, Tuple(DateTime64(9, 'UTC'), FixedString(16)), UInt8),
    first_kind AggregateFunction(argMin, String, Tuple(DateTime64(9, 'UTC'), FixedString(16))),
    last_kind AggregateFunction(argMax, String, Tuple(DateTime64(9, 'UTC'), FixedString(16)))
)
ENGINE = AggregatingMergeTree
ORDER BY (flow_id, service, environment)
`

const createFlowSummaryView = `
CREATE MATERIALIZED VIEW IF NOT EXISTS trail_flow_summaries_mv TO trail_flow_summaries AS
SELECT
    flow_id,
    service,
    environment,
    minState(event_time) AS started_at,
    maxState(event_time) AS finished_at,
    uniqExactState(event_id) AS event_count,
    argMaxState(entity_type, tuple(event_time, event_id)) AS entity_type,
    argMaxState(entity_id, tuple(event_time, event_id)) AS entity_id,
    argMaxState(execution_id, tuple(event_time, event_id)) AS execution_id,
    argMaxIfState(flow_status, tuple(event_time, event_id), notEmpty(flow_status)) AS status,
    argMinState(kind, tuple(event_time, event_id)) AS first_kind,
    argMaxState(kind, tuple(event_time, event_id)) AS last_kind
FROM trail_events
WHERE flow_id != unhex('00000000000000000000000000000000')
GROUP BY flow_id, service, environment
`

const createExecutionSummaries = `
CREATE TABLE IF NOT EXISTS trail_execution_summaries
(
    execution_id FixedString(16),
    service LowCardinality(String),
    environment LowCardinality(String),
    started_at AggregateFunction(min, DateTime64(9, 'UTC')),
    finished_at AggregateFunction(max, DateTime64(9, 'UTC')),
    event_count AggregateFunction(uniqExact, FixedString(16)),
    flow_count AggregateFunction(uniqExact, FixedString(16)),
    kind AggregateFunction(argMaxIf, String, Tuple(DateTime64(9, 'UTC'), FixedString(16)), UInt8),
    source AggregateFunction(argMaxIf, String, Tuple(DateTime64(9, 'UTC'), FixedString(16)), UInt8),
    status AggregateFunction(argMaxIf, String, Tuple(DateTime64(9, 'UTC'), FixedString(16)), UInt8),
    attempt AggregateFunction(max, UInt32),
    parent_execution_id AggregateFunction(argMax, FixedString(16), Tuple(DateTime64(9, 'UTC'), FixedString(16))),
    retry_of_execution_id AggregateFunction(argMax, FixedString(16), Tuple(DateTime64(9, 'UTC'), FixedString(16)))
)
ENGINE = AggregatingMergeTree
ORDER BY (execution_id, service, environment)
`

const createExecutionSummaryView = `
CREATE MATERIALIZED VIEW IF NOT EXISTS trail_execution_summaries_mv TO trail_execution_summaries AS
SELECT
    execution_id,
    service,
    environment,
    minState(event_time) AS started_at,
    maxState(event_time) AS finished_at,
    uniqExactState(event_id) AS event_count,
    uniqExactStateIf(flow_id, flow_id != unhex('00000000000000000000000000000000')) AS flow_count,
    argMaxIfState(execution_kind, tuple(event_time, event_id), notEmpty(execution_kind)) AS kind,
    argMaxIfState(execution_source, tuple(event_time, event_id), notEmpty(execution_source)) AS source,
    argMaxIfState(execution_status, tuple(event_time, event_id), notEmpty(execution_status)) AS status,
    maxState(execution_attempt) AS attempt,
    argMaxState(parent_execution_id, tuple(event_time, event_id)) AS parent_execution_id,
    argMaxState(retry_of_execution_id, tuple(event_time, event_id)) AS retry_of_execution_id
FROM trail_events
WHERE execution_id != unhex('00000000000000000000000000000000')
GROUP BY execution_id, service, environment
`

const createFlowTerminals = `
CREATE TABLE IF NOT EXISTS trail_flow_terminals
(
    flow_id FixedString(16), event_time DateTime64(9, 'UTC'),
    service LowCardinality(String), environment LowCardinality(String),
    entity_type LowCardinality(String), entity_id String,
    execution_id FixedString(16), status LowCardinality(String),
    PROJECTION by_recent (SELECT _part_offset ORDER BY (service, environment, event_time, flow_id)),
    PROJECTION by_entity (SELECT _part_offset ORDER BY (entity_type, entity_id, event_time, flow_id)),
    PROJECTION by_execution (SELECT _part_offset ORDER BY (execution_id, event_time, flow_id))
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY flow_id
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
`

const createFlowTerminalsView = `
CREATE MATERIALIZED VIEW IF NOT EXISTS trail_flow_terminals_mv TO trail_flow_terminals AS
SELECT flow_id,event_time,service,environment,entity_type,entity_id,execution_id,flow_status AS status
FROM trail_events
WHERE flow_id != unhex('00000000000000000000000000000000')
  AND flow_status IN ('succeeded','failed','cancelled')
`

var migrationStatements = []string{
	createEventsTable,
	`CREATE TABLE IF NOT EXISTS trail_payload_gc_candidates (
		store LowCardinality(String), object_key String, ref_json String,
		eligible_at DateTime64(9, 'UTC'), candidate_at DateTime64(9, 'UTC'),
		retry_after DateTime64(9, 'UTC'), attempts UInt32, last_error String, archive_id String DEFAULT '',
		version DateTime64(9, 'UTC')
	) ENGINE=ReplacingMergeTree(version) ORDER BY (store,object_key)`,
	`ALTER TABLE trail_payload_gc_candidates ADD COLUMN IF NOT EXISTS archive_id String DEFAULT ''`,
	`CREATE TABLE IF NOT EXISTS trail_retention_summary_rebuilds (
		kind Enum8('flow'=1,'execution'=2), identity FixedString(16), version DateTime64(9,'UTC')
	) ENGINE=ReplacingMergeTree(version) ORDER BY (kind,identity)`,
	`CREATE TABLE IF NOT EXISTS trail_archives (archive_id String, manifest_json String, manifest_ref_json String, created_at DateTime64(9,'UTC'), version DateTime64(9,'UTC')) ENGINE=ReplacingMergeTree(version) ORDER BY archive_id`,
	`CREATE TABLE IF NOT EXISTS trail_event_archives (event_id FixedString(16), archive_id String, archived_at DateTime64(9,'UTC'), version DateTime64(9,'UTC')) ENGINE=ReplacingMergeTree(version) ORDER BY event_id SETTINGS deduplicate_merge_projection_mode='rebuild'`,
	`ALTER TABLE trail_event_archives MODIFY SETTING deduplicate_merge_projection_mode='rebuild'`,
	`ALTER TABLE trail_event_archives ADD PROJECTION IF NOT EXISTS by_archive (SELECT _part_offset ORDER BY (archive_id,event_id))`,
	createFlowSummaries,
	createFlowSummaryView,
	createExecutionSummaries,
	createExecutionSummaryView,
	createFlowTerminals,
	createFlowTerminalsView,
	`ALTER TABLE trail_events ADD PROJECTION IF NOT EXISTS by_global_time (SELECT _part_offset ORDER BY (event_time, event_id))`,
	`ALTER TABLE trail_events ADD PROJECTION IF NOT EXISTS by_kind (SELECT _part_offset ORDER BY (kind, event_time, event_id))`,
	`ALTER TABLE trail_events ADD PROJECTION IF NOT EXISTS by_http_status (SELECT _part_offset ORDER BY (http_status_code, event_time, event_id))`,
}
