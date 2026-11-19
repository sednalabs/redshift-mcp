# Tool guide

All tools use the single operator-configured Redshift target. Arguments cannot
select another AWS profile, cluster, workgroup or database. Unknown argument
fields are rejected. Inspect the exact JSON schemas with
`redshift-mcp --print-tool-schema`.

## Tool profiles

`REDSHIFT_TOOL_PROFILE=analytics` is the default and exposes all twelve tools.
`REDSHIFT_TOOL_PROFILE=metadata` exposes connection status, the four metadata
tools, and local SQL validation. The metadata profile cannot submit, inspect,
retrieve or cancel SQL queries. Profiles are enforced during listing, individual
tool lookup and calls; hiding a tool in the client is not the enforcement mechanism.

`query_cancel` is correctly annotated as changing query state, even though the
SQL execution surface is read-only. Query submission is not advertised as
universally idempotent; use an explicit `request_id` when retrying a submission.

## Connection and metadata

| Tool | Arguments | Behavior |
| --- | --- | --- |
| `connection_status` | Optional `verify` (default `false`). | Returns redacted configuration status. `verify=true` makes a bounded AWS metadata request to check access. |
| `list_databases` | Optional `cursor`, `limit`. | Lists accessible database names on the target. This does not change the configured query database. |
| `list_schemas` | Optional `pattern`, `cursor`, `limit`. | Lists schemas; `pattern` is an SQL LIKE pattern. |
| `list_objects` | Optional `schema`, `pattern`, `cursor`, `limit`. | Lists tables/views; both `schema` and object `pattern` use SQL LIKE matching. |
| `get_object_details` | Required `schema`, `name`; optional `cursor`, `limit`. | Returns column metadata for an exact schema and table/view name. |

Percent (`%`) and underscore (`_`) are wildcards in LIKE patterns. For example,
`{"schema":"reporting","pattern":"daily%"}` searches object names beginning
with `daily` in matching schemas. An underscore in a schema pattern is not an
exact literal match. Use exact schema/name values with `get_object_details`
when describing a known object.

Metadata pages are bounded. If a continuation cursor is returned, repeat the
same operation and filters, supplying that cursor. The MCP page `limit` may
change between calls because the underlying AWS page size stays fixed. The
default limit is the configured row limit; an explicit limit must be positive
and may only reduce it.

Local `connection_status` does not contact AWS, validate credentials or prove
table access. A successful verification establishes access to that metadata
operation; individual table grants and SQL execution permissions can still differ.

## Validate and explain

`validate_sql` accepts `sql` and an optional `parameters` array. It parses the
supported Redshift dialect and checks single-statement structure, the built-in
function allowlist and exact named-parameter bindings. It returns a fingerprint
of the trimmed SQL and the parameter count. No query or metadata is sent to AWS.
The fingerprint excludes parameter values and must not be used alone as an
execution-deduplication key.

```json
{
  "sql": "SELECT COUNT(*) FROM reporting.events WHERE event_date >= CAST(:cutoff AS DATE)",
  "parameters": [{"name": "cutoff", "value": "2026-01-01"}]
}
```

`explain_query` accepts the same submission arguments as `query_start` below,
but adds plain `EXPLAIN` before submission. Supply the `SELECT`/`WITH` query
without an EXPLAIN prefix. It returns a query handle; use `query_status` and
`query_results` to retrieve the plan. It does not run an `ANALYZE` execution mode
or turn optimizer estimates into a monetary cost estimate.

See [SQL compatibility](sql-compatibility.md) for the exact supported surface
and parameter/function limits. Local validation does not resolve tables,
column names, permissions or view definitions against a warehouse.

## Start, inspect and retrieve queries

`query_start` accepts:

| Argument | Required | Meaning |
| --- | --- | --- |
| `sql` | Yes | One supported read-only SQL statement. |
| `parameters` | No | Array of named string bindings, default `[]`. |
| `request_id` | No | Stable key for retrying the same submission within this server process. |
| `timeout_ms` | No | Warehouse statement timeout, bounded by operator configuration. |

For example:

```json
{
  "sql": "SELECT event_type, COUNT(*) AS events FROM reporting.events WHERE event_date >= CAST(:cutoff AS DATE) GROUP BY event_type ORDER BY events DESC",
  "parameters": [{"name": "cutoff", "value": "2026-01-01"}],
  "request_id": "events-by-type-2026-01-01",
  "timeout_ms": 30000
}
```

The response contains a private `query_id` and its current `state`. Save the
handle. Poll `query_status` with `{"query_id":"<returned-handle>"}` until the
state is terminal. `submitting` means the process owns the request but has not
yet received its AWS statement identifier. AWS active states are `submitted`,
`picked` and `started`; terminal states are `finished`, `failed` and `aborted`.
`submission_unknown` or `status_unknown` preserves an uncertain outcome and its
handle instead of pretending the query failed safely. Do not submit a duplicate
query to resolve an uncertain outcome. Such queries retain their concurrency
slot until a terminal state is confirmed or their retained record expires.
Metrics are reported when
AWS supplies them; unknown metrics are not invented as zero.

After `finished`, call `query_results` with `query_id` and optionally `limit`.
To continue, pass the returned `next_cursor` as the next call's `cursor`. Stop
when `complete=true` and `next_cursor` is null. A page can
end before the row limit because of the serialized-size budget. The controller
keeps the remaining offset within the provider page so continuation does not
skip rows. Do not rerun SQL merely to fetch the next page.

Reusing a `request_id` returns the existing handle for the same trimmed SQL,
binding array and timeout. Keep the binding order unchanged when retrying.
Changed inputs with an existing key are rejected. Use a new key for a new
execution. Request keys expire with the retained query record and are
process-local; they are not a durable recovery mechanism after a restart.

## Short queries

`query_sql` accepts the `query_start` arguments plus optional `wait_ms` (default
1,000; maximum 10,000) and `limit`. These local bounds are checked before
submitting SQL. It returns the query handle and state; if the query finishes
during that wait, its first page is included in a nested `result` field. If it
remains active, use the same handle with the explicit lifecycle tools.
If that first-page retrieval fails, `result_error` carries the failure while
the response retains `query_id` and the finished state. Retry `query_results`
with that handle instead of executing the SQL again.

The brief wait is not a warehouse execution deadline. Set `timeout_ms` or the
operator's `REDSHIFT_STATEMENT_TIMEOUT_MS` for that purpose. Do not repeatedly
call `query_sql` to poll one pending query.

## Cancel

`query_cancel` takes `query_id`. It requests cancellation only for a statement
owned by this server process and returns its query view. `cancel_requested`
records the local request; `cancellation_accepted` records acceptance once known.
A request cancelled before its first submission can become `aborted` locally
without contacting the warehouse. Check `query_status` afterwards. For work
already submitted, AWS accepting cancellation does not prove that the statement
terminated before it finished.

An API error, MCP disconnection or server restart does not itself stop SQL.
The warehouse statement timeout is independent of the local MCP connection.

## Result representation

Result pages contain ordered column metadata and tuple rows. A row's positions
match the columns in order; duplicate column labels do not overwrite values.
Column metadata includes name, type, nullability, precision and scale when known.

| Provider value | JSON representation |
| --- | --- |
| SQL NULL | `null` |
| Boolean | Boolean |
| Integer within JavaScript's exact integer range | JSON integer |
| Integer outside ±9,007,199,254,740,991 | Decimal string, preserving its exact value |
| Finite floating-point number | JSON number |
| Non-finite floating-point number | `"NaN"`, `"Infinity"` or `"-Infinity"` |
| DECIMAL, date/time, SUPER and other provider strings | String, without lossy numeric/date coercion |
| Binary value | Object with a `base64` string |

The server does not silently shorten a cell to fit a page. If an individual
row or its column metadata exceeds the configured budget, narrow the projection
or explicitly transform large values in SQL.

MCP results contain a bounded JSON text block plus the same structured payload:
`{"ok":true,"data":...}` or `{"ok":false,"error":...}`. Tool validation,
authorization and retrieval errors set MCP `isError=true`. A query-view response
can successfully report a query whose state is `failed`, or whose completed
result could not yet be fetched. Inspect `state`, `error` and `result_error`, not
only the tool-call success flag. This representation remains useful to clients
that read only text.

Provider errors use a fixed category and safe message, plus a retryability flag.
They do not return raw AWS diagnostics or SQL. Inspect the same handle after a
status/result error; do not interpret a transport error as permission to submit
the query again without its request key.
