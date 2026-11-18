# PR1: local Redshift MCP for Kiro

## Delivery contract

Create and merge the first implementation PR in `sednalabs/redshift-mcp`.
First improve `sednalabs/mcp-toolkit-rs` scaffolding, land that upstream PR,
then generate this server from that revision and pin the resulting dependency.

The initial server is a native Rust stdio executable with one configured Redshift
target, lazy AWS authentication, read-only SQL, metadata discovery, asynchronous
query handles, cancellation, and bounded, lossless result pages. It supports
Serverless workgroups and provisioned clusters through the Redshift Data API.
Kiro edits workspace files; this server performs database operations.

Acceptance requires real executable MCP contract tests, deterministic AWS adapter
and lifecycle tests, a committed schema snapshot and lockfile, documented Kiro
configuration, successful format/lint/test checks, and hosted PR checks. Live
warehouse and actual Kiro application verification are reported separately and
must not be inferred from mocked tests.

## Work lanes

| Lane | Responsibility | Branch |
| --- | --- | --- |
| Toolkit | Immutable revision pins, Kiro JSON, safe existing-repo generation | `sednalabs/mcp-toolkit-rs:feat/portable-kiro-scaffolding` |
| AWS adapter | Data API requests, metadata, result types, status and cancellation | `feat/pr1-kiro-redshift` |
| Server | Configuration, SQL policy, job ownership, bounded cursors, MCP surface | `feat/pr1-kiro-redshift` |
| Client and delivery | Kiro configuration, documentation, CI, independent review | `feat/pr1-kiro-redshift` |

## Checkpoint

- Repository baseline: `cfd25f52ac89d4c0ec0eff23cbf6fad662a8f81d` (licence only).
- Toolkit improvements are in PR368. The improved generator created this repository
  with a pinned toolkit dependency and preserved its original licence. Local
  toolkit checks and generated-application tests pass; hosted checks are running.
- AWS adapter implementation passes 12 tests using the actual SDK with an
  in-memory HTTP fixture. SQL policy, configuration, MCP surface and Kiro docs
  are present. The controller and full integrated verification are in progress.
- Explicit `query_start`, `query_status`, `query_results`, and `query_cancel`
  avoid relying on undocumented client support for native MCP Tasks.
- Ops tracking: `w19007`. The initial service errors recovered; progress and
  recovery references are recorded there. Redshift PR: #1; toolkit PR: #368.

## Operating boundaries

Database grants remain the authorization boundary. SQL validation and a
read-only transaction add restrictions but do not grant access. The first
release has no administrative SQL, data exports to external destinations,
hosted listener, or general multi-statement execution. Query status distinguishes
cancellation acceptance from confirmed cancellation. A process restart expires
local query handles and cursors; server-side statement timeouts remain active.

No credentials, customer records, or organization-specific configuration belong
in this public repository.
