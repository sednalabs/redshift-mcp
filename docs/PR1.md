# PR1: local Redshift MCP for Kiro

## Delivery contract

Create and merge the first implementation PR in `sednalabs/redshift-mcp`.
First improve `sednalabs/mcp-toolkit-rs` scaffolding, generate this server with
those improvements, and pin the final dependency to the merged upstream commit.

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

## Implementation and verification

- Repository baseline: `cfd25f52ac89d4c0ec0eff23cbf6fad662a8f81d` (licence only).
- [Toolkit PR368](https://github.com/sednalabs/mcp-toolkit-rs/pull/368) adds
  immutable Git pins with exact crate versions, Kiro JSON configuration, safe
  existing-repository generation, and portable coverage defaults. The improved
  generator created this repository and preserved its original licence.
- The twelve-tool surface, strict metadata profile, SQL admission policy,
  owned query lifecycle, scoped cursors and Kiro configuration are implemented.
  The canonical schema and dependency graph are committed under `spec/` and
  `Cargo.lock`.
- Hosted validation covers 68 Rust tests: 60 unit tests, two catalogue contracts,
  five real executable stdio/CLI tests and one strict schema snapshot. The unit
  suite includes 17 actual-SDK HTTP fixtures and 18 lifecycle/pagination tests.
  Formatting, strict Clippy, dependency governance, coverage generation and
  CodeQL run in CI. See [PR1](https://github.com/sednalabs/redshift-mcp/pull/1)
  for checks on the precise candidate commit.
- Independent review corrected AWS batch setup semantics and ambiguous service
  errors. Only fixed setup SQL is transmitted initially; every setup child and
  the private session are verified before caller SQL is sent. Unknown execution
  outcomes retain capacity, and stale polls cannot overwrite terminal outcomes.
- Explicit `query_start`, `query_status`, `query_results`, and `query_cancel`
  avoid relying on undocumented client support for native MCP Tasks.
- Ops tracking: `w19007` records delivery and remote recovery checkpoints.
  `w19008` tracks removal of the exact, expiring Chrono maintenance exception.
- Automated tests use deterministic backends and actual SDK request/response
  fixtures. No live warehouse or installed Kiro application validation is
  represented by these results.

## Operating boundaries

Database grants remain the authorization boundary. SQL validation and a
read-only transaction add restrictions but do not grant access. The first
release has no administrative SQL, data exports to external destinations,
hosted listener, or general multi-statement execution. Query status distinguishes
cancellation acceptance from confirmed cancellation. A process restart expires
local query handles and cursors; server-side statement timeouts remain active.

No credentials, customer records, or organization-specific configuration belong
in this public repository.
