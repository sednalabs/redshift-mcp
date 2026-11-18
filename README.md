# Redshift MCP

A local Rust MCP server for working with Amazon Redshift from Kiro and other
stdio MCP clients. Discover database objects, validate read-only SQL, inspect
plans, and run queries with bounded results and explicit query handles.

The server uses the AWS SDK for Rust and Redshift Data API. It connects to one
operator-configured Serverless workgroup or provisioned cluster. AWS credentials
come from the normal SDK credential chain, including an AWS IAM Identity Center
profile. There is no MCP HTTP listener or separately hosted service.

## What PR1 provides

| Workflow | Tools |
| --- | --- |
| Inspect configuration | `connection_status` |
| Discover metadata | `list_databases`, `list_schemas`, `list_objects`, `get_object_details` |
| Check SQL and plans | `validate_sql`, `explain_query` |
| Run a short query | `query_sql` |
| Manage longer queries | `query_start`, `query_status`, `query_results`, `query_cancel` |

Query results contain ordered column metadata and tuple rows. Pages are bounded
by both row count and serialized size. Opaque continuation cursors preserve
unreturned rows from an AWS result page. Long queries return a handle rather
than depending on a client's support for MCP Tasks.

SQL execution is read-only: the service validates a single query, sets a
statement timeout, and executes it inside a read-only transaction that is rolled
back. Use a dedicated database identity with read-only grants as the access
boundary. PR1 has no write/admin tools, persistent SQL sessions, or export tools.

## Build

Install Rust through [rustup](https://rustup.rs/), then build from the repository:

```bash
git clone https://github.com/sednalabs/redshift-mcp.git
cd redshift-mcp
cargo build --release --locked
```

The repository pins its Rust toolchain, toolkit Git revision, MCP SDK revision,
and dependency lockfile. The executable is `target/release/redshift-mcp` on Linux
and macOS, or `target/release/redshift-mcp.exe` on Windows.

## Connect Kiro

For an AWS IAM Identity Center profile, sign in using the AWS CLI before opening
the server in Kiro:

```bash
aws sso login --profile redshift-readonly
```

Copy [config/kiro.example.json](config/kiro.example.json) into your workspace's
`.kiro/settings/mcp.json`, or merge its `redshift` entry into an existing file.
Replace the executable path, AWS profile, region, database and workgroup with
your own values. For a provisioned cluster, replace `REDSHIFT_WORKGROUP_NAME`
with `REDSHIFT_CLUSTER_ID`.

```json
{
  "mcpServers": {
    "redshift": {
      "command": "/absolute/path/to/redshift-mcp",
      "args": [],
      "env": {
        "AWS_PROFILE": "redshift-readonly",
        "AWS_REGION": "ap-southeast-2",
        "REDSHIFT_DATABASE": "analytics",
        "REDSHIFT_WORKGROUP_NAME": "your-workgroup"
      },
      "disabled": false,
      "autoApprove": [],
      "disabledTools": []
    }
  }
}
```

Kiro also accepts a user-level `~/.kiro/settings/mcp.json`. Save the configuration
and check the MCP panel in the IDE or `/mcp` in the CLI. Start with
`connection_status`, then list schemas or inspect a known table. Tool discovery
does not require a live AWS connection; the first backend operation resolves
AWS credentials lazily.

The example leaves auto-approval empty so you can choose which tools to trust.
An optional [steering file](.kiro/steering/redshift.md) describes an efficient SQL
workflow. Copy it to `.kiro/steering/` in your SQL workspace to apply it when
working with SQL files. For a custom Kiro agent, include the steering file in
the agent's `resources`; see [Kiro steering](https://kiro.dev/docs/steering/).
Kiro edits local SQL files; the MCP server handles database operations.

Full setup details, authentication modes, limits and troubleshooting are in
[configuration](docs/configuration.md). Kiro's official
[MCP configuration documentation](https://kiro.dev/docs/mcp/configuration/)
describes configuration locations and client permissions.

## Query workflow

Ask Kiro to inspect the relevant schema before writing SQL. Use `validate_sql`
for local structural checks and `explain_query` for a Redshift-generated plan.
`validate_sql` does not resolve database objects or guarantee that a query will
run successfully. See [SQL compatibility](docs/sql-compatibility.md) for the
supported syntax, named parameters and built-in function allowlist.

For execution, `query_sql` waits briefly and returns either a bounded first
page or a pending query handle. For a longer query, call `query_start`, poll
`query_status`, then fetch `query_results`. Continue with the returned cursor
until the page is complete. Use `query_cancel` if the query is no longer needed,
then check its status: acceptance of cancellation is not proof that Redshift
stopped before finishing.

Query handles and cursors are private to the current server process. Restarting
Kiro's MCP process invalidates them; it does not itself cancel work already
submitted to Redshift. The configured warehouse statement timeout remains the
backstop. See the [tool guide](docs/tool-guide.md) for arguments, pagination and
parameter examples.

## Development and verification

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
python3 scripts/workflow_runner_policy_check.py --self-test
python3 scripts/native_release_artifact.py --self-test
```

The test suite exercises query lifecycle and pagination with deterministic
backends, the AWS adapter with mock HTTP responses, and the compiled binary over
real stdio. These tests do not use a live Redshift warehouse or a signed-in Kiro
session. Verify those integrations in your own environment before relying on
the server for warehouse work.

Inspect the binary's tool surface without starting Kiro:

```bash
target/release/redshift-mcp --print-tools
target/release/redshift-mcp --print-tool-schema
target/release/redshift-mcp --print-client-config
```

Schema snapshots are checked in under `spec/`. After an intentional public tool
contract change, rebaseline and review the diff:

```bash
MCP_TOOLKIT_UPDATE_TOOL_SNAPSHOTS=1 \
  cargo test --locked tool_schema_snapshot_contract_is_stable
```

The repository includes CI for Rust checks, dependency governance, CodeQL and
coverage. Trusted pushes to `main` and version tags also build native Linux
x86_64/arm64, macOS x86_64/arm64 and Windows x86_64 artifacts. The release workflow
verifies checksums, tool-schema parity and SBOMs before attesting artifacts. It
does not create a GitHub Release or install binaries. Dependency policy is
documented in [dependency governance](docs/dependency-governance.md).

## Architecture and boundaries

The server was scaffolded with
[Sedna Labs MCP Toolkit for Rust](https://github.com/sednalabs/mcp-toolkit-rs).
The toolkit supplies stdio composition, tool catalogues, result helpers,
continuation storage and protocol test utilities. Redshift authentication,
SQL policy, query ownership and Data API behavior belong to this service.

The database identity controls which data can be read. Local stdio describes
where the MCP process runs; data returned to Kiro can still be sent to the
model used by the client. See [security model](docs/security-model.md) for the
specific boundaries, cancellation semantics and error handling.

Licensed under [Apache-2.0](LICENSE).
