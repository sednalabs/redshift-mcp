# Configuration

Configure one database in either a Redshift Serverless workgroup or a provisioned
cluster. Target selection is operator configuration, not a tool argument.

## Target and authentication

| Environment variable | Meaning |
| --- | --- |
| `REDSHIFT_DATABASE` | Required database name for queries and object discovery. |
| `REDSHIFT_WORKGROUP_NAME` | Serverless workgroup; set this or `REDSHIFT_CLUSTER_ID`. |
| `REDSHIFT_CLUSTER_ID` | Provisioned cluster identifier; set this or `REDSHIFT_WORKGROUP_NAME`. |
| `AWS_PROFILE` | Optional SDK shared-configuration profile, including IAM Identity Center profiles. |
| `AWS_REGION` | AWS region. Otherwise the SDK's normal region provider chain applies. |
| `REDSHIFT_DB_USER` | Optional provisioned-cluster database user for temporary credentials. Incompatible with Serverless or `REDSHIFT_SECRET_ARN`. |
| `REDSHIFT_SECRET_ARN` | Optional Secrets Manager secret for database authentication. Incompatible with `REDSHIFT_DB_USER`. |
| `REDSHIFT_TOOL_PROFILE` | `analytics` (default) for all tools, or `metadata` for discovery and local SQL validation only. |

With neither `REDSHIFT_DB_USER` nor `REDSHIFT_SECRET_ARN`, the Data API uses the
configured IAM identity for temporary database authentication. A Serverless
workgroup uses Serverless credentials. For provisioned clusters, set
`REDSHIFT_DB_USER` only when the intended IAM policy and database setup use an
explicit database user.

The server uses the AWS SDK's credential provider chain. It does not store AWS
access keys, launch an interactive login, or expose credentials through MCP.
For IAM Identity Center, first run `aws sso login --profile <profile-name>`
outside Kiro. Sign in again when the session expires. Kiro's process needs access
to the same AWS configuration and cache; launching it from a different account
or a restricted environment can change what the SDK can read.

The Data API must be enabled/available for the selected Redshift target and
region. The SDK makes HTTPS AWS API calls; it does not open a PostgreSQL wire
connection to the cluster endpoint.

## AWS and database permissions

The configured IAM principal needs the Data API actions used by the enabled
tools: `BatchExecuteStatement`, `DescribeStatement`, `GetStatementResult`,
`CancelStatement`, `ListDatabases`, `ListSchemas`, `ListTables` and
`DescribeTable` under the `redshift-data` service prefix.

Additional authentication permissions depend on your chosen mode:

| Mode | Authentication API permission |
| --- | --- |
| Serverless IAM credentials | `redshift-serverless:GetCredentials` |
| Provisioned cluster, IAM identity | `redshift:GetClusterCredentialsWithIAM` |
| Provisioned cluster, explicit `REDSHIFT_DB_USER` | `redshift:GetClusterCredentials` |
| Secrets Manager database credentials | `secretsmanager:GetSecretValue` for the chosen secret; KMS permissions may also be needed for its encryption key. |

Scope permissions to the intended resources where the IAM action supports it.
The server does not request control-plane cluster discovery. AWS permissions to
call the Data API and database grants to read tables are separate requirements.
Use a dedicated database principal with only the required schema/table/view
access. A missing IAM grant is not a reason to broaden the database identity.

The authoritative API and IAM details are in AWS's
[Data API guide](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html),
[Data API permissions](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-access.html)
and [BatchExecuteStatement reference](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_BatchExecuteStatement.html).

## Bounds

| Environment variable | Default | Allowed range | Purpose |
| --- | ---: | --- | --- |
| `REDSHIFT_STATEMENT_TIMEOUT_MS` | 60,000 | 1,000–300,000 | Redshift statement timeout for submitted queries. |
| `REDSHIFT_API_TIMEOUT_SECS` | 30 | 1–60 | Deadline for an individual AWS API operation. |
| `REDSHIFT_MAX_PAGE_ROWS` | 100 | 1–1,000 | Maximum rows in a returned result page. |
| `REDSHIFT_MAX_PAGE_BYTES` | 12,000 | 1,024–24,000 | Serialized data-page budget; row count alone is insufficient for wide rows. |
| `REDSHIFT_MAX_CONCURRENT_QUERIES` | 4 | 1–32 | Maximum tracked queries running concurrently. |
| `REDSHIFT_MAX_JOBS` | 128 | 1–1,024 | Maximum query records retained by the current process. |
| `REDSHIFT_JOB_TTL_SECS` | 3,600 | 600–86,400 | Query-handle lifetime. |

Uncertain submission or termination outcomes retain their query-concurrency
slot until a terminal state is confirmed or the query record expires. This
prevents transport failures from immediately freeing capacity for duplicate
warehouse work. Cursor storage is also bounded and expires with a finite TTL.

These bounds do not estimate warehouse cost. Redshift can scan substantial data
to produce a small result. Keep queries selective and examine plans where
appropriate. Backend timeouts and MCP request timeouts are different: a client
disconnect or API deadline does not prove that submitted SQL stopped running.

## Kiro configuration

Use the example in [config/kiro.example.json](../config/kiro.example.json).
The executable path must point to the built binary, not the repository folder.
On Windows, escape backslashes in JSON, for example
`"C:\\tools\\redshift-mcp.exe"`.

Workspace configuration lives at `.kiro/settings/mcp.json`; user configuration
lives at `~/.kiro/settings/mcp.json`. Merge entries into existing files instead
of replacing other MCP servers. The example uses a Serverless target; switch
to `REDSHIFT_CLUSTER_ID` for a provisioned cluster.

`--print-client-config` prints a JSON configuration for the exact executable
being run. It does not copy credentials or private environment values into the
output. Add the target and AWS settings from the table above before use.

Leave `autoApprove` empty initially. You can choose individual trusted tools in
Kiro once you have reviewed their arguments. Client approval is separate from
the server's read-only SQL policy and IAM/database authorization.

Current Kiro CLI versions distinguish connection `timeout` from per-call
`requestTimeout`. The server's asynchronous query tools avoid using a long MCP
request as the lifetime of a warehouse query. Consult
[Kiro MCP configuration](https://kiro.dev/docs/mcp/configuration/) and
[CLI agent configuration](https://kiro.dev/docs/cli/v3/agent-config/) for the
fields supported by your installed version.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Kiro cannot start the server | Run `redshift-mcp --print-tools` directly; check the absolute path and executable permissions. |
| Tool discovery works but metadata fails | Check the target configuration, AWS profile, region, IAM credentials and Data API permissions. Discovery itself does not establish a database connection. |
| Expired SSO credentials | Run `aws sso login --profile <profile-name>`, then retry the operation. |
| IAM or database access denied | Check the IAM action and database grant independently. Do not switch to an administrator identity as a default workaround. |
| Query remains pending | Poll `query_status`; use `query_cancel` if it is no longer needed. The status after cancellation determines whether the query stopped or finished. |
| A row is too large for the page | Select fewer columns, aggregate, or shorten large string/SUPER values explicitly in SQL. Results are not silently clipped. |
| Cursor or handle is unavailable | It may have expired or belong to a previous server process. Reconnect/restart invalidates process-local handles and cursors. |

`--doctor` checks source-project scaffolding. It does not authenticate to AWS or
query Redshift. Kiro's MCP logs and the server's structured error category can
help identify setup issues without recording raw SQL or credentials.
