---
inclusion: fileMatch
fileMatchPattern: '**/*.sql'
---

# Redshift SQL workflow

Use the `redshift` MCP server for database discovery and query execution. Keep
SQL source in the workspace using Kiro's file tools.

Inspect the relevant schemas and objects before relying on column names or
types. Prefer explicit columns, named parameters, and selective predicates.
Redshift's SQL dialect and warehouse execution differ from PostgreSQL; do not
suggest PostgreSQL index DDL or `EXPLAIN ANALYZE` as a Redshift workflow.

Use `validate_sql` for the server's local structural checks. It does not resolve
tables or validate a query against Redshift. Use `explain_query` for a warehouse
plan before an unfamiliar or potentially expensive query when useful.

For longer work, start the query once, keep its returned handle, poll status,
and retrieve result pages. Continue with the supplied cursor rather than
resubmitting the query. Treat pending or partial results as incomplete.

Use `query_cancel` when a query is no longer needed, then check its status.
Cancellation acceptance and terminal cancellation are different outcomes.

Prefer counts and aggregates when row-level data is unnecessary. Do not put
credentials into SQL, tool arguments or committed configuration. Use the
operator-configured database permissions; do not broaden grants or switch
identities to work around a rejected operation.
