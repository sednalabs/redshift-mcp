# Security

This server executes restricted read queries against one operator-configured
Redshift target. Use a database identity limited to the required views/tables and
restrict function execution. Local SQL validation supplements those grants.

See [the security model](docs/security-model.md) for query ownership, result
handling, resource budgets, AWS authentication and cancellation semantics.

Report suspected security defects privately through the repository's GitHub
security advisory reporting channel when available. Do not place credentials,
private SQL or customer data in public issues. If private reporting is disabled,
contact the repository maintainers without including sensitive material.
