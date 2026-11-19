# Redshift MCP engineering guidance

This is a public Rust stdio MCP server. Follow the contracts in
`docs/security-model.md`, `docs/tool-guide.md` and `docs/configuration.md`.

- Keep AWS transport in `aws.rs`, owned handles and paging in `controller.rs`,
  structural SQL policy in `sql.rs`, and MCP catalogue policy in `server.rs`.
- Startup and tool discovery must work without AWS credentials or a warehouse.
- Treat database grants as the access boundary. Do not weaken the SQL guard,
  expose arbitrary provider IDs, or add implicit fallback execution modes.
- Never log SQL, bound values, credentials or raw provider error text. Keep stdout
  exclusive to MCP unless an explicit CLI introspection option was requested.
- Preserve complete pages. Byte limits must not skip rows or silently clip cells.
- Test behaviour through the existing fake backend and SDK HTTP fixtures. Never
  claim these tests exercised a real Redshift warehouse or Kiro installation.
- Reuse the pinned toolkit; upstream transport-neutral improvements there.
  Commit `Cargo.lock` and keep RMCP runtime/macros pinned together.
- For changed contracts, update the schema snapshot and real stdio tests. Run
  `cargo fmt --all --check`, `cargo clippy --locked --all-targets --all-features
  -- -D warnings`, and `cargo test --locked --all-targets --all-features`.
- Land changes through PRs with passing hosted checks. Keep meaningful checkpoints
  on remote branches and link delivery evidence in Ops when requested.

No customer records, private configuration, credentials or organization-specific
business logic belong in this repository.
