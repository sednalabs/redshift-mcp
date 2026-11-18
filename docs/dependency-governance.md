# Dependency Governance

Redshift MCP uses the dependency-governance lane from `mcp-toolkit-rs`:

- `cargo-deny` is blocking for advisories, licenses, bans, and sources.
- `cargo-audit` is blocking for RustSec advisories.
- `cargo-outdated` is advisory by default and blocking only when
  `STRICT_OUTDATED=1`.
- `scripts/rmcp_macro_runtime_pin_check.py` ensures direct `rmcp`
  dependencies use one exact SDK pin and any direct `rmcp-macros` dependency
  stays aligned with the pinned `rmcp` runtime, while allowing the recommended
  `rmcp`-with-`macros` setup.

Install the local tooling with:

```bash
cargo install --locked cargo-deny cargo-audit cargo-outdated
```

Run the full check with:

```bash
./scripts/dependency_governance_check.sh
```

Use the stricter stale-dependency mode only when you want dependency freshness
to block the lane:

```bash
STRICT_OUTDATED=1 ./scripts/dependency_governance_check.sh
```

When you add a new direct dependency or make a major upgrade, include a short
PR note that records why the crate is needed, which safer alternatives were
considered, and how you would roll the change back.

## Temporary Chrono maintenance exception

PR1 explicitly adopts the existing SednaLabs maintenance exception tracked under
**Ops w19008 (parent w19007)**. It covers only
[RUSTSEC-2026-0543](https://rustsec.org/advisories/RUSTSEC-2026-0543.html), the
informational unmaintained notice issued on 2026-11-17, and only the locked
`chrono` **0.4.45** version. There is no patched version of that maintenance
notice. The Redshift lockfile's direct Chrono dependents are RMCP, schemars and
cbor-diag, so removal requires coordinated upstream dependency changes.

The decision is owned by **SednaLabs maintainers**, was recorded on **2026-11-18**,
and expires on **2026-12-18 at 00:00 UTC**. Track migration to maintained upstream
dependencies and removal of the exception under the same work item. This
repository's adoption is explicit; toolkit templates remain exception-free.

The exact record lives in `.cargo/advisory-exceptions.toml`. Its advisory ID must
match the entries in both `.cargo/audit.toml` and `deny.toml`. Before dependency
tools run, `scripts/advisory_exception_check.py` checks ownership, tracking,
the primary source, exact locked versions and UTC expiry. Hosted CI runs this
preflight even when dependency changes are otherwise irrelevant, and the final
governance job fails if the preflight fails.

This does not waive new vulnerabilities or unrelated advisories. Cargo-deny
checks and `cargo audit --deny warnings` remain enabled. A changed or removed
Chrono version requires reassessing or removing its record; an expired record
fails closed. The checker tests cover expiry boundaries, missing ownership,
version changes, mismatched ignores and malformed records:

```bash
python3 scripts/advisory_exception_check.py
python3 -m unittest discover -s scripts -p test_advisory_exception_check.py
```
