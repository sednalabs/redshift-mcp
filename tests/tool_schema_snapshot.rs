use mcp_toolkit_testing::assert_tool_schema_snapshot;
use redshift_mcp::{Config, RedshiftServer};
use std::path::PathBuf;

#[test]
fn tool_schema_snapshot_contract_is_stable() {
    let server = RedshiftServer::new(Config::default()).expect("server");
    assert_tool_schema_snapshot(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("spec/tool_schema_snapshot.v1.json"),
        &server.active_tools().expect("tools"),
    );
}
