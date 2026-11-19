use mcp_toolkit::rmcp::ServerHandler;
use mcp_toolkit_core::tool_inventory::ToolOperation;
use mcp_toolkit_testing::catalog_profile_contract::{
    assert_tool_catalog_profile_contains_tools, assert_tool_catalog_profile_contract,
};
use redshift_mcp::{
    server::{METADATA_TOOLS, TOOL_NAMES},
    Config, RedshiftServer,
};

#[test]
fn catalog_profiles_match_the_exported_tool_surface() {
    let server = RedshiftServer::new(Config::default()).unwrap();
    for (key, names) in [("analytics", TOOL_NAMES), ("metadata", METADATA_TOOLS)] {
        let profile = server.catalog().require_profile(key).unwrap();
        let contract = server
            .inventory()
            .catalog_contract(profile, ToolOperation::List);
        assert_tool_catalog_profile_contract(&contract);
        assert_tool_catalog_profile_contains_tools(&contract.to_value(), names);
        let tools = server.tool_schema_snapshot_for_profile(key).unwrap();
        assert_eq!(
            tools.iter().map(|t| t.name.as_ref()).collect::<Vec<_>>(),
            names
        );
    }
}

#[test]
fn metadata_profile_hides_execution_from_get_tool_as_well_as_discovery() {
    let server = RedshiftServer::new(Config {
        tool_profile: "metadata".into(),
        ..Config::default()
    })
    .unwrap();
    for name in ["query_start", "query_sql", "query_cancel", "explain_query"] {
        assert!(server.get_tool(name).is_none());
    }
    assert!(server.get_tool("list_schemas").is_some());
    assert!(server.get_tool("unregistered_tool").is_none());
}
