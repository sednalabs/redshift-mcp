use mcp_toolkit_testing::stdio_contract::StdioMcpProcess;
use redshift_mcp::server::TOOL_NAMES;
use serde_json::{json, Value};
use std::process::Command;

const EXE: &str = env!("CARGO_BIN_EXE_redshift-mcp");

#[test]
fn real_stdio_supports_current_and_legacy_clients_without_aws() {
    for version in ["2025-03-26", "2025-11-25", "2026-07-28"] {
        let mut process = StdioMcpProcess::start_with_client(EXE, "redshift-contract", version);
        assert_eq!(process.list_tool_names(), TOOL_NAMES);
        let valid = process.call_tool(10, "validate_sql", json!({"sql":"SELECT count(*) FROM analytics.events"}));
        assert_eq!(valid["result"]["isError"], false, "{valid}");
        let structured = &valid["result"]["structuredContent"];
        assert_eq!(structured["data"]["validation"], "local_structural");
        let text: Value = serde_json::from_str(valid["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(&text, structured);
        let invalid = process.call_tool(11, "validate_sql", json!({"sql":"CREATE TABLE private_canary (id int)"}));
        assert_eq!(invalid["result"]["isError"], true, "{invalid}");
        assert!(!invalid.to_string().contains("private_canary"));
        let status = process.call_tool(12, "connection_status", json!({"verify":false}));
        assert_eq!(status["result"]["isError"], false, "{status}");
        assert_eq!(status["result"]["structuredContent"]["data"]["authentication"], "not_checked");
        let unknown = process.call_tool(13, "query_status", json!({"query_id":"not-owned"}));
        assert_eq!(unknown["result"]["isError"], true, "{unknown}");
    }
}

#[test]
fn cli_inventory_and_schema_are_consistent() {
    let output = Command::new(EXE).arg("--print-tools").env_remove("REDSHIFT_TOOL_PROFILE").output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap().lines().collect::<Vec<_>>(), TOOL_NAMES);
    let output = Command::new(EXE).arg("--print-tool-schema").env_remove("REDSHIFT_TOOL_PROFILE").output().unwrap();
    assert!(output.status.success());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["schema"], "mcp_tool_schema_snapshot");
    assert_eq!(data["tools"].as_array().unwrap().len(), TOOL_NAMES.len());
}

#[test]
fn copied_binary_generates_kiro_configuration_without_environment_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let binary = dir.path().join(format!("redshift copied{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(EXE, &binary).unwrap();
    let output = Command::new(&binary).arg("--print-client-config").current_dir(dir.path())
        .env("AWS_PROFILE", "private-profile-canary").env("AWS_SECRET_ACCESS_KEY", "private-key-canary")
        .output().unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["mcpServers"]["redshift-mcp"]["command"], binary.to_str().unwrap());
    assert_eq!(data["mcpServers"]["redshift-mcp"]["autoApprove"], json!([]));
    assert_eq!(data["mcpServers"]["redshift-mcp"]["env"]["REDSHIFT_TOOL_PROFILE"], "analytics");
    assert!(!data.to_string().contains("private-"));
}

#[test]
fn cli_reports_unknown_arguments_without_echoing_their_values() {
    let output = Command::new(EXE).arg("private-secret-canary").output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-secret-canary"));
}

#[test]
fn generated_project_passes_static_doctor() {
    let output = Command::new(EXE).arg("--doctor").current_dir(env!("CARGO_MANIFEST_DIR")).output().unwrap();
    assert!(output.status.success(), "{} {}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("Ready: yes"));
}
