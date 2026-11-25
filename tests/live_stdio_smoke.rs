//! Explicit opt-in smoke test against the operator-configured warehouse.
//! Uses synthetic constants only. Never print AWS responses or target details.

use mcp_toolkit_testing::stdio_contract::StdioMcpProcess;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const EXE: &str = env!("CARGO_BIN_EXE_redshift-mcp");

fn call(process: &mut StdioMcpProcess, id: u64, name: &str, args: Value) -> Value {
    process.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {"name": name, "arguments": args},
    }));
    let response = process.response_with_timeout(id, Duration::from_secs(65));
    let result = &response["result"];
    assert!(
        result["isError"] == false && result["structuredContent"]["ok"] == true,
        "live MCP call {name} failed; inspect local redacted diagnostics"
    );
    result["structuredContent"]["data"].clone()
}

#[test]
#[ignore = "requires an explicitly approved live Redshift target and AWS credentials"]
fn live_redshift_read_only_stdio_contract() {
    assert!(
        std::env::var("REDSHIFT_LIVE_SMOKE").as_deref() == Ok("1"),
        "Set REDSHIFT_LIVE_SMOKE=1 only for an approved synthetic-query smoke test"
    );
    let mut process =
        StdioMcpProcess::start_legacy_with_client(EXE, "redshift-live-smoke", "2025-11-25");
    let names = process.list_tool_names();
    assert!(
        names.iter().any(|name| name == "query_start"),
        "select the analytics tool profile"
    );
    let status = call(
        &mut process,
        10,
        "connection_status",
        json!({"verify":false}),
    );
    assert!(status["configured"] == true && status["verified"] == false);
    let status = call(
        &mut process,
        11,
        "connection_status",
        json!({"verify":true}),
    );
    assert!(
        status["verified"] == true,
        "metadata authorization was not verified"
    );

    let query = json!({
        "sql": "SELECT CAST(:first AS INTEGER) AS value UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY 1",
        "parameters": [{"name":"first","value":"1"}],
        "request_id": "synthetic-live-smoke",
        "timeout_ms": 30000,
    });
    let started = call(&mut process, 12, "query_start", query.clone());
    let handle = started["query_id"]
        .as_str()
        .expect("owned query handle missing")
        .to_owned();
    let retry = call(&mut process, 13, "query_start", query);
    assert!(
        retry["query_id"].as_str() == Some(handle.as_str()),
        "idempotent submission returned a different handle"
    );

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut id = 14;
    loop {
        assert!(
            Instant::now() < deadline,
            "synthetic query deadline expired"
        );
        let status = call(&mut process, id, "query_status", json!({"query_id":handle}));
        id += 1;
        match status["state"].as_str() {
            Some("finished") => break,
            Some("submitting" | "submitted" | "picked" | "started") => {}
            _ => panic!("synthetic query did not reach successful completion"),
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    let mut cursor: Option<String> = None;
    for expected in 1..=3 {
        let page = call(
            &mut process,
            id,
            "query_results",
            json!({"query_id":handle,"cursor":cursor,"limit":1}),
        );
        id += 1;
        assert!(
            page["rows"] == json!([[expected]]),
            "synthetic row mismatch"
        );
        assert!(page["returned_rows"] == 1);
        assert!(page["complete"] == (expected == 3));
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        assert!(cursor.is_some() == (expected < 3));
    }
}
