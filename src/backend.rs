//! Backend contracts keep AWS transport separate from query ownership and MCP policy.
//!
//! The server validates SQL and owns query handles. A backend receives only validated
//! requests and returns data without raw SQL, parameters, credentials, or provider errors.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SqlParameter {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryState {
    Submitted,
    Picked,
    Started,
    Finished,
    Failed,
    Aborted,
}

impl QueryState {
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Aborted)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StatementStatus {
    pub state: QueryState,
    pub result_id: Option<String>,
    pub duration_ns: Option<i64>,
    pub result_rows: Option<i64>,
    pub result_bytes: Option<i64>,
    pub redshift_query_id: Option<i64>,
    pub error: Option<AppError>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub type_name: String,
    pub nullable: Option<bool>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct ResultPage {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub next_token: Option<String>,
    pub total_rows: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataKind {
    Databases,
    Schemas,
    Tables,
    Columns,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetadataRequest {
    pub kind: MetadataKind,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub pattern: Option<String>,
    pub next_token: Option<String>,
    pub max_results: i32,
}

#[derive(Clone, Debug)]
pub struct MetadataPage {
    pub items: Vec<Value>,
    pub next_token: Option<String>,
}

/// Implements bounded calls against one operator-configured database target.
///
/// The caller owns authentication scope, validated SQL, query handle authority,
/// and response pagination. Backend errors must never contain raw provider data.
#[async_trait]
pub trait Backend: Send + Sync {
    async fn submit(
        &self,
        sql: &str,
        parameters: &[SqlParameter],
        client_token: &str,
        timeout_ms: u64,
    ) -> AppResult<String>;

    async fn status(&self, id: &str) -> AppResult<StatementStatus>;
    async fn results(&self, result_id: &str, next_token: Option<&str>) -> AppResult<ResultPage>;
    async fn cancel(&self, id: &str) -> AppResult<bool>;
    async fn metadata(&self, request: &MetadataRequest) -> AppResult<MetadataPage>;
}
