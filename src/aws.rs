//! Redshift Data API adapter for a single immutable warehouse target.
//!
//! SDK configuration is loaded on first use so MCP startup needs no credentials.
//! User queries run inside a server-owned read-only batch. Database grants remain
//! the access boundary; the server separately validates the SQL syntax it accepts.
//! Raw AWS errors, SQL text and parameter values never cross this adapter boundary.

use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_redshiftdata::{
    config::Region,
    error::{ProvideErrorMetadata, SdkError},
    operation::{
        describe_statement::DescribeStatementOutput, get_statement_result::GetStatementResultOutput,
    },
    types::{ColumnMetadata, ExecutionMode, Field, ResultFormatString},
    Client,
};
use aws_smithy_types::timeout::TimeoutConfig;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use tokio::sync::OnceCell;

use crate::{
    backend::{
        Backend, Column, MetadataKind, MetadataPage, MetadataRequest, QueryState, ResultPage,
        SqlParameter, StatementStatus,
    },
    config::AwsConfig,
    error::{AppError, AppResult},
};

const QUERY_SUBSTATEMENT: usize = 3;
const BATCH_LENGTH: usize = 4;
const JS_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

pub struct AwsBackend {
    config: AwsConfig,
    client: OnceCell<Client>,
}

impl AwsBackend {
    pub fn new(config: AwsConfig) -> Self {
        Self {
            config,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> AppResult<&Client> {
        self.config.validate()?;
        self.client
            .get_or_try_init(|| async {
                let timeout = Duration::from_secs(self.config.api_timeout_secs);
                let mut loader = aws_config::defaults(BehaviorVersion::latest()).timeout_config(
                    TimeoutConfig::builder()
                        .operation_timeout(timeout)
                        .operation_attempt_timeout(timeout)
                        .build(),
                );
                if let Some(profile) = &self.config.profile {
                    loader = loader.profile_name(profile);
                }
                if let Some(region) = &self.config.region {
                    loader = loader.region(Region::new(region.clone()));
                }
                let sdk_config = tokio::time::timeout(timeout, loader.load())
                    .await
                    .map_err(|_| {
                        AppError::new(
                            "aws_config_timeout",
                            "AWS configuration timed out. Check the configured region and credential profile.",
                        )
                        .retryable()
                    })?;
                Ok(Client::new(&sdk_config))
            })
            .await
    }
}

#[async_trait]
impl Backend for AwsBackend {
    async fn submit(
        &self,
        sql: &str,
        parameters: &[SqlParameter],
        client_token: &str,
        timeout_ms: u64,
    ) -> AppResult<String> {
        // This is defence in depth for integer interpolation. SQL policy is owned
        // by the caller, and no SQL or parameter value is interpolated here.
        if timeout_ms == 0 || timeout_ms > i32::MAX as u64 {
            return Err(AppError::new(
                "invalid_timeout",
                "The statement timeout must be a positive 32-bit millisecond value.",
            ));
        }
        let parameters = parameters
            .iter()
            .map(|parameter| {
                aws_sdk_redshiftdata::types::SqlParameter::builder()
                    .name(&parameter.name)
                    .value(&parameter.value)
                    .build()
                    .map_err(|_| AppError::new("invalid_parameter", "A SQL parameter is invalid."))
            })
            .collect::<AppResult<Vec<_>>>()?;
        let response = self
            .client()
            .await?
            .batch_execute_statement()
            .set_database(self.config.database.clone())
            .set_cluster_identifier(self.config.cluster_id.clone())
            .set_workgroup_name(self.config.workgroup_name.clone())
            .set_db_user(self.config.db_user.clone())
            .set_secret_arn(self.config.secret_arn.clone())
            .execution_mode(ExecutionMode::AutoCommit)
            .result_format(ResultFormatString::Json)
            .client_token(client_token)
            .session_keep_alive_seconds(0)
            .sqls(format!("SET statement_timeout = {timeout_ms}"))
            .sqls("BEGIN READ ONLY")
            .sqls(sql)
            .sqls("ROLLBACK")
            .set_parameters((!parameters.is_empty()).then_some(parameters))
            .send()
            .await
            .map_err(provider_error)?;
        response
            .id()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(unexpected_response)
    }

    async fn status(&self, id: &str) -> AppResult<StatementStatus> {
        let response = self
            .client()
            .await?
            .describe_statement()
            .id(id)
            .send()
            .await
            .map_err(provider_error)?;
        if response.id() != id {
            return Err(unexpected_response());
        }
        statement_status(&response)
    }

    async fn results(&self, result_id: &str, next_token: Option<&str>) -> AppResult<ResultPage> {
        let response = self
            .client()
            .await?
            .get_statement_result()
            .id(result_id)
            .set_next_token(next_token.map(str::to_owned))
            .send()
            .await
            .map_err(provider_error)?;
        result_page(response)
    }

    async fn cancel(&self, id: &str) -> AppResult<bool> {
        let response = self
            .client()
            .await?
            .cancel_statement()
            .id(id)
            .send()
            .await
            .map_err(provider_error)?;
        response.status().ok_or_else(unexpected_response)
    }

    async fn metadata(&self, request: &MetadataRequest) -> AppResult<MetadataPage> {
        if !(1..=1000).contains(&request.max_results) {
            return Err(AppError::new(
                "invalid_page_size",
                "Metadata page size must be between 1 and 1000.",
            ));
        }
        let client = self.client().await?;
        match request.kind {
            MetadataKind::Databases => {
                let response = client
                    .list_databases()
                    .set_database(self.config.database.clone())
                    .set_cluster_identifier(self.config.cluster_id.clone())
                    .set_workgroup_name(self.config.workgroup_name.clone())
                    .set_db_user(self.config.db_user.clone())
                    .set_secret_arn(self.config.secret_arn.clone())
                    .max_results(request.max_results)
                    .set_next_token(request.next_token.clone())
                    .send()
                    .await
                    .map_err(provider_error)?;
                Ok(MetadataPage {
                    items: response
                        .databases()
                        .iter()
                        .map(|name| json!({"database": name}))
                        .collect(),
                    next_token: nonempty_token(response.next_token()),
                })
            }
            MetadataKind::Schemas => {
                let response = client
                    .list_schemas()
                    .set_database(self.config.database.clone())
                    .set_cluster_identifier(self.config.cluster_id.clone())
                    .set_workgroup_name(self.config.workgroup_name.clone())
                    .set_db_user(self.config.db_user.clone())
                    .set_secret_arn(self.config.secret_arn.clone())
                    .set_schema_pattern(request.pattern.clone())
                    .max_results(request.max_results)
                    .set_next_token(request.next_token.clone())
                    .send()
                    .await
                    .map_err(provider_error)?;
                Ok(MetadataPage {
                    items: response
                        .schemas()
                        .iter()
                        .map(|name| json!({"schema": name}))
                        .collect(),
                    next_token: nonempty_token(response.next_token()),
                })
            }
            MetadataKind::Tables => {
                let response = client
                    .list_tables()
                    .set_database(self.config.database.clone())
                    .set_cluster_identifier(self.config.cluster_id.clone())
                    .set_workgroup_name(self.config.workgroup_name.clone())
                    .set_db_user(self.config.db_user.clone())
                    .set_secret_arn(self.config.secret_arn.clone())
                    .set_schema_pattern(request.schema.clone())
                    .set_table_pattern(request.pattern.clone())
                    .max_results(request.max_results)
                    .set_next_token(request.next_token.clone())
                    .send()
                    .await
                    .map_err(provider_error)?;
                Ok(MetadataPage {
                    items: response
                        .tables()
                        .iter()
                        .map(|table| {
                            json!({
                                "schema": table.schema(),
                                "name": table.name(),
                                "type": table.r#type(),
                            })
                        })
                        .collect(),
                    next_token: nonempty_token(response.next_token()),
                })
            }
            MetadataKind::Columns => {
                if request.schema.as_deref().is_none_or(str::is_empty)
                    || request.table.as_deref().is_none_or(str::is_empty)
                {
                    return Err(AppError::new(
                        "invalid_object",
                        "Object details require both an exact schema and table name.",
                    ));
                }
                let response = client
                    .describe_table()
                    .set_database(self.config.database.clone())
                    .set_cluster_identifier(self.config.cluster_id.clone())
                    .set_workgroup_name(self.config.workgroup_name.clone())
                    .set_db_user(self.config.db_user.clone())
                    .set_secret_arn(self.config.secret_arn.clone())
                    .set_schema(request.schema.clone())
                    .set_table(request.table.clone())
                    .max_results(request.max_results)
                    .set_next_token(request.next_token.clone())
                    .send()
                    .await
                    .map_err(provider_error)?;
                Ok(MetadataPage {
                    items: response
                        .column_list()
                        .iter()
                        .enumerate()
                        .map(|(index, column)| {
                            serde_json::to_value(column_metadata(column, index))
                                .map_err(|_| unexpected_response())
                        })
                        .collect::<AppResult<Vec<_>>>()?,
                    next_token: nonempty_token(response.next_token()),
                })
            }
        }
    }
}

fn nonempty_token(value: Option<&str>) -> Option<String> {
    value.filter(|token| !token.is_empty()).map(str::to_owned)
}

fn query_state(value: Option<&str>) -> AppResult<QueryState> {
    match value {
        Some("SUBMITTED") => Ok(QueryState::Submitted),
        Some("PICKED") => Ok(QueryState::Picked),
        Some("STARTED") => Ok(QueryState::Started),
        Some("FINISHED") => Ok(QueryState::Finished),
        Some("FAILED") => Ok(QueryState::Failed),
        Some("ABORTED") => Ok(QueryState::Aborted),
        _ => Err(AppError::new(
            "unknown_query_state",
            "AWS returned an unrecognised query state. Retry status; do not resubmit the query.",
        )
        .retryable()),
    }
}

fn statement_status(response: &DescribeStatementOutput) -> AppResult<StatementStatus> {
    let parent_state = query_state(response.status().map(|status| status.as_str()))?;
    let mut state = parent_state;
    let children = response.sub_statements();
    if children.len() > BATCH_LENGTH {
        return Err(unexpected_response());
    }
    let mut failed = response.error().is_some();
    let mut aborted = false;
    let mut all_finished = children.len() == BATCH_LENGTH;
    let mut seen = std::collections::HashSet::new();
    for child in children {
        if !seen.insert(child.id()) {
            return Err(unexpected_response());
        }
        let child_state = match child.status() {
            None if !parent_state.terminal() => {
                all_finished = false;
                continue;
            }
            status => query_state(status.map(|status| status.as_str()))?,
        };
        failed |= child_state == QueryState::Failed || child.error().is_some();
        aborted |= child_state == QueryState::Aborted;
        all_finished &= child_state == QueryState::Finished;
    }
    // AUTO_COMMIT may complete the final ROLLBACK despite an earlier failure.
    // Until the parent is terminal, preserve its live state so the caller keeps
    // the handle and can cancel the still-active batch.
    if parent_state == QueryState::Finished {
        if failed {
            state = QueryState::Failed;
        } else if aborted {
            state = QueryState::Aborted;
        } else if !all_finished {
            return Err(unexpected_response());
        }
    }
    let query_id = format!("{}:{QUERY_SUBSTATEMENT}", response.id());
    let query = children.iter().find(|child| child.id() == query_id);
    if state == QueryState::Finished {
        for index in 1..=BATCH_LENGTH {
            if !seen.contains(format!("{}:{index}", response.id()).as_str()) {
                return Err(unexpected_response());
            }
        }
        if query.is_none() {
            return Err(unexpected_response());
        }
    }
    Ok(StatementStatus {
        state,
        result_id: query
            .filter(|query| state == QueryState::Finished && query.has_result_set() == Some(true))
            .map(|query| query.id().to_owned()),
        duration_ns: query.and_then(|query| nonnegative(query.duration())),
        result_rows: query.and_then(|query| nonnegative(query.result_rows())),
        result_bytes: query.and_then(|query| nonnegative(query.result_size())),
        redshift_query_id: query
            .map(|query| query.redshift_query_id())
            .filter(|value| *value > 0),
        error: (state == QueryState::Failed).then(|| {
            AppError::new(
                "query_failed",
                "Redshift rejected the query or its read-only wrapper. Check SQL syntax, object permissions, and read-only compatibility.",
            )
        }),
    })
}

fn nonnegative(value: i64) -> Option<i64> {
    (value >= 0).then_some(value)
}

fn column_metadata(column: &ColumnMetadata, index: usize) -> Column {
    Column {
        name: column
            .label()
            .filter(|label| !label.is_empty())
            .or_else(|| column.name())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("column_{}", index + 1)),
        type_name: column.type_name().unwrap_or("unknown").to_owned(),
        nullable: match column.nullable() {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        precision: (column.precision() > 0).then_some(column.precision()),
        scale: (column.precision() > 0 && column.scale() >= 0).then_some(column.scale()),
    }
}

fn result_page(response: GetStatementResultOutput) -> AppResult<ResultPage> {
    let columns: Vec<_> = response
        .column_metadata()
        .iter()
        .enumerate()
        .map(|(index, column)| column_metadata(column, index))
        .collect();
    let rows = response
        .records()
        .iter()
        .map(|row| {
            if row.len() != columns.len() {
                return Err(unexpected_response());
            }
            row.iter().map(field_value).collect::<AppResult<Vec<_>>>()
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(ResultPage {
        columns,
        rows,
        next_token: nonempty_token(response.next_token()),
        total_rows: nonnegative(response.total_num_rows()),
    })
}

fn field_value(field: &Field) -> AppResult<Value> {
    match field {
        Field::IsNull(true) => Ok(Value::Null),
        Field::BooleanValue(value) => Ok(Value::Bool(*value)),
        Field::LongValue(value) if (-JS_SAFE_INTEGER..=JS_SAFE_INTEGER).contains(value) => {
            Ok(json!(*value))
        }
        Field::LongValue(value) => Ok(Value::String(value.to_string())),
        Field::DoubleValue(value) if value.is_finite() => Ok(json!(*value)),
        Field::DoubleValue(value) => Ok(Value::String(if value.is_nan() {
            "NaN".to_owned()
        } else if value.is_sign_positive() {
            "Infinity".to_owned()
        } else {
            "-Infinity".to_owned()
        })),
        Field::StringValue(value) => Ok(Value::String(value.clone())),
        Field::BlobValue(value) => Ok(json!({"base64": STANDARD.encode(value.as_ref())})),
        _ => Err(AppError::new(
            "unsupported_result_type",
            "AWS returned an unsupported result value. Cast the column to text or update the SDK.",
        )),
    }
}

fn unexpected_response() -> AppError {
    AppError::new(
        "unexpected_aws_response",
        "AWS returned an incomplete or inconsistent response. Retry status or result retrieval; do not resubmit the query.",
    )
    .retryable()
}

fn provider_error<E: ProvideErrorMetadata>(error: SdkError<E>) -> AppError {
    // Only a fixed code allowlist is inspected. Display/Debug can expose SQL,
    // parameter values, resource names, signed URLs, or credential diagnostics.
    if let Some(code) = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
    {
        return match code {
            "AccessDenied" | "AccessDeniedException" | "UnauthorizedException" => AppError::new(
                "aws_access_denied",
                "AWS denied this operation. Check the configured IAM role, Data API permissions, and database grants.",
            ),
            "ExpiredToken" | "ExpiredTokenException" | "UnrecognizedClientException"
            | "InvalidClientTokenId" | "InvalidSignatureException" => AppError::new(
                "aws_credentials",
                "AWS credentials are missing, invalid, or expired. Refresh the configured AWS profile or SSO session.",
            ),
            "ThrottlingException" | "TooManyRequestsException" | "ActiveStatementsExceededException"
            | "ActiveSessionsExceededException" | "ActiveWaitingRequestsExceededException" => {
                AppError::new("aws_throttled", "AWS is throttling requests. Retry after a short delay.")
                    .retryable()
            }
            "ResourceNotFoundException" => AppError::new(
                "aws_resource_not_found",
                "AWS could not find the configured resource or statement. Results may have expired.",
            ),
            "ValidationException" => AppError::new(
                "aws_request_rejected",
                "AWS rejected the request. Check the target configuration, parameters, and supported Data API options.",
            ),
            "DatabaseConnectionException" => AppError::new(
                "aws_database_connection",
                "AWS could not connect to the configured database. Check warehouse availability and database access.",
            )
            .retryable(),
            "InternalServerException" | "ServiceUnavailableException" => AppError::new(
                "aws_unavailable",
                "AWS is temporarily unavailable. Retry the same operation.",
            )
            .retryable(),
            _ => AppError::new("aws_error", "The AWS Data API operation failed."),
        };
    }
    match error {
        SdkError::TimeoutError(_) => AppError::new(
            "aws_timeout",
            "The AWS request timed out. A submitted query may still be running; use its handle to check status.",
        )
        .retryable(),
        SdkError::DispatchFailure(_) => AppError::new(
            "aws_transport",
            "Could not reach AWS or obtain credentials. Check connectivity and refresh the configured AWS profile.",
        )
        .retryable(),
        SdkError::ConstructionFailure(_) => AppError::new(
            "aws_configuration",
            "The AWS request could not be prepared. Check the configured region and credential profile.",
        ),
        _ => AppError::new("aws_response", "The AWS response could not be read.").retryable(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_redshiftdata::{
        config::Credentials,
        types::{StatementStatusString, StatusString, SubStatementData},
    };
    use aws_smithy_http_client::test_util::{capture_request, CaptureRequestReceiver};
    use aws_smithy_types::{body::SdkBody, retry::RetryConfig};

    const ID: &str = "b8878892-cb3d-4638-8dd1-970d74130d47";

    fn config() -> AwsConfig {
        AwsConfig {
            profile: None,
            region: Some("ap-southeast-2".to_owned()),
            database: Some("analytics".to_owned()),
            cluster_id: None,
            workgroup_name: Some("warehouse".to_owned()),
            db_user: None,
            secret_arn: None,
            api_timeout_secs: 30,
        }
    }

    fn fixture(status: u16, body: Value) -> (AwsBackend, CaptureRequestReceiver) {
        let (http_client, requests) = capture_request(Some(
            http::Response::builder()
                .status(status)
                .header("content-type", "application/x-amz-json-1.1")
                .body(SdkBody::from(body.to_string()))
                .expect("valid fixture response"),
        ));
        let sdk_config = aws_sdk_redshiftdata::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("ap-southeast-2"))
            .credentials_provider(Credentials::new(
                "fixture-key",
                "fixture-secret",
                None,
                None,
                "fixture",
            ))
            .retry_config(RetryConfig::disabled())
            .http_client(http_client)
            .build();
        let backend = AwsBackend {
            config: config(),
            client: OnceCell::new_with(Some(Client::from_conf(sdk_config))),
        };
        (backend, requests)
    }

    fn request_body(requests: CaptureRequestReceiver) -> Value {
        let request = requests.expect_request();
        serde_json::from_slice(request.body().bytes().expect("buffered body"))
            .expect("valid request JSON")
    }

    #[tokio::test]
    async fn submit_sends_read_only_batch_parameters_and_stable_token() {
        let (backend, requests) = fixture(200, json!({"Id": ID}));
        let submitted = backend
            .submit(
                "SELECT :value AS example",
                &[SqlParameter {
                    name: "value".to_owned(),
                    value: "data-value".to_owned(),
                }],
                "token-for-one-logical-submission",
                12_000,
            )
            .await
            .expect("submitted");
        assert_eq!(submitted, ID);
        let body = request_body(requests);
        assert_eq!(body["WorkgroupName"], "warehouse");
        assert_eq!(body["Database"], "analytics");
        assert_eq!(body["ExecutionMode"], "AUTO_COMMIT");
        assert_eq!(body["ClientToken"], "token-for-one-logical-submission");
        assert_eq!(body["SessionKeepAliveSeconds"], 0);
        assert_eq!(body["ResultFormat"], "JSON");
        assert_eq!(
            body["Sqls"],
            json!([
                "SET statement_timeout = 12000",
                "BEGIN READ ONLY",
                "SELECT :value AS example",
                "ROLLBACK"
            ])
        );
        assert_eq!(
            body["Parameters"],
            json!([{"name":"value", "value":"data-value"}])
        );
        assert!(body.get("ClusterIdentifier").is_none());
        assert!(body.get("SecretArn").is_none());
        assert!(body.get("SessionId").is_none());
    }

    #[tokio::test]
    async fn provisioned_target_uses_configured_database_user() {
        let (mut backend, requests) = fixture(200, json!({"Id": ID}));
        backend.config.cluster_id = Some("provisioned-warehouse".to_owned());
        backend.config.workgroup_name = None;
        backend.config.db_user = Some("readonly".to_owned());
        backend
            .submit("SELECT 1", &[], "token", 1000)
            .await
            .expect("submitted");
        let body = request_body(requests);
        assert_eq!(body["ClusterIdentifier"], "provisioned-warehouse");
        assert_eq!(body["DbUser"], "readonly");
        assert!(body.get("WorkgroupName").is_none());
        assert!(body.get("Parameters").is_none());
    }

    #[tokio::test]
    async fn parameters_never_reappear_in_provider_errors() {
        let (backend, _requests) = fixture(
            400,
            json!({
                "__type":"AccessDeniedException",
                "message":"SELECT secret FROM private; password=do-not-leak"
            }),
        );
        let error = backend
            .submit("SELECT :secret", &[], "token", 1000)
            .await
            .expect_err("provider rejection");
        assert_eq!(error.code, "aws_access_denied");
        let serialised = serde_json::to_string(&error).expect("serialised");
        assert!(!serialised.contains("do-not-leak"));
        assert!(!serialised.contains("SELECT"));
        assert!(!error.retryable);
    }

    #[tokio::test]
    async fn metadata_passes_patterns_and_paging_without_sql_concatenation() {
        let (backend, requests) = fixture(
            200,
            json!({
                "Tables":[{"name":"campaigns", "schema":"marketing", "type":"TABLE"}],
                "NextToken":"provider-next"
            }),
        );
        let page = backend
            .metadata(&MetadataRequest {
                kind: MetadataKind::Tables,
                schema: Some("marketing".to_owned()),
                table: None,
                pattern: Some("campaign%".to_owned()),
                next_token: Some("provider-current".to_owned()),
                max_results: 25,
            })
            .await
            .expect("metadata");
        assert_eq!(page.items[0]["name"], "campaigns");
        assert_eq!(page.next_token.as_deref(), Some("provider-next"));
        let body = request_body(requests);
        assert_eq!(body["SchemaPattern"], "marketing");
        assert_eq!(body["TablePattern"], "campaign%");
        assert_eq!(body["MaxResults"], 25);
        assert_eq!(body["NextToken"], "provider-current");
    }

    #[tokio::test]
    async fn cancellation_reports_provider_outcome_without_claiming_terminal_state() {
        let (backend, requests) = fixture(200, json!({"Status": false}));
        assert!(!backend.cancel(ID).await.expect("cancel outcome"));
        assert_eq!(request_body(requests), json!({"Id": ID}));
    }

    #[tokio::test]
    async fn results_preserve_provider_pagination_and_column_metadata() {
        let result_id = format!("{ID}:3");
        let (backend, requests) = fixture(
            200,
            json!({
                "Records":[[{"stringValue":"123456789.001"}]],
                "ColumnMetadata":[{"label":"amount","typeName":"numeric","precision":18,"scale":3,"nullable":1}],
                "TotalNumRows":2,
                "NextToken":"next-page"
            }),
        );
        let page = backend
            .results(&result_id, Some("current-page"))
            .await
            .expect("results");
        assert_eq!(page.rows, vec![vec![json!("123456789.001")]]);
        assert_eq!(page.columns[0].precision, Some(18));
        assert_eq!(page.columns[0].scale, Some(3));
        assert_eq!(page.total_rows, Some(2));
        assert_eq!(page.next_token.as_deref(), Some("next-page"));
        assert_eq!(
            request_body(requests),
            json!({"Id":result_id,"NextToken":"current-page"})
        );
    }

    fn completed_batch() -> DescribeStatementOutput {
        let mut output = DescribeStatementOutput::builder()
            .id(ID)
            .status(StatusString::Finished);
        for index in 1..=BATCH_LENGTH {
            output = output.sub_statements(
                SubStatementData::builder()
                    .id(format!("{ID}:{index}"))
                    .status(StatementStatusString::Finished)
                    .has_result_set(index == QUERY_SUBSTATEMENT)
                    .duration(123)
                    .result_rows(2)
                    .result_size(42)
                    .redshift_query_id(100)
                    .build()
                    .expect("substatement"),
            );
        }
        output.build().expect("batch")
    }

    #[test]
    fn successful_status_uses_the_user_query_instead_of_the_rollback() {
        let output = completed_batch();
        let status = statement_status(&output).expect("status");
        assert_eq!(status.state, QueryState::Finished);
        assert_eq!(status.result_id, Some(format!("{ID}:3")));
        assert_eq!(status.duration_ns, Some(123));
        assert_eq!(status.result_rows, Some(2));
        assert_eq!(status.redshift_query_id, Some(100));
        assert!(status.error.is_none());
    }

    #[test]
    fn successful_rollback_cannot_hide_a_failed_statement() {
        let mut output = completed_batch();
        let children = output.sub_statements.as_mut().expect("children");
        children[1].status = Some(StatementStatusString::Failed);
        children[1].error = Some("Sensitive provider explanation".to_owned());
        let status = statement_status(&output).expect("status");
        assert_eq!(status.state, QueryState::Failed);
        assert!(status.result_id.is_none());
        assert_eq!(status.error.expect("failure").code, "query_failed");
        output.status = Some(StatusString::Started);
        let status = statement_status(&output).expect("still live");
        assert_eq!(status.state, QueryState::Started);
        assert!(!status.state.terminal());
    }

    #[test]
    fn completed_batch_requires_all_expected_statement_ids() {
        let mut output = completed_batch();
        output.sub_statements.as_mut().expect("children")[0].id = format!("{ID}:9");
        assert!(statement_status(&output).is_err());
    }

    #[test]
    fn unknown_query_status_never_becomes_success() {
        let output = DescribeStatementOutput::builder()
            .id(ID)
            .status(StatusString::from("FUTURE_STATE"))
            .build()
            .expect("output");
        assert_eq!(
            statement_status(&output).expect_err("unknown").code,
            "unknown_query_state"
        );
    }

    #[test]
    fn result_cells_preserve_values_and_reject_unknown_unions() {
        assert_eq!(
            field_value(&Field::LongValue(i64::MAX)).expect("integer"),
            json!(i64::MAX.to_string())
        );
        assert_eq!(
            field_value(&Field::LongValue(42)).expect("integer"),
            json!(42)
        );
        assert_eq!(
            field_value(&Field::StringValue("0.00000000000001".to_owned())).expect("decimal"),
            json!("0.00000000000001")
        );
        assert_eq!(
            field_value(&Field::IsNull(true)).expect("null"),
            Value::Null
        );
        assert_eq!(
            field_value(&Field::DoubleValue(f64::NAN)).expect("nan"),
            json!("NaN")
        );
        assert_eq!(
            field_value(&Field::DoubleValue(f64::INFINITY)).expect("infinity"),
            json!("Infinity")
        );
        assert_eq!(
            field_value(&Field::BlobValue(aws_smithy_types::Blob::new([0_u8, 1]))).expect("blob"),
            json!({"base64":"AAE="})
        );
        assert!(field_value(&Field::IsNull(false)).is_err());
    }

    #[test]
    fn malformed_row_width_is_not_silently_truncated() {
        let output = GetStatementResultOutput::builder()
            .column_metadata(ColumnMetadata::builder().name("a").build())
            .records(vec![Field::LongValue(1), Field::LongValue(2)])
            .build()
            .expect("output");
        assert!(result_page(output).is_err());
    }
}
