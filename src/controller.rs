//! Own query lifecycles, capacity and continuation state independently of MCP calls.
//!
//! A disconnected tool call does not cancel a submitted worker. Handles and cursors
//! belong to this process and its immutable target. Unknown AWS outcomes retain
//! capacity until a terminal state is observed or their documented lifetime ends.
//! Result pages contain complete rows within both row and serialized-byte budgets.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use mcp_toolkit_core::scoped_cursor::{CursorError, CursorLimits, CursorScope, ScopedCursorStore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    backend::{Backend, MetadataKind, MetadataRequest, QueryState, SqlParameter, StatementStatus},
    config::Config,
    error::{AppError, AppResult},
    sql,
};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const CANCELLATION_CONFIRMATION: Duration = Duration::from_secs(30);
const CURSOR_PLACEHOLDER: &str = "00000000-0000-4000-8000-000000000000";
const ENVELOPE_RESERVE_BYTES: usize = 256;

#[derive(Clone)]
pub struct StartQuery {
    pub sql: String,
    pub parameters: Vec<SqlParameter>,
    pub request_id: Option<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobView {
    pub query_id: String,
    pub state: String,
    pub query_fingerprint: String,
    pub submitted_at_unix_ms: u64,
    pub duration_ns: Option<i64>,
    pub total_rows: Option<i64>,
    pub result_bytes: Option<i64>,
    pub redshift_query_id: Option<i64>,
    pub cancel_requested: bool,
    pub cancellation_accepted: Option<bool>,
    pub error: Option<AppError>,
}

impl JobView {
    pub fn terminal(&self) -> bool {
        matches!(self.state.as_str(), "finished" | "failed" | "aborted")
    }
}

struct JobData {
    view: JobView,
    provider_id: Option<String>,
    result_id: Option<String>,
}

struct Job {
    data: Mutex<JobData>,
    created_at: Instant,
    worker_done: AtomicBool,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    cancel_lock: tokio::sync::Mutex<()>,
}

impl Job {
    fn snapshot(&self) -> AppResult<JobView> {
        Ok(lock(&self.data)?.view.clone())
    }

    fn release_terminal_capacity(&self) -> AppResult<()> {
        if self.snapshot()?.terminal() {
            lock(&self.permit)?.take();
        }
        Ok(())
    }
}

#[derive(Default)]
struct Registry {
    jobs: HashMap<String, Arc<Job>>,
    requests: HashMap<String, (String, String)>,
}

impl Registry {
    fn prune(&mut self, ttl: Duration, now: Instant) {
        self.jobs.retain(|_, job| {
            !job.worker_done.load(Ordering::Acquire)
                || now.saturating_duration_since(job.created_at) < ttl
        });
        self.requests
            .retain(|_, (_, query_id)| self.jobs.contains_key(query_id));
    }
}

struct Inner {
    config: Config,
    backend: Arc<dyn Backend>,
    registry: Mutex<Registry>,
    query_capacity: Arc<Semaphore>,
    read_capacity: Arc<Semaphore>,
    cursors: ScopedCursorStore,
    generation: String,
}

#[derive(Clone)]
pub struct Controller {
    inner: Arc<Inner>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PagePosition {
    provider_token: Option<String>,
    offset: usize,
    // The same provider page must be replayed before applying a local offset.
    page_fingerprint: Option<String>,
}

impl Controller {
    pub fn new(config: Config, backend: Arc<dyn Backend>) -> AppResult<Self> {
        if !(1..=1000).contains(&config.max_page_rows)
            || !(1024..=24_000).contains(&config.max_page_bytes)
            || !(1..=32).contains(&config.max_concurrent_queries)
            || !(1..=1024).contains(&config.max_jobs)
            || !(600..=86_400).contains(&config.job_ttl_secs)
            || !(1..=60).contains(&config.aws.api_timeout_secs)
            || !(1..=300_000).contains(&config.statement_timeout_ms)
        {
            return Err(AppError::new(
                "invalid_configuration",
                "Controller resource limits are outside their documented bounds.",
            ));
        }
        let cursors = ScopedCursorStore::new(CursorLimits {
            ttl: Duration::from_secs(config.job_ttl_secs),
            max_entries: config.max_jobs * 16,
            max_payload_bytes: 16_384,
        })
        .map_err(cursor_error)?;
        Ok(Self {
            inner: Arc::new(Inner {
                query_capacity: Arc::new(Semaphore::new(config.max_concurrent_queries)),
                read_capacity: Arc::new(Semaphore::new(8)),
                config,
                backend,
                registry: Mutex::new(Registry::default()),
                cursors,
                generation: Uuid::new_v4().to_string(),
            }),
        })
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub async fn connection_status(&self, verify: bool) -> AppResult<Value> {
        let mut posture = self.config().aws.posture();
        posture["verified"] = Value::Bool(false);
        if verify {
            self.config().aws.validate()?;
            let _permit = self.inner.read_permit()?;
            self.inner
                .bounded(self.inner.backend.metadata(&MetadataRequest {
                    kind: MetadataKind::Schemas,
                    schema: None,
                    table: None,
                    pattern: None,
                    next_token: None,
                    max_results: 1,
                }))
                .await?;
            posture["verified"] = Value::Bool(true);
        }
        Ok(posture)
    }

    pub async fn start(&self, mut request: StartQuery) -> AppResult<JobView> {
        self.config().aws.validate()?;
        let validated = sql::validate(&request.sql, &request.parameters)?;
        let timeout_ms = request
            .timeout_ms
            .unwrap_or(self.config().statement_timeout_ms);
        if timeout_ms == 0 || timeout_ms > self.config().statement_timeout_ms {
            return Err(AppError::new(
                "invalid_timeout",
                "Query timeout must be positive and within the configured statement timeout.",
            ));
        }
        if request.request_id.as_ref().is_some_and(|key| {
            key.trim().is_empty() || key.len() > 128 || key.chars().any(char::is_control)
        }) {
            return Err(AppError::new("invalid_request_id", "Request IDs must be nonempty text of at most 128 bytes without control characters."));
        }
        // Named bindings are a map semantically. Validation has already rejected
        // duplicate names. Canonicalize their order for retries and submission.
        request.parameters.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let digest = fingerprint(&json!({
            "sql": validated.sql, "parameters": request.parameters, "timeout_ms": timeout_ms,
        }))?;
        let job = {
            let mut registry = lock(&self.inner.registry)?;
            registry.prune(
                Duration::from_secs(self.config().job_ttl_secs),
                Instant::now(),
            );
            if let Some(key) = &request.request_id {
                if let Some((existing_digest, query_id)) = registry.requests.get(key) {
                    if existing_digest != &digest {
                        return Err(AppError::new("request_id_conflict", "This request ID already identifies different SQL, bindings, or timeout settings."));
                    }
                    return registry
                        .jobs
                        .get(query_id)
                        .ok_or_else(unknown_query)?
                        .snapshot();
                }
            }
            if registry.jobs.len() >= self.config().max_jobs {
                return Err(AppError::new(
                    "job_capacity",
                    "The retained query limit has been reached. Wait for old handles to expire.",
                )
                .retryable());
            }
            let permit = self.inner.query_capacity.clone().try_acquire_owned().map_err(|_| {
                AppError::new("query_capacity", "The active query limit has been reached. Wait, cancel a query, or resolve an unknown outcome.").retryable()
            })?;
            let query_id = format!("query_{}", Uuid::new_v4());
            let submitted_at_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .ok_or_else(|| {
                    AppError::new("clock_unavailable", "The system clock is unavailable.")
                })?;
            let job = Arc::new(Job {
                data: Mutex::new(JobData {
                    view: JobView {
                        query_id: query_id.clone(),
                        state: "submitting".to_owned(),
                        query_fingerprint: validated.fingerprint,
                        submitted_at_unix_ms,
                        duration_ns: None,
                        total_rows: None,
                        result_bytes: None,
                        redshift_query_id: None,
                        cancel_requested: false,
                        cancellation_accepted: None,
                        error: None,
                    },
                    provider_id: None,
                    result_id: None,
                }),
                created_at: Instant::now(),
                worker_done: AtomicBool::new(false),
                permit: Mutex::new(Some(permit)),
                cancel_lock: tokio::sync::Mutex::new(()),
            });
            registry.jobs.insert(query_id.clone(), job.clone());
            if let Some(key) = request.request_id {
                registry.requests.insert(key, (digest, query_id));
            }
            job
        };
        let inner = self.inner.clone();
        let worker_job = job.clone();
        // Own the worker before returning so client cancellation cannot orphan submission.
        tokio::spawn(async move {
            let result = run_query(
                &inner,
                &worker_job,
                validated.sql,
                request.parameters,
                Uuid::new_v4().to_string(),
                timeout_ms,
            )
            .await;
            if let Err(error) = result {
                if let Ok(mut data) = lock(&worker_job.data) {
                    if !data.view.terminal() {
                        data.view.state = if data.provider_id.is_some() {
                            "status_unknown"
                        } else {
                            "submission_unknown"
                        }
                        .to_owned();
                        data.view.error = Some(error);
                    }
                }
            }
            worker_job.worker_done.store(true, Ordering::Release);
            let _ = worker_job.release_terminal_capacity();
        });
        job.snapshot()
    }
}

impl Controller {
    pub async fn status(&self, query_id: &str) -> AppResult<JobView> {
        let job = self.inner.job(query_id)?;
        let (provider_id, terminal) = {
            let data = lock(&job.data)?;
            (data.provider_id.clone(), data.view.terminal())
        };
        if let Some(provider_id) = provider_id.filter(|_| !terminal) {
            let _permit = self.inner.read_permit()?;
            let status = self
                .inner
                .bounded(self.inner.backend.status(&provider_id))
                .await?;
            apply_status(&job, status)?;
        }
        job.snapshot()
    }

    pub async fn wait(&self, query_id: &str, wait_ms: u64) -> AppResult<JobView> {
        if wait_ms > 10_000 {
            return Err(AppError::new(
                "invalid_wait",
                "Wait duration must be at most 10000 milliseconds.",
            ));
        }
        let job = self.inner.job(query_id)?;
        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let view = job.snapshot()?;
            if view.terminal()
                || job.worker_done.load(Ordering::Acquire)
                || Instant::now() >= deadline
            {
                return Ok(view);
            }
            tokio::time::sleep(
                Duration::from_millis(50).min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }

    pub async fn cancel(&self, query_id: &str) -> AppResult<JobView> {
        let job = self.inner.job(query_id)?;
        let provider_id = {
            let mut data = lock(&job.data)?;
            if data.view.terminal() {
                return Ok(data.view.clone());
            }
            data.view.cancel_requested = true;
            data.provider_id.clone()
        };
        if let Some(provider_id) = provider_id {
            let _permit = self.inner.read_permit()?;
            self.inner
                .bounded(self.inner.cancel_provider(&job, &provider_id))
                .await?;
        }
        job.snapshot()
    }

    pub async fn results(
        &self,
        query_id: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> AppResult<Value> {
        let limit = self.limit(limit)?;
        let job = self.inner.job(query_id)?;
        let (result_id, total_rows) = {
            let data = lock(&job.data)?;
            if data.view.state != "finished" {
                return Err(AppError::new("query_not_finished", "Query results are available only after successful completion. Check its status."));
            }
            (
                data.result_id.clone().ok_or_else(|| {
                    AppError::new("no_result_set", "This query did not produce a result set.")
                })?,
                data.view.total_rows,
            )
        };
        let scope = self.inner.scope(&format!("query:{query_id}"))?;
        let position = self.position(&scope, cursor)?;
        let _permit = self.inner.read_permit()?;
        let page = self
            .inner
            .bounded(
                self.inner
                    .backend
                    .results(&result_id, position.provider_token.as_deref()),
            )
            .await?;
        let total_rows = page.total_rows.or(total_rows);
        let items = page.rows.into_iter().map(Value::Array).collect();
        self.paginate(
            scope,
            position,
            items,
            page.next_token,
            limit,
            |rows, next_cursor, complete| {
                json!({
                    "query_id": query_id, "columns": page.columns, "rows": rows,
                    "returned_rows": rows.len(), "total_rows": total_rows,
                    "next_cursor": next_cursor, "complete": complete, "format": "tuples",
                })
            },
        )
    }

    pub async fn metadata(
        &self,
        mut request: MetadataRequest,
        cursor: Option<&str>,
        limit: Option<usize>,
    ) -> AppResult<Value> {
        self.config().aws.validate()?;
        let limit = self.limit(limit)?;
        if request.next_token.is_some()
            || [&request.schema, &request.table, &request.pattern]
                .into_iter()
                .flatten()
                .any(|value| value.len() > 1024 || value.chars().any(char::is_control))
        {
            return Err(AppError::new("invalid_metadata_request", "Use bounded schema/table filters and the MCP cursor; provider tokens are not accepted."));
        }
        let key = fingerprint(&json!({"kind": request.kind, "schema": request.schema,
            "table": request.table, "pattern": request.pattern}))?;
        let scope = self.inner.scope(&format!("metadata:{key}"))?;
        let position = self.position(&scope, cursor)?;
        request.next_token = position.provider_token.clone();
        // A fixed provider page size preserves offsets if MCP page size changes.
        request.max_results = self.config().max_page_rows as i32;
        let _permit = self.inner.read_permit()?;
        let page = self
            .inner
            .bounded(self.inner.backend.metadata(&request))
            .await?;
        self.paginate(
            scope,
            position,
            page.items,
            page.next_token,
            limit,
            |items, next_cursor, complete| {
                json!({"items":items,
                "returned_items":items.len(),"next_cursor":next_cursor,"complete":complete})
            },
        )
    }

    fn limit(&self, limit: Option<usize>) -> AppResult<usize> {
        let limit = limit.unwrap_or(self.config().max_page_rows);
        if limit == 0 || limit > self.config().max_page_rows {
            return Err(AppError::new(
                "invalid_page_size",
                "Page size must be positive and within the configured row limit.",
            ));
        }
        Ok(limit)
    }

    fn position(&self, scope: &CursorScope, cursor: Option<&str>) -> AppResult<PagePosition> {
        match cursor {
            None => Ok(PagePosition::default()),
            Some(cursor) => serde_json::from_value(
                self.inner
                    .cursors
                    .resolve(cursor, scope)
                    .map_err(cursor_error)?,
            )
            .map_err(|_| cursor_error(CursorError::InvalidCursor)),
        }
    }

    fn paginate(
        &self,
        scope: CursorScope,
        position: PagePosition,
        items: Vec<Value>,
        next_token: Option<String>,
        limit: usize,
        make_payload: impl Fn(&[Value], Option<&str>, bool) -> Value,
    ) -> AppResult<Value> {
        if position.offset > items.len()
            || next_token
                .as_ref()
                .is_some_and(|token| Some(token.as_str()) == position.provider_token.as_deref())
        {
            return Err(AppError::new(
                "invalid_provider_page",
                "The provider page changed or repeated its continuation token. Restart pagination.",
            ));
        }
        // Bind the continuation to the full provider page, including columns
        // and other page metadata, rather than trusting an offset into a fresh
        // provider response. No original rows are retained in the cursor store.
        let page_fingerprint =
            fingerprint(&(&items, &next_token, make_payload(&[], None, true)))?;
        if position.offset > 0
            && position.page_fingerprint.as_deref() != Some(page_fingerprint.as_str())
        {
            return Err(AppError::new(
                "invalid_provider_page",
                "The provider page changed during continuation. Restart pagination.",
            ));
        }
        let mut end = position.offset;
        let last = position.offset.saturating_add(limit).min(items.len());
        while end < last {
            let candidate_end = end + 1;
            let more = candidate_end < items.len() || next_token.is_some();
            let candidate = make_payload(
                &items[position.offset..candidate_end],
                more.then_some(CURSOR_PLACEHOLDER),
                !more,
            );
            if self.payload_bytes(&candidate)? > self.config().max_page_bytes {
                if end == position.offset {
                    return Err(AppError::new("result_row_too_large", "One complete row plus its columns exceeds the response budget. Select fewer columns, shorten large values in SQL, or use aggregates."));
                }
                break;
            }
            end = candidate_end;
        }
        let next_position = if end < items.len() {
            Some(PagePosition {
                provider_token: position.provider_token,
                offset: end,
                page_fingerprint: Some(page_fingerprint),
            })
        } else {
            next_token.map(|token| PagePosition {
                provider_token: Some(token),
                offset: 0,
                page_fingerprint: None,
            })
        };
        let complete = next_position.is_none();
        let preview = make_payload(
            &items[position.offset..end],
            (!complete).then_some(CURSOR_PLACEHOLDER),
            complete,
        );
        if self.payload_bytes(&preview)? > self.config().max_page_bytes {
            return Err(AppError::new(
                "result_metadata_too_large",
                "Column metadata exceeds the response budget. Select fewer columns.",
            ));
        }
        let next_cursor = next_position
            .map(|position| {
                let token = Uuid::new_v4().to_string();
                let payload = serde_json::to_value(position).map_err(|_| internal_error())?;
                self.inner
                    .cursors
                    .insert(&token, scope, payload)
                    .map_err(cursor_error)?;
                Ok::<_, AppError>(token)
            })
            .transpose()?;
        Ok(make_payload(
            &items[position.offset..end],
            next_cursor.as_deref(),
            complete,
        ))
    }

    fn payload_bytes(&self, payload: &Value) -> AppResult<usize> {
        serde_json::to_vec(&json!({"ok":true,"data":payload}))
            .map(|bytes| bytes.len().saturating_add(ENVELOPE_RESERVE_BYTES))
            .map_err(|_| internal_error())
    }
}

impl Inner {
    fn job(&self, query_id: &str) -> AppResult<Arc<Job>> {
        if query_id.len() > 64 {
            return Err(unknown_query());
        }
        let mut registry = lock(&self.registry)?;
        registry.prune(
            Duration::from_secs(self.config.job_ttl_secs),
            Instant::now(),
        );
        registry
            .jobs
            .get(query_id)
            .cloned()
            .ok_or_else(unknown_query)
    }

    fn scope(&self, resource: &str) -> AppResult<CursorScope> {
        CursorScope::new("local-stdio-principal", resource, &self.generation).map_err(cursor_error)
    }

    fn read_permit(&self) -> AppResult<OwnedSemaphorePermit> {
        self.read_capacity.clone().try_acquire_owned().map_err(|_| {
            AppError::new(
                "backend_busy",
                "Too many concurrent metadata or result requests. Retry shortly.",
            )
            .retryable()
        })
    }

    async fn bounded<T>(&self, future: impl Future<Output = AppResult<T>>) -> AppResult<T> {
        tokio::time::timeout(
            Duration::from_secs(self.config.aws.api_timeout_secs),
            future,
        )
        .await
        .map_err(|_| {
            AppError::new(
                "aws_timeout",
                "The backend operation timed out. Its remote outcome may still be unknown.",
            )
            .retryable()
        })?
    }

    async fn cancel_provider(&self, job: &Job, provider_id: &str) -> AppResult<()> {
        let _guard = job.cancel_lock.lock().await;
        let view = job.snapshot()?;
        if view.terminal() || view.cancellation_accepted == Some(true) {
            return Ok(());
        }
        match self.bounded(self.backend.cancel(provider_id)).await {
            Ok(accepted) => lock(&job.data)?.view.cancellation_accepted = Some(accepted),
            Err(error) => {
                let mut data = lock(&job.data)?;
                if !data.view.terminal() {
                    data.view.error = Some(error.clone());
                }
                return Err(error);
            }
        }
        Ok(())
    }
}

async fn run_query(
    inner: &Inner,
    job: &Job,
    sql: String,
    parameters: Vec<SqlParameter>,
    client_token: String,
    timeout_ms: u64,
) -> AppResult<()> {
    let deadline = job.created_at + Duration::from_millis(timeout_ms);
    let mut provider_id = None;
    let mut submission_ambiguous = false;
    for attempt in 0..2 {
        if attempt == 0 && job.snapshot()?.cancel_requested {
            let mut data = lock(&job.data)?;
            data.view.state = "aborted".to_owned();
            data.view.cancellation_accepted = Some(true);
            return Ok(());
        }
        if Instant::now() >= deadline {
            if !submission_ambiguous {
                let mut data = lock(&job.data)?;
                data.view.state = "aborted".to_owned();
                data.view.cancel_requested = true;
                data.view.cancellation_accepted = Some(true);
                data.view.error = Some(AppError::new(
                    "query_deadline_exceeded",
                    "The query deadline elapsed before submission. No caller SQL was submitted.",
                ));
                return Ok(());
            }
            break;
        }
        // Staged AWS session setup also consumes the logical query deadline.
        // Dropping an overdue submission never claims the remote outcome is known.
        let submitted = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            inner.bounded(
                inner
                    .backend
                    .submit(&sql, &parameters, &client_token, timeout_ms),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            Err(AppError::new(
                "aws_timeout",
                "Submission exceeded the query deadline; its remote outcome may be unknown.",
            )
            .retryable())
        });
        match submitted {
            Ok(id) => {
                provider_id = Some(id.clone());
                let mut data = lock(&job.data)?;
                data.provider_id = Some(id);
                data.view.state = "submitted".to_owned();
                data.view.error = None;
                break;
            }
            Err(error) if !error.retryable && !submission_ambiguous => {
                let mut data = lock(&job.data)?;
                data.view.state = "failed".to_owned();
                data.view.error = Some(error);
                return Ok(());
            }
            Err(_) if attempt == 0 => {
                submission_ambiguous = true;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {}
        }
    }
    let Some(provider_id) = provider_id else {
        let mut data = lock(&job.data)?;
        data.view.state = "submission_unknown".to_owned();
        data.view.error = Some(AppError::new("submission_unknown", "AWS did not confirm submission after idempotent retries. The query may be running. Reusing this request ID returns the same handle; investigate before starting a new request."));
        return Ok(());
    };
    monitor_query(inner, job, &provider_id, deadline).await
}

async fn monitor_query(
    inner: &Inner,
    job: &Job,
    provider_id: &str,
    deadline: Instant,
) -> AppResult<()> {
    let confirmation_deadline = deadline + CANCELLATION_CONFIRMATION;
    loop {
        let mut view = job.snapshot()?;
        if view.terminal() {
            return Ok(());
        }
        if Instant::now() >= deadline && !view.cancel_requested {
            lock(&job.data)?.view.cancel_requested = true;
            view.cancel_requested = true;
        }
        if view.cancel_requested && view.cancellation_accepted != Some(true) {
            // Neither failed cancellation nor acceptance proves termination.
            let _ = inner.bounded(inner.cancel_provider(job, provider_id)).await;
        }
        match inner.bounded(inner.backend.status(provider_id)).await {
            Ok(status) => apply_status(job, status)?,
            Err(error) => {
                let mut data = lock(&job.data)?;
                if !data.view.terminal() {
                    data.view.error = Some(error);
                }
            }
        }
        if job.snapshot()?.terminal() {
            return Ok(());
        }
        if Instant::now() >= confirmation_deadline {
            let mut data = lock(&job.data)?;
            if data.view.terminal() {
                return Ok(());
            }
            data.view.state = "status_unknown".to_owned();
            data.view.error = Some(AppError::new("query_status_unknown", "The query deadline passed but AWS did not confirm termination. Capacity remains reserved; refresh credentials and check status or cancel again."));
            return Ok(());
        }
        tokio::time::sleep(
            POLL_INTERVAL.min(confirmation_deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
}

fn apply_status(job: &Job, status: StatementStatus) -> AppResult<()> {
    {
        let mut data = lock(&job.data)?;
        // Concurrent provider polls can complete out of order. Preserve both
        // observed terminal states and known forward progress.
        if data.view.terminal()
            || matches!(
                (data.view.state.as_str(), status.state),
                ("picked", QueryState::Submitted)
                    | ("started", QueryState::Submitted | QueryState::Picked)
            )
        {
            return Ok(());
        }
        data.view.state = match status.state {
            QueryState::Submitted => "submitted",
            QueryState::Picked => "picked",
            QueryState::Started => "started",
            QueryState::Finished => "finished",
            QueryState::Failed => "failed",
            QueryState::Aborted => "aborted",
        }
        .to_owned();
        data.view.duration_ns = status.duration_ns.or(data.view.duration_ns);
        data.view.total_rows = status.result_rows.or(data.view.total_rows);
        data.view.result_bytes = status.result_bytes.or(data.view.result_bytes);
        data.view.redshift_query_id = status.redshift_query_id.or(data.view.redshift_query_id);
        data.view.error = status.error;
        data.result_id = status.result_id;
    }
    job.release_terminal_capacity()
}

fn lock<T>(mutex: &Mutex<T>) -> AppResult<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| internal_error())
}

fn fingerprint(value: &impl Serialize) -> AppResult<String> {
    serde_json::to_vec(value)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|_| internal_error())
}

fn internal_error() -> AppError {
    AppError::new("state_unavailable", "Internal query state is unavailable.")
}

fn unknown_query() -> AppError {
    AppError::new(
        "unknown_query",
        "The query handle is unknown, expired, or belongs to another server process.",
    )
}

fn cursor_error(error: CursorError) -> AppError {
    match error {
        CursorError::CapacityReached => AppError::new("cursor_capacity", "Continuation capacity has been reached. Wait for old cursors to expire.").retryable(),
        CursorError::PayloadTooLarge => AppError::new("cursor_payload_too_large", "The provider continuation exceeds the bounded cursor capacity."),
        _ => AppError::new("invalid_cursor", "The cursor is unknown, expired, or does not match this query or metadata filter. Restart pagination."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Column, MetadataPage, ResultPage};
    use async_trait::async_trait;
    use std::{collections::VecDeque, sync::atomic::AtomicUsize};

    struct FakeBackend {
        submits: AtomicUsize,
        tokens: Mutex<Vec<String>>,
        state: Mutex<QueryState>,
        outcomes: Mutex<VecDeque<AppResult<String>>>,
        submit_delay: Duration,
        result_tokens: Mutex<Vec<Option<String>>>,
        metadata_requests: Mutex<Vec<MetadataRequest>>,
        first_rows: Vec<Vec<Value>>,
        second_rows: Vec<Vec<Value>>,
        columns: Vec<Column>,
        credentials_expired: AtomicBool,
        change_page: AtomicBool,
        cancel_accepted: bool,
        cancel_state: QueryState,
    }

    impl Default for FakeBackend {
        fn default() -> Self {
            Self {
                submits: AtomicUsize::new(0),
                tokens: Mutex::new(Vec::new()),
                state: Mutex::new(QueryState::Finished),
                outcomes: Mutex::new(VecDeque::new()),
                submit_delay: Duration::ZERO,
                result_tokens: Mutex::new(Vec::new()),
                metadata_requests: Mutex::new(Vec::new()),
                first_rows: vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
                second_rows: vec![vec![json!(4)], vec![json!(5)]],
                columns: vec![Column {
                    name: "value".into(),
                    type_name: "varchar".into(),
                    nullable: Some(true),
                    precision: None,
                    scale: None,
                }],
                credentials_expired: AtomicBool::new(false),
                change_page: AtomicBool::new(false),
                cancel_accepted: true,
                cancel_state: QueryState::Aborted,
            }
        }
    }

    #[async_trait]
    impl Backend for FakeBackend {
        async fn submit(
            &self,
            _sql: &str,
            _parameters: &[SqlParameter],
            token: &str,
            _timeout: u64,
        ) -> AppResult<String> {
            self.submits.fetch_add(1, Ordering::SeqCst);
            self.tokens.lock().unwrap().push(token.into());
            if !self.submit_delay.is_zero() {
                tokio::time::sleep(self.submit_delay).await;
            }
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok("provider-id".into()))
        }

        async fn status(&self, _id: &str) -> AppResult<StatementStatus> {
            if self.credentials_expired.load(Ordering::SeqCst) {
                return Err(AppError::new("aws_credentials", "Refresh the AWS profile."));
            }
            let state = *self.state.lock().unwrap();
            Ok(StatementStatus {
                state,
                result_id: (state == QueryState::Finished).then(|| "provider-result-id".into()),
                duration_ns: Some(100),
                result_rows: Some(5),
                result_bytes: Some(100),
                redshift_query_id: Some(99),
                error: None,
            })
        }

        async fn results(
            &self,
            _result_id: &str,
            next_token: Option<&str>,
        ) -> AppResult<ResultPage> {
            self.result_tokens
                .lock()
                .unwrap()
                .push(next_token.map(str::to_owned));
            let (mut rows, next_token) = match next_token {
                None => (
                    self.first_rows.clone(),
                    (!self.second_rows.is_empty()).then(|| "page-2".into()),
                ),
                Some("page-2") => (self.second_rows.clone(), None),
                _ => {
                    return Err(AppError::new(
                        "invalid_provider_token",
                        "Unknown fixture page.",
                    ))
                }
            };
            if self.change_page.load(Ordering::SeqCst) && !rows.is_empty() {
                rows[0][0] = json!("changed");
            }
            Ok(ResultPage {
                columns: self.columns.clone(),
                rows,
                next_token,
                total_rows: Some((self.first_rows.len() + self.second_rows.len()) as i64),
            })
        }

        async fn cancel(&self, _id: &str) -> AppResult<bool> {
            *self.state.lock().unwrap() = self.cancel_state;
            Ok(self.cancel_accepted)
        }

        async fn metadata(&self, request: &MetadataRequest) -> AppResult<MetadataPage> {
            self.metadata_requests.lock().unwrap().push(request.clone());
            if self.credentials_expired.load(Ordering::SeqCst) {
                return Err(AppError::new("aws_credentials", "Refresh the AWS profile."));
            }
            Ok(MetadataPage {
                items: vec![
                    if self.change_page.load(Ordering::SeqCst) {
                        json!({"schema":"changed"})
                    } else {
                        json!({"schema":"one"})
                    },
                    json!({"schema":"two"}),
                    json!({"schema":"three"}),
                ],
                next_token: None,
            })
        }
    }

    fn config() -> Config {
        let mut config = Config::default();
        config.aws.database = Some("analytics".into());
        config.aws.workgroup_name = Some("warehouse".into());
        config
    }

    fn request(key: Option<&str>) -> StartQuery {
        StartQuery {
            sql: "SELECT 1".into(),
            parameters: vec![],
            request_id: key.map(str::to_owned),
            timeout_ms: None,
        }
    }

    fn metadata_request() -> MetadataRequest {
        MetadataRequest {
            kind: MetadataKind::Schemas,
            schema: None,
            table: None,
            pattern: None,
            next_token: None,
            max_results: 100,
        }
    }

    async fn completed(controller: &Controller) -> JobView {
        let view = controller.start(request(None)).await.unwrap();
        let done = controller.wait(&view.query_id, 1000).await.unwrap();
        assert_eq!(done.state, "finished");
        done
    }

    async fn submitted(backend: &FakeBackend) {
        for _ in 0..100 {
            if backend.submits.load(Ordering::SeqCst) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("worker did not submit");
    }

    #[tokio::test]
    async fn discovery_is_offline_and_verification_probes_without_returning_schema_data() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        assert_eq!(
            controller.connection_status(false).await.unwrap()["verified"],
            false
        );
        assert!(backend.metadata_requests.lock().unwrap().is_empty());
        let verified = controller.connection_status(true).await.unwrap();
        assert_eq!(verified["verified"], true);
        assert!(!verified.to_string().contains("schema"));
        assert_eq!(backend.metadata_requests.lock().unwrap()[0].max_results, 1);
    }

    #[tokio::test]
    async fn page_offsets_cover_every_row_and_retries_do_not_consume_cursors() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let done = completed(&controller).await;
        let first = controller
            .results(&done.query_id, None, Some(2))
            .await
            .unwrap();
        assert_eq!(first["rows"], json!([[1], [2]]));
        assert_eq!(first["complete"], false);
        let cursor = first["next_cursor"].as_str().unwrap();
        let second = controller
            .results(&done.query_id, Some(cursor), Some(1))
            .await
            .unwrap();
        assert_eq!(second["rows"], json!([[3]]));
        let retry = controller
            .results(&done.query_id, Some(cursor), Some(1))
            .await
            .unwrap();
        assert_eq!(retry["rows"], second["rows"]);
        let last = controller
            .results(&done.query_id, second["next_cursor"].as_str(), Some(2))
            .await
            .unwrap();
        assert_eq!(last["rows"], json!([[4], [5]]));
        assert_eq!(last["complete"], true);
        assert!(last["next_cursor"].is_null());
        assert_eq!(
            *backend.result_tokens.lock().unwrap(),
            vec![None, None, None, Some("page-2".into())]
        );
        assert_eq!(backend.submits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn partial_page_replay_rejects_same_length_provider_drift() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let done = completed(&controller).await;
        let result = controller.results(&done.query_id, None, Some(1)).await.unwrap();
        let metadata = controller.metadata(metadata_request(), None, Some(1)).await.unwrap();

        backend.change_page.store(true, Ordering::SeqCst);
        let error = controller
            .results(&done.query_id, result["next_cursor"].as_str(), Some(1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_provider_page");
        let error = controller
            .metadata(metadata_request(), metadata["next_cursor"].as_str(), Some(1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_provider_page");

        // Rejected replay does not consume the cursor. A stable retry works.
        backend.change_page.store(false, Ordering::SeqCst);
        let retry = controller
            .results(&done.query_id, result["next_cursor"].as_str(), Some(1))
            .await
            .unwrap();
        assert_eq!(retry["rows"], json!([[2]]));
    }

    #[test]
    fn partial_page_replay_binds_columns_and_next_token() {
        let controller =
            Controller::new(config(), Arc::new(FakeBackend::default())).unwrap();
        let scope = controller.inner.scope("page-integrity").unwrap();
        let payload = |rows: &[Value], cursor: Option<&str>, complete: bool| {
            json!({"rows":rows,"columns":["original"],"next_cursor":cursor,"complete":complete})
        };
        let first = controller
            .paginate(
                scope.clone(),
                PagePosition::default(),
                vec![json!(1), json!(2)],
                Some("next".into()),
                1,
                payload,
            )
            .unwrap();
        let position = controller.position(&scope, first["next_cursor"].as_str()).unwrap();
        let error = controller
            .paginate(
                scope.clone(),
                position.clone(),
                vec![json!(1), json!(2)],
                Some("different".into()),
                1,
                payload,
            )
            .unwrap_err();
        assert_eq!(error.code, "invalid_provider_page");
        let error = controller
            .paginate(
                scope,
                position,
                vec![json!(1), json!(2)],
                Some("next".into()),
                1,
                |rows, cursor, complete| {
                    json!({"rows":rows,"columns":["changed"],"next_cursor":cursor,"complete":complete})
                },
            )
            .unwrap_err();
        assert_eq!(error.code, "invalid_provider_page");
    }

    #[tokio::test]
    async fn query_handles_and_cursors_cannot_be_reused_for_another_query_or_process() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let a = completed(&controller).await;
        let b = completed(&controller).await;
        let page = controller
            .results(&a.query_id, None, Some(1))
            .await
            .unwrap();
        let error = controller
            .results(&b.query_id, page["next_cursor"].as_str(), Some(1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_cursor");
        assert_eq!(
            controller.status("provider-id").await.unwrap_err().code,
            "unknown_query"
        );
        let restarted = Controller::new(config(), backend).unwrap();
        assert_eq!(
            restarted.status(&a.query_id).await.unwrap_err().code,
            "unknown_query"
        );
    }

    #[tokio::test]
    async fn metadata_cursors_bind_filters_and_provider_page_size_stays_fixed() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let first = controller
            .metadata(metadata_request(), None, Some(1))
            .await
            .unwrap();
        let mut changed = metadata_request();
        changed.pattern = Some("different%".into());
        assert_eq!(
            controller
                .metadata(changed, first["next_cursor"].as_str(), Some(1))
                .await
                .unwrap_err()
                .code,
            "invalid_cursor"
        );
        let last = controller
            .metadata(metadata_request(), first["next_cursor"].as_str(), Some(2))
            .await
            .unwrap();
        assert_eq!(last["returned_items"], 2);
        assert_eq!(last["complete"], true);
        assert!(backend
            .metadata_requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.max_results == 100));
    }

    #[tokio::test]
    async fn byte_budget_counts_utf8_and_keeps_complete_rows() {
        let text = "é".repeat(300);
        let backend = Arc::new(FakeBackend {
            first_rows: vec![vec![json!(text)], vec![json!(text)], vec![json!(text)]],
            second_rows: vec![],
            ..FakeBackend::default()
        });
        let mut config = config();
        config.max_page_bytes = 1600;
        let controller = Controller::new(config, backend).unwrap();
        let done = completed(&controller).await;
        let page = controller
            .results(&done.query_id, None, Some(3))
            .await
            .unwrap();
        assert_eq!(page["returned_rows"], 1);
        assert_eq!(page["rows"][0][0], text);
        assert!(!page["next_cursor"].is_null());
        assert!(controller.payload_bytes(&page).unwrap() <= 1600);
    }

    #[tokio::test]
    async fn an_oversized_row_is_an_error_instead_of_silent_truncation() {
        let backend = Arc::new(FakeBackend {
            first_rows: vec![vec![json!("s".repeat(4000))]],
            second_rows: vec![],
            ..FakeBackend::default()
        });
        let mut config = config();
        config.max_page_bytes = 1024;
        let controller = Controller::new(config, backend).unwrap();
        let done = completed(&controller).await;
        let error = controller
            .results(&done.query_id, None, None)
            .await
            .unwrap_err();
        assert_eq!(error.code, "result_row_too_large");
    }

    #[tokio::test]
    async fn identical_request_ids_reuse_the_handle_and_conflicting_bindings_are_rejected() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let mut request = request(Some("logical-request"));
        request.sql = "SELECT :value".into();
        request.parameters = vec![SqlParameter {
            name: "value".into(),
            value: "one".into(),
        }];
        let first = controller.start(request.clone()).await.unwrap();
        let retry = controller.start(request.clone()).await.unwrap();
        assert_eq!(first.query_id, retry.query_id);
        request.parameters[0].value = "two".into();
        assert_eq!(
            controller.start(request).await.unwrap_err().code,
            "request_id_conflict"
        );
        controller.wait(&first.query_id, 1000).await.unwrap();
        assert_eq!(backend.submits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reordered_named_bindings_reuse_the_same_request_handle() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let mut input = request(Some("binding-order"));
        input.sql = "SELECT :a, :b".into();
        input.parameters = vec![
            SqlParameter { name: "b".into(), value: "2".into() },
            SqlParameter { name: "a".into(), value: "1".into() },
        ];
        let first = controller.start(input.clone()).await.unwrap();
        input.parameters.reverse();
        let retry = controller.start(input.clone()).await.unwrap();
        assert_eq!(first.query_id, retry.query_id);
        input.parameters[0].value = "different".into();
        assert_eq!(
            controller.start(input).await.unwrap_err().code,
            "request_id_conflict"
        );
        controller.wait(&first.query_id, 1000).await.unwrap();
        assert_eq!(backend.submits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_use_the_same_provider_token_after_an_ambiguous_submit() {
        let backend = Arc::new(FakeBackend::default());
        backend
            .outcomes
            .lock()
            .unwrap()
            .push_back(Err(AppError::new(
                "aws_transport",
                "Unknown transport outcome.",
            )
            .retryable()));
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let done = completed(&controller).await;
        assert_eq!(done.state, "finished");
        let tokens = backend.tokens.lock().unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], tokens[1]);
    }

    #[tokio::test]
    async fn a_later_auth_failure_does_not_erase_an_ambiguous_first_submission() {
        let backend = Arc::new(FakeBackend::default());
        backend.outcomes.lock().unwrap().extend([
            Err(AppError::new("aws_timeout", "Unknown submit outcome.").retryable()),
            Err(AppError::new("aws_credentials", "Credentials expired.")),
        ]);
        let mut config = config();
        config.max_concurrent_queries = 1;
        let controller = Controller::new(config, backend).unwrap();
        let first = controller
            .start(request(Some("unknown-request")))
            .await
            .unwrap();
        let view = controller.wait(&first.query_id, 400).await.unwrap();
        assert_eq!(view.state, "submission_unknown");
        assert!(!view.terminal());
        assert_eq!(
            controller.start(request(None)).await.unwrap_err().code,
            "query_capacity"
        );
        let same = controller
            .start(request(Some("unknown-request")))
            .await
            .unwrap();
        assert_eq!(same.query_id, first.query_id);
        controller.inner.registry.lock().unwrap().prune(
            Duration::from_secs(600),
            Instant::now() + Duration::from_secs(601),
        );
        assert_eq!(
            controller.status(&first.query_id).await.unwrap_err().code,
            "unknown_query"
        );
        assert_eq!(controller.inner.query_capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn query_capacity_is_released_only_after_observed_termination() {
        let backend = Arc::new(FakeBackend {
            state: Mutex::new(QueryState::Started),
            ..FakeBackend::default()
        });
        let mut config = config();
        config.max_concurrent_queries = 1;
        let controller = Controller::new(config, backend.clone()).unwrap();
        let first = controller.start(request(None)).await.unwrap();
        submitted(&backend).await;
        assert_eq!(
            controller.start(request(None)).await.unwrap_err().code,
            "query_capacity"
        );
        let cancelling = controller.cancel(&first.query_id).await.unwrap();
        assert_eq!(cancelling.cancellation_accepted, Some(true));
        assert!(!cancelling.terminal());
        assert_eq!(controller.inner.query_capacity.available_permits(), 0);
        assert_eq!(
            controller.status(&first.query_id).await.unwrap().state,
            "aborted"
        );
        assert_eq!(controller.inner.query_capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn older_active_observations_cannot_regress_query_progress() {
        let backend = Arc::new(FakeBackend {
            state: Mutex::new(QueryState::Started),
            ..FakeBackend::default()
        });
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let first = controller.start(request(None)).await.unwrap();
        submitted(&backend).await;
        assert_eq!(controller.status(&first.query_id).await.unwrap().state, "started");
        let job = controller.inner.job(&first.query_id).unwrap();
        let mut older = backend.status("provider-id").await.unwrap();
        older.state = QueryState::Submitted;
        older.duration_ns = Some(1);
        apply_status(&job, older).unwrap();
        let mut older = backend.status("provider-id").await.unwrap();
        older.state = QueryState::Picked;
        apply_status(&job, older).unwrap();
        let current = job.snapshot().unwrap();
        assert_eq!(current.state, "started");
        assert_eq!(current.duration_ns, Some(100));
        *backend.state.lock().unwrap() = QueryState::Finished;
        assert_eq!(controller.status(&first.query_id).await.unwrap().state, "finished");
    }

    #[tokio::test]
    async fn query_completion_can_win_the_cancellation_race() {
        let backend = Arc::new(FakeBackend {
            state: Mutex::new(QueryState::Started),
            cancel_accepted: false,
            cancel_state: QueryState::Finished,
            ..FakeBackend::default()
        });
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let first = controller.start(request(None)).await.unwrap();
        submitted(&backend).await;
        let cancellation = controller.cancel(&first.query_id).await.unwrap();
        assert!(cancellation.cancel_requested);
        assert_eq!(cancellation.cancellation_accepted, Some(false));
        let done = controller.status(&first.query_id).await.unwrap();
        assert_eq!(done.state, "finished");
        assert!(done.cancel_requested);
    }

    #[tokio::test]
    async fn dropping_a_waiter_does_not_drop_the_owned_query_worker() {
        let backend = Arc::new(FakeBackend {
            state: Mutex::new(QueryState::Started),
            ..FakeBackend::default()
        });
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let first = controller.start(request(None)).await.unwrap();
        submitted(&backend).await;
        assert!(tokio::time::timeout(
            Duration::from_millis(1),
            controller.wait(&first.query_id, 1000)
        )
        .await
        .is_err());
        *backend.state.lock().unwrap() = QueryState::Finished;
        assert_eq!(
            controller.status(&first.query_id).await.unwrap().state,
            "finished"
        );
        assert_eq!(backend.submits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_credentials_are_actionable_and_do_not_discard_the_handle() {
        let backend = Arc::new(FakeBackend {
            state: Mutex::new(QueryState::Started),
            ..FakeBackend::default()
        });
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let first = controller.start(request(None)).await.unwrap();
        submitted(&backend).await;
        backend.credentials_expired.store(true, Ordering::SeqCst);
        assert_eq!(
            controller.status(&first.query_id).await.unwrap_err().code,
            "aws_credentials"
        );
        assert!(!controller
            .inner
            .job(&first.query_id)
            .unwrap()
            .snapshot()
            .unwrap()
            .terminal());
        backend.credentials_expired.store(false, Ordering::SeqCst);
        *backend.state.lock().unwrap() = QueryState::Finished;
        assert_eq!(
            controller.status(&first.query_id).await.unwrap().state,
            "finished"
        );
    }

    #[tokio::test]
    async fn metadata_and_result_concurrency_are_bounded_separately() {
        let backend = Arc::new(FakeBackend::default());
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let _permits = controller
            .inner
            .read_capacity
            .clone()
            .try_acquire_many_owned(8)
            .unwrap();
        assert_eq!(
            controller
                .metadata(metadata_request(), None, None)
                .await
                .unwrap_err()
                .code,
            "backend_busy"
        );
        assert!(backend.metadata_requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unresolved_cancellation_retains_capacity_until_a_later_terminal_observation() {
        let backend = Arc::new(FakeBackend {
            state: Mutex::new(QueryState::Started),
            cancel_accepted: false,
            cancel_state: QueryState::Started,
            ..FakeBackend::default()
        });
        let mut config = config();
        config.max_concurrent_queries = 1;
        let controller = Controller::new(config, backend.clone()).unwrap();
        let query_id = format!("query_{}", Uuid::new_v4());
        // An elapsed deadline exercises confirmation without a 30-second sleep.
        let job = Arc::new(Job {
            data: Mutex::new(JobData {
                view: JobView {
                    query_id: query_id.clone(),
                    state: "submitted".into(),
                    query_fingerprint: "fixture".into(),
                    submitted_at_unix_ms: 0,
                    duration_ns: None,
                    total_rows: None,
                    result_bytes: None,
                    redshift_query_id: None,
                    cancel_requested: false,
                    cancellation_accepted: None,
                    error: None,
                },
                provider_id: Some("provider-id".into()),
                result_id: None,
            }),
            created_at: Instant::now() - Duration::from_secs(40),
            worker_done: AtomicBool::new(false),
            permit: Mutex::new(Some(
                controller
                    .inner
                    .query_capacity
                    .clone()
                    .try_acquire_owned()
                    .unwrap(),
            )),
            cancel_lock: tokio::sync::Mutex::new(()),
        });
        controller
            .inner
            .registry
            .lock()
            .unwrap()
            .jobs
            .insert(query_id.clone(), job.clone());
        monitor_query(
            &controller.inner,
            &job,
            "provider-id",
            job.created_at + Duration::from_millis(1),
        )
        .await
        .unwrap();
        job.worker_done.store(true, Ordering::Release);
        job.release_terminal_capacity().unwrap();
        let unknown = job.snapshot().unwrap();
        assert_eq!(unknown.state, "status_unknown");
        assert!(unknown.cancel_requested);
        assert_eq!(unknown.cancellation_accepted, Some(false));
        assert_eq!(
            controller.start(request(None)).await.unwrap_err().code,
            "query_capacity"
        );
        *backend.state.lock().unwrap() = QueryState::Finished;
        assert_eq!(
            controller.status(&query_id).await.unwrap().state,
            "finished"
        );
        assert_eq!(controller.inner.query_capacity.available_permits(), 1);
        *backend.state.lock().unwrap() = QueryState::Started;
        apply_status(&job, backend.status("provider-id").await.unwrap()).unwrap();
        assert_eq!(job.snapshot().unwrap().state, "finished");
        assert_eq!(controller.inner.query_capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn a_definite_initial_credential_failure_releases_capacity() {
        let backend = Arc::new(FakeBackend::default());
        backend
            .outcomes
            .lock()
            .unwrap()
            .push_back(Err(AppError::new(
                "aws_credentials",
                "Refresh the AWS profile.",
            )));
        let controller = Controller::new(config(), backend).unwrap();
        let started = controller.start(request(None)).await.unwrap();
        let failed = controller.wait(&started.query_id, 1000).await.unwrap();
        assert_eq!(failed.state, "failed");
        assert_eq!(failed.error.unwrap().code, "aws_credentials");
        assert_eq!(controller.inner.query_capacity.available_permits(), 4);
    }

    #[tokio::test]
    async fn submission_setup_cannot_outlive_the_logical_deadline() {
        let backend = Arc::new(FakeBackend {
            submit_delay: Duration::from_secs(10),
            ..FakeBackend::default()
        });
        let controller = Controller::new(config(), backend.clone()).unwrap();
        let mut input = request(None);
        input.timeout_ms = Some(100);
        let started = controller.start(input).await.unwrap();
        let outcome = controller.wait(&started.query_id, 1000).await.unwrap();
        assert_eq!(outcome.state, "submission_unknown");
        assert_eq!(backend.submits.load(Ordering::SeqCst), 1);
        assert_eq!(controller.inner.query_capacity.available_permits(), 3);
    }

    #[tokio::test]
    async fn ambiguous_service_errors_retain_the_handle_and_query_capacity() {
        let backend = Arc::new(FakeBackend::default());
        backend.outcomes.lock().unwrap().extend([
            Err(AppError::new("aws_execution_unknown", "Unknown execution outcome.").retryable()),
            Err(AppError::new("aws_error", "Unknown service outcome.").retryable()),
        ]);
        let mut config = config();
        config.max_concurrent_queries = 1;
        let controller = Controller::new(config, backend.clone()).unwrap();
        let first = controller
            .start(request(Some("execution-request")))
            .await
            .unwrap();
        let outcome = controller.wait(&first.query_id, 1000).await.unwrap();
        assert_eq!(outcome.state, "submission_unknown");
        assert!(!outcome.terminal());
        assert_eq!(
            controller
                .start(request(Some("execution-request")))
                .await
                .unwrap()
                .query_id,
            first.query_id
        );
        assert_eq!(
            controller
                .start(request(Some("new-request")))
                .await
                .unwrap_err()
                .code,
            "query_capacity"
        );
        assert_eq!(backend.submits.load(Ordering::SeqCst), 2);
        assert_eq!(controller.inner.query_capacity.available_permits(), 0);
    }
}
