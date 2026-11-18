//! Operator configuration fixes the target and resource budgets for one process.
//!
//! Missing backend configuration does not prevent MCP discovery. Values are never
//! copied into generated client configuration or logs. Database grants own access.

use crate::error::{AppError, AppResult};
use serde_json::{json, Value};

#[derive(Clone, Default)]
pub struct AwsConfig {
    pub profile: Option<String>,
    pub region: Option<String>,
    pub database: Option<String>,
    pub cluster_id: Option<String>,
    pub workgroup_name: Option<String>,
    pub db_user: Option<String>,
    pub secret_arn: Option<String>,
    pub api_timeout_secs: u64,
}

impl AwsConfig {
    pub fn validate(&self) -> AppResult<()> {
        if self.database.is_none() {
            return Err(AppError::new("configuration_required", "Set REDSHIFT_DATABASE and exactly one of REDSHIFT_WORKGROUP_NAME or REDSHIFT_CLUSTER_ID."));
        }
        if self.cluster_id.is_some() == self.workgroup_name.is_some() {
            return Err(AppError::new("configuration_required", "Configure exactly one Redshift cluster or Serverless workgroup."));
        }
        if self.db_user.is_some() && (self.secret_arn.is_some() || self.workgroup_name.is_some()) {
            return Err(AppError::new("invalid_configuration", "REDSHIFT_DB_USER is only supported for provisioned-cluster temporary credentials without a secret."));
        }
        for value in [&self.profile,&self.region,&self.database,&self.cluster_id,&self.workgroup_name,&self.db_user,&self.secret_arn].into_iter().flatten() {
            if value.is_empty() || value.len() > 2048 || value.chars().any(char::is_control) {
                return Err(AppError::new("invalid_configuration", "AWS target settings must be nonempty, bounded text without control characters."));
            }
        }
        Ok(())
    }

    pub fn posture(&self) -> Value {
        json!({
            "configured": self.validate().is_ok(),
            "target_kind": if self.workgroup_name.is_some() { "serverless" } else if self.cluster_id.is_some() { "provisioned" } else { "unconfigured" },
            "credential_source": "aws_sdk_default_chain",
            "profile_configured": self.profile.is_some(),
            "region_configured": self.region.is_some(),
            "database_auth": if self.secret_arn.is_some() { "secrets_manager" } else if self.db_user.is_some() { "temporary_db_user" } else { "temporary_iam" },
            "read_only": true,
            "transport": "stdio"
        })
    }
}

#[derive(Clone)]
pub struct Config {
    pub aws: AwsConfig,
    pub tool_profile: String,
    pub statement_timeout_ms: u64,
    pub max_page_rows: usize,
    pub max_page_bytes: usize,
    pub max_concurrent_queries: usize,
    pub max_jobs: usize,
    pub job_ttl_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            aws: AwsConfig { api_timeout_secs: 30, ..AwsConfig::default() },
            tool_profile: "analytics".into(),
            statement_timeout_ms: 60_000,
            max_page_rows: 100,
            max_page_bytes: 12_000,
            max_concurrent_queries: 4,
            max_jobs: 128,
            job_ttl_secs: 3600,
        }
    }
}

impl Config {
    pub fn from_env() -> AppResult<Self> { Self::from_lookup(|key| std::env::var(key).ok()) }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> AppResult<Self> {
        let tool_profile = get("REDSHIFT_TOOL_PROFILE").unwrap_or_else(|| "analytics".into());
        if !matches!(tool_profile.as_str(), "analytics" | "metadata") {
            return Err(AppError::new("invalid_configuration", "REDSHIFT_TOOL_PROFILE must be analytics or metadata."));
        }
        let text = |key: &str| get(key).filter(|s| !s.is_empty());
        let number = |key: &str, default: u64, min: u64, max: u64| -> AppResult<u64> {
            let n = match get(key) {
                None => default,
                Some(v) => v.parse::<u64>().map_err(|_| AppError::new("invalid_configuration", "A Redshift numeric setting is invalid; see configuration documentation."))?,
            };
            if !(min..=max).contains(&n) { return Err(AppError::new("invalid_configuration", "A Redshift numeric setting exceeds its documented bounds.")); }
            Ok(n)
        };
        Ok(Self {
            tool_profile,
            aws: AwsConfig {
                profile: text("AWS_PROFILE"), region: text("AWS_REGION"), database: text("REDSHIFT_DATABASE"),
                cluster_id: text("REDSHIFT_CLUSTER_ID"), workgroup_name: text("REDSHIFT_WORKGROUP_NAME"),
                db_user: text("REDSHIFT_DB_USER"), secret_arn: text("REDSHIFT_SECRET_ARN"),
                api_timeout_secs: number("REDSHIFT_API_TIMEOUT_SECS", 30, 1, 60)?,
            },
            statement_timeout_ms: number("REDSHIFT_STATEMENT_TIMEOUT_MS", 60_000, 1000, 300_000)?,
            max_page_rows: number("REDSHIFT_MAX_PAGE_ROWS", 100, 1, 1000)? as usize,
            max_page_bytes: number("REDSHIFT_MAX_PAGE_BYTES", 12_000, 1024, 24_000)? as usize,
            max_concurrent_queries: number("REDSHIFT_MAX_CONCURRENT_QUERIES", 4, 1, 32)? as usize,
            max_jobs: number("REDSHIFT_MAX_JOBS", 128, 1, 1024)? as usize,
            job_ttl_secs: number("REDSHIFT_JOB_TTL_SECS", 3600, 600, 86400)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_can_start_without_aws_configuration() {
        let cfg = Config::from_lookup(|_| None).unwrap();
        assert!(cfg.aws.validate().is_err());
        assert_eq!(cfg.aws.posture()["configured"], false);
    }

    #[test]
    fn mutually_exclusive_targets_and_credentials_are_enforced() {
        let mut cfg = AwsConfig { database: Some("analytics".into()), workgroup_name: Some("workgroup".into()), ..AwsConfig::default() };
        assert!(cfg.validate().is_ok());
        cfg.cluster_id = Some("cluster".into());
        assert!(cfg.validate().is_err());
        cfg.workgroup_name = None;
        cfg.db_user = Some("reader".into());
        assert!(cfg.validate().is_ok());
        cfg.secret_arn = Some("secret-identifier".into());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn invalid_budgets_do_not_echo_environment_values() {
        for value in ["private-canary", "0", "300001"] {
            let result = Config::from_lookup(|key| (key == "REDSHIFT_STATEMENT_TIMEOUT_MS").then(|| value.into()));
            let Err(error) = result else { panic!("invalid value accepted") };
            assert!(!error.to_string().contains(value));
        }
    }
}
