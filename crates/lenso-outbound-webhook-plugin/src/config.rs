use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::schema::schema_plan;

const MAX_REFERENCE_BYTES: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Immutable policy and resource references for one Outbound Webhook Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, lenso::PluginConfig)]
#[serde(deny_unknown_fields)]
pub struct OutboundWebhookConfig {
    schema: String,
    database_url_secret: String,
    signing_secret: String,
    endpoint_url: String,
    queue_name: String,
    max_attempts: i64,
    max_payload_bytes: usize,
    lease_seconds: i64,
    retry_base_seconds: i64,
    retry_max_seconds: i64,
    producer_instances: Vec<String>,
    admin_instances: Vec<String>,
}

impl OutboundWebhookConfig {
    /// Creates one fixed-endpoint Webhook policy.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        signing_secret: impl Into<String>,
        endpoint_url: impl Into<String>,
        queue_name: impl Into<String>,
        max_attempts: i64,
        max_payload_bytes: usize,
        lease_seconds: i64,
        retry_base_seconds: i64,
        retry_max_seconds: i64,
        producer_instances: Vec<String>,
        admin_instances: Vec<String>,
    ) -> Result<Self, String> {
        let config = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            signing_secret: signing_secret.into(),
            endpoint_url: endpoint_url.into(),
            queue_name: queue_name.into(),
            max_attempts,
            max_payload_bytes,
            lease_seconds,
            retry_base_seconds,
            retry_max_seconds,
            producer_instances,
            admin_instances,
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        schema_plan(self.schema.clone())
            .map_err(|error| format!("invalid owned PostgreSQL schema: {error}"))?;
        if !valid_secret_reference(&self.database_url_secret)
            || !valid_secret_reference(&self.signing_secret)
            || self.database_url_secret == self.signing_secret
        {
            return Err("database and signing secret references must be valid and distinct".into());
        }
        fixed_endpoint(&self.endpoint_url)?;
        if !valid_name(&self.queue_name) {
            return Err("queue_name is invalid".into());
        }
        if !(1..=100).contains(&self.max_attempts) {
            return Err("max_attempts must be between 1 and 100".into());
        }
        if !(1..=MAX_PAYLOAD_BYTES).contains(&self.max_payload_bytes) {
            return Err(format!(
                "max_payload_bytes must be between 1 and {MAX_PAYLOAD_BYTES}"
            ));
        }
        if !(1..=3_600).contains(&self.lease_seconds) {
            return Err("lease_seconds must be between 1 and 3600".into());
        }
        if !(1..=86_400).contains(&self.retry_base_seconds)
            || !(self.retry_base_seconds..=86_400).contains(&self.retry_max_seconds)
        {
            return Err(
                "retry delays must satisfy 1 <= retry_base_seconds <= retry_max_seconds <= 86400"
                    .into(),
            );
        }
        validate_callers(&self.producer_instances, "producer")?;
        validate_callers(&self.admin_instances, "admin")?;
        Ok(())
    }

    pub(crate) fn endpoint(&self) -> Url {
        fixed_endpoint(&self.endpoint_url).expect("validated fixed Webhook endpoint")
    }

    pub(crate) fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    pub(crate) fn database_url_secret(&self) -> &str {
        &self.database_url_secret
    }

    pub(crate) fn signing_secret(&self) -> &str {
        &self.signing_secret
    }

    pub(crate) fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub(crate) const fn max_attempts(&self) -> i64 {
        self.max_attempts
    }

    pub(crate) const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    pub(crate) const fn lease_seconds(&self) -> i64 {
        self.lease_seconds
    }

    pub(crate) const fn retry_base_seconds(&self) -> i64 {
        self.retry_base_seconds
    }

    pub(crate) const fn retry_max_seconds(&self) -> i64 {
        self.retry_max_seconds
    }

    pub(crate) fn producer_allowed(&self, caller: Option<&str>) -> bool {
        caller.is_some_and(|caller| self.producer_instances.iter().any(|item| item == caller))
    }

    pub(crate) fn admin_allowed(&self, caller: Option<&str>) -> bool {
        caller.is_some_and(|caller| self.admin_instances.iter().any(|item| item == caller))
    }
}

pub(crate) fn endpoint_origin(endpoint: &Url) -> String {
    endpoint.origin().ascii_serialization()
}

fn fixed_endpoint(value: &str) -> Result<Url, String> {
    let endpoint = Url::parse(value).map_err(|_| "endpoint_url is invalid".to_owned())?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(
            "endpoint_url must be one fixed HTTP(S) URL without credentials, query, or fragment"
                .into(),
        );
    }
    Ok(endpoint)
}

fn validate_callers(values: &[String], role: &str) -> Result<(), String> {
    if values.is_empty() || values.iter().any(|value| !valid_name(value)) {
        return Err(format!("at least one valid {role} caller is required"));
    }
    if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(format!("{role} caller list contains duplicates"));
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

fn valid_secret_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("//")
        && value
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: &str) -> Result<OutboundWebhookConfig, String> {
        OutboundWebhookConfig::new(
            "webhooks",
            "webhook/database-url",
            "webhook/signing-key",
            endpoint,
            "webhooks",
            5,
            262_144,
            30,
            5,
            300,
            vec!["orders".into()],
            vec!["operations".into()],
        )
    }

    #[test]
    fn endpoint_is_fixed_by_host_configuration() {
        let policy = config("https://hooks.example.test/lenso").unwrap();
        assert_eq!(
            endpoint_origin(&policy.endpoint()),
            "https://hooks.example.test"
        );
        assert!(config("https://hooks.example.test/lenso?token=secret").is_err());
        assert!(config("file:///tmp/webhook").is_err());
    }

    #[test]
    fn caller_authority_is_explicit_and_closed() {
        let config = config("https://hooks.example.test/lenso").unwrap();
        assert!(config.producer_allowed(Some("orders")));
        assert!(!config.producer_allowed(Some("operations")));
        assert!(config.admin_allowed(Some("operations")));
        assert!(!config.admin_allowed(None));
    }

    #[test]
    fn retry_lease_queue_and_secret_policy_fail_closed() {
        let mut policy = config("https://hooks.example.test/lenso").unwrap();
        policy.queue_name = "bad queue".into();
        assert!(policy.validate().is_err());

        let mut policy = config("https://hooks.example.test/lenso").unwrap();
        policy.retry_base_seconds = 301;
        assert!(policy.validate().is_err());

        let mut policy = config("https://hooks.example.test/lenso").unwrap();
        policy.database_url_secret = policy.signing_secret.clone();
        assert!(policy.validate().is_err());
    }
}
