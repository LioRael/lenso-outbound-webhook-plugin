//! Authoritative source for the Outbound Webhook Admin Capability contract.

use lenso_contract_authoring as lenso;

#[derive(serde::Deserialize)]
pub struct Nullable<T>(Option<T>);

impl<T: lenso::JsonSchema> lenso::JsonSchema for Nullable<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("Nullable_{}", T::schema_name()).into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        format!("Nullable<{}>", T::schema_id()).into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <Option<T> as lenso::JsonSchema>::json_schema(generator)
    }
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DispatchRequest {}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchResponseOutcome {
    Idle,
    Delivered,
    RetryScheduled,
    DeadLettered,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DispatchResponseReceipt {
    #[schemars(range(min = 1, max = 100))]
    pub attempt: i64,
    pub outcome: String,
    pub http_status: Nullable<String>,
    pub response_sha256: Nullable<String>,
    #[schemars(extend("format" = "date-time"))]
    pub occurred_at: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DispatchResponse {
    pub outcome: DispatchResponseOutcome,
    pub delivery_id: Nullable<String>,
    pub receipt: Nullable<DispatchResponseReceipt>,
}

#[derive(lenso::DomainError)]
pub enum DispatchError {
    Forbidden,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct InspectRequest {
    pub delivery_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectResponseStatus {
    Queued,
    Delivering,
    RetryScheduled,
    Delivered,
    DeadLetter,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct InspectResponseLastReceipt {
    #[schemars(range(min = 1, max = 100))]
    pub attempt: i64,
    pub outcome: String,
    pub http_status: Nullable<String>,
    pub response_sha256: Nullable<String>,
    #[schemars(extend("format" = "date-time"))]
    pub occurred_at: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct InspectResponse {
    pub delivery_id: String,
    pub event_id: String,
    pub event_type: String,
    pub endpoint_origin: String,
    pub payload_sha256: String,
    pub status: InspectResponseStatus,
    #[schemars(range(min = 0, max = 100))]
    pub attempts: i64,
    #[schemars(range(min = 1, max = 100))]
    pub max_attempts: i64,
    #[schemars(range(min = 0, max = 1_000_000))]
    pub replay_count: i64,
    pub last_receipt: Nullable<InspectResponseLastReceipt>,
}

#[derive(lenso::DomainError)]
pub enum InspectError {
    Forbidden,
    InvalidDelivery,
    NotFound,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReplayRequest {
    pub delivery_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReplayResponse {
    #[schemars(range(min = 1, max = 1_000_000))]
    pub replay_count: i64,
}

#[derive(lenso::DomainError)]
pub enum ReplayError {
    Forbidden,
    InvalidDelivery,
    NotFound,
    NotDeadLetter,
}

#[lenso::capability(
    id = "lenso.outbound-webhook-admin",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait OutboundWebhookAdmin {
    async fn dispatch(
        &self,
        context: lenso::Ctx<'_>,
        request: DispatchRequest,
    ) -> Result<DispatchResponse, DispatchError>;

    async fn inspect(
        &self,
        context: lenso::Ctx<'_>,
        request: InspectRequest,
    ) -> Result<InspectResponse, InspectError>;

    async fn replay(
        &self,
        context: lenso::Ctx<'_>,
        request: ReplayRequest,
    ) -> Result<ReplayResponse, ReplayError>;
}
