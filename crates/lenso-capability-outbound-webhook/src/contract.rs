//! Authoritative source for the Outbound Webhook Capability contract.

use lenso_contract_authoring as lenso;

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct EnqueueRequest {
    pub event_id: String,
    pub event_type: String,
    #[schemars(extend("x-lenso-sensitive" = true))]
    pub payload: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnqueueResponseStatus {
    Queued,
    Delivering,
    Delivered,
    RetryScheduled,
    DeadLetter,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct EnqueueResponse {
    pub delivery_id: String,
    pub created: bool,
    pub status: EnqueueResponseStatus,
}

#[derive(lenso::DomainError)]
pub enum EnqueueError {
    InvalidEvent,
    IdempotencyConflict,
    Forbidden,
}

#[lenso::capability(
    id = "lenso.outbound-webhook",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait OutboundWebhook {
    async fn enqueue(
        &self,
        context: lenso::Ctx<'_>,
        request: EnqueueRequest,
    ) -> Result<EnqueueResponse, EnqueueError>;
}
