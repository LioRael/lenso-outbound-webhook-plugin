use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use lenso_capability_http_client::{SendError, SendRequestHeadersItem};
use lenso_capability_outbound_webhook::{EnqueueRequest, EnqueueResponseStatus};
use lenso_capability_outbound_webhook_admin::{
    DispatchResponse, DispatchResponseOutcome, DispatchResponseReceipt, InspectResponse,
    InspectResponseLastReceipt, InspectResponseStatus,
};
use lenso_kernel::RuntimeFailure;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::storage::{DeliveryReceipt, DeliveryRecord, DeliveryStatus};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub(crate) struct DeliveryFailure {
    pub(crate) code: String,
    pub(crate) retryable: bool,
}

pub(crate) fn webhook_headers(
    delivery: &DeliveryRecord,
    timestamp: &str,
    signature: &str,
) -> Vec<SendRequestHeadersItem> {
    [
        ("content-type", "application/json".to_owned()),
        ("x-lenso-webhook-id", delivery.delivery_id.clone()),
        ("x-lenso-webhook-event-id", delivery.event_id.clone()),
        ("x-lenso-webhook-timestamp", timestamp.to_owned()),
        ("x-lenso-webhook-signature", format!("v1={signature}")),
    ]
    .into_iter()
    .map(|(name, value)| SendRequestHeadersItem {
        name: name.to_owned(),
        value,
    })
    .collect()
}

pub(crate) fn sign_payload(
    key: &[u8],
    delivery_id: &str,
    timestamp: &str,
    payload: &[u8],
) -> Result<String, RuntimeFailure> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| RuntimeFailure::PluginFailure {
        detail: "Outbound Webhook signing secret is invalid".into(),
    })?;
    mac.update(delivery_id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload);
    Ok(hex(mac.finalize().into_bytes().as_slice()))
}

#[derive(Serialize)]
struct WebhookEnvelope<'a> {
    event_id: &'a str,
    event_type: &'a str,
    payload: &'a BTreeMap<String, Value>,
}

pub(crate) fn payload_snapshot(request: &EnqueueRequest) -> Result<Vec<u8>, RuntimeFailure> {
    serde_json::to_vec(&WebhookEnvelope {
        event_id: &request.event_id,
        event_type: &request.event_type,
        payload: &request.payload,
    })
    .map_err(|error| RuntimeFailure::PluginFailure {
        detail: format!("Outbound Webhook payload could not be snapshotted: {error}"),
    })
}

pub(crate) fn stable_delivery_id(endpoint_url: &str, event_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(endpoint_url.as_bytes());
    digest.update([0]);
    digest.update(event_id.as_bytes());
    format!("wh_{}", &hex(&digest.finalize())[..48])
}

pub(crate) fn sha256_hex(value: &[u8]) -> String {
    hex(&Sha256::digest(value))
}

fn hex(value: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(value.len() * 2);
    for byte in value {
        result.push(char::from(ALPHABET[usize::from(byte >> 4)]));
        result.push(char::from(ALPHABET[usize::from(byte & 0x0f)]));
    }
    result
}

pub(crate) fn http_status_failure(status: i64) -> DeliveryFailure {
    DeliveryFailure {
        code: format!("http_status_{status}"),
        retryable: status == 408 || status == 429 || (500..600).contains(&status),
    }
}

pub(crate) fn http_domain_failure(error: &SendError) -> DeliveryFailure {
    match error {
        SendError::Timeout => failure("http_timeout", true),
        SendError::TransportFailure => failure("http_transport_failure", true),
        SendError::ResponseTooLarge => failure("http_response_too_large", false),
        SendError::DestinationNotAllowed => failure("http_destination_not_allowed", false),
        SendError::InvalidRequest => failure("http_invalid_request", false),
        SendError::RequestTooLarge => failure("http_request_too_large", false),
        SendError::Unknown(_) => failure("http_unknown_failure", false),
    }
}

fn failure(code: &str, retryable: bool) -> DeliveryFailure {
    DeliveryFailure {
        code: code.to_owned(),
        retryable,
    }
}

pub(crate) fn enqueue_status(status: DeliveryStatus) -> EnqueueResponseStatus {
    match status {
        DeliveryStatus::Queued => EnqueueResponseStatus::Queued,
        DeliveryStatus::Delivering => EnqueueResponseStatus::Delivering,
        DeliveryStatus::RetryScheduled => EnqueueResponseStatus::RetryScheduled,
        DeliveryStatus::Delivered => EnqueueResponseStatus::Delivered,
        DeliveryStatus::DeadLetter => EnqueueResponseStatus::DeadLetter,
    }
}

fn inspect_status(status: DeliveryStatus) -> InspectResponseStatus {
    match status {
        DeliveryStatus::Queued => InspectResponseStatus::Queued,
        DeliveryStatus::Delivering => InspectResponseStatus::Delivering,
        DeliveryStatus::RetryScheduled => InspectResponseStatus::RetryScheduled,
        DeliveryStatus::Delivered => InspectResponseStatus::Delivered,
        DeliveryStatus::DeadLetter => InspectResponseStatus::DeadLetter,
    }
}

pub(crate) fn inspect_response(delivery: DeliveryRecord) -> InspectResponse {
    InspectResponse {
        attempts: delivery.attempts,
        delivery_id: delivery.delivery_id,
        endpoint_origin: delivery.endpoint_origin,
        event_id: delivery.event_id,
        event_type: delivery.event_type,
        last_receipt: delivery.last_receipt.map(inspect_receipt),
        max_attempts: delivery.max_attempts,
        payload_sha256: delivery.payload_sha256,
        replay_count: delivery.replay_count,
        status: inspect_status(delivery.status),
    }
}

fn inspect_receipt(receipt: DeliveryReceipt) -> InspectResponseLastReceipt {
    InspectResponseLastReceipt {
        attempt: receipt.attempt,
        http_status: receipt.http_status.map(|status| status.to_string()),
        occurred_at: format_time(receipt.occurred_at).expect("stored PostgreSQL timestamp"),
        outcome: receipt.outcome,
        response_sha256: receipt.response_sha256,
    }
}

pub(crate) fn dispatch_receipt(receipt: DeliveryReceipt) -> DispatchResponseReceipt {
    DispatchResponseReceipt {
        attempt: receipt.attempt,
        http_status: receipt.http_status.map(|status| status.to_string()),
        occurred_at: format_time(receipt.occurred_at).expect("stored PostgreSQL timestamp"),
        outcome: receipt.outcome,
        response_sha256: receipt.response_sha256,
    }
}

pub(crate) fn dispatch_response(
    delivery_id: String,
    outcome: DispatchResponseOutcome,
    receipt: DeliveryReceipt,
) -> DispatchResponse {
    DispatchResponse {
        delivery_id: Some(delivery_id),
        outcome,
        receipt: Some(dispatch_receipt(receipt)),
    }
}

pub(crate) fn idle_dispatch() -> DispatchResponse {
    DispatchResponse {
        delivery_id: None,
        outcome: DispatchResponseOutcome::Idle,
        receipt: None,
    }
}

pub(crate) fn format_time(value: OffsetDateTime) -> Result<String, RuntimeFailure> {
    value
        .format(&Rfc3339)
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: format!("Outbound Webhook timestamp could not be encoded: {error}"),
        })
}

pub(crate) fn database_timestamp_now() -> OffsetDateTime {
    let now = OffsetDateTime::now_utc();
    now.replace_nanosecond((now.nanosecond() / 1_000) * 1_000)
        .expect("truncated nanosecond remains valid")
}

pub(crate) fn valid_event_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

pub(crate) fn valid_delivery_id(value: &str) -> bool {
    value.len() == 51
        && value.starts_with("wh_")
        && value[3..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn request(payload: BTreeMap<String, Value>) -> EnqueueRequest {
        EnqueueRequest {
            event_id: "order-42".into(),
            event_type: "order.created".into(),
            payload,
        }
    }

    #[test]
    fn payload_snapshot_and_delivery_identity_are_stable() {
        let left = payload_snapshot(&request(BTreeMap::from([
            ("z".into(), json!(2)),
            ("a".into(), json!(1)),
        ])))
        .unwrap();
        let right = payload_snapshot(&request(BTreeMap::from([
            ("a".into(), json!(1)),
            ("z".into(), json!(2)),
        ])))
        .unwrap();
        assert_eq!(left, right);
        assert_eq!(
            stable_delivery_id("https://hooks.example.test/events", "order-42"),
            stable_delivery_id("https://hooks.example.test/events", "order-42")
        );
        assert_ne!(
            stable_delivery_id("https://hooks.example.test/events", "order-42"),
            stable_delivery_id("https://other.example.test/events", "order-42")
        );
    }

    #[test]
    fn signatures_bind_event_timestamp_and_exact_snapshot() {
        let key = b"0123456789abcdef0123456789abcdef";
        let payload = br#"{"event_id":"order-42"}"#;
        let delivery_id = stable_delivery_id("https://hooks.example.test/events", "order-42");
        let signature = sign_payload(key, &delivery_id, "2026-08-30T00:00:00Z", payload).unwrap();
        assert_eq!(signature.len(), 64);
        assert_eq!(
            signature,
            sign_payload(key, &delivery_id, "2026-08-30T00:00:00Z", payload).unwrap()
        );
        assert_ne!(
            signature,
            sign_payload(key, "wh_different", "2026-08-30T00:00:00Z", payload).unwrap()
        );
    }

    #[test]
    fn delivery_header_uses_stable_delivery_identity() {
        let delivery_id = stable_delivery_id("https://hooks.example.test/events", "order-42");
        let delivery = DeliveryRecord {
            delivery_id: delivery_id.clone(),
            event_id: "order-42".into(),
            event_type: "order.created".into(),
            endpoint_url: "https://hooks.example.test/events".into(),
            endpoint_origin: "https://hooks.example.test".into(),
            payload_snapshot: Vec::new(),
            payload_sha256: sha256_hex(&[]),
            queue_name: "webhooks".into(),
            status: DeliveryStatus::Delivering,
            attempts: 1,
            max_attempts: 5,
            replay_count: 0,
            last_receipt: None,
        };
        let headers = webhook_headers(&delivery, "2026-08-30T00:00:00Z", "signature");
        assert!(
            headers.iter().any(|header| {
                header.name == "x-lenso-webhook-id" && header.value == delivery_id
            })
        );
        assert!(headers.iter().any(|header| {
            header.name == "x-lenso-webhook-event-id" && header.value == "order-42"
        }));
    }

    #[test]
    fn retry_classification_is_bounded_and_fail_closed() {
        assert!(http_status_failure(429).retryable);
        assert!(http_status_failure(503).retryable);
        assert!(!http_status_failure(400).retryable);
        assert!(http_domain_failure(&SendError::Timeout).retryable);
        assert!(!http_domain_failure(&SendError::DestinationNotAllowed).retryable);
    }
}
