use lenso_capability_http_client::{
    ClientInvocationError, SendRequest, SendResponse as HttpSendResponse,
};
use lenso_capability_outbound_webhook::{EnqueueError, EnqueueRequest, EnqueueResponse};
use lenso_capability_outbound_webhook_admin::{
    DispatchError, DispatchResponse, DispatchResponseOutcome, InspectError, InspectRequest,
    InspectResponse, ReplayError, ReplayRequest, ReplayResponse,
};
use lenso_kernel::{InvocationContext, RuntimeFailure};
use time::{Duration, OffsetDateTime};

use crate::{
    OutboundWebhookPlugin, PreparedWebhook,
    protocol::{
        database_timestamp_now, dispatch_response, enqueue_status, format_time,
        http_domain_failure, http_status_failure, idle_dispatch, inspect_response,
        payload_snapshot, sha256_hex, sign_payload, stable_delivery_id, valid_delivery_id,
        valid_event_name, webhook_headers,
    },
    storage::{
        self, DeliveryReceipt, DeliveryRecord, DeliveryStatus, InsertOutcome, NewDelivery,
        StoreError,
    },
};

#[derive(Debug)]
pub(crate) enum FlowError {
    Domain(EnqueueError),
    Runtime(RuntimeFailure),
}

#[derive(Debug)]
pub(crate) enum AdminFlowError {
    Dispatch(DispatchError),
    Inspect(InspectError),
    Replay(ReplayError),
    Runtime(RuntimeFailure),
}

impl OutboundWebhookPlugin {
    pub(crate) async fn enqueue_delivery(
        &self,
        context: InvocationContext,
        request: EnqueueRequest,
    ) -> Result<EnqueueResponse, FlowError> {
        if !self.config.producer_allowed(context.caller_instance()) {
            return Err(FlowError::Domain(EnqueueError::Forbidden));
        }
        if !valid_event_name(&request.event_id) || !valid_event_name(&request.event_type) {
            return Err(FlowError::Domain(EnqueueError::InvalidEvent));
        }
        let prepared = self.prepared().map_err(FlowError::Runtime)?;
        let snapshot = payload_snapshot(&request).map_err(FlowError::Runtime)?;
        if snapshot.len() > self.config.max_payload_bytes() {
            return Err(FlowError::Domain(EnqueueError::InvalidEvent));
        }
        let delivery_id = stable_delivery_id(self.config.endpoint_url(), &request.event_id);
        let outcome = storage::insert_or_get(
            &prepared.postgres,
            NewDelivery {
                delivery_id: delivery_id.clone(),
                event_id: request.event_id,
                event_type: request.event_type,
                endpoint_url: self.config.endpoint_url().to_owned(),
                endpoint_origin: prepared.endpoint_origin,
                payload_sha256: sha256_hex(&snapshot),
                payload_snapshot: snapshot,
                queue_name: self.config.queue_name().to_owned(),
                max_attempts: self.config.max_attempts(),
                available_at: database_timestamp_now(),
            },
        )
        .await
        .map_err(|error| FlowError::Runtime(store_failure(&error)))?;
        let (created, delivery) = match outcome {
            InsertOutcome::Created(delivery) => (true, delivery),
            InsertOutcome::Existing(delivery) => (false, delivery),
            InsertOutcome::Conflict => {
                return Err(FlowError::Domain(EnqueueError::IdempotencyConflict));
            }
        };
        Ok(EnqueueResponse {
            delivery_id,
            created,
            status: enqueue_status(delivery.status),
        })
    }

    pub(crate) async fn inspect_delivery(
        &self,
        context: InvocationContext,
        request: InspectRequest,
    ) -> Result<InspectResponse, AdminFlowError> {
        if !self.config.admin_allowed(context.caller_instance()) {
            return Err(AdminFlowError::Inspect(InspectError::Forbidden));
        }
        if !valid_delivery_id(&request.delivery_id) {
            return Err(AdminFlowError::Inspect(InspectError::InvalidDelivery));
        }
        let prepared = self.prepared().map_err(AdminFlowError::Runtime)?;
        let delivery = storage::load(&prepared.postgres, &request.delivery_id)
            .await
            .map_err(|error| AdminFlowError::Runtime(store_failure(&error)))?
            .ok_or(AdminFlowError::Inspect(InspectError::NotFound))?;
        Ok(inspect_response(delivery))
    }

    pub(crate) async fn replay_delivery(
        &self,
        context: InvocationContext,
        request: ReplayRequest,
    ) -> Result<ReplayResponse, AdminFlowError> {
        if !self.config.admin_allowed(context.caller_instance()) {
            return Err(AdminFlowError::Replay(ReplayError::Forbidden));
        }
        if !valid_delivery_id(&request.delivery_id) {
            return Err(AdminFlowError::Replay(ReplayError::InvalidDelivery));
        }
        let prepared = self.prepared().map_err(AdminFlowError::Runtime)?;
        let existing = storage::load(&prepared.postgres, &request.delivery_id)
            .await
            .map_err(|error| AdminFlowError::Runtime(store_failure(&error)))?
            .ok_or(AdminFlowError::Replay(ReplayError::NotFound))?;
        if existing.status != DeliveryStatus::DeadLetter {
            return Err(AdminFlowError::Replay(ReplayError::NotDeadLetter));
        }
        let delivery = storage::begin_replay(&prepared.postgres, &request.delivery_id)
            .await
            .map_err(|error| AdminFlowError::Runtime(store_failure(&error)))?
            .ok_or(AdminFlowError::Replay(ReplayError::NotDeadLetter))?;
        Ok(ReplayResponse {
            replay_count: delivery.replay_count,
        })
    }

    /// Claims and delivers at most one due item. No background task is created by this Plugin.
    pub(crate) async fn dispatch_delivery(
        &self,
        context: InvocationContext,
    ) -> Result<DispatchResponse, AdminFlowError> {
        if !self.config.admin_allowed(context.caller_instance()) {
            return Err(AdminFlowError::Dispatch(DispatchError::Forbidden));
        }
        let prepared = self.prepared().map_err(AdminFlowError::Runtime)?;
        let Some(delivery) = storage::claim_due(
            &prepared.postgres,
            self.config.queue_name(),
            self.config.lease_seconds(),
        )
        .await
        .map_err(|error| AdminFlowError::Runtime(store_failure(&error)))?
        else {
            return Ok(idle_dispatch());
        };

        if delivery.endpoint_url != self.config.endpoint_url()
            || delivery.endpoint_origin != prepared.endpoint_origin
        {
            let receipt = DeliveryReceipt {
                attempt: delivery.attempts,
                outcome: "endpoint_configuration_changed".into(),
                http_status: None,
                response_sha256: None,
                occurred_at: OffsetDateTime::now_utc(),
            };
            storage::record_outcome(
                &prepared.postgres,
                &delivery.delivery_id,
                delivery.replay_count,
                &receipt,
                DeliveryStatus::DeadLetter,
                None,
            )
            .await
            .map_err(|error| AdminFlowError::Runtime(store_failure(&error)))?;
            return Ok(dispatch_response(
                delivery.delivery_id,
                DispatchResponseOutcome::DeadLettered,
                receipt,
            ));
        }

        let timestamp = format_time(OffsetDateTime::now_utc()).map_err(AdminFlowError::Runtime)?;
        let signature = sign_payload(
            &prepared.signing_key,
            &delivery.delivery_id,
            &timestamp,
            &delivery.payload_snapshot,
        )
        .map_err(AdminFlowError::Runtime)?;
        let response = self
            .http
            .send_with_context(
                context,
                SendRequest {
                    body: delivery.payload_snapshot.clone().into(),
                    headers: webhook_headers(&delivery, &timestamp, &signature),
                    method: "POST".into(),
                    url: prepared.endpoint.as_str().to_owned(),
                },
            )
            .await;
        self.finish_dispatch(prepared, delivery, response).await
    }

    async fn finish_dispatch(
        &self,
        prepared: PreparedWebhook,
        delivery: DeliveryRecord,
        response: Result<HttpSendResponse, ClientInvocationError>,
    ) -> Result<DispatchResponse, AdminFlowError> {
        let (receipt, failure) = match response {
            Ok(response) if (200..300).contains(&response.status) => (
                DeliveryReceipt {
                    attempt: delivery.attempts,
                    outcome: "delivered".into(),
                    http_status: Some(response.status),
                    response_sha256: Some(sha256_hex(response.body.as_slice())),
                    occurred_at: OffsetDateTime::now_utc(),
                },
                None,
            ),
            Ok(response) => {
                let failure = http_status_failure(response.status);
                (
                    DeliveryReceipt {
                        attempt: delivery.attempts,
                        outcome: failure.code.clone(),
                        http_status: Some(response.status),
                        response_sha256: Some(sha256_hex(response.body.as_slice())),
                        occurred_at: OffsetDateTime::now_utc(),
                    },
                    Some(failure),
                )
            }
            Err(ClientInvocationError::Domain(error)) => {
                let failure = http_domain_failure(&error);
                (
                    DeliveryReceipt {
                        attempt: delivery.attempts,
                        outcome: failure.code.clone(),
                        http_status: None,
                        response_sha256: None,
                        occurred_at: OffsetDateTime::now_utc(),
                    },
                    Some(failure),
                )
            }
            Err(ClientInvocationError::Runtime(error)) => {
                let receipt = DeliveryReceipt {
                    attempt: delivery.attempts,
                    outcome: "http_runtime_failure".into(),
                    http_status: None,
                    response_sha256: None,
                    occurred_at: OffsetDateTime::now_utc(),
                };
                storage::record_outcome(
                    &prepared.postgres,
                    &delivery.delivery_id,
                    delivery.replay_count,
                    &receipt,
                    DeliveryStatus::Delivering,
                    None,
                )
                .await
                .map_err(|store| AdminFlowError::Runtime(store_failure(&store)))?;
                return Err(AdminFlowError::Runtime(error));
            }
        };

        let (status, outcome, next_available_at) = if let Some(failure) = failure {
            if failure.retryable && delivery.attempts < delivery.max_attempts {
                let delay = retry_delay_seconds(
                    delivery.attempts,
                    self.config.retry_base_seconds(),
                    self.config.retry_max_seconds(),
                );
                (
                    DeliveryStatus::RetryScheduled,
                    DispatchResponseOutcome::RetryScheduled,
                    Some(database_timestamp_now() + Duration::seconds(delay)),
                )
            } else {
                (
                    DeliveryStatus::DeadLetter,
                    DispatchResponseOutcome::DeadLettered,
                    None,
                )
            }
        } else {
            (
                DeliveryStatus::Delivered,
                DispatchResponseOutcome::Delivered,
                None,
            )
        };
        storage::record_outcome(
            &prepared.postgres,
            &delivery.delivery_id,
            delivery.replay_count,
            &receipt,
            status,
            next_available_at,
        )
        .await
        .map_err(|store| AdminFlowError::Runtime(store_failure(&store)))?;
        Ok(dispatch_response(delivery.delivery_id, outcome, receipt))
    }
}

fn retry_delay_seconds(attempt: i64, base_seconds: i64, max_seconds: i64) -> i64 {
    let exponent = u32::try_from(attempt.saturating_sub(1).clamp(0, 62)).unwrap_or(62);
    base_seconds
        .saturating_mul(1_i64.checked_shl(exponent).unwrap_or(i64::MAX))
        .min(max_seconds)
}

fn store_failure(error: &StoreError) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    }
}

pub(crate) fn invariant_failure(detail: &str) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_is_exponential_and_bounded() {
        assert_eq!(retry_delay_seconds(1, 5, 300), 5);
        assert_eq!(retry_delay_seconds(2, 5, 300), 10);
        assert_eq!(retry_delay_seconds(7, 5, 300), 300);
        assert_eq!(retry_delay_seconds(100, 5, 300), 300);
    }
}
