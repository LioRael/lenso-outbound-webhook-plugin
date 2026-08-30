//! Durable, signed outbound Webhook delivery over explicit Lenso dependencies.

mod config;
mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod protocol;
mod schema;
mod service;
mod storage;

use std::{cell::RefCell, fmt, rc::Rc, time::Duration as StdDuration};

use lenso::{ActivateContext, DeactivateContext, Lifecycle, Port, provides};
use lenso_capability_http_client as http_client;
use lenso_capability_outbound_webhook as webhook;
use lenso_capability_outbound_webhook::{EnqueueRequest, OutboundWebhook};
use lenso_capability_outbound_webhook_admin as admin;
use lenso_capability_outbound_webhook_admin::{
    DispatchRequest, InspectRequest, OutboundWebhookAdminDispatch, OutboundWebhookAdminInspect,
    OutboundWebhookAdminReplay, ReplayRequest,
};
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsInvocationError};
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use url::Url;
use zeroize::Zeroizing;

pub use config::OutboundWebhookConfig;
pub use operator::{OutboundWebhookOperator, OutboundWebhookOperatorError};
use service::{AdminFlowError, FlowError};

const DEPENDENCY_TIMEOUT: StdDuration = StdDuration::from_secs(10);

fn validate_config(config: &OutboundWebhookConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("Outbound Webhook configuration is invalid: {detail}"),
        })
}

#[derive(Clone)]
struct PreparedWebhook {
    postgres: OwnedPostgres,
    endpoint: Url,
    endpoint_origin: String,
    signing_key: Zeroizing<Vec<u8>>,
}

impl fmt::Debug for PreparedWebhook {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedWebhook")
            .field("schema", &self.postgres.schema())
            .field("endpoint_origin", &self.endpoint_origin)
            .field("signing_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[lenso::plugin(lifecycle, validate = validate_config)]
#[derive(Clone)]
struct OutboundWebhookPlugin {
    #[config]
    config: OutboundWebhookConfig,
    secrets: Port<secrets::SecretsClient>,
    http: Port<http_client::ClientClient>,
    prepared: Rc<RefCell<Option<PreparedWebhook>>>,
}

impl fmt::Debug for OutboundWebhookPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboundWebhookPlugin")
            .field("config", &self.config)
            .field("prepared", &self.prepared.borrow().is_some())
            .finish_non_exhaustive()
    }
}

#[provides(webhook::OutboundWebhook, admin::OutboundWebhookAdmin)]
impl OutboundWebhookPlugin {}

impl OutboundWebhookPlugin {
    fn enqueue(
        &self,
        context: InvocationContext,
        request: EnqueueRequest,
    ) -> NativeRequestFuture<OutboundWebhook> {
        let plugin = self.clone();
        Box::pin(async move {
            match plugin.enqueue_delivery(context, request).await {
                Ok(response) => Ok(Ok(response)),
                Err(FlowError::Domain(error)) => Ok(Err(error)),
                Err(FlowError::Runtime(error)) => Err(error),
            }
        })
    }

    fn dispatch(
        &self,
        context: InvocationContext,
        _request: DispatchRequest,
    ) -> NativeRequestFuture<OutboundWebhookAdminDispatch> {
        let plugin = self.clone();
        Box::pin(async move {
            match plugin.dispatch_delivery(context).await {
                Ok(response) => Ok(Ok(response)),
                Err(AdminFlowError::Dispatch(error)) => Ok(Err(error)),
                Err(AdminFlowError::Runtime(error)) => Err(error),
                Err(AdminFlowError::Inspect(_) | AdminFlowError::Replay(_)) => {
                    Err(service::invariant_failure("unexpected Admin flow error"))
                }
            }
        })
    }

    fn inspect(
        &self,
        context: InvocationContext,
        request: InspectRequest,
    ) -> NativeRequestFuture<OutboundWebhookAdminInspect> {
        let plugin = self.clone();
        Box::pin(async move {
            match plugin.inspect_delivery(context, request).await {
                Ok(response) => Ok(Ok(response)),
                Err(AdminFlowError::Inspect(error)) => Ok(Err(error)),
                Err(AdminFlowError::Runtime(error)) => Err(error),
                Err(AdminFlowError::Dispatch(_) | AdminFlowError::Replay(_)) => {
                    Err(service::invariant_failure("unexpected Admin flow error"))
                }
            }
        })
    }

    fn replay(
        &self,
        context: InvocationContext,
        request: ReplayRequest,
    ) -> NativeRequestFuture<OutboundWebhookAdminReplay> {
        let plugin = self.clone();
        Box::pin(async move {
            match plugin.replay_delivery(context, request).await {
                Ok(response) => Ok(Ok(response)),
                Err(AdminFlowError::Replay(error)) => Ok(Err(error)),
                Err(AdminFlowError::Runtime(error)) => Err(error),
                Err(AdminFlowError::Dispatch(_) | AdminFlowError::Inspect(_)) => {
                    Err(service::invariant_failure("unexpected Admin flow error"))
                }
            }
        })
    }
}

impl Lifecycle for OutboundWebhookPlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let dependencies = context.dependencies().clone();
        let cancellation = context.cancellation();
        let database_url = resolve_secret(
            &self.secrets,
            &dependencies,
            cancellation.clone(),
            self.config.database_url_secret(),
        )
        .await?;
        let signing_key = resolve_secret(
            &self.secrets,
            &dependencies,
            cancellation,
            self.config.signing_secret(),
        )
        .await?;
        if signing_key.len() < 32 {
            return Err(RuntimeFailure::PluginFailure {
                detail: "Outbound Webhook signing secret must contain at least 32 bytes".into(),
            });
        }
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema().to_owned()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("Outbound Webhook schema plan is invalid: {error}"),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: format!("Outbound Webhook storage is unavailable: {error}"),
        })?;
        let endpoint = self.config.endpoint();
        self.prepared.borrow_mut().replace(PreparedWebhook {
            postgres,
            endpoint_origin: config::endpoint_origin(&endpoint),
            endpoint,
            signing_key: Zeroizing::new(signing_key.as_bytes().to_vec()),
        });
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

impl OutboundWebhookPlugin {
    fn prepared(&self) -> Result<PreparedWebhook, RuntimeFailure> {
        self.prepared
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Outbound Webhook Plugin is not active".into(),
            })
    }
}

async fn resolve_secret(
    secrets: &secrets::SecretsClient,
    dependencies: &lenso_kernel::PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|response| Zeroizing::new(response.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("required Outbound Webhook secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}
