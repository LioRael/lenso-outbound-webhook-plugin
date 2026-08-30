use lenso_postgres_kit::{
    OwnedPostgres, PostgresKitError, SchemaOperator, SetupOutcome, UpgradeOutcome,
};
use thiserror::Error;

use crate::schema::schema_plan;

/// Explicit schema administration for one Outbound Webhook Instance.
#[derive(Clone, Debug)]
pub struct OutboundWebhookOperator {
    postgres: OwnedPostgres,
}

impl OutboundWebhookOperator {
    /// Creates a missing schema and installs all authored migrations.
    pub async fn setup(
        database_url: &str,
        schema: &str,
    ) -> Result<SetupOutcome, OutboundWebhookOperatorError> {
        Ok(SchemaOperator::connect(database_url, schema_plan(schema)?)
            .await?
            .setup()
            .await?)
    }

    /// Applies pending authored migrations to an existing managed schema.
    pub async fn upgrade(
        database_url: &str,
        schema: &str,
    ) -> Result<UpgradeOutcome, OutboundWebhookOperatorError> {
        Ok(SchemaOperator::connect(database_url, schema_plan(schema)?)
            .await?
            .upgrade()
            .await?)
    }

    /// Connects only when the exact authored schema is already installed.
    pub async fn connect(
        database_url: &str,
        schema: &str,
    ) -> Result<Self, OutboundWebhookOperatorError> {
        Ok(Self {
            postgres: OwnedPostgres::prepare(database_url, schema_plan(schema)?).await?,
        })
    }

    /// Returns the verified Plugin-owned schema name.
    pub fn schema(&self) -> &str {
        self.postgres.schema()
    }
}

/// Failure from the explicit Outbound Webhook schema workflow.
#[derive(Debug, Error)]
pub enum OutboundWebhookOperatorError {
    #[error(transparent)]
    Plan(#[from] lenso_postgres_kit::PlanError),
    #[error(transparent)]
    Postgres(#[from] PostgresKitError),
}
