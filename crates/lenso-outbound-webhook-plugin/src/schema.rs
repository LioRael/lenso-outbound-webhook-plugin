use lenso_postgres_kit::{Migration, PlanError, SchemaPlan, sql_migrations};

const MIGRATIONS: &[Migration] = sql_migrations![
    (
        1,
        "create-outbound-webhook",
        "migrations/001_create_outbound_webhook.sql",
    ),
    (
        2,
        "index-expired-delivery-maintenance",
        "migrations/002_index_expired_delivery_maintenance.sql",
    ),
];

pub(crate) fn schema_plan(schema: impl Into<std::sync::Arc<str>>) -> Result<SchemaPlan, PlanError> {
    SchemaPlan::new(schema, MIGRATIONS)
}
