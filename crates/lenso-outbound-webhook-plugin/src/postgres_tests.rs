use lenso_postgres_kit::OwnedPostgres;
use sqlx::{AssertSqlSafe, Connection};
use time::OffsetDateTime;
use url::Url;

use crate::{
    OutboundWebhookOperator,
    protocol::stable_delivery_id,
    schema,
    storage::{self, DeliveryReceipt, DeliveryStatus, InsertOutcome, NewDelivery},
};

fn delivery(event_type: &str) -> NewDelivery {
    let event_id = "order-42".to_owned();
    NewDelivery {
        delivery_id: stable_delivery_id("https://hooks.example.test/events", &event_id),
        event_id,
        event_type: event_type.to_owned(),
        endpoint_url: "https://hooks.example.test/events".to_owned(),
        endpoint_origin: "https://hooks.example.test".to_owned(),
        payload_snapshot: br#"{"event_id":"order-42","payload":{"total":42}}"#.to_vec(),
        payload_sha256: "snapshot-sha256".to_owned(),
        queue_name: "webhooks".to_owned(),
        max_attempts: 3,
        available_at: OffsetDateTime::now_utc(),
    }
}

fn receipt(attempt: i64, outcome: &str) -> DeliveryReceipt {
    DeliveryReceipt {
        attempt,
        outcome: outcome.to_owned(),
        http_status: None,
        response_sha256: None,
        occurred_at: OffsetDateTime::now_utc(),
    }
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn postgres_queue_preserves_idempotency_fencing_retry_receipts_and_replay() {
    let Some(database_url) = std::env::var("LENSO_OUTBOUND_WEBHOOK_TEST_DATABASE_URL").ok() else {
        eprintln!(
            "skipping PostgreSQL acceptance; LENSO_OUTBOUND_WEBHOOK_TEST_DATABASE_URL is unset"
        );
        return;
    };
    let parsed = Url::parse(&database_url).expect("test database URL must be valid");
    assert!(
        parsed
            .path()
            .trim_start_matches('/')
            .starts_with("lenso_outbound_webhook_test"),
        "acceptance requires a disposable lenso_outbound_webhook_test* database"
    );

    let schema_name = format!("outbound_webhook_acceptance_{}", std::process::id());
    let mut cleanup = sqlx::PgConnection::connect(&database_url).await.unwrap();
    let drop_schema = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    sqlx::query(AssertSqlSafe(drop_schema.as_str()))
        .execute(&mut cleanup)
        .await
        .unwrap();

    OutboundWebhookOperator::setup(&database_url, &schema_name)
        .await
        .expect("operator setup should install the owned schema");
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.as_str()).unwrap(),
    )
    .await
    .expect("runtime preparation should verify the exact schema");

    let first = storage::insert_or_get(&postgres, delivery("order.created"))
        .await
        .unwrap();
    let delivery_id = match first {
        InsertOutcome::Created(record) => record.delivery_id,
        _ => panic!("first enqueue must create a durable queue row"),
    };
    assert!(matches!(
        storage::insert_or_get(&postgres, delivery("order.created"))
            .await
            .unwrap(),
        InsertOutcome::Existing(_)
    ));
    assert!(matches!(
        storage::insert_or_get(&postgres, delivery("order.cancelled"))
            .await
            .unwrap(),
        InsertOutcome::Conflict
    ));

    let first_claim = storage::claim_due(&postgres, "webhooks", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_claim.attempts, 1);
    sqlx::query(
        "UPDATE webhook_deliveries SET lease_expires_at = transaction_timestamp() - interval '1 second' \
         WHERE delivery_id = $1",
    )
    .bind(&delivery_id)
    .execute(postgres.pool())
    .await
    .unwrap();
    let second_claim = storage::claim_due(&postgres, "webhooks", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_claim.attempts, 2);
    assert!(
        storage::record_outcome(
            &postgres,
            &delivery_id,
            0,
            &receipt(1, "late_success"),
            DeliveryStatus::Delivered,
            None,
        )
        .await
        .is_err(),
        "an expired attempt must be fenced from changing state"
    );

    storage::record_outcome(
        &postgres,
        &delivery_id,
        0,
        &receipt(2, "http_status_503"),
        DeliveryStatus::RetryScheduled,
        Some(OffsetDateTime::now_utc()),
    )
    .await
    .unwrap();
    let final_claim = storage::claim_due(&postgres, "webhooks", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_claim.attempts, 3);
    storage::record_outcome(
        &postgres,
        &delivery_id,
        0,
        &receipt(3, "http_status_503"),
        DeliveryStatus::DeadLetter,
        None,
    )
    .await
    .unwrap();
    let dead = storage::load(&postgres, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dead.status, DeliveryStatus::DeadLetter);
    assert_eq!(dead.last_receipt.unwrap().attempt, 3);

    let replay = storage::begin_replay(&postgres, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.replay_count, 1);
    assert_eq!(replay.attempts, 0);
    assert_eq!(
        replay.payload_snapshot,
        delivery("order.created").payload_snapshot
    );
    let replay_claim = storage::claim_due(&postgres, "webhooks", 30)
        .await
        .unwrap()
        .unwrap();
    storage::record_outcome(
        &postgres,
        &delivery_id,
        1,
        &receipt(replay_claim.attempts, "delivered"),
        DeliveryStatus::Delivered,
        None,
    )
    .await
    .unwrap();
    let delivered = storage::load(&postgres, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.status, DeliveryStatus::Delivered);
    assert_eq!(delivered.replay_count, 1);
    assert_eq!(delivered.last_receipt.unwrap().outcome, "delivered");
    assert!(
        storage::begin_replay(&postgres, &delivery_id)
            .await
            .unwrap()
            .is_none(),
        "a delivered cycle cannot be replayed"
    );
    assert!(
        storage::record_outcome(
            &postgres,
            &delivery_id,
            1,
            &receipt(replay_claim.attempts, "late_failure"),
            DeliveryStatus::DeadLetter,
            None,
        )
        .await
        .is_err(),
        "terminal success and its immutable receipt cannot be overwritten"
    );
    assert_eq!(
        storage::load(&postgres, &delivery_id)
            .await
            .unwrap()
            .unwrap()
            .last_receipt
            .unwrap()
            .outcome,
        "delivered"
    );

    bounded_expired_maintenance_acceptance(&postgres).await;

    postgres.pool().close().await;
    sqlx::query(AssertSqlSafe(drop_schema.as_str()))
        .execute(&mut cleanup)
        .await
        .unwrap();
}

async fn bounded_expired_maintenance_acceptance(postgres: &OwnedPostgres) {
    const EXHAUSTED_DELIVERIES: i64 = storage::EXPIRED_DELIVERY_MAINTENANCE_BATCH_LIMIT * 2 + 16;
    seed_expired_maintenance_backlog(postgres, EXHAUSTED_DELIVERIES).await;

    let recovered = storage::claim_due(postgres, "backlog", 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.delivery_id, "backlog-retryable");
    assert_eq!(recovered.attempts, 2);
    let (first_receipts, first_retired): (i64, i64) = sqlx::query_as(
        "SELECT \
             (SELECT count(*) FROM webhook_delivery_attempts AS attempt \
              JOIN webhook_deliveries AS delivery USING (delivery_id) \
              WHERE delivery.queue_name = 'backlog'), \
             (SELECT count(*) FROM webhook_deliveries \
              WHERE queue_name = 'backlog' AND status = 'dead_letter')",
    )
    .fetch_one(postgres.pool())
    .await
    .unwrap();
    assert_eq!(
        first_receipts,
        storage::EXPIRED_DELIVERY_MAINTENANCE_BATCH_LIMIT
    );
    assert_eq!(first_retired, first_receipts - 1);

    let (first_worker, second_worker) = tokio::join!(
        storage::claim_due(postgres, "backlog", 30),
        storage::claim_due(postgres, "backlog", 30),
    );
    let first_worker = first_worker.unwrap().unwrap();
    let second_worker = second_worker.unwrap().unwrap();
    assert_ne!(first_worker.delivery_id, second_worker.delivery_id);
    assert!(first_worker.delivery_id.starts_with("backlog-queued-"));
    assert!(second_worker.delivery_id.starts_with("backlog-queued-"));

    let (receipts, unique_receipts, retired): (i64, i64, i64) = sqlx::query_as(
        "SELECT \
             count(*), \
             count(DISTINCT (attempt.delivery_id, attempt.replay_count, attempt.attempt)), \
             count(*) FILTER (WHERE delivery.status = 'dead_letter') \
         FROM webhook_delivery_attempts AS attempt \
         JOIN webhook_deliveries AS delivery USING (delivery_id) \
         WHERE delivery.queue_name = 'backlog'",
    )
    .fetch_one(postgres.pool())
    .await
    .unwrap();
    assert_eq!(receipts, EXHAUSTED_DELIVERIES + 1);
    assert_eq!(unique_receipts, receipts);
    assert_eq!(retired, EXHAUSTED_DELIVERIES);

    assert!(
        storage::claim_due(postgres, "backlog", 30)
            .await
            .unwrap()
            .is_none()
    );
    let receipts_after_idle_claim: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM webhook_delivery_attempts AS attempt \
         JOIN webhook_deliveries AS delivery USING (delivery_id) \
         WHERE delivery.queue_name = 'backlog'",
    )
    .fetch_one(postgres.pool())
    .await
    .unwrap();
    assert_eq!(receipts_after_idle_claim, receipts);
}

async fn seed_expired_maintenance_backlog(postgres: &OwnedPostgres, exhausted_deliveries: i64) {
    sqlx::query(
        "INSERT INTO webhook_deliveries (\
             delivery_id, event_id, event_type, endpoint_url, endpoint_origin, \
             payload_snapshot, payload_sha256, queue_name, status, available_at, \
             lease_expires_at, attempts, max_attempts\
         ) \
         SELECT 'backlog-exhausted-' || item, 'backlog-event-' || item, 'backlog.test', \
                'https://hooks.example.test/events', 'https://hooks.example.test', \
                '{}'::bytea, 'snapshot-sha256', 'backlog', 'delivering', \
                transaction_timestamp() - interval '1 hour', \
                transaction_timestamp() - interval '1 hour', 3, 3 \
         FROM generate_series(1, $1) AS item",
    )
    .bind(exhausted_deliveries)
    .execute(postgres.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO webhook_deliveries (\
             delivery_id, event_id, event_type, endpoint_url, endpoint_origin, \
             payload_snapshot, payload_sha256, queue_name, status, available_at, \
             lease_expires_at, attempts, max_attempts\
         ) VALUES \
             ('backlog-retryable', 'backlog-retryable-event', 'backlog.test', \
              'https://hooks.example.test/events', 'https://hooks.example.test', \
              '{}'::bytea, 'snapshot-sha256', 'backlog', 'delivering', \
              transaction_timestamp() - interval '4 hours', \
              transaction_timestamp() - interval '2 hours', 1, 3), \
             ('backlog-queued-1', 'backlog-queued-event-1', 'backlog.test', \
              'https://hooks.example.test/events', 'https://hooks.example.test', \
              '{}'::bytea, 'snapshot-sha256', 'backlog', 'queued', \
              transaction_timestamp(), NULL, 0, 3), \
             ('backlog-queued-2', 'backlog-queued-event-2', 'backlog.test', \
              'https://hooks.example.test/events', 'https://hooks.example.test', \
              '{}'::bytea, 'snapshot-sha256', 'backlog', 'queued', \
              transaction_timestamp(), NULL, 0, 3)",
    )
    .execute(postgres.pool())
    .await
    .unwrap();
}
