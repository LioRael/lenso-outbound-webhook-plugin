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

    postgres.pool().close().await;
    sqlx::query(AssertSqlSafe(drop_schema.as_str()))
        .execute(&mut cleanup)
        .await
        .unwrap();
}
