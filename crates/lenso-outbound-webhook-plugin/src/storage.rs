use lenso_postgres_kit::OwnedPostgres;
use sqlx::Row;
use thiserror::Error;
use time::OffsetDateTime;

pub(crate) const EXPIRED_DELIVERY_MAINTENANCE_BATCH_LIMIT: i64 = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryStatus {
    Queued,
    Delivering,
    RetryScheduled,
    Delivered,
    DeadLetter,
}

impl DeliveryStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivering => "delivering",
            Self::RetryScheduled => "retry_scheduled",
            Self::Delivered => "delivered",
            Self::DeadLetter => "dead_letter",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "delivering" => Ok(Self::Delivering),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "delivered" => Ok(Self::Delivered),
            "dead_letter" => Ok(Self::DeadLetter),
            _ => Err(StoreError::Invariant("unknown delivery status")),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NewDelivery {
    pub(crate) delivery_id: String,
    pub(crate) event_id: String,
    pub(crate) event_type: String,
    pub(crate) endpoint_url: String,
    pub(crate) endpoint_origin: String,
    pub(crate) payload_snapshot: Vec<u8>,
    pub(crate) payload_sha256: String,
    pub(crate) queue_name: String,
    pub(crate) max_attempts: i64,
    pub(crate) available_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryRecord {
    pub(crate) delivery_id: String,
    pub(crate) event_id: String,
    pub(crate) event_type: String,
    pub(crate) endpoint_url: String,
    pub(crate) endpoint_origin: String,
    pub(crate) payload_snapshot: Vec<u8>,
    pub(crate) payload_sha256: String,
    pub(crate) queue_name: String,
    pub(crate) status: DeliveryStatus,
    pub(crate) attempts: i64,
    pub(crate) max_attempts: i64,
    pub(crate) replay_count: i64,
    pub(crate) last_receipt: Option<DeliveryReceipt>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryReceipt {
    pub(crate) attempt: i64,
    pub(crate) outcome: String,
    pub(crate) http_status: Option<i64>,
    pub(crate) response_sha256: Option<String>,
    pub(crate) occurred_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub(crate) enum InsertOutcome {
    Created(DeliveryRecord),
    Existing(DeliveryRecord),
    Conflict,
}

pub(crate) async fn insert_or_get(
    postgres: &OwnedPostgres,
    delivery: NewDelivery,
) -> Result<InsertOutcome, StoreError> {
    let mut transaction = postgres.pool().begin().await.map_err(db("begin enqueue"))?;
    let inserted = sqlx::query(
        "INSERT INTO webhook_deliveries \
         (delivery_id, event_id, event_type, endpoint_url, endpoint_origin, payload_snapshot, \
          payload_sha256, queue_name, status, max_attempts, available_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', $9, $10) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&delivery.delivery_id)
    .bind(&delivery.event_id)
    .bind(&delivery.event_type)
    .bind(&delivery.endpoint_url)
    .bind(&delivery.endpoint_origin)
    .bind(&delivery.payload_snapshot)
    .bind(&delivery.payload_sha256)
    .bind(&delivery.queue_name)
    .bind(i32::try_from(delivery.max_attempts).expect("validated attempts"))
    .bind(delivery.available_at)
    .execute(&mut *transaction)
    .await
    .map_err(db("insert delivery"))?;
    if inserted.rows_affected() == 1 {
        transaction
            .commit()
            .await
            .map_err(db("commit delivery enqueue"))?;
        return Ok(InsertOutcome::Created(DeliveryRecord {
            delivery_id: delivery.delivery_id,
            event_id: delivery.event_id,
            event_type: delivery.event_type,
            endpoint_url: delivery.endpoint_url,
            endpoint_origin: delivery.endpoint_origin,
            payload_snapshot: delivery.payload_snapshot,
            payload_sha256: delivery.payload_sha256,
            queue_name: delivery.queue_name,
            status: DeliveryStatus::Queued,
            attempts: 0,
            max_attempts: delivery.max_attempts,
            replay_count: 0,
            last_receipt: None,
        }));
    }

    let row = sqlx::query(
        "SELECT delivery_id, event_id, event_type, endpoint_url, endpoint_origin, \
                payload_snapshot, payload_sha256, queue_name, status, attempts, \
                max_attempts, replay_count \
         FROM webhook_deliveries WHERE event_id = $1 FOR UPDATE",
    )
    .bind(&delivery.event_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(db("read idempotent delivery"))?;
    let Some(row) = row else {
        return Err(StoreError::Invariant("delivery identity collision"));
    };
    let existing = decode_delivery(&row)?;
    let same = existing.event_type == delivery.event_type
        && existing.endpoint_url == delivery.endpoint_url
        && existing.endpoint_origin == delivery.endpoint_origin
        && existing.payload_snapshot == delivery.payload_snapshot
        && existing.payload_sha256 == delivery.payload_sha256
        && existing.queue_name == delivery.queue_name;
    transaction
        .commit()
        .await
        .map_err(db("commit idempotent delivery"))?;
    Ok(if same {
        InsertOutcome::Existing(existing)
    } else {
        InsertOutcome::Conflict
    })
}

pub(crate) async fn load(
    postgres: &OwnedPostgres,
    delivery_id: &str,
) -> Result<Option<DeliveryRecord>, StoreError> {
    let row = sqlx::query(
        "SELECT delivery_id, event_id, event_type, endpoint_url, endpoint_origin, \
                payload_snapshot, payload_sha256, queue_name, status, attempts, \
                max_attempts, replay_count \
         FROM webhook_deliveries WHERE delivery_id = $1",
    )
    .bind(delivery_id)
    .fetch_optional(postgres.pool())
    .await
    .map_err(db("load delivery"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut delivery = decode_delivery(&row)?;
    delivery.last_receipt = last_receipt(postgres, delivery_id).await?;
    Ok(Some(delivery))
}

/// Claims one due delivery with a bounded lease. Expired leases are receipted before recovery.
pub(crate) async fn claim_due(
    postgres: &OwnedPostgres,
    queue_name: &str,
    lease_seconds: i64,
) -> Result<Option<DeliveryRecord>, StoreError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(db("begin delivery claim"))?;

    let row = sqlx::query(
        "WITH expired AS MATERIALIZED ( \
             SELECT delivery_id, replay_count, attempts, max_attempts \
             FROM webhook_deliveries \
             WHERE queue_name = $1 AND status = 'delivering' \
               AND lease_expires_at <= transaction_timestamp() \
             ORDER BY lease_expires_at, delivery_id \
             FOR UPDATE SKIP LOCKED LIMIT $3 \
         ), receipted AS ( \
             INSERT INTO webhook_delivery_attempts \
                 (delivery_id, replay_count, attempt, outcome, occurred_at) \
             SELECT delivery_id, replay_count, attempts, 'lease_expired', \
                    transaction_timestamp() \
             FROM expired \
             ON CONFLICT (delivery_id, replay_count, attempt) DO NOTHING \
         ), retired AS ( \
             UPDATE webhook_deliveries AS delivery \
             SET status = 'dead_letter', lease_expires_at = NULL, \
                 updated_at = transaction_timestamp() \
             FROM expired \
             WHERE delivery.delivery_id = expired.delivery_id \
               AND expired.attempts >= expired.max_attempts \
             RETURNING delivery.delivery_id \
         ), candidate AS ( \
             SELECT delivery.delivery_id FROM webhook_deliveries AS delivery \
             WHERE delivery.queue_name = $1 \
               AND delivery.attempts < delivery.max_attempts AND ( \
                 (delivery.status IN ('queued', 'retry_scheduled') \
                     AND delivery.available_at <= transaction_timestamp()) \
                 OR (delivery.status = 'delivering' \
                     AND delivery.lease_expires_at <= transaction_timestamp() \
                     AND delivery.delivery_id IN (SELECT delivery_id FROM expired)) \
             ) \
             ORDER BY delivery.available_at, delivery.created_at, delivery.delivery_id \
             FOR UPDATE OF delivery SKIP LOCKED LIMIT 1 \
         ) \
         UPDATE webhook_deliveries AS delivery \
         SET status = 'delivering', attempts = delivery.attempts + 1, \
             lease_expires_at = transaction_timestamp() \
                 + make_interval(secs => $2::double precision), \
             updated_at = transaction_timestamp() \
         FROM candidate WHERE delivery.delivery_id = candidate.delivery_id \
         RETURNING delivery.delivery_id, delivery.event_id, delivery.event_type, \
                   delivery.endpoint_url, delivery.endpoint_origin, delivery.payload_snapshot, \
                   delivery.payload_sha256, delivery.queue_name, delivery.status, \
                   delivery.attempts, delivery.max_attempts, \
                   delivery.replay_count",
    )
    .bind(queue_name)
    .bind(f64::from(
        i32::try_from(lease_seconds).expect("validated lease seconds"),
    ))
    .bind(EXPIRED_DELIVERY_MAINTENANCE_BATCH_LIMIT)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(db("claim due delivery"))?;
    transaction
        .commit()
        .await
        .map_err(db("commit delivery claim"))?;
    row.as_ref().map(decode_delivery).transpose()
}

/// Persists one immutable attempt receipt and fences the state transition to its active lease.
pub(crate) async fn record_outcome(
    postgres: &OwnedPostgres,
    delivery_id: &str,
    replay_count: i64,
    receipt: &DeliveryReceipt,
    status: DeliveryStatus,
    next_available_at: Option<OffsetDateTime>,
) -> Result<(), StoreError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(db("begin record receipt"))?;
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts \
         (delivery_id, replay_count, attempt, outcome, http_status, response_sha256, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (delivery_id, replay_count, attempt) DO NOTHING",
    )
    .bind(delivery_id)
    .bind(i32::try_from(replay_count).expect("bounded replay count"))
    .bind(i32::try_from(receipt.attempt).expect("bounded attempt"))
    .bind(&receipt.outcome)
    .bind(
        receipt
            .http_status
            .map(|status| i32::try_from(status).expect("HTTP status")),
    )
    .bind(&receipt.response_sha256)
    .bind(receipt.occurred_at)
    .execute(&mut *transaction)
    .await
    .map_err(db("insert delivery receipt"))?;
    let updated = sqlx::query(
        "UPDATE webhook_deliveries \
         SET status = $4, available_at = COALESCE($5, available_at), \
             lease_expires_at = CASE WHEN $4 = 'delivering' THEN lease_expires_at ELSE NULL END, \
             updated_at = transaction_timestamp() \
         WHERE delivery_id = $1 AND replay_count = $2 AND attempts = $3 \
           AND status = 'delivering' AND lease_expires_at > transaction_timestamp()",
    )
    .bind(delivery_id)
    .bind(i32::try_from(replay_count).expect("bounded replay count"))
    .bind(i32::try_from(receipt.attempt).expect("bounded attempt"))
    .bind(status.as_str())
    .bind(next_available_at)
    .execute(&mut *transaction)
    .await
    .map_err(db("update delivery outcome"))?;
    ensure_updated(updated.rows_affected(), "record fenced delivery outcome")?;
    transaction
        .commit()
        .await
        .map_err(db("commit delivery receipt"))
}

pub(crate) async fn begin_replay(
    postgres: &OwnedPostgres,
    delivery_id: &str,
) -> Result<Option<DeliveryRecord>, StoreError> {
    let row = sqlx::query(
        "UPDATE webhook_deliveries \
         SET replay_count = replay_count + 1, status = 'queued', attempts = 0, \
             available_at = transaction_timestamp(), lease_expires_at = NULL, \
             updated_at = transaction_timestamp() \
         WHERE delivery_id = $1 AND status = 'dead_letter' AND replay_count < 1000000 \
         RETURNING delivery_id, event_id, event_type, endpoint_url, endpoint_origin, \
                   payload_snapshot, payload_sha256, queue_name, status, attempts, \
                   max_attempts, replay_count",
    )
    .bind(delivery_id)
    .fetch_optional(postgres.pool())
    .await
    .map_err(db("begin dead-letter replay"))?;
    row.as_ref().map(decode_delivery).transpose()
}

async fn last_receipt(
    postgres: &OwnedPostgres,
    delivery_id: &str,
) -> Result<Option<DeliveryReceipt>, StoreError> {
    let row = sqlx::query(
        "SELECT attempt, outcome, http_status, response_sha256, occurred_at \
         FROM webhook_delivery_attempts WHERE delivery_id = $1 \
         ORDER BY replay_count DESC, attempt DESC LIMIT 1",
    )
    .bind(delivery_id)
    .fetch_optional(postgres.pool())
    .await
    .map_err(db("load delivery receipt"))?;
    row.as_ref().map(decode_receipt).transpose()
}

fn decode_delivery(row: &sqlx::postgres::PgRow) -> Result<DeliveryRecord, StoreError> {
    let status: String = decode(row, "status", "decode delivery")?;
    Ok(DeliveryRecord {
        delivery_id: decode(row, "delivery_id", "decode delivery")?,
        event_id: decode(row, "event_id", "decode delivery")?,
        event_type: decode(row, "event_type", "decode delivery")?,
        endpoint_url: decode(row, "endpoint_url", "decode delivery")?,
        endpoint_origin: decode(row, "endpoint_origin", "decode delivery")?,
        payload_snapshot: decode(row, "payload_snapshot", "decode delivery")?,
        payload_sha256: decode(row, "payload_sha256", "decode delivery")?,
        queue_name: decode(row, "queue_name", "decode delivery")?,
        status: DeliveryStatus::parse(&status)?,
        attempts: i64::from(decode::<i32>(row, "attempts", "decode delivery")?),
        max_attempts: i64::from(decode::<i32>(row, "max_attempts", "decode delivery")?),
        replay_count: i64::from(decode::<i32>(row, "replay_count", "decode delivery")?),
        last_receipt: None,
    })
}

fn decode_receipt(row: &sqlx::postgres::PgRow) -> Result<DeliveryReceipt, StoreError> {
    Ok(DeliveryReceipt {
        attempt: i64::from(decode::<i32>(row, "attempt", "decode receipt")?),
        outcome: decode(row, "outcome", "decode receipt")?,
        http_status: decode::<Option<i32>>(row, "http_status", "decode receipt")?.map(i64::from),
        response_sha256: decode(row, "response_sha256", "decode receipt")?,
        occurred_at: decode(row, "occurred_at", "decode receipt")?,
    })
}

fn decode<T>(
    row: &sqlx::postgres::PgRow,
    column: &'static str,
    operation: &'static str,
) -> Result<T, StoreError>
where
    for<'row> T: sqlx::Decode<'row, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(column)
        .map_err(|source| StoreError::Database { operation, source })
}

fn ensure_updated(rows: u64, operation: &'static str) -> Result<(), StoreError> {
    if rows == 1 {
        Ok(())
    } else {
        Err(StoreError::Invariant(operation))
    }
}

fn db(operation: &'static str) -> impl Fn(sqlx::Error) -> StoreError {
    move |source| StoreError::Database { operation, source }
}

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("PostgreSQL operation `{operation}` failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("Outbound Webhook persistence invariant failed: {0}")]
    Invariant(&'static str),
}
