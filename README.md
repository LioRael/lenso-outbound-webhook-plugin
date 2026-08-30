# Lenso Outbound Webhook Plugin

`lenso-outbound-webhook-plugin` is a removable, durable outbound Webhook Plugin for Lenso Apps.
It snapshots one event, queues it in its private PostgreSQL schema, signs every delivery, and
records retry, receipt, replay, and dead-letter state.

## First runnable slice

The source-first contracts are:

- `lenso.outbound-webhook@1`: `enqueue`
- `lenso.outbound-webhook-admin@1`: `dispatch`, `inspect`, and `replay`

`enqueue` accepts an application-owned stable `event_id`, event type, and JSON payload. An exact
duplicate returns the existing delivery; reuse of the same `event_id` with different immutable
content is rejected. The Plugin persists the exact serialized envelope before it reports success.

`dispatch` is an explicitly authorized worker trigger. One call claims at most one due row under a
bounded PostgreSQL lease and attempts one HTTP POST. It returns `idle`, `delivered`,
`retry_scheduled`, or `dead_lettered`. There is no background task or hidden scheduler in this
Plugin.

Delivery is at-least-once. A timeout or worker loss can make the remote outcome ambiguous, so the
receiver must deduplicate on the stable delivery identity in `x-lenso-webhook-id`. The original
business identity remains available as `x-lenso-webhook-event-id`. Expired leases are receipted and reclaimed;
attempt-number fencing prevents a late worker from changing newer state. Retryable failures are
timeouts, transport failures, HTTP 408, HTTP 429, and HTTP 5xx. Retry delay is bounded exponential
backoff. All other failures, and exhausted attempts, become dead letters. `replay` starts a new
attempt cycle over the original payload snapshot and increments `replay_count`.

## Authority and dependencies

One immutable Plugin Instance configures exactly one HTTP(S) endpoint URL and its derived origin.
The request contract contains no destination field. Before every attempt, the Plugin checks the
stored URL and origin against the active configuration. The Host must separately bind an HTTP
Client provider whose egress policy allows exactly that configured origin; runtime requests cannot
expand the allowlist.

The runnable slice has real required Ports for:

- `lenso.secrets@1`, used during activation for the PostgreSQL URL and HMAC-SHA256 signing key;
- `lenso.http-client@1`, used for the outbound POST under Host-owned egress policy.

Producer and Admin caller Instance keys are separate immutable allowlists. Missing caller identity
fails closed. Only callers in `admin_instances` can dispatch, inspect receipts, or replay a dead
letter.

The current configuration shape is:

```json
{
  "schema": "outbound_webhooks",
  "database_url_secret": "outbound-webhook/database-url",
  "signing_secret": "outbound-webhook/signing-key",
  "endpoint_url": "https://hooks.example.com/lenso/events",
  "queue_name": "default",
  "max_attempts": 5,
  "max_payload_bytes": 262144,
  "lease_seconds": 30,
  "retry_base_seconds": 5,
  "retry_max_seconds": 300,
  "producer_instances": ["orders"],
  "admin_instances": ["outbound-webhook-worker", "operations"]
}
```

The signing header is `x-lenso-webhook-signature: v1=<hex-hmac-sha256>`. The signed bytes are
`delivery_id + "." + RFC3339 timestamp + "." + exact payload snapshot`. Responses are not retained;
only status and a SHA-256 body digest are stored as receipt evidence.

## PostgreSQL ownership and operations

PostgreSQL is private Plugin persistence. The migration owns the delivery queue, attempt receipts,
due index, leases, retry time, and replay cycles. App activation only verifies an already-current
managed schema. Setup and upgrades are explicit operator work:

```rust,no_run
use lenso_outbound_webhook_plugin::OutboundWebhookOperator;

# async fn setup(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
OutboundWebhookOperator::setup(database_url, "outbound_webhooks").await?;
# Ok(())
# }
```

## Automatic scheduling gap

Automatic worker scheduling is intentionally not claimed in v1. Published Jobs 0.1.1 and the
current Plugin-authoring/HTTP baseline resolve `lenso-plugin-authoring` from different Git source
identities, so a `Port<JobsClient>` cannot satisfy the current `CapabilityClient` trait even though
the Jobs wire contract is otherwise suitable. This repository does not add a compatibility
newtype, a temporary path dependency, or a private scheduler.

The minimum Jobs-owner upgrade is to publish `lenso-capability-jobs` against the same
`lenso-plugin-authoring` source identity used by the current Host and HTTP Client provider. No Jobs
operation or schema change is required for that alignment. After the provider set is aligned, a
separate slice can use Jobs only to trigger the already-public Admin `dispatch` operation; the
Webhook Plugin remains the owner of delivery state and retry semantics.

Until then, an explicitly configured worker or operator invokes `dispatch` repeatedly. It must use
an exact caller Instance key listed in `admin_instances`.

## Development

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
```

The PostgreSQL acceptance test requires a disposable database whose name starts with
`lenso_outbound_webhook_test`:

```sh
LENSO_OUTBOUND_WEBHOOK_TEST_DATABASE_URL=postgres://.../lenso_outbound_webhook_test \
  cargo test --locked -p lenso-outbound-webhook-plugin --features postgres-acceptance
```
