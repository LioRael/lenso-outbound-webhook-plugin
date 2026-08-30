# Outbound Webhook Plugin card

## Owner and deletion boundary

`lenso-outbound-webhook-plugin` owns Webhook delivery identity, the immutable event/payload
snapshot, fixed endpoint snapshot, named due queue, attempt count, lease recovery, retry schedule,
HMAC signing, receipts, replay cycles, and dead letters. Removing the Plugin Instance and its owned
PostgreSQL schema removes all of that behavior and state. Kernel, Web, Jobs, and business Plugins
retain no private Webhook table or delivery registry.

The producing business Plugin owns the meaning of the event and must supply a stable `event_id`.
The remote receiver owns effect idempotency because delivery is at-least-once. The Host owns the
actual network grant and must restrict the bound HTTP Client to the configured origin.

## Roles

- Provides `lenso.outbound-webhook@1` to exact `producer_instances`; `enqueue` durably snapshots an
  event and returns its stable delivery identity.
- Provides `lenso.outbound-webhook-admin@1` to exact `admin_instances`; `dispatch` claims one due
  delivery, `inspect` returns durable state and the last receipt, and `replay` requeues only a dead
  letter using the original snapshot.
- Requires `lenso.secrets@1` during activation for the PostgreSQL URL and signing key.
- Requires `lenso.http-client@1` for each signed POST. The Plugin supplies only the one
  Host-configured endpoint URL.

## Lifecycle and private state

Composition supplies immutable schema, secret references, endpoint, queue/retry/lease limits, and
caller allowlists. Activation resolves Secrets and verifies the exact authored PostgreSQL schema;
it never installs or upgrades migrations. Deactivation closes the owned pool. Explicit
`OutboundWebhookOperator::setup` and `upgrade` calls are the only schema mutation workflow.

`enqueue` inserts a `queued` row transactionally. `dispatch` uses `FOR UPDATE SKIP LOCKED`, an
expiring lease, and the monotonically increasing attempt number as a fencing condition. Every
completed attempt stores an immutable receipt. A retry stores its next due time; exhausted or
non-retryable failures enter `dead_letter`. Replay increments its cycle, resets attempts, and keeps
the original bytes and earlier receipts.

No background thread, timer, scheduler, or mutable endpoint registry exists in this slice.

## First observable workflow

1. An allowed producer enqueues `event_id=order-42`; the payload snapshot is committed before the
   Capability responds.
2. An allowed worker invokes Admin `dispatch`; the Plugin claims one due row and signs the stable
   delivery identity, timestamp, and exact snapshot.
3. The bound HTTP Client enforces Host egress policy and sends the POST to the fixed endpoint.
4. The Plugin records delivery, bounded retry, or dead-letter evidence. Operations can inspect it
   and explicitly replay a dead letter.

Missing caller identity, a changed endpoint snapshot, invalid event identity, stale attempt, and
unavailable dependencies all fail closed. HTTP response bodies are reduced to a SHA-256 digest.

## Known capability gap

Automatic scheduling remains blocked, not simulated. Jobs 0.1.1 uses a different
`lenso-plugin-authoring` Git source identity from the current authoring/HTTP provider baseline, so
its generated client cannot currently be used as a modern `Port` without compatibility glue. This
Plugin deliberately has no Jobs Port in v1.

The minimum owner-side fix is a Jobs capability release generated and compiled against the same
authoring source set as the current Host and HTTP Client provider. The Jobs wire semantics need not
change. A later integration can trigger Admin `dispatch`; delivery queue, lease, retry, receipt,
replay, and dead-letter authority stay here.

## Deferred slices

- automatic Jobs-backed dispatch triggering after source-identity alignment;
- endpoint rotation/multi-endpoint routing as a new explicit policy and migration;
- Web or Console management surfaces;
- delivery history pagination beyond the last receipt;
- receiver verification libraries and secret rotation.
