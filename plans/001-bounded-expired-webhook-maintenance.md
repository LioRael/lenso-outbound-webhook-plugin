# Plan 001: Bound expired-webhook maintenance performed by each claim

> Drift check: `git diff --stat 3a17aee..HEAD -- crates/lenso-outbound-webhook-plugin/src/storage.rs crates/lenso-outbound-webhook-plugin/migrations crates/lenso-outbound-webhook-plugin/tests`.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `3a17aee`, 2026-08-30

## Why this matters

Each single-delivery claim inserts receipts for all expired leases and updates all
exhausted deliveries. Repeated claims over a backlog create avoidable scans, conflicts,
locks, and WAL before useful work starts.

## Current state

- `src/storage.rs:197-209` scans all expired leases for receipt insertion.
- `src/storage.rs:211-221` retires all exhausted rows.
- `src/storage.rs:223-245` claims one row with `SKIP LOCKED`.

## Scope

In scope: webhook storage, additive indexes/migrations, and PostgreSQL backlog tests.
Out of scope: delivery retry policy, receipt identity, replay count, and stale-worker fencing.

## Steps

1. Add concurrent backlog tests covering receipt uniqueness, bounded maintenance,
   exhausted retirement, and recovery of retryable expired deliveries.
2. Select a bounded expired batch with `FOR UPDATE SKIP LOCKED`; create receipts and
   retire exhausted rows only for those selected IDs, then claim one due delivery.
3. Add a lease-expiry index supporting the selection and confirm normal due ordering.

## Verification

- `cargo test -p lenso-outbound-webhook-plugin --include-ignored` -> all pass.
- `cargo check -p lenso-outbound-webhook-plugin --all-targets` -> exit 0.
- `git diff --check` -> no output.

## STOP conditions

Stop if a refactor would weaken receipt idempotency or lease-generation fencing.
