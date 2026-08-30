CREATE INDEX webhook_deliveries_expired_lease_idx
    ON webhook_deliveries (queue_name, lease_expires_at, delivery_id)
    WHERE status = 'delivering';
