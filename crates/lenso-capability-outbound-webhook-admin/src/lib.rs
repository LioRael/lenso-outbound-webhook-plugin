//! Portable operational contract for one Outbound Webhook Plugin Instance.

#[allow(dead_code)]
mod contract;

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_and_replay_contracts_round_trip() {
        let dispatch = DispatchResponse {
            delivery_id: Some("wh_012345678901234567890123456789012345678901234567".into()),
            outcome: DispatchResponseOutcome::Delivered,
            receipt: Some(DispatchResponseReceipt {
                attempt: 1,
                http_status: Some("204".into()),
                occurred_at: "2026-08-30T00:00:00Z".into(),
                outcome: "delivered".into(),
                response_sha256: Some("abc".into()),
            }),
        };
        let wire = encode_dispatch_response(&dispatch).unwrap();
        assert_eq!(decode_dispatch_response(&wire).unwrap(), dispatch);

        let replay = ReplayResponse { replay_count: 1 };
        let wire = encode_replay_response(&replay).unwrap();
        assert_eq!(decode_replay_response(&wire).unwrap(), replay);
        assert!(!wire.contains("job_id"));
        assert_eq!(CAPABILITY_ID, "lenso.outbound-webhook-admin@1");
    }
}
