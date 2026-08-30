//! Portable request contract for durable outbound Webhook submission.

#[allow(dead_code)]
mod contract;

include!("generated.rs");

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;

    #[test]
    fn portable_enqueue_round_trips_without_debugging_payload() {
        let request = EnqueueRequest {
            event_id: "order-42".into(),
            event_type: "order.created".into(),
            payload: BTreeMap::from([("secret".into(), json!("not-for-debug"))]),
        };
        let wire = encode_enqueue_request(&request).unwrap();
        assert_eq!(decode_enqueue_request(&wire).unwrap(), request);
        assert!(!format!("{request:?}").contains("not-for-debug"));
        assert_eq!(CAPABILITY_ID, "lenso.outbound-webhook@1");
    }
}
