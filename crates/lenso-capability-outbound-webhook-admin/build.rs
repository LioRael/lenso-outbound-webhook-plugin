use std::{env, path::Path};

use lenso_contract_codegen::{
    ProjectionLanguage, check_projection, check_source_snapshot, write_source_snapshot,
};

#[allow(dead_code)]
#[path = "src/contract.rs"]
mod contract_source;

fn main() {
    println!("cargo:rerun-if-changed=capability.json");
    println!("cargo:rerun-if-changed=schemas");
    println!("cargo:rerun-if-changed=src/contract.rs");
    println!("cargo:rerun-if-changed=src/generated.rs");
    println!("cargo:rerun-if-env-changed=LENSO_UPDATE_CONTRACT_SNAPSHOT");

    let mut snapshot = contract_source::__lenso_capability_snapshot();
    normalize_snapshot(&mut snapshot);
    if env::var_os("LENSO_UPDATE_CONTRACT_SNAPSHOT").is_some() {
        write_source_snapshot(&snapshot, Path::new("capability.json")).unwrap_or_else(|error| {
            panic!("failed to update Outbound Webhook Admin snapshot: {error}")
        });
    } else {
        check_source_snapshot(&snapshot, Path::new("capability.json")).unwrap_or_else(|error| {
            panic!("Outbound Webhook Admin Descriptor or Schemas are stale: {error}")
        });
    }
    check_projection(
        Path::new("capability.json"),
        ProjectionLanguage::Rust,
        Path::new("src/generated.rs"),
    )
    .unwrap_or_else(|error| panic!("Outbound Webhook Admin Rust projection is stale: {error}"));
}

fn normalize_snapshot(snapshot: &mut lenso_contract_authoring::CapabilitySnapshot) {
    for operation in &mut snapshot.operations {
        for schema in [
            &mut operation.request_schema,
            &mut operation.response_schema,
            &mut operation.domain_error_schema,
        ] {
            normalize_schema(schema);
        }
    }
}

fn normalize_schema(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for value in object.values_mut() {
                normalize_schema(value);
            }
            if object.get("additionalProperties") == Some(&serde_json::Value::Bool(true))
                && object
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(serde_json::Map::is_empty)
            {
                object.remove("properties");
            }
            if object
                .get("enum")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| values.iter().all(serde_json::Value::is_string))
                && object.get("type").and_then(serde_json::Value::as_str) == Some("string")
            {
                object.remove("type");
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(normalize_schema),
        _ => {}
    }
}
