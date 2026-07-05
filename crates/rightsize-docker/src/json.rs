//! `serde`/`serde_json` types for the Docker Engine API bodies this backend sends and
//! reads. Request bodies are derived structs serialized with `serde_json::to_string`;
//! response bodies are deserialized into structs carrying only the fields this backend
//! actually reads (`#[serde(default)]`/`Option` on anything Docker might omit), so an
//! extra field a future daemon version adds is silently ignored rather than failing.

use serde::{Deserialize, Serialize};

/// `POST /containers/create` request body.
#[derive(Serialize)]
pub(crate) struct CreateContainerBody {
    #[serde(rename = "Image")]
    pub image: String,
    #[serde(rename = "Env")]
    pub env: Vec<String>,
    #[serde(rename = "Cmd", skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,
    #[serde(rename = "ExposedPorts")]
    pub exposed_ports: std::collections::BTreeMap<String, EmptyObject>,
    #[serde(rename = "Labels")]
    pub labels: std::collections::BTreeMap<String, String>,
    #[serde(rename = "HostConfig")]
    pub host_config: HostConfig,
}

/// Docker's `ExposedPorts`/`{}` marker value shape — an empty object per port key.
#[derive(Serialize)]
pub(crate) struct EmptyObject {}

/// The `HostConfig` object nested in `POST /containers/create`.
#[derive(Serialize)]
pub(crate) struct HostConfig {
    #[serde(rename = "PortBindings")]
    pub port_bindings: std::collections::BTreeMap<String, Vec<PortBinding>>,
    #[serde(rename = "Binds")]
    pub binds: Vec<String>,
    #[serde(rename = "ExtraHosts")]
    pub extra_hosts: Vec<String>,
    #[serde(rename = "Memory", skip_serializing_if = "Option::is_none")]
    pub memory: Option<u64>,
}

/// One entry in `HostConfig.PortBindings`'s per-port array.
#[derive(Serialize)]
pub(crate) struct PortBinding {
    #[serde(rename = "HostIp")]
    pub host_ip: String,
    #[serde(rename = "HostPort")]
    pub host_port: String,
}

/// `POST /networks/{id}/connect` request body.
#[derive(Serialize)]
pub(crate) struct ConnectNetworkBody {
    #[serde(rename = "Container")]
    pub container: String,
    #[serde(rename = "EndpointConfig")]
    pub endpoint_config: EndpointConfig,
}

#[derive(Serialize)]
pub(crate) struct EndpointConfig {
    #[serde(rename = "Aliases")]
    pub aliases: Vec<String>,
}

/// `POST /networks/create` request body.
#[derive(Serialize)]
pub(crate) struct CreateNetworkBody {
    #[serde(rename = "Name")]
    pub name: String,
}

/// `GET /networks?filters=` request query's JSON value — `{"name": [network_id]}`.
#[derive(Serialize)]
pub(crate) struct NameFilter {
    pub name: Vec<String>,
}

/// `GET /containers/json?filters=` request query's JSON value — `{"label": [selector]}`.
#[derive(Serialize)]
pub(crate) struct LabelFilter {
    pub label: Vec<String>,
}

/// `POST /containers/{id}/exec` request body — only the fields this backend sets.
#[derive(Serialize)]
pub(crate) struct CreateExecBody {
    #[serde(rename = "AttachStdout")]
    pub attach_stdout: bool,
    #[serde(rename = "AttachStderr")]
    pub attach_stderr: bool,
    #[serde(rename = "Cmd")]
    pub cmd: Vec<String>,
}

/// `POST /exec/{id}/start` request body — always non-detached (this backend always
/// reads the exec's output before returning).
#[derive(Serialize)]
pub(crate) struct StartExecBody {
    #[serde(rename = "Detach")]
    pub detach: bool,
}

/// A response carrying only an `Id` field — `POST /containers/create`,
/// `POST /networks/create`, `POST /containers/{id}/exec`.
#[derive(Deserialize)]
pub(crate) struct IdResponse {
    #[serde(rename = "Id")]
    pub id: String,
}

/// One entry of `GET /networks?filters=` or `GET /containers/json?...` — only the `Id`
/// field this backend reads; everything else Docker returns is ignored.
#[derive(Deserialize)]
pub(crate) struct ListedEntry {
    #[serde(rename = "Id")]
    pub id: String,
}

/// `GET /exec/{id}/json` response — only `ExitCode`, which older daemons may return as
/// `null` (mapped to `None`) while an exec is still running.
#[derive(Deserialize)]
pub(crate) struct ExecInspect {
    #[serde(rename = "ExitCode")]
    pub exit_code: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_container_body_serializes_expected_shape() {
        let body = CreateContainerBody {
            image: "redis:8.6-alpine".to_string(),
            env: vec!["A=1".to_string()],
            cmd: None,
            exposed_ports: [("6379/tcp".to_string(), EmptyObject {})].into(),
            labels: [("dev.rightsize.run_id".to_string(), "deadbeef".to_string())].into(),
            host_config: HostConfig {
                port_bindings: [(
                    "6379/tcp".to_string(),
                    vec![PortBinding {
                        host_ip: "127.0.0.1".to_string(),
                        host_port: "32768".to_string(),
                    }],
                )]
                .into(),
                binds: vec!["/host/a:/guest/a:ro".to_string()],
                extra_hosts: vec!["host.docker.internal:host-gateway".to_string()],
                memory: Some(536_870_912),
            },
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"Image\":\"redis:8.6-alpine\""));
        assert!(json.contains("\"HostIp\":\"127.0.0.1\""));
        assert!(json.contains("\"HostPort\":\"32768\""));
        assert!(json.contains("\"6379/tcp\":{}"));
        assert!(json.contains("\"dev.rightsize.run_id\":\"deadbeef\""));
        assert!(json.contains("host.docker.internal:host-gateway"));
        assert!(json.contains("\"Memory\":536870912"));
        assert!(!json.contains("\"Cmd\""));
    }

    #[test]
    fn create_container_body_includes_cmd_when_some() {
        let body = CreateContainerBody {
            image: "redis:8.6-alpine".to_string(),
            env: vec![],
            cmd: Some(vec!["redis-server".to_string(), "--port".to_string()]),
            exposed_ports: Default::default(),
            labels: Default::default(),
            host_config: HostConfig {
                port_bindings: Default::default(),
                binds: vec![],
                extra_hosts: vec![],
                memory: None,
            },
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"Cmd\":[\"redis-server\",\"--port\"]"));
        assert!(!json.contains("\"Memory\""));
    }

    #[test]
    fn id_response_extracts_id_regardless_of_other_fields() {
        let body = r#"{"Warnings":[],"Id":"abc123"}"#;
        let parsed: IdResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.id, "abc123");
    }

    #[test]
    fn listed_entries_parse_from_a_json_array() {
        let body = r#"[{"Id":"c1","Names":["/a"]},{"Id":"c2","Names":["/b"]}]"#;
        let parsed: Vec<ListedEntry> = serde_json::from_str(body).unwrap();
        assert_eq!(
            parsed.into_iter().map(|e| e.id).collect::<Vec<_>>(),
            vec!["c1".to_string(), "c2".to_string()]
        );
    }

    #[test]
    fn exec_inspect_parses_a_bare_integer_exit_code() {
        let body = r#"{"ExitCode":137}"#;
        let parsed: ExecInspect = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.exit_code, Some(137));
    }

    #[test]
    fn exec_inspect_parses_null_exit_code_as_none() {
        let body = r#"{"ExitCode":null}"#;
        let parsed: ExecInspect = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.exit_code, None);
    }

    #[test]
    fn name_filter_serializes_as_a_single_key_object_of_an_array() {
        let filter = NameFilter {
            name: vec!["rz-net-1".to_string()],
        };
        assert_eq!(
            serde_json::to_string(&filter).unwrap(),
            r#"{"name":["rz-net-1"]}"#
        );
    }

    #[test]
    fn label_filter_serializes_as_a_single_key_object_of_an_array() {
        let filter = LabelFilter {
            label: vec!["dev.rightsize.run_id=deadbeef".to_string()],
        };
        assert_eq!(
            serde_json::to_string(&filter).unwrap(),
            r#"{"label":["dev.rightsize.run_id=deadbeef"]}"#
        );
    }
}
