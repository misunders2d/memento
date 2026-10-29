//! Stateless Streamable HTTP MCP; memory tools and agent-neutral processing.
use crate::{AppState, CAPTURE_BYTES, RecallRequest, blocking, parse};
use axum::{
    Json,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use memento_core::{Error, Result};
use memento_domain::{dto, processing};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
const LATEST: &str = "2026-07-28";
const INSTRUCTIONS: &str = "Routine capture and passive recall belong to the harness. Use recall(deep) when passive context is insufficient; cite memory ids. Remember only explicit saves. Every assertion cites an exact source episode quote. Correct beliefs with one atomic supersede; history explains when, why, and evidence. Reuse command_id only when retrying the identical command. Tools cannot approve sensitive changes or forget data.";
pub async fn redirect(request: Request) -> Response {
    let location = match request.uri().query() {
        Some(query) => format!("/mcp/?{query}"),
        None => "/mcp/".into(),
    };
    (StatusCode::TEMPORARY_REDIRECT, [("location", location)]).into_response()
}
fn trusted_host(state: &AppState, host: &str) -> bool {
    let Some((name, port)) = crate::host_parts(host) else {
        return false;
    };
    if state.allowed_hosts.is_empty() {
        return ["127.0.0.1", "localhost", "::1"].contains(&name.as_str());
    }
    state.allowed_hosts.iter().any(|allowed| {
        let wildcard = allowed.ends_with(":*");
        let allowed = allowed.strip_suffix(":*").unwrap_or(allowed);
        crate::host_parts(allowed).is_some_and(|(candidate, expected)| {
            candidate == name && (wildcard || expected.is_none() || expected == port)
        })
    })
}

pub fn admit(
    state: &AppState,
    headers: &HeaderMap,
    post: bool,
) -> std::result::Result<(), StatusCode> {
    if post
        && !headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case("application/json")
            })
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|host| trusted_host(state, host))
    {
        return Err(StatusCode::MISDIRECTED_REQUEST);
    }
    if let Some(origin) = headers.get("origin") {
        let origin = origin.to_str().map_err(|_| StatusCode::FORBIDDEN)?;
        let host = origin
            .strip_prefix("https://")
            .or_else(|| origin.strip_prefix("http://"))
            .ok_or(StatusCode::FORBIDDEN)?;
        if !trusted_host(state, host) {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    Ok(())
}
pub async fn unsupported(State(state): State<AppState>, request: Request) -> Response {
    match admit(&state,request.headers(),false){Err(status)=>status.into_response(),Ok(())=>(StatusCode::METHOD_NOT_ALLOWED,Json(json!({"jsonrpc":"2.0","id":null,"error":{"code":-32000,"message":"Method not allowed"}}))).into_response()}
}
fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
pub async fn request(State(state): State<AppState>, request: Request) -> Response {
    if let Err(status) = admit(&state, request.headers(), true) {
        return status.into_response();
    }
    let accept = request
        .headers()
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !accept.contains("application/json") || !accept.contains("text/event-stream") {
        return (
            StatusCode::NOT_ACCEPTABLE,
            Json(rpc_error(
                Value::Null,
                -32600,
                "Accept must include application/json and text/event-stream",
            )),
        )
            .into_response();
    }
    let bytes = match to_bytes(request.into_body(), CAPTURE_BYTES).await {
        Ok(v) => v,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let request: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(rpc_error(Value::Null, -32700, "Parse error")),
            )
                .into_response();
        }
    };
    let Some(object) = request.as_object() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(rpc_error(Value::Null, -32600, "Invalid request")),
        )
            .into_response();
    };
    let id = object.get("id").cloned();
    let method = object.get("method").and_then(Value::as_str);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(rpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid request",
            )),
        )
            .into_response();
    }
    if id.is_none() {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = id.unwrap();
    let params = object.get("params").cloned().unwrap_or(json!({}));
    let method = method.unwrap();
    let result = match method {
        "initialize" => {
            let requested = params["protocolVersion"].as_str().unwrap_or(LATEST);
            let version = if [
                "2024-11-05",
                "2025-03-26",
                "2025-06-18",
                "2025-11-25",
                LATEST,
            ]
            .contains(&requested)
            {
                requested
            } else {
                LATEST
            };
            json!({"protocolVersion":version,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"Memento","version":env!("CARGO_PKG_VERSION")},"instructions":INSTRUCTIONS})
        }
        "ping" => json!({}),
        "tools/list" => {
            json!({"tools":serde_json::from_str::<Value>(include_str!("protocol-tools.json")).expect("frozen public tool schemas")})
        }
        "tools/call" => {
            let Some(name) = params["name"].as_str() else {
                return Json(rpc_error(id, -32602, "Invalid tool name")).into_response();
            };
            let name = name.to_owned();
            let processing_tool = name == "work";
            let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
            let result = blocking(move || call(&state, &name, arguments)).await;
            match result {
                Ok((mut value, text)) => {
                    if processing_tool {
                        processing_view(&mut value);
                        json!({"content":[{"type":"text","text":serde_json::to_string(&value).unwrap()}],"isError":false})
                    } else {
                        json!({"content":[{"type":"text","text":text}],"structuredContent":value,"isError":false})
                    }
                }
                Err(error) => {
                    json!({"content":[{"type":"text","text":error.0.to_string()}],"isError":true})
                }
            }
        }
        _ => return Json(rpc_error(id, -32601, "Method not found")).into_response(),
    };
    let mut response = Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response();
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}
/// Native clients have different output caps. MCP exposes lossless read pages;
/// the shared HTTP API still returns a complete packet in one request.
fn processing_view(value: &mut Value) {
    if let Some(work) = value["work"].as_array_mut() {
        for item in work {
            let id=item["id"].clone(); let owner=item["owner"].clone();
            for key in ["input","instruction","schema"] { item.as_object_mut().unwrap().remove(key); }
            item["read"]=json!({"id":id,"owner":owner,"after":0,"limit":2000});
            item["policy"]=json!({"policy":item["kind"]});
        }
    }
    if let Some(next)=value.get_mut("next").filter(|next|next.is_object()) { processing_view(next); }
    if value.get("policies").is_some() {
        value.as_object_mut().unwrap().remove("policies");
        value["policy_pages"]=json!({"extract":{"policy":"extract"},"consolidate":{"policy":"consolidate"}});
    }
    while serde_json::to_vec(value).unwrap().len()>16_000 && value["items"].as_array().is_some_and(|items|items.len()>1) {
        value["items"].as_array_mut().unwrap().pop();
        value["next"]=value["items"].as_array().unwrap().last().unwrap()["position"].clone();
    }
    if serde_json::to_vec(value).unwrap().len()>16_000 && value.get("result").is_some() {
        value.as_object_mut().unwrap().remove("result");
        value["result_omitted"]=json!(true);
        value["result_transport"]=json!("Use HTTP work/status for the complete committed result");
    }
}
fn omit_empty(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, value)| value != "")
                .map(|(key, value)| (key, omit_empty(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(omit_empty).collect()),
        other => other,
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecallArgs {
    #[serde(default)]
    query: String,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default = "limit")]
    limit: usize,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    source: Option<String>,
}
fn limit() -> usize {
    10
}
fn optional(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.is_empty())
}
fn call(state: &AppState, name: &str, arguments: Value) -> Result<(Value, String)> {
    let value = match name {
        "work" => state.processing.work(parse::<processing::Work>(arguments)?)?,
        "recall" => {
            let args: RecallArgs = parse(arguments)?;
            let since = optional(args.since);
            if let Some(since) = since.as_deref() {
                memento_core::utc_time(since)?;
            }
            let request = RecallRequest {
                query: args.query,
                effort: optional(args.effort).unwrap_or_else(|| "normal".into()),
                limit: args.limit,
                scope: optional(args.scope),
                since,
                source: optional(args.source),
                automatic: false,
                context: String::new(),
            };
            let memories =
                state.recall_memories(&request, Instant::now() + Duration::from_secs(30))?;
            let lines = memories
                .iter()
                .map(|item| {
                    format!(
                        "{} | {} | {} | {:.3}",
                        item["id"].as_str().unwrap_or(""),
                        item["type"].as_str().unwrap_or(""),
                        item["text"]
                            .as_str()
                            .unwrap_or("")
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ")
                            .chars()
                            .take(240)
                            .collect::<String>(),
                        item["score"].as_f64().unwrap_or(0.)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Ok((
                json!({"memories":memories}),
                if lines.is_empty() {
                    "No matching memories.".into()
                } else {
                    lines
                },
            ));
        }
        "remember" => {
            let mut outer = arguments
                .as_object()
                .cloned()
                .ok_or(Error::Validation("Invalid tool arguments"))?;
            let command = omit_empty(
                outer
                    .remove("command")
                    .ok_or(Error::Validation("Missing command"))?,
            );
            if !outer.is_empty() {
                return Err(Error::Validation("Invalid tool arguments"));
            }
            if command.get("sha256").is_some() {
                state.submit_media(command)?
            } else {
                state.graph.remember(parse(command)?)?
            }
        }
        "link" | "supersede" | "reconcile" => {
            let mut outer = arguments
                .as_object()
                .cloned()
                .ok_or(Error::Validation("Invalid tool arguments"))?;
            let command = omit_empty(
                outer
                    .remove("command")
                    .ok_or(Error::Validation("Missing command"))?,
            );
            if !outer.is_empty() {
                return Err(Error::Validation("Invalid tool arguments"));
            }
            if name == "link" {
                state.graph.link(parse::<dto::Link>(command)?)?
            } else if name == "reconcile" {
                state.graph.reconcile(parse::<dto::Reconcile>(command)?)?
            } else {
                state.graph.supersede(parse::<dto::Supersede>(command)?)?
            }
        }
        "history" => {
            let id = arguments["id"]
                .as_str()
                .ok_or(Error::Validation("Memory identifier is required"))?;
            if arguments.as_object().is_none_or(|v| v.len() != 1) {
                return Err(Error::Validation("Invalid tool arguments"));
            }
            let _leases = state.prefetch_episode(id)?;
            let versions = state.graph.history(id)?;
            if versions.is_empty() {
                return Err(Error::Validation("Node not found"));
            }
            return Ok((
                json!({"result":versions}),
                serde_json::to_string(&versions)?,
            ));
        }
        "loop" => {
            let mut command=omit_empty(arguments);
            if let Some(fields)=command.as_object_mut(){
                if fields.get("evidence").is_some_and(Value::is_null){fields.remove("evidence");}
            }
            state.graph.loop_command(parse::<dto::Loop>(command)?)?
        },
        _ => return Err(Error::Validation("Unknown memory tool")),
    };
    let text = serde_json::to_string(&value)?;
    Ok((value, text))
}
