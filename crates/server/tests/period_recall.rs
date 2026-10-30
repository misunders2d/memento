// Stored-memory period recall through the real HTTP and MCP routers.
// The embedding runtime is deliberately absent here: period-only recall is
// SQL-only, and a query plus a period must fail closed instead of falling
// back to raw episodes. Semantic+period ranking is covered in-crate.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use memento_core::Store;
use memento_server::{AppState, router};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn request(
    state: &AppState,
    method: &str,
    path: &str,
    value: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1:8765");
    let body = if let Some(value) = value {
        req = req.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&value).unwrap())
    } else {
        Body::empty()
    };
    let response = router(state.clone())
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn tool(state: &AppState, arguments: Value) -> Value {
    let request = Request::post("/mcp/")
        .header("host", "127.0.0.1:8765")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(Body::from(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"recall","arguments":arguments}})).unwrap()))
        .unwrap();
    let response = router(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
}
struct Fixture {
    _dir: tempfile::TempDir,
    state: AppState,
    recent: String,
    personal_recent: String,
    old: String,
    undated: String,
    raw_only: String,
}
async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(Store::open(dir.path().join("data")).unwrap()).unwrap();
    let mut ids = Vec::new();
    for (event, text, occurred_at, scope) in [
        (
            "recent",
            "The harbor crane inspection moved to Thursday.",
            json!("2026-10-20T12:00:00+02:00"),
            Some("professional"),
        ),
        (
            "personal",
            "Mira asked for the blue notebook back.",
            json!("2026-10-18T09:30:00Z"),
            Some("personal"),
        ),
        (
            "old",
            "The supplier invoice format was agreed in January.",
            json!("2026-01-05T08:00:00Z"),
            Some("professional"),
        ),
        (
            "undated",
            "The archive cabinet key is in the left drawer.",
            Value::Null,
            Some("personal"),
        ),
        (
            "raw",
            "Unextracted chatter about lunch in the window.",
            json!("2026-10-21T12:00:00Z"),
            None,
        ),
    ] {
        let (status, episode) = request(&state, "POST", "/episodes", Some(json!({"source":"native:synthetic:pi","event_id":event,"conversation_id":"period","kind":"user","text":text,"occurred_at":occurred_at,"metadata":{}}))).await;
        assert!(status.is_success(), "{status} {episode}");
        let Some(scope) = scope else {
            ids.push(episode["id"].as_str().unwrap().to_owned());
            continue;
        };
        let (status, node) = request(&state, "POST", "/remember", Some(json!({
            "command_id":format!("period-{event}"),"actor":"fixture","why":"Exact synthetic period source",
            "type":"knowledge","text":text,"scope":[scope],
            "evidence":[{"episode_id":episode["id"],"quote":text}]
        }))).await;
        assert_eq!(status, StatusCode::OK, "{node}");
        ids.push(node["id"].as_str().unwrap().to_owned());
    }
    let mut ids = ids.into_iter();
    Fixture {
        _dir: dir,
        state,
        recent: ids.next().unwrap(),
        personal_recent: ids.next().unwrap(),
        old: ids.next().unwrap(),
        undated: ids.next().unwrap(),
        raw_only: ids.next().unwrap(),
    }
}
fn ids(memories: &Value) -> Vec<&str> {
    memories
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect()
}
const WINDOW: &str = "memory_since=2026-10-16T00:00:00%2B00:00&memory_until=2026-10-30T00:00:00Z";

#[tokio::test]
async fn http_period_only_returns_stored_memories_with_original_source_dates() {
    let f = fixture().await;
    let (status, body) = request(&f.state, "GET", &format!("/recall?{WINDOW}"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let memories = &body["memories"];
    // Newest original source first; no raw episode, undated or out-of-window memory.
    assert_eq!(
        ids(memories),
        [f.recent.as_str(), f.personal_recent.as_str()]
    );
    assert_eq!(
        memories[0]["source_occurred_at"],
        "2026-10-20T10:00:00+00:00"
    );
    assert_eq!(memories[0]["tier"], "durable");
    assert_eq!(memories[0]["match"], "period");
    assert_eq!(
        memories[0]["evidence"][0]["quote_excerpt"],
        "The harbor crane inspection moved to Thursday."
    );
    assert!(
        memories
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["type"] != "raw_episode")
    );
    let (_, scoped) = request(
        &f.state,
        "GET",
        &format!("/recall?{WINDOW}&scope=personal&effort=cheap"),
        None,
    )
    .await;
    assert_eq!(ids(&scoped["memories"]), [f.personal_recent.as_str()]);
    let (_, january) = request(
        &f.state,
        "GET",
        "/recall?memory_since=2026-01-01T00:00:00Z&memory_until=2026-02-01T00:00:00Z",
        None,
    )
    .await;
    assert_eq!(ids(&january["memories"]), [f.old.as_str()]);
    let (_, open) = request(
        &f.state,
        "GET",
        "/recall?memory_until=2026-10-19T00:00:00Z&limit=5",
        None,
    )
    .await;
    assert_eq!(
        ids(&open["memories"]),
        [f.personal_recent.as_str(), f.old.as_str()]
    );
    assert!(!ids(&open["memories"]).contains(&f.undated.as_str()));
}

#[tokio::test]
async fn http_semantic_period_fails_closed_and_raw_filters_keep_their_contract() {
    let f = fixture().await;
    // A query plus a period needs semantic retrieval; never substitute raw history.
    let (status, body) = request(&f.state, "GET", &format!("/recall?q=crane&{WINDOW}"), None).await;
    assert_eq!(
        (status, body["detail"].as_str()),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Some("Native embedding runtime is disabled")
        )
    );
    // Raw since still filters episodes by original-or-capture time, including unextracted raw chatter.
    let (status, raw) = request(
        &f.state,
        "GET",
        "/recall?since=2026-10-16T00:00:00Z&limit=10",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let raw_ids = ids(&raw["memories"]);
    assert!(raw_ids.contains(&f.raw_only.as_str()));
    assert!(
        raw["memories"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["type"] == "raw_episode")
    );
    let (_, sourced) = request(
        &f.state,
        "GET",
        "/recall?source=native%3Asynthetic%3Api&q=crane",
        None,
    )
    .await;
    assert!(
        sourced["memories"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["type"] == "raw_episode")
    );
    assert!(!sourced["memories"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn http_period_validation_is_strict() {
    let f = fixture().await;
    for (query, detail) in [
        (
            "memory_since=2026-10-16T00:00:00",
            "Memory period bounds must be aware ISO timestamps",
        ),
        (
            "memory_since=2026-10-16",
            "Memory period bounds must be aware ISO timestamps",
        ),
        (
            "memory_since=",
            "Memory period bounds must be aware ISO timestamps",
        ),
        (
            "memory_until=",
            "Memory period bounds must be aware ISO timestamps",
        ),
        (
            "memory_since=2026-10-30T00:00:00Z&memory_until=2026-10-16T00:00:00Z",
            "Memory period start must be before its end",
        ),
        (
            "memory_since=2026-10-16T02:00:00%2B02:00&memory_until=2026-10-16T00:00:00Z",
            "Memory period start must be before its end",
        ),
        (
            "memory_since=2026-10-16T00:00:00Z&since=2026-10-16T00:00:00Z",
            "Raw since/source filters cannot be combined with a memory period",
        ),
        (
            "memory_since=2026-10-16T00:00:00Z&source=native%3Asynthetic%3Api",
            "Raw since/source filters cannot be combined with a memory period",
        ),
        (
            "memory_since=2026-10-16T00:00:00Z&automatic=true&q=crane",
            "Automatic recall does not accept a memory period",
        ),
    ] {
        let (status, body) = request(&f.state, "GET", &format!("/recall?{query}"), None).await;
        assert_eq!(
            (status, body["detail"].as_str()),
            (StatusCode::UNPROCESSABLE_ENTITY, Some(detail)),
            "{query}"
        );
    }
    let (_, schema) = request(&f.state, "GET", "/openapi.json", None).await;
    let names = schema["paths"]["/recall"]["get"]["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        names.contains(&"memory_since")
            && names.contains(&"memory_until")
            && names.contains(&"since")
    );
}

#[tokio::test]
async fn mcp_period_schema_omission_dates_and_errors() {
    let f = fixture().await;
    let listed = {
        let request = Request::post("/mcp/")
            .header("host", "127.0.0.1:8765")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(Body::from(
                serde_json::to_vec(
                    &json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
                )
                .unwrap(),
            ))
            .unwrap();
        let response = router(f.state.clone()).oneshot(request).await.unwrap();
        serde_json::from_slice::<Value>(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
            .unwrap()
    };
    let recall = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "recall")
        .unwrap();
    let properties = &recall["inputSchema"]["properties"];
    assert_eq!(properties["memory_since"], properties["since"]);
    assert_eq!(properties["memory_until"], properties["since"]);
    assert!(
        recall["description"]
            .as_str()
            .unwrap()
            .contains("memory_since (inclusive) and memory_until (exclusive)")
    );
    let period = tool(&f.state, json!({"query":"","memory_since":"2026-10-16T00:00:00+00:00","memory_until":"","effort":"","scope":""})).await;
    assert_eq!(period["isError"], false);
    assert_eq!(
        ids(&period["structuredContent"]["memories"]),
        [f.recent.as_str(), f.personal_recent.as_str()]
    );
    let text = period["content"][0]["text"].as_str().unwrap();
    assert!(
        text.lines()
            .next()
            .unwrap()
            .ends_with("| source 2026-10-20T10:00:00+00:00"),
        "{text}"
    );
    // Empty optional period strings mean omitted: recent raw activity, unchanged.
    let omitted = tool(
        &f.state,
        json!({"query":"","memory_since":"","memory_until":"","since":"","source":""}),
    )
    .await;
    assert_eq!(omitted["isError"], false);
    assert!(
        omitted["structuredContent"]["memories"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["type"] == "raw_episode")
    );
    assert!(
        !omitted["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("| source ")
    );
    for (arguments, message) in [
        (
            json!({"memory_since":"2026-10-16"}),
            "Memory period bounds must be aware ISO timestamps",
        ),
        (
            json!({"memory_since":"2026-10-30T00:00:00Z","memory_until":"2026-10-16T00:00:00Z"}),
            "Memory period start must be before its end",
        ),
        (
            json!({"memory_since":"2026-10-16T00:00:00Z","source":"native:synthetic:pi"}),
            "Raw since/source filters cannot be combined with a memory period",
        ),
    ] {
        let result = tool(&f.state, arguments).await;
        assert_eq!(result["isError"], true);
        assert_eq!(result["content"], json!([{"type":"text","text":message}]));
    }
}
