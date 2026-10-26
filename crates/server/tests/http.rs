use axum::{
    body::{Body, Bytes, to_bytes},
    http::{Request, StatusCode},
};
use memento_core::{Store, raw};
use memento_server::{AppState, CAPTURE_BYTES, router};
use serde_json::{Value, json};
use tower::ServiceExt;
fn fixture() -> (tempfile::TempDir, AppState) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let state = AppState::new(store).unwrap();
    (dir, state)
}
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
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({"bytes":bytes.len()}));
    (status, value)
}
async fn rpc(state: &AppState, method: &str, params: Value) -> (StatusCode, Value) {
    let request = Request::post("/mcp/")
        .header("host", "127.0.0.1:8765")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(Body::from(
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
                .unwrap(),
        ))
        .unwrap();
    let response = router(state.clone()).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
fn capture(text: &str) -> Value {
    json!({"source":"native:synthetic:pi","event_id":"one","conversation_id":"thread","kind":"user","text":text,"occurred_at":"2026-01-01T12:00:00+02:00","metadata":{}})
}
#[tokio::test]
async fn actual_http_capture_graph_raw_recall_and_history_share_one_authority() {
    let (_dir, state) = fixture();
    let (status, created) = request(
        &state,
        "POST",
        "/episodes",
        Some(capture("Ω🧠 exact durable source")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        request(
            &state,
            "POST",
            "/episodes",
            Some(capture("Ω🧠 exact durable source"))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        request(
            &state,
            "POST",
            "/episodes",
            Some(capture("changed evidence"))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let mut command = json!({"command_id":"remember","actor":"synthetic-owner","why":"Exact fixture evidence","type":"knowledge","text":"Stored synthetic fact","scope":["personal"],"evidence":[{"episode_id":created["id"],"quote":"Ω🧠"}]});
    let (status, node) = request(&state, "POST", "/remember", Some(command.clone())).await;
    assert_eq!(status, StatusCode::OK);
    let (status, history) = request(
        &state,
        "GET",
        &format!("/history/{}", node["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(history["versions"][0]["evidence"][0]["start"], 0);
    assert_eq!(history["versions"][0]["evidence"][0]["end"], 2);
    let episode = request(
        &state,
        "GET",
        &format!("/episodes/{}", created["id"].as_str().unwrap()),
        None,
    )
    .await
    .1;
    assert_eq!(episode["text"], "Ω🧠 exact durable source");
    assert_eq!(episode["occurred_at"], "2026-01-01T12:00:00+02:00");
    let raw = request(
        &state,
        "GET",
        "/recall?source=native%3Asynthetic%3Api&q=durable",
        None,
    )
    .await;
    assert_eq!(raw.0, StatusCode::OK);
    assert_eq!(raw.1["memories"][0]["id"], created["id"]);
    assert!(raw.1["client_revision"].as_str().unwrap().len() == 64);
    assert_eq!(
        request(&state, "GET", "/recall?q=durable", None).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    command["valid_from"] = json!("");
    assert_eq!(
        request(&state, "POST", "/remember", Some(command)).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}
#[tokio::test]
async fn actual_mcp_lists_six_tools_normalizes_only_optional_empties_and_wraps_history() {
    let (_dir, state) = fixture();
    let (_, created) = request(
        &state,
        "POST",
        "/episodes",
        Some(capture("Exact quoted source")),
    )
    .await;
    let initialized=rpc(&state,"initialize",json!({"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"synthetic","version":"1"}})).await;
    assert_eq!(initialized.0, StatusCode::OK);
    assert_eq!(initialized.1["result"]["protocolVersion"], "2026-07-28");
    let tools = rpc(&state, "tools/list", json!({})).await.1;
    let names = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["recall", "remember", "link", "supersede", "history", "loop"]
    );
    let command = json!({"command_id":"tool-save","actor":"synthetic","why":"Grounded fixture","type":"knowledge","text":"Remembered source","scope":["personal"],"valid_from":"","source":"","conversation_id":"","evidence":[{"episode_id":created["id"],"quote":"Exact quoted source"}]});
    let saved = rpc(
        &state,
        "tools/call",
        json!({"name":"remember","arguments":{"command":command}}),
    )
    .await
    .1;
    assert_eq!(saved["result"]["isError"], false);
    let id = saved["result"]["structuredContent"]["id"].as_str().unwrap();
    let history = rpc(
        &state,
        "tools/call",
        json!({"name":"history","arguments":{"id":id}}),
    )
    .await
    .1;
    assert_eq!(
        history["result"]["structuredContent"]["result"][0]["id"],
        id
    );
    let empty = rpc(
        &state,
        "tools/call",
        json!({"name":"recall","arguments":{"query":"","effort":"","scope":""}}),
    )
    .await
    .1;
    assert_eq!(empty["result"]["isError"], false);
    assert_eq!(
        empty["result"]["structuredContent"]["memories"][0]["id"],
        created["id"]
    );
}
#[tokio::test]
async fn host_origin_error_body_and_chunked_limits_preserve_privacy() {
    let (_dir, state) = fixture();
    for (host, origin, expected) in [
        ("evil.example", None, 421),
        ("127.0.0.1:8765", Some("https://evil.example"), 403),
    ] {
        let mut builder = Request::post("/mcp/")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("host", host);
        if let Some(origin) = origin {
            builder = builder.header("origin", origin)
        }
        let response = router(state.clone())
            .oneshot(builder.body(Body::from("{}")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
    }
    let secret = "synthetic-never-leak-private://credential@host";
    let response = request(
        &state,
        "POST",
        "/episodes",
        Some(json!({"source":42,"text":secret})),
    )
    .await;
    assert_eq!(response.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!response.1.to_string().contains(secret));
    let chunks = (0..17)
        .map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; CAPTURE_BYTES / 16])))
        .collect::<Vec<_>>();
    let body = Body::from_stream(futures_util::stream::iter(chunks));
    let response = router(state.clone())
        .oneshot(
            Request::post("/episodes")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response = router(state)
        .oneshot(
            Request::post("/episodes")
                .header("content-length", "invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
#[tokio::test]
async fn actual_streaming_blob_roundtrip_and_client_release_bytes_are_verified() {
    let (_dir, state) = fixture();
    let bytes = b"synthetic original artifact bytes";
    let chunks = vec![
        Ok::<_, std::io::Error>(Bytes::copy_from_slice(&bytes[..12])),
        Ok(Bytes::copy_from_slice(&bytes[12..])),
    ];
    let response = router(state.clone())
        .oneshot(
            Request::post("/blobs")
                .header("content-type", "application/pdf")
                .body(Body::from_stream(futures_util::stream::iter(chunks)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
    assert_eq!(value["sha256"], raw::sha256(bytes));
    let response = router(state.clone())
        .oneshot(
            Request::get(format!("/blobs/{}", value["sha256"].as_str().unwrap()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/pdf");
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        bytes
    );
    let release = request(&state, "GET", "/client-release", None).await.1;
    use base64::{Engine, engine::general_purpose::STANDARD};
    for (name, record) in release["files"].as_object().unwrap() {
        let decoded = STANDARD
            .decode(record["content"].as_str().unwrap())
            .unwrap();
        assert_eq!(raw::sha256(&decoded), record["sha256"]);
        if name == "bridge.py" {
            assert!(String::from_utf8(decoded).unwrap().contains(&format!(
                "CLIENT_REVISION = '{}'",
                release["revision"].as_str().unwrap()
            )));
        }
    }
}
#[tokio::test]
async fn work_claim_uses_real_grounded_queue_and_rejects_invalid_schedules_before_writes() {
    let (_dir, state) = fixture();
    request(
        &state,
        "POST",
        "/episodes",
        Some(capture("A pending source fact")),
    )
    .await;
    assert_eq!(
        request(
            &state,
            "POST",
            "/work/claim",
            Some(json!({"agent":"synthetic","timezone":"invalid/zone"}))
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        state
            .store
            .read(|tx| Ok(tx
                .sql()
                .query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        0
    );
    let compact = request(
        &state,
        "POST",
        "/work/compaction",
        Some(json!({"source":"native:synthetic:pi","conversation_id":"thread"})),
    )
    .await;
    assert_eq!(compact.0, StatusCode::OK);
    let claimed = request(
        &state,
        "POST",
        "/work/claim",
        Some(json!({"agent":"synthetic","timezone":"Europe/Berlin","kinds":["extract"]})),
    )
    .await;
    assert_eq!(claimed.0, StatusCode::OK);
    assert_eq!(claimed.1["work"][0]["kind"], "extract");
    let body = claimed.1.to_string();
    assert!(body.contains("A pending source fact"));
    assert!(body.contains("schema"));
    assert!(body.contains("lease_seconds"));
}

#[tokio::test]
async fn status_connect_and_viewer_routes_keep_real_counts_static_bytes_and_private_headers() {
    let (_dir, state) = fixture();
    let (status, value) = request(&state, "GET", "/status", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["captured"]["episodes"], 0);
    assert_eq!(value["indexing"]["available"], false);
    let created = request(
        &state,
        "POST",
        "/episodes",
        Some(capture("Ω exact viewer source")),
    )
    .await
    .1;
    let (status, page) = request(
        &state,
        "GET",
        "/ui/api/conversation?source=native%3Asynthetic%3Api&conversation_id=thread",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"][0]["id"], created["id"]);
    assert_eq!(page["items"][0]["text"], "Ω exact viewer source");
    let graph = request(&state, "GET", "/ui/api/graph", None).await;
    assert_eq!(graph.0, StatusCode::OK);
    assert!(graph.1["nodes"].is_array());
    for (path, mime) in [
        ("/connect", "text/plain"),
        ("/ui/", "text/html"),
        ("/ui/static/app.js", "text/javascript"),
    ] {
        let response = router(state.clone())
            .oneshot(
                Request::get(path)
                    .header("host", "localhost:8765")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with(mime)
        );
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(response.headers()["referrer-policy"], "no-referrer");
        assert!(
            response.headers()["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("frame-ancestors 'none'")
        );
    }
    let response = router(state)
        .oneshot(
            Request::get("/ui/api/graph")
                .header("host", "attacker.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn configured_bare_mcp_hosts_accept_valid_ports_and_reject_host_origin_confusion() {
    let (_dir, mut state) = fixture();
    state.allowed_hosts = std::sync::Arc::new(vec!["127.0.0.1".into(), "localhost".into()]);
    for host in ["127.0.0.1:18775", "localhost:18775"] {
        let response=router(state.clone()).oneshot(Request::post("/mcp/").header("host",host).header("content-type","application/json").header("accept","application/json, text/event-stream").body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#)).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    for host in [
        "evil.invalid",
        "localhost:123@evil.invalid",
        "127.0.0.1:999999",
    ] {
        let response = router(state.clone())
            .oneshot(
                Request::post("/mcp/")
                    .header("host", host)
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
        let response = router(state.clone())
            .oneshot(
                Request::get("/ui/")
                    .header("host", host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    for path in [
        "/ui/static/app.js",
        "/ui/static/app.css",
        "/ui/static/layout-worker.js",
        "/ui/static/vendor/graphology.umd.min.js",
        "/ui/static/vendor/sigma.min.js",
        "/ui/static/vendor/forceatlas2.min.js",
        "/docs",
        "/redoc",
        "/openapi.json",
    ] {
        let response = router(state.clone())
            .oneshot(
                Request::get(path)
                    .header("host", "127.0.0.1:18775")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(
            !to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .is_empty()
        );
    }
    let response = router(state)
        .oneshot(Request::get("/client-release").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
}

#[tokio::test]
async fn strict_json_media_and_drive_validation_fail_before_authority_or_provider_work() {
    let (_dir, state) = fixture();
    let response = router(state.clone())
        .oneshot(
            Request::post("/episodes")
                .header("content-type", "text/plain")
                .body(Body::from(
                    serde_json::to_vec(&capture("Rejected plain MIME source")).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        request(&state, "GET", "/status", None).await.1["captured"]["episodes"],
        0
    );
    for (path, value) in [
        ("/drive/connect/begin", json!({"callback_port":0})),
        (
            "/drive/connect/finish",
            json!({"state":"short","code":"code"}),
        ),
        (
            "/drive/connect/finish",
            json!({"state":"synthetic-state-valid","code":"code","error":"denied"}),
        ),
    ] {
        let response = request(&state, "POST", path, Some(value)).await;
        assert_eq!(response.0, StatusCode::UNPROCESSABLE_ENTITY);
    }
    let response = router(state)
        .oneshot(
            Request::post("/drive/connect/begin")
                .header("content-type", "application/json")
                .header("origin", "https://evil.invalid")
                .body(Body::from(r#"{"callback_port":12345}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["cache-control"], "no-store");
}
