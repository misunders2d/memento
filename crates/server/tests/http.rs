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
async fn open_directional_predicates_work_over_http_mcp_and_advertised_schemas() {
    let (_dir, state) = fixture();
    let (_, episode) = request(
        &state,
        "POST",
        "/episodes",
        Some(capture("Rhea is the parent of Sol.")),
    )
    .await;
    let mut nodes = Vec::new();
    for name in ["Rhea", "Sol"] {
        let (status, node) = request(&state, "POST", "/remember", Some(json!({
            "command_id":format!("person-{name}"),"actor":"fixture","why":"Explicit synthetic person",
            "type":"person","text":name,"scope":["personal"],
            "evidence":[{"episode_id":episode["id"],"quote":"Rhea is the parent of Sol."}]
        }))).await;
        assert_eq!(status, StatusCode::OK);
        nodes.push(node["id"].clone());
    }
    let command = json!({"command_id":"family-link","actor":"fixture","why":"Explicit synthetic direction",
        "a":nodes[0],"rel":"parent_of","b":nodes[1],
        "evidence":[{"episode_id":episode["id"],"quote":"Rhea is the parent of Sol."}]});
    let (status, linked) = request(&state, "POST", "/link", Some(command.clone())).await;
    assert_eq!(status, StatusCode::OK);
    let retried = rpc(
        &state,
        "tools/call",
        json!({"name":"link","arguments":{"command":command}}),
    )
    .await
    .1;
    assert_eq!(retried["result"]["isError"], false);
    assert_eq!(retried["result"]["structuredContent"], linked);
    let (_, history) = request(
        &state,
        "GET",
        &format!("/history/{}", linked["id"].as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(history["versions"][0]["src"], nodes[0]);
    assert_eq!(history["versions"][0]["dst"], nodes[1]);
    assert_eq!(history["versions"][0]["rel"], "parent_of");
    assert_eq!(
        history["versions"][0]["evidence"][0]["episode_id"],
        episode["id"]
    );
    let advertised = rpc(&state, "tools/list", json!({})).await.1;
    let link = advertised["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "link")
        .unwrap();
    let predicate = &link["inputSchema"]["properties"]["command"]["properties"]["rel"];
    assert_eq!(predicate["type"], "string");
    assert!(predicate.get("enum").is_none());
    assert_eq!(predicate["not"]["const"], "supersedes");
    let mut invalid = command;
    invalid["command_id"] = json!("reserved");
    invalid["rel"] = json!("supersedes");
    let rejected = rpc(
        &state,
        "tools/call",
        json!({"name":"link","arguments":{"command":invalid}}),
    )
    .await
    .1;
    assert_eq!(rejected["result"]["isError"], true);
}
#[tokio::test]
async fn loop_owner_assignment_and_filter_work_through_http_and_mcp() {
    let (_dir, state) = fixture();
    let quote = "I still need to send the Orion checklist.";
    let (_, episode) = request(&state, "POST", "/episodes", Some(capture(quote))).await;
    let (_, memory) = request(
        &state,
        "POST",
        "/remember",
        Some(json!({
            "command_id":"owner-task","actor":"fixture","why":"Explicit synthetic obligation",
            "type":"knowledge","text":quote,"scope":["professional"],
            "evidence":[{"episode_id":episode["id"],"quote":quote}]
        })),
    )
    .await;
    let (status, opened) = request(&state, "POST", "/loop", Some(json!({
        "action":"open","id":memory["id"],"command_id":"open-owner-task","actor":"fixture","why":"Explicit synthetic obligation"
    }))).await;
    assert_eq!(status, StatusCode::OK);
    let assignment = json!({"action":"assign","id":memory["id"],"responsibility":"owner",
        "command_id":"assign-owner-task","actor":"fixture","why":"Original first-person native user quote",
        "evidence":[{"episode_id":episode["id"],"quote":quote}]});
    let assigned = rpc(
        &state,
        "tools/call",
        json!({"name":"loop","arguments":assignment}),
    )
    .await
    .1;
    assert_eq!(assigned["result"]["isError"], false);
    let value = &assigned["result"]["structuredContent"];
    assert_eq!(value["responsibility"], "owner");
    for field in ["state", "due_at", "deferred_until"] {
        assert_eq!(value[field], opened[field]);
    }
    let listed = rpc(
        &state,
        "tools/call",
        json!({"name":"loop","arguments":{
            "action":"list","responsibility":"owner","evidence":null
        }}),
    )
    .await
    .1;
    assert_eq!(listed["result"]["isError"], false);
    assert_eq!(listed["result"]["structuredContent"]["total"], 1);
    assert_eq!(
        listed["result"]["structuredContent"]["loops"][0]["id"],
        memory["id"]
    );
    let (_, history) = request(
        &state,
        "GET",
        &format!("/history/{}", memory["id"].as_str().unwrap()),
        None,
    )
    .await;
    let event = history["versions"][0]["loop_events"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(event["assignment"]["responsibility"], "owner");
    assert_eq!(
        event["assignment"]["evidence"][0]["episode_id"],
        episode["id"]
    );
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
async fn actual_mcp_lists_shared_tools_normalizes_only_optional_empties_and_wraps_history() {
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
        ["recall", "remember", "link", "supersede", "reconcile", "history", "loop", "work"]
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
async fn shared_processing_tool_uses_checked_api_and_one_evidence_envelope() {
    let (_dir,state)=fixture();
    request(&state,"POST","/episodes",Some(capture("🧠 The synthetic laboratory uses a copper spectrometer."))).await;
    let (_,response)=rpc(&state,"tools/call",json!({"name":"work","arguments":{"action":"pool","command":{"kind":"extract","policies":true}}})).await;
    assert_eq!(response["result"]["isError"],false);
    assert!(response["result"].get("structuredContent").is_none());
    let page:Value=serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(page["worker_protocol"],1);
    assert_eq!(page["items"].as_array().unwrap().len(),1);
    assert!(page["items"][0].get("text").is_none());
    assert_eq!(page["policy_pages"]["consolidate"]["policy"],"consolidate");
    let mut after=0;
    let mut policy=String::new();
    loop {
        let (_,response)=rpc(&state,"tools/call",json!({"name":"work","arguments":{"action":"read","command":{"policy":"consolidate","after":after}}})).await;
        let text=response["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.len()<16_000);
        let part:Value=serde_json::from_str(text).unwrap();
        policy.push_str(part["text"].as_str().unwrap());
        if part["next"].is_null(){break;}
        after=part["next"].as_u64().unwrap();
    }
    let policy:Value=serde_json::from_str(&policy).unwrap();
    assert_eq!(policy["schema"]["properties"]["actions"]["maxItems"],128);
    let (_,bad)=rpc(&state,"tools/call",json!({"name":"work","arguments":{"action":"pool","command":{"kind":"extract","path":"/private"}}})).await;
    assert_eq!(bad["result"]["isError"],true);
    let (status,_)=request(&state,"POST","/work",Some(json!({"action":"pool","command":{"kind":"extract"}}))).await;
    assert_eq!(status,StatusCode::OK);
    let (_,response)=rpc(&state,"tools/call",json!({"name":"work","arguments":{"action":"claim","command":{"agent":"fixture","timezone":"UTC","kinds":["extract"],"compaction":{"source":"native:synthetic:pi","conversation_id":"thread"}}}})).await;
    let claim:Value=serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    let mut read=claim["work"][0]["read"].clone();
    assert!(claim["work"][0].get("input").is_none());
    read["limit"]=json!(61);
    let mut payload=String::new();
    loop {
        let (_,response)=rpc(&state,"tools/call",json!({"name":"work","arguments":{"action":"read","command":read}})).await;
        let part:Value=serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        payload.push_str(part["text"].as_str().unwrap());
        if part["next"].is_null(){break;}
        read["after"]=part["next"].clone();
    }
    let restored:Value=serde_json::from_str(&payload).unwrap();
    assert_eq!(restored["input"]["windows"][0]["text"],"🧠 The synthetic laboratory uses a copper spectrometer.");
}
#[tokio::test]
async fn actual_mcp_scoped_recall_validates_since_before_dispatch() {
    let (_dir, state) = fixture();
    for (query, since, is_error) in [
        ("", "invalid", true),
        ("", "2026-01-01T12:00:00", true),
        ("", "2026-01-01T12:00:00+02:00", false),
        ("", "", false),
        ("synthetic semantic query", "invalid", true),
    ] {
        let (status, response) = rpc(
            &state,
            "tools/call",
            json!({"name":"recall","arguments":{"query":query,"scope":"personal","since":since}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response["result"]["isError"], is_error,
            "{query:?} {since:?}"
        );
        if is_error {
            assert!(response["result"].get("structuredContent").is_none());
            assert_eq!(
                response["result"]["content"],
                json!([{"type":"text","text":"World time must be an aware ISO timestamp"}])
            );
        } else {
            assert_eq!(
                response["result"]["structuredContent"],
                json!({"memories":[]})
            );
            assert_eq!(
                response["result"]["content"],
                json!([{"type":"text","text":"No matching memories."}])
            );
        }
    }
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

#[tokio::test]
async fn explicit_reconcile_http_and_mcp_preserve_the_private_review_boundary() {
    for transport in ["http", "mcp"] {
        let (_dir, state) = fixture();
        let (_, episode) = request(&state, "POST", "/episodes", Some(capture("Vega Incorporated is Vega."))).await;
        let mut ids = Vec::new();
        for name in ["Vega", "Vega Incorporated"] {
            let (status, node) = request(&state, "POST", "/remember", Some(json!({
                "command_id":name,"actor":"fixture","why":"Original identity evidence",
                "type":"organization","text":name,"scope":["professional"],
                "evidence":[{"episode_id":episode["id"],"quote":"Vega Incorporated is Vega."}]
            }))).await;
            assert_eq!(status, StatusCode::OK);
            ids.push(node["id"].as_str().unwrap().to_owned());
        }
        let command = json!({"command_id":"merge","actor":"agent","why":"Owner requested same-referent merge",
            "survivor":ids[0],"absorbed":[ids[1]],"expected_versions":{&ids[0]:1,&ids[1]:1},
            "source":"native:synthetic:pi","conversation_id":"thread",
            "evidence":[{"quote":"Vega Incorporated is Vega."}]});
        let mut bypass = command.clone();
        bypass["approved"] = json!(true);
        assert_eq!(request(&state, "POST", "/reconcile", Some(bypass)).await.0, StatusCode::UNPROCESSABLE_ENTITY);
        let mut stale = command.clone();
        stale["expected_versions"][&ids[0]] = json!(2);
        assert_eq!(request(&state, "POST", "/reconcile", Some(stale)).await.0, StatusCode::CONFLICT);
        let result = if transport == "http" {
            let response = request(&state, "POST", "/reconcile", Some(command.clone())).await;
            assert_eq!(response.0, StatusCode::OK);
            assert_eq!(request(&state, "POST", "/reconcile", Some(command)).await.1, response.1);
            response.1
        } else {
            let response = rpc(&state, "tools/call", json!({"name":"reconcile","arguments":{"command":command}})).await.1;
            assert_eq!(response["result"]["isError"], false, "{response}");
            response["result"]["structuredContent"].clone()
        };
        assert_eq!(result["status"], "absorbed", "{result}");
        let (_, history) = request(&state, "GET", &format!("/history/{}", ids[1]), None).await;
        assert_eq!(history["versions"].as_array().unwrap().last().unwrap()["state"], "merged");
    }
}
