// Same-ID revisions through the HTTP API and the real retrieval ranking path.
// Query vectors are synthetic; the embedding model itself is not exercised.
struct FixedRecall(Arc<Projection>);
impl crate::Recall for FixedRecall {
    fn recall(&self, request: &RecallRequest, deadline: Instant) -> Result<Vec<Value>> {
        self.0.rebuild(Tier::Hot, &AtomicBool::new(false))?;
        let mut vector = [0f32; 768];
        vector[0] = 1.;
        Ok(self
            .0
            .snapshot_until(true, false, deadline, |tx, view| {
                Retrieval::current(tx, view, request, &vector, false, deadline)
            })?
            .0)
    }
    fn ready(&self) -> bool {
        true
    }
}
async fn call(state: &crate::AppState, method: &str, path: &str, body: Option<Value>) -> Value {
    use tower::ServiceExt;
    let request = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1:8765")
        .header("content-type", "application/json")
        .body(body.map_or(axum::body::Body::empty(), |body| {
            axum::body::Body::from(body.to_string())
        }))
        .unwrap();
    let response = crate::router(state.clone()).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 4 << 20).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(status.is_success(), "{method} {path}: {status} {value}");
    value
}
fn current_vector(store: &Store, target: &str, version: i64, seq: i64) {
    let mut vector = [0f32; 768];
    vector[0] = 0.9;
    vector[1] = (1f32 - 0.81).sqrt();
    store.write(|tx| tx.put_vector(&VectorWrite{seq,target:target.into(),kind:VectorKind::Node,version,embedding:F32Blob::from_values(&vector)?,current:true,tier:Tier::Hot})).unwrap();
}
fn node_vectors(store: &Store, target: &str) -> Vec<(i64, i64)> {
    store.read(|tx| {
        let mut statement = tx.sql().prepare("SELECT i.version,b.current FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=? AND i.kind='node' ORDER BY i.version")?;
        let values = statement.query_map([target], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(values)
    }).unwrap()
}
#[test]
fn same_id_revisions_flow_through_http_history_and_recall_across_restart() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let text = "Quartz launch report is pending. Quartz launch report is pending legal approval. Quartz launch report was approved on Friday.";
    let (a, episode) = runtime.block_on(async {
        let store = Store::open(dir.path().join("data")).unwrap();
        let episode=store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"revision-api","conversation_id":"revision-api","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap().id;
        let state = crate::AppState::new(store).unwrap();
        let created = call(&state,"POST","/remember",Some(json!({"command_id":"api-v1","actor":"owner","why":"Original state","type":"knowledge","text":"Quartz launch report is pending.","scope":["personal"],"evidence":[{"episode_id":episode,"quote":"Quartz launch report is pending."}]}))).await;
        let a = created["id"].as_str().unwrap().to_owned();
        let refine = json!({"command_id":"api-v2","actor":"owner","why":"Legal condition","old":a,"kind":"refinement","new":{"type":"knowledge","text":"Quartz launch report is pending legal approval.","scope":["personal"]},"evidence":[{"episode_id":episode,"quote":"Quartz launch report is pending legal approval."}]});
        let v2 = call(&state, "POST", "/supersede", Some(refine.clone())).await;
        assert_eq!((v2["id"].as_str(), v2["version"].as_i64()), (Some(a.as_str()), Some(2)));
        assert_eq!(call(&state, "POST", "/supersede", Some(refine)).await, v2);
        let v3 = call(&state,"POST","/supersede",Some(json!({"command_id":"api-v3","actor":"owner","why":"Approved on Friday","old":a,"kind":"change","valid_at":"2026-10-23T09:00:00+03:00","new":{"type":"knowledge","text":"Quartz launch report was approved on Friday.","scope":["personal"]},"evidence":[{"episode_id":episode,"quote":"Quartz launch report was approved on Friday."}]}))).await;
        assert_eq!((v3["id"].as_str(), v3["version"].as_i64()), (Some(a.as_str()), Some(3)));
        (a, episode)
    });
    // Restart: a fresh store, projection and service state over the same files.
    runtime.block_on(async {
        let store = Store::open(dir.path().join("data")).unwrap();
        let projection = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
        let mut state = crate::AppState::new(store.clone()).unwrap();
        state.recall = Some(Arc::new(FixedRecall(projection)));
        let history = call(&state, "GET", &format!("/history/{a}"), None).await;
        let versions = history["versions"].as_array().unwrap();
        let ids = versions.iter().map(|n| (n["id"].as_str().unwrap(), n["version"].as_i64().unwrap())).collect::<Vec<_>>();
        assert_eq!(ids, vec![(a.as_str(), 1), (a.as_str(), 2), (a.as_str(), 3)]);
        assert_eq!(versions[0]["revised_by"], json!({"version":2,"kind":"refinement","valid_at":null}));
        assert_eq!(versions[1]["revised_by"], json!({"version":3,"kind":"change","valid_at":"2026-10-23T06:00:00+00:00"}));
        assert_eq!(versions[2]["text"], "Quartz launch report was approved on Friday.");
        assert_eq!(versions[2]["valid_from"], "2026-10-23T06:00:00+00:00");
        assert_eq!(versions[2]["why"], "Approved on Friday");
        assert_eq!(versions[2]["actor"], "owner");
        assert_eq!(versions[2]["evidence"], json!([{"episode_id":episode,"start":81,"end":125,"quote":"Quartz launch report was approved on Friday."}]));
        assert!(versions[2]["diff"].to_string().contains("Friday"));
        // Prior vectors are retired; the indexer embeds only the current version.
        assert!(node_vectors(&store, &a).is_empty());
        current_vector(&store, &a, 3, 1);
        let recalled = call(&state, "GET", "/recall?q=Quartz%20launch%20report&limit=5", None).await;
        let memories = recalled["memories"].as_array().unwrap();
        assert_eq!(memories[0]["id"], a.as_str());
        assert_eq!(memories[0]["version"], 3);
        assert_eq!(memories[0]["text"], "Quartz launch report was approved on Friday.");
        assert!(memories.iter().all(|m| m["id"] == a.as_str() || m["type"] == "raw_episode"));
    });
}
#[test]
fn revision_rebinds_node_vectors_to_the_current_version() {
    let mut f = Fixture::new();
    let a = f.add("Quartz cache rotation is weekly.", "knowledge", "2026-10-01T00:00:00Z", 0.5, 0.9, "personal");
    let text = "Quartz cache rotation is daily.";
    let source=f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"rotation","conversation_id":"rotation","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap();
    let correct = |key: &str, scope: &str| f.graph.supersede(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Corrected rotation","old":a,"kind":"correction","new":{"type":"knowledge","text":text,"scope":[scope]},"evidence":[{"episode_id":source.id,"quote":text}]})).unwrap()).unwrap();
    assert_eq!(correct("daily", "personal")["version"], 2);
    // Changed text retires the old vector; recall never serves stale content.
    assert_eq!(node_vectors(&f.store, &a), vec![(1, 0)]);
    assert!(f.recall("quartz", 5, false, Some("personal")).iter().all(|row| row["id"] != a.as_str() || row["text"] == text));
    current_vector(&f.store, &a, 2, 100);
    let rows = f.recall("quartz", 5, false, Some("personal"));
    assert_eq!((rows[0]["id"].as_str(), rows[0]["version"].as_i64(), rows[0]["text"].as_str()), (Some(a.as_str()), Some(2), Some(text)));
    // Unchanged text keeps the current embedding under the next version.
    assert_eq!(correct("scope", "professional")["version"], 3);
    assert_eq!(node_vectors(&f.store, &a), vec![(1, 0), (3, 1)]);
    let rows = f.recall("quartz", 5, false, Some("professional"));
    assert_eq!((rows[0]["id"].as_str(), rows[0]["version"].as_i64()), (Some(a.as_str()), Some(3)));
    assert!(f.recall("quartz", 5, false, Some("personal")).iter().all(|row| row["id"] != a.as_str()));
}
