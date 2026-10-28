//! Native service transport. Owner commands use a separate peer-gated Unix socket.
mod anchors;
pub mod assets;
pub mod bootstrap;
mod connect_guide;
mod drive_api;
pub mod indexing;
mod legacy_text;
pub mod mcp;
mod media_input;
pub mod operational_status;
mod query_api;
mod query_rows;
mod records;
pub mod retrieval;
pub mod runtime;
#[cfg(test)]
mod status_count_tests;
pub mod text_reranker;
pub mod viewer;
#[cfg(test)]
mod viewer_tests;
use assets::ClientAssets;
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Path, Query, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use memento_core::{Error, Result, Store, capture::EpisodeInput};
use memento_domain::{
    Graph, dto,
    processing::{self, Processing},
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    io::{Cursor, Read},
    sync::Arc,
    time::Instant,
};
pub(crate) fn host_parts(host: &str) -> Option<(String, Option<u16>)> {
    if host.is_empty()
        || host
            .chars()
            .any(|c| c.is_ascii_control() || c == ' ' || ['@', '/', '\\'].contains(&c))
    {
        return None;
    }
    let (name, port) = if let Some(rest) = host.strip_prefix('[') {
        let end = rest.find(']')?;
        let name = &rest[..end];
        name.parse::<std::net::Ipv6Addr>().ok()?;
        let tail = &rest[end + 1..];
        (
            name,
            if tail.is_empty() {
                None
            } else {
                Some(tail.strip_prefix(':')?)
            },
        )
    } else if host.matches(':').count() <= 1 {
        match host.split_once(':') {
            Some((name, port)) => (name, Some(port)),
            None => (host, None),
        }
    } else {
        host.parse::<std::net::Ipv6Addr>().ok()?;
        (host, None)
    };
    if name.is_empty() {
        return None;
    }
    // HTTP authority parsing validates DNS/IPv4 spellings; bracketed IPv6 was validated above.
    if !name.contains(':') {
        name.parse::<ureq::http::uri::Authority>().ok()?;
    }
    Some((
        name.to_ascii_lowercase(),
        port.map(str::parse::<u16>).transpose().ok()?,
    ))
}
pub const CAPTURE_BYTES: usize = 1024 * 1024;
pub const BLOB_BYTES: usize = 1024 * 1024 * 1024;
#[derive(Clone, Debug, Deserialize)]
pub struct RecallRequest {
    #[serde(default, rename = "q")]
    pub query: String,
    #[serde(default = "normal")]
    pub effort: String,
    #[serde(default = "ten")]
    pub limit: usize,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub automatic: bool,
    #[serde(default)]
    pub context: String,
}
impl Default for RecallRequest {
    fn default() -> Self {
        Self {
            query: String::new(),
            effort: normal(),
            limit: ten(),
            scope: None,
            since: None,
            source: None,
            automatic: false,
            context: String::new(),
        }
    }
}
fn normal() -> String {
    "normal".into()
}
fn ten() -> usize {
    10
}
fn twenty() -> usize {
    20
}
/// Implemented by the native retrieval coordinator. Absence fails semantic requests closed.
pub trait Recall: Send + Sync {
    fn recall(&self, request: &RecallRequest, deadline: Instant) -> Result<Vec<Value>>;
    fn ready(&self) -> bool;
}
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub graph: Graph,
    pub processing: Processing,
    pub recall: Option<Arc<dyn Recall>>,
    pub assets: Arc<ClientAssets>,
    pub allowed_hosts: Arc<Vec<String>>,
    pub media_enabled: bool,
    pub services: Option<Arc<runtime::Services>>,
    status_admission: Arc<tokio::sync::Semaphore>,
}
impl AppState {
    pub fn new(store: Arc<Store>) -> Result<Self> {
        let graph = Graph::new(store.clone());
        Ok(Self {
            store,
            processing: Processing::new(graph.clone()),
            graph,
            recall: None,
            assets: Arc::new(ClientAssets::build()?),
            allowed_hosts: Arc::new(Vec::new()),
            media_enabled: false,
            services: None,
            status_admission: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }
    /// Hydrate cold opaque runs outside the SQL snapshot and retain leases while
    /// the local-only raw decoder verifies the exact immutable bytes.
    pub(crate) fn prefetch_episode(
        &self,
        id: &str,
    ) -> Result<Vec<memento_core::blobs::BlobReader>> {
        let Some(cold) = self.services.as_ref().and_then(|s| s.cold()) else {
            return Ok(Vec::new());
        };
        let refs=self.store.read(|tx|{use rusqlite::OptionalExtension;let value:Option<Option<String>>=tx.sql().query_row("SELECT text_runs FROM episodes WHERE id=coalesce((SELECT episode_id FROM episode_chunks WHERE id=?1),?1)",[id],|r|r.get(0)).optional()?;Ok(value.flatten())})?;
        let mut leases = Vec::new();
        if let Some(refs) = refs {
            let rows: Vec<(usize, String, usize)> = serde_json::from_str(&refs)?;
            for (_, sha, _) in rows {
                leases.push(cold.reader(&sha)?);
            }
        }
        Ok(leases)
    }
    pub(crate) fn submit_media(&self, value: Value) -> Result<Value> {
        let command = parse::<media_input::MediaInput>(value)?.checked()?;
        if !self.media_enabled {
            return Err(Error::Unavailable("Native embedding runtime is disabled"));
        }
        let sha = command["sha256"].as_str().unwrap();
        if !self.store.read(|tx| {
            Ok(tx.sql().query_row(
                "SELECT EXISTS(SELECT 1 FROM blobs WHERE sha256=?)",
                [sha],
                |r| r.get::<_, bool>(0),
            )?)
        })? {
            return Err(Error::NotFound("Source blob not found"));
        }
        self.processing.queue.submit_media(&command)
    }
    pub fn recall_memories(
        &self,
        request: &RecallRequest,
        deadline: Instant,
    ) -> Result<Vec<Value>> {
        if !["cheap", "normal", "deep"].contains(&request.effort.as_str())
            || !(1..=100).contains(&request.limit)
            || request
                .scope
                .as_ref()
                .is_some_and(|s| !["personal", "professional"].contains(&s.as_str()))
        {
            return Err(Error::Validation("Invalid effort, limit or scope"));
        }
        if request
            .source
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.chars().count() > 256)
        {
            return Err(Error::Validation("Invalid source filter"));
        }
        if request.query.chars().count() > 4096
            || request.query.contains('\0')
            || request.context.chars().count() > 4096
        {
            return Err(Error::Validation(
                "Recall text must be at most 4096 characters without NUL",
            ));
        }
        if !request.query.trim().is_empty() && request.since.is_none() && request.source.is_none() {
            return self
                .recall
                .as_ref()
                .ok_or(Error::Unavailable("Native embedding runtime is disabled"))?
                .recall(request, deadline);
        }
        if request.scope.is_some() {
            return Ok(Vec::new());
        }
        self.store
            .search_episodes(
                Some(&request.query),
                request.limit,
                None,
                request.source.as_deref(),
                request.since.as_deref(),
            )?
            .into_iter()
            .map(|mut row| {
                row["type"] = json!("raw_episode");
                row["tier"] = json!("raw");
                row["state"] = json!("raw");
                row["confirmed"] = json!(false);
                row["text"] = row["excerpt"].clone();
                row["score"] = json!(0.0);
                row["source_url"] =
                    json!(format!("/episodes/{}", row["id"].as_str().unwrap_or("")));
                row["match"] = json!(if request.query.trim().is_empty() {
                    "activity"
                } else {
                    "keyword"
                });
                Ok(row)
            })
            .collect()
    }
}
#[derive(Debug)]
pub struct ApiError(pub Error);
impl From<Error> for ApiError {
    fn from(value: Error) -> Self {
        Self(value)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::Validation("Request body exceeds the endpoint size limit") => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Error::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, Json(json!({"detail":self.0.to_string()}))).into_response()
    }
}
pub(crate) fn parse<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| Error::Validation("Invalid request fields"))
}
async fn json_body<T: DeserializeOwned>(request: Request) -> std::result::Result<T, ApiError> {
    if let Some(kind) = request.headers().get("content-type") {
        let valid = kind
            .to_str()
            .ok()
            .and_then(|s| s.split(';').next())
            .is_some_and(|s| {
                let s = s.trim().to_ascii_lowercase();
                s == "application/json" || (s.starts_with("application/") && s.ends_with("+json"))
            });
        if !valid {
            return Err(ApiError(Error::Validation("Invalid request fields")));
        }
    }
    let bytes = to_bytes(request.into_body(), CAPTURE_BYTES)
        .await
        .map_err(|_| {
            ApiError(Error::Validation(
                "Request body exceeds the endpoint size limit",
            ))
        })?;
    serde_json::from_slice(&bytes)
        .map_err(|_| ApiError(Error::Validation("Invalid request fields")))
}
pub(crate) async fn blocking<T: Send + 'static>(
    apply: impl FnOnce() -> Result<T> + Send + 'static,
) -> std::result::Result<T, ApiError> {
    tokio::task::spawn_blocking(apply)
        .await
        .map_err(|_| ApiError(Error::Unavailable("Service worker is unavailable")))?
        .map_err(ApiError)
}
async fn limits(request: Request, next: Next) -> Response {
    let maximum = if request.uri().path() == "/blobs" {
        BLOB_BYTES
    } else {
        CAPTURE_BYTES
    };
    if let Some(length) = request.headers().get("content-length") {
        let Ok(length) = length.to_str() else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let Ok(length) = length.parse::<u64>() else {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        };
        if length > maximum as u64 {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
    }
    next.run(request).await
}
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/docs", get(api_docs))
        .route("/redoc", get(api_redoc))
        .route("/openapi.json", get(api_schema))
        .route("/status", get(query_api::status))
        .route("/connect", get(query_api::connect))
        .route("/ui", get(query_api::redirect))
        .route("/ui/", get(query_api::index))
        .route("/ui/api/graph", get(query_api::graph))
        .route("/ui/api/node/{id}", get(query_api::node))
        .route("/ui/api/search", get(query_api::search))
        .route("/ui/api/conversations", get(query_api::conversations))
        .route("/ui/api/conversation", get(query_api::conversation))
        .route("/ui/api/candidates", get(query_api::candidates))
        .route("/ui/static/{*file}", get(query_api::asset))
        .route("/drive/connect/begin", post(drive_api::begin))
        .route("/drive/connect/finish", post(drive_api::finish))
        .route("/episodes", post(capture))
        .route("/episodes/search", get(search))
        .route("/episodes/{id}", get(episode))
        .route("/remember", post(remember))
        .route("/link", post(link))
        .route("/supersede", post(supersede))
        .route("/loop", post(loop_command))
        .route("/history/{id}", get(history))
        .route("/usage", post(usage))
        .route("/blobs", post(upload_blob))
        .route("/blobs/{sha}", get(get_blob))
        .route("/media", post(media))
        .route("/jobs/{id}", get(job))
        .route("/recall", get(recall))
        .route("/work/claim", post(claim))
        .route("/work/pool", post(pool))
        .route("/work", post(work))
        .route("/work/compaction", post(compact))
        .route("/work/renew", post(renew))
        .route("/work/complete", post(complete))
        .route("/work/release", post(release))
        .route("/work/maintenance", post(maintenance))
        .route("/runner", get(runner))
        .route("/bridge", get(bridge))
        .route("/client-release", get(client_release))
        .route("/integrations/{name}", get(integration))
        .route(
            "/mcp",
            get(mcp::redirect).post(mcp::redirect).delete(mcp::redirect),
        )
        .route(
            "/mcp/",
            post(mcp::request)
                .get(mcp::unsupported)
                .delete(mcp::unsupported),
        )
        .layer(middleware::from_fn(limits))
        .with_state(state)
}
async fn health(State(state): State<AppState>) -> std::result::Result<Json<Value>, ApiError> {
    let store = state.store.clone();
    blocking(move || store.health()).await?;
    if let Some(services) = &state.services {
        if services
            .index_error
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unavailable("Native indexing failed; work remains pending").into());
        }
        if services
            .reauthorization
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unavailable("Drive reauthorization required").into());
        }
        if let Some(message) = services.startup_error.lock().ok().and_then(|e| *e) {
            return Err(Error::Unavailable(message).into());
        }
        if services
            .cold_status()?
            .is_some_and(|s| !s["last_error"].is_null())
        {
            return Err(Error::Unavailable("Cold archival incomplete").into());
        }
    }
    let mut result = json!({"status":"ok","processing":"host_runner"});
    if let Some(recall) = &state.recall {
        result["search_ready"] = json!(recall.ready());
    }
    Ok(Json(result))
}
async fn capture(
    State(state): State<AppState>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    let command: EpisodeInput = json_body(request).await?;
    let result = blocking(move || state.store.capture(command)).await?;
    Ok((
        if result.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(json!({"id":result.id})),
    )
        .into_response())
}
#[derive(Deserialize)]
struct SearchRequest {
    #[serde(default)]
    q: Option<String>,
    #[serde(default = "twenty")]
    limit: usize,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    since: Option<String>,
}
async fn search(
    State(state): State<AppState>,
    query: std::result::Result<Query<SearchRequest>, axum::extract::rejection::QueryRejection>,
) -> std::result::Result<Json<Value>, ApiError> {
    let Query(q) = query.map_err(|_| ApiError(Error::Validation("Invalid search query")))?;
    let episodes = blocking(move || {
        state.store.search_episodes(
            q.q.as_deref(),
            q.limit,
            q.conversation_id.as_deref(),
            q.source.as_deref(),
            q.since.as_deref(),
        )
    })
    .await?;
    Ok(Json(json!({"episodes":episodes})))
}
async fn episode(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<Value>, ApiError> {
    Ok(Json(
        blocking(move || {
            let _leases = state.prefetch_episode(&id)?;
            state
                .store
                .episode(&id)?
                .ok_or(Error::NotFound("Episode not found"))
        })
        .await?,
    ))
}
async fn remember(
    State(state): State<AppState>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    let value: Value = json_body(request).await?;
    if value.get("sha256").is_some() {
        return queue_media(state, value).await;
    }
    let command: dto::Remember = parse(value)?;
    Ok(Json(blocking(move || state.graph.remember(command)).await?).into_response())
}
macro_rules! command {
    ($name:ident,$dto:ty,$field:ident,$method:ident) => {
        async fn $name(
            State(state): State<AppState>,
            request: Request,
        ) -> std::result::Result<Json<Value>, ApiError> {
            let command: $dto = json_body(request).await?;
            Ok(Json(blocking(move || state.$field.$method(command)).await?))
        }
    };
}
command!(link, dto::Link, graph, link);
command!(supersede, dto::Supersede, graph, supersede);
command!(loop_command, dto::Loop, graph, loop_command);
command!(usage, dto::Citation, graph, cite);
command!(claim, processing::Claim, processing, claim);
command!(pool, processing::Pool, processing, pool);
command!(work, processing::Work, processing, work);
command!(compact, processing::Compaction, processing, compact);
command!(renew, processing::Lease, processing, renew);
command!(complete, processing::Complete, processing, complete);
command!(release, processing::Release, processing, release);
command!(maintenance, processing::Maintenance, processing, maintain);
async fn history(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<Value>, ApiError> {
    let versions = blocking(move || {
        let _leases = state.prefetch_episode(&id)?;
        state.graph.history(&id)
    })
    .await?;
    if versions.is_empty() {
        return Err(Error::NotFound("Node not found").into());
    }
    Ok(Json(json!({"versions":versions})))
}
async fn recall(
    State(state): State<AppState>,
    query: std::result::Result<Query<RecallRequest>, axum::extract::rejection::QueryRejection>,
) -> std::result::Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError(Error::Validation("Invalid recall query")))?;
    let revision = state.assets.revision.clone();
    let deadline =
        Instant::now() + std::time::Duration::from_secs(if query.automatic { 15 } else { 30 });
    let memories = blocking(move || state.recall_memories(&query, deadline)).await?;
    Ok(Json(
        json!({"client_revision":revision,"memories":memories}),
    ))
}
struct ChannelReader {
    receiver: tokio::sync::mpsc::Receiver<std::io::Result<Bytes>>,
    pending: Cursor<Bytes>,
}
impl Read for ChannelReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let read = self.pending.read(buffer)?;
            if read != 0 || buffer.is_empty() {
                return Ok(read);
            }
            match self.receiver.blocking_recv() {
                Some(Ok(bytes)) => self.pending = Cursor::new(bytes),
                Some(Err(error)) => return Err(error),
                None => return Ok(0),
            }
        }
    }
}
async fn upload_blob(
    State(state): State<AppState>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    let mime = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    memento_core::blobs::normalize_mime(&mime)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let blobs = state.store.blobs().clone();
    let writer = tokio::task::spawn_blocking(move || {
        blobs.put(
            &mut ChannelReader {
                receiver,
                pending: Cursor::new(Bytes::new()),
            },
            BLOB_BYTES as u64,
        )
    });
    let mut stream = request.into_body().into_data_stream();
    let mut size = 0;
    let mut failed = None;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                size += bytes.len();
                if size > BLOB_BYTES {
                    failed = Some(StatusCode::PAYLOAD_TOO_LARGE);
                    let _ = sender.send(Err(std::io::Error::other("body limit"))).await;
                    break;
                }
                if sender.send(Ok(bytes)).await.is_err() {
                    break;
                }
            }
            Err(_) => {
                failed = Some(StatusCode::BAD_REQUEST);
                let _ = sender
                    .send(Err(std::io::Error::other("body stream failed")))
                    .await;
                break;
            }
        }
    }
    drop(sender);
    let result = writer
        .await
        .map_err(|_| ApiError(Error::Unavailable("Artifact worker is unavailable")))?;
    if let Some(status) = failed {
        return Ok(status.into_response());
    }
    let info = result?;
    let registered =
        blocking(move || state.store.write(|tx| tx.register_blob(&info, &mime))).await?;
    Ok((StatusCode::CREATED, Json(registered)).into_response())
}
async fn get_blob(
    State(state): State<AppState>,
    Path(sha): Path<String>,
) -> std::result::Result<Response, ApiError> {
    let disposition = format!("attachment; filename=\"{sha}\"");
    let (reader, mime, size) = blocking(move || {
        let info = state
            .store
            .read(|tx| {
                use rusqlite::OptionalExtension;
                Ok(tx
                    .sql()
                    .query_row("SELECT mime,size FROM blobs WHERE sha256=?", [&sha], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?))
                    })
                    .optional()?)
            })?
            .ok_or(Error::NotFound("Blob not found"))?;
        let reader = match state.services.as_ref().and_then(|s| s.cold()) {
            Some(cold) => cold.reader(&sha),
            None => state.store.blobs().reader(&sha),
        }
        .map_err(|e| match e {
            Error::Io(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Error::NotFound("Blob not found")
            }
            other => other,
        })?;
        Ok((reader, info.0, info.1))
    })
    .await?;
    let (sender, receiver) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(2);
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        loop {
            let mut chunk = vec![0; 1024 * 1024];
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(length) => {
                    chunk.truncate(length);
                    if sender.blocking_send(Ok(Bytes::from(chunk))).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = sender.blocking_send(Err(e));
                    break;
                }
            }
        }
    });
    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|value| (value, receiver))
    });
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_str(&mime)
            .map_err(|_| ApiError(Error::Unavailable("Invalid artifact metadata")))?,
    );
    response.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );
    response.headers_mut().insert(
        "content-disposition",
        HeaderValue::from_str(&disposition)
            .map_err(|_| ApiError(Error::Validation("Invalid artifact digest")))?,
    );
    Ok(response)
}
async fn media(
    State(state): State<AppState>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    queue_media(state, json_body(request).await?).await
}
async fn queue_media(state: AppState, value: Value) -> std::result::Result<Response, ApiError> {
    let result = blocking(move || state.submit_media(value)).await?;
    Ok((StatusCode::ACCEPTED, Json(result)).into_response())
}

async fn job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<Value>, ApiError> {
    Ok(Json(
        blocking(move || {
            state
                .processing
                .queue
                .get(&id)?
                .ok_or(Error::NotFound("Job not found"))
        })
        .await?,
    ))
}
async fn runner() -> Response {
    (
        [("content-type", "text/x-python; charset=utf-8")],
        ClientAssets::raw("runner").unwrap(),
    )
        .into_response()
}
async fn bridge(State(state): State<AppState>) -> Response {
    (
        [("content-type", "text/x-python; charset=utf-8")],
        state.assets.bridge.clone(),
    )
        .into_response()
}
async fn client_release(State(state): State<AppState>) -> Response {
    (
        [("cache-control", "no-store")],
        Json(state.assets.release.clone()),
    )
        .into_response()
}
async fn integration(Path(name): Path<String>) -> Response {
    match ClientAssets::raw(&name) {
        Some(bytes) => ([("content-type", "text/plain; charset=utf-8")], bytes).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn api_docs() -> Response {
    (
        [
            ("content-type", "text/html; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        include_bytes!("swagger.html").as_slice(),
    )
        .into_response()
}
async fn api_redoc() -> Response {
    (
        [
            ("content-type", "text/html; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        include_bytes!("redoc.html").as_slice(),
    )
        .into_response()
}
async fn api_schema() -> Response {
    (
        [
            ("content-type", "application/json"),
            ("cache-control", "no-store"),
        ],
        include_bytes!("protocol-openapi.json").as_slice(),
    )
        .into_response()
}
