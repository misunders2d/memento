//! Read-only query adapters; no authority mutation or index/model work.
use crate::{
    ApiError, AppState,
    assets::ClientAssets,
    blocking,
    operational_status::{self, DriveStatus, RuntimeStatus},
    viewer,
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use memento_core::{Error, Result, Tier};
use serde_json::{Value, json};
use std::{
    io::Write,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::Semaphore;
fn protect(mut response: Response, cache: &'static str) -> Response {
    let headers = response.headers_mut();
    headers.insert("cache-control", HeaderValue::from_static(cache));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(viewer::CSP),
    );
    response
}
fn host(state: &AppState, headers: &HeaderMap) -> Result<()> {
    if !headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|h| viewer::host_allowed(h, &state.allowed_hosts))
    {
        return Err(Error::Validation("Viewer host is not allowed"));
    }
    Ok(())
}
fn denied() -> Response {
    protect(StatusCode::FORBIDDEN.into_response(), "no-store")
}
fn reply(value: Value, headers: &HeaderMap) -> Response {
    let body = match serde_json::to_vec(&value) {
        Ok(b) => b,
        Err(_) => {
            return protect(
                ApiError(Error::Unavailable("Query serialization failed")).into_response(),
                "no-store",
            );
        }
    };
    let gzip = headers
        .get("accept-encoding")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| {
            s.split(',')
                .any(|part| part.trim().split(';').next() == Some("gzip"))
        });
    let response = if body.len() > 1024 && gzip {
        let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(5));
        if writer.write_all(&body).is_err() {
            return protect(
                ApiError(Error::Unavailable("Query compression failed")).into_response(),
                "no-store",
            );
        }
        match writer.finish() {
            Ok(bytes) => (
                [
                    ("content-type", "application/json"),
                    ("content-encoding", "gzip"),
                    ("vary", "Accept-Encoding"),
                ],
                bytes,
            )
                .into_response(),
            Err(_) => {
                return protect(
                    ApiError(Error::Unavailable("Query compression failed")).into_response(),
                    "no-store",
                );
            }
        }
    } else {
        Json(value).into_response()
    };
    protect(response, "no-store")
}
fn result(value: std::result::Result<Value, ApiError>, headers: &HeaderMap) -> Response {
    match value {
        Ok(v) => reply(v, headers),
        Err(e) => protect(e.into_response(), "no-store"),
    }
}
pub async fn connect() -> Response {
    protect(
        (
            [("content-type", "text/plain; charset=utf-8")],
            crate::connect_guide::connect_guide(),
        )
            .into_response(),
        "no-store",
    )
}
pub async fn redirect(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if host(&state, &headers).is_err() {
        return denied();
    }
    protect(
        (StatusCode::TEMPORARY_REDIRECT, [("location", "/ui/")]).into_response(),
        "no-cache",
    )
}
pub async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    static_asset(state, headers, "index.html")
}
pub async fn asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(file): Path<String>,
) -> Response {
    if !viewer::STATIC_FILES.contains(&file.as_str()) {
        return protect(StatusCode::NOT_FOUND.into_response(), "no-cache");
    }
    static_asset(state, headers, &file)
}
fn static_asset(state: AppState, headers: HeaderMap, file: &str) -> Response {
    if host(&state, &headers).is_err() {
        return denied();
    }
    match ClientAssets::ui(file) {
        Some((mime, bytes)) => protect(
            ([("content-type", mime)], bytes).into_response(),
            "no-cache",
        ),
        None => protect(StatusCode::NOT_FOUND.into_response(), "no-cache"),
    }
}
async fn admitted_blocking<T: Send + 'static>(
    admission: Arc<Semaphore>,
    apply: impl FnOnce() -> Result<T> + Send + 'static,
) -> std::result::Result<T, ApiError> {
    let permit = admission
        .acquire_owned()
        .await
        .map_err(|_| ApiError(Error::Unavailable("Service worker is unavailable")))?;
    blocking(move || {
        // A canceled HTTP waiter cannot release admission while its SQL still runs.
        let _permit = permit;
        apply()
    })
    .await
}
pub async fn status(
    State(state): State<AppState>,
    query: std::result::Result<
        Query<operational_status::StatusQuery>,
        axum::extract::rejection::QueryRejection,
    >,
) -> std::result::Result<Response, ApiError> {
    let Query(query) = query.map_err(|_| ApiError(Error::Validation("Invalid status query")))?;
    Ok(protect(
        Json(
            admitted_blocking(state.status_admission.clone(), move || {
                let mut runtime = RuntimeStatus::disabled();
                if let Some(services) = &state.services {
                    let cold = services.cold_status()?;
                    runtime.drive = DriveStatus {
                        connected: services.cold().is_some(),
                        reauthorization_required: services.reauthorization.load(Ordering::Acquire),
                        archive_failure_count: cold
                            .as_ref()
                            .and_then(|v| v["sweep_failure_count"].as_u64()),
                        archive_error: cold
                            .as_ref()
                            .and_then(|v| v["last_error"].as_str())
                            .map(str::to_owned),
                    };
                }
                state.store.read(|tx| {
                    if state.recall.is_some() {
                        let projection = state.services.as_ref().and_then(|s| s.projection());
                        let failed = state
                            .services
                            .as_ref()
                            .is_some_and(|s| s.index_error.load(Ordering::Acquire));
                        runtime.indexing = crate::indexing::status(
                            tx.sql(),
                            query.source.as_deref(),
                            failed,
                            projection.as_ref().is_some_and(|p| p.ready(Tier::Hot)),
                            projection.as_ref().is_some_and(|p| p.ready(Tier::Cold)),
                        )?;
                    }
                    operational_status::operational_status(
                        tx.sql(),
                        query.source.as_deref(),
                        &runtime,
                        memento_domain::jobs::timestamp(),
                    )
                })
            })
            .await?,
        )
        .into_response(),
        "no-store",
    ))
}
#[cfg(test)]
#[path = "status_admission_tests.rs"]
mod status_admission_tests;
macro_rules! query {
    ($name:ident,$dto:ty,$method:ident) => {
        pub async fn $name(
            State(state): State<AppState>,
            headers: HeaderMap,
            query: std::result::Result<Query<$dto>, axum::extract::rejection::QueryRejection>,
        ) -> Response {
            if host(&state, &headers).is_err() {
                return denied();
            }
            let Query(query) = match query {
                Ok(q) => q,
                Err(_) => {
                    return protect(
                        ApiError(Error::Validation("Invalid viewer query")).into_response(),
                        "no-store",
                    )
                }
            };
            let admission = state.viewer_admission.clone();
            let value = admitted_blocking(admission, move || {
                state.store.read(|tx| viewer::$method(tx.sql(), &query))
            })
            .await;
            result(value, &headers)
        }
    };
}
query!(search, viewer::SearchQuery, search);
query!(conversations, viewer::PageQuery, conversations);
query!(candidates, viewer::CandidatesQuery, candidates);
pub async fn graph(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: std::result::Result<Query<viewer::GraphQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    if host(&state, &headers).is_err() {
        return denied();
    }
    let Query(query) = match query {
        Ok(q) => q,
        Err(_) => {
            return protect(
                ApiError(Error::Validation("Invalid viewer query")).into_response(),
                "no-store",
            );
        }
    };
    let admission = state.viewer_admission.clone();
    result(
        admitted_blocking(admission, move || {
            state
                .store
                .read(|tx| viewer::graph(tx.sql(), &query, &memento_core::now_utc()))
        })
        .await,
        &headers,
    )
}
pub async fn node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if host(&state, &headers).is_err() {
        return denied();
    }
    let admission = state.viewer_admission.clone();
    result(
        admitted_blocking(admission, move || {
            state
                .store
                .read(|tx| viewer::node(tx.sql(), &id, &memento_core::now_utc()))
        })
        .await,
        &headers,
    )
}
pub async fn conversation(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: std::result::Result<
        Query<viewer::ConversationQuery>,
        axum::extract::rejection::QueryRejection,
    >,
) -> Response {
    if host(&state, &headers).is_err() {
        return denied();
    }
    let Query(query) = match query {
        Ok(q) => q,
        Err(_) => {
            return protect(
                ApiError(Error::Validation("Invalid viewer query")).into_response(),
                "no-store",
            );
        }
    };
    result(
        blocking(move || {
            // Select the immutable source identities without fetching anything under the snapshot.
            let mut sources = Vec::new();
            let mut value = state.store.read(|tx| {
                viewer::conversation(tx.sql(), &query, |row| {
                    sources.push(
                        row["id"]
                            .as_str()
                            .ok_or(Error::Unavailable("Conversation source is unavailable"))?
                            .to_owned(),
                    );
                    Ok(None)
                })
            })?;
            for (item, id) in value["items"]
                .as_array_mut()
                .ok_or(Error::Unavailable("Conversation page is unavailable"))?
                .iter_mut()
                .zip(sources)
            {
                let text = (|| {
                    let _leases = state.prefetch_episode(&id)?;
                    state.store.read(|tx| tx.original_text(&id))
                })();
                match text {
                    Ok(Some(text)) => {
                        item["truncated"] = json!(text.chars().count() > 4000);
                        item["text"] = json!(text.chars().take(4000).collect::<String>());
                    }
                    Ok(None) | Err(Error::Unavailable(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(value)
        })
        .await,
        &headers,
    )
}
