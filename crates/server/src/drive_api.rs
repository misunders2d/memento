//! Native runner-only OAuth relay; owner consent stays in the browser loopback flow.
use crate::{ApiError, AppState, json_body};
use axum::{
    Json,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use memento_core::Error;
use serde::Deserialize;
use serde_json::{Value, json};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Begin {
    callback_port: u16,
    #[serde(default)]
    reconnect: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Finish {
    state: String,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    error: Option<String>,
}
fn denied() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"detail":"Use the Memento runner for Drive authorization"})),
    )
        .into_response()
}
fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}
fn authorization(
    state: &AppState,
) -> Result<std::sync::Arc<memento_maintenance::drive_auth::DriveAuthorization>, ApiError> {
    state
        .services
        .as_ref()
        .and_then(|s| s.drive_authorization.read().ok().and_then(|v| v.clone()))
        .ok_or_else(|| ApiError(Error::Unavailable("Drive authorization is unavailable")))
}
pub async fn begin(State(state): State<AppState>, request: Request) -> Response {
    if request.headers().contains_key("origin") {
        return no_store(denied());
    }
    let value: Begin = match json_body(request).await {
        Ok(v) => v,
        Err(e) => return no_store(e.into_response()),
    };
    if state.services.as_ref().and_then(|s| s.cold()).is_some() && !value.reconnect {
        return no_store(
            (
                StatusCode::CONFLICT,
                Json(json!({"detail":"Drive is already connected"})),
            )
                .into_response(),
        );
    }
    if value.callback_port == 0 {
        return no_store(ApiError(Error::Validation("Invalid request fields")).into_response());
    }
    let auth = match authorization(&state) {
        Ok(a) => a,
        Err(e) => return no_store(e.into_response()),
    };
    no_store(
        match tokio::task::spawn_blocking(move || auth.begin(value.callback_port, value.reconnect))
            .await
        {
            Ok(Ok(value)) => Json(json!(value)).into_response(),
            Ok(Err(error)) => (
                StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
                Json(json!({"detail":error.code})),
            )
                .into_response(),
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({"detail":"drive_authorization_failed"})),
            )
                .into_response(),
        },
    )
}
pub async fn finish(State(state): State<AppState>, request: Request) -> Response {
    if request.headers().contains_key("origin") {
        return no_store(denied());
    }
    let value: Finish = match json_body(request).await {
        Ok(v) => v,
        Err(e) => return no_store(e.into_response()),
    };
    if !(16..=512).contains(&value.state.chars().count())
        || value
            .code
            .as_ref()
            .is_some_and(|v| v.is_empty() || v.chars().count() > 4096)
        || value
            .error
            .as_ref()
            .is_some_and(|v| v.is_empty() || v.chars().count() > 256)
        || (value.code.is_some() == value.error.is_some())
    {
        return no_store(ApiError(Error::Validation("Invalid request fields")).into_response());
    }
    let auth = match authorization(&state) {
        Ok(a) => a,
        Err(e) => return no_store(e.into_response()),
    };
    no_store(
        match tokio::task::spawn_blocking(move || {
            auth.finish(&value.state, value.code.as_deref(), value.error.as_deref())
        })
        .await
        {
            Ok(Ok(value)) => Json::<Value>(value).into_response(),
            Ok(Err(error)) => (
                StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
                Json(json!({"detail":error.code})),
            )
                .into_response(),
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({"detail":"drive_authorization_failed"})),
            )
                .into_response(),
        },
    )
}
