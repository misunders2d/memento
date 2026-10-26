//! Explicit Desktop OAuth, original binding only. Secret-bearing types have no
//! Debug; all provider failures become fixed, non-reflective status codes.
use crate::{
    binding::{self, PublicBinding},
    fs_safe,
    transport::{self, Request, Transport},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use memento_core::{Error, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
pub const SCOPE: &str = "https://www.googleapis.com/auth/drive.file";
pub const AUTH_URI: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
#[derive(Clone)]
pub struct Client {
    pub(crate) id: String,
    pub(crate) secret: String,
}
impl Client {
    pub fn from_desktop(value: &Value) -> Result<Self> {
        let installed = &value["installed"];
        if value.get("web").is_some()
            || !installed.is_object()
            || ![AUTH_URI, "https://accounts.google.com/o/oauth2/auth"]
                .contains(&installed["auth_uri"].as_str().unwrap_or(""))
            || installed["token_uri"] != TOKEN_URI
        {
            return Err(Error::Validation(
                "Original Google Desktop OAuth client is required",
            ));
        }
        for key in ["client_id", "client_secret"] {
            let text = installed[key]
                .as_str()
                .ok_or(Error::Validation("Invalid Desktop OAuth client"))?;
            if text.is_empty() || text != text.trim() {
                return Err(Error::Validation("Invalid Desktop OAuth client"));
            }
        }
        Ok(Self {
            id: installed["client_id"].as_str().unwrap().into(),
            secret: installed["client_secret"].as_str().unwrap().into(),
        })
    }
    fn desktop(&self) -> Value {
        json!({"installed":{"client_id":self.id,"client_secret":self.secret,"auth_uri":AUTH_URI,"token_uri":TOKEN_URI}})
    }
}
#[derive(Clone)]
pub struct Binding {
    pub public: PublicBinding,
    pub(crate) client: Client,
}
impl Binding {
    fn matches(&self, other: &Self) -> bool {
        self.public.matches(&other.public) && self.client.secret == other.client.secret
    }
}
pub struct ConnectionGrant {
    pub(crate) binding: Binding,
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) expires_at: DateTime<Utc>,
}
impl ConnectionGrant {
    pub fn public_binding(&self) -> &PublicBinding {
        &self.binding.public
    }
    fn from_value(value: &Value) -> Result<Self> {
        if value["type"] != "authorized_user"
            || value["scopes"] != json!([SCOPE])
            || value["token_uri"].as_str().unwrap_or(TOKEN_URI) != TOKEN_URI
        {
            return Err(Error::Validation(
                "A renewable drive.file-only grant is required",
            ));
        }
        let client = Client::from_desktop(
            &json!({"installed":{"client_id":value["client_id"],"client_secret":value["client_secret"],"auth_uri":AUTH_URI,"token_uri":TOKEN_URI}}),
        )?;
        let public = PublicBinding {
            client_id: client.id.clone(),
            account: value["memento"]["account"].as_str().unwrap_or("").into(),
            folder_id: value["memento"]["folder_id"].as_str().unwrap_or("").into(),
        };
        public.validate()?;
        let refresh_token = value["refresh_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Validation(
                "A renewable drive.file-only grant is required",
            ))?
            .to_owned();
        let expires_at = value["expiry"]
            .as_str()
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        Ok(Self {
            binding: Binding { public, client },
            access_token: value["token"].as_str().unwrap_or("").into(),
            refresh_token,
            expires_at,
        })
    }
    fn private_json(&self) -> Value {
        json!({"type":"authorized_user","token":self.access_token,"refresh_token":self.refresh_token,"token_uri":TOKEN_URI,"client_id":self.binding.client.id,"client_secret":self.binding.client.secret,"scopes":[SCOPE],"expiry":self.expires_at.to_rfc3339(),"memento":{"account":self.binding.public.account,"folder_id":self.binding.public.folder_id}})
    }
    pub fn publish(&self, data: &Path) -> Result<()> {
        binding::check_binding(data, &self.binding.public)?;
        fs_safe::atomic_private(
            &data.join("drive-oauth.json"),
            &serde_json::to_vec(&self.private_json())?,
        )
    }
    /// Explicit owner connection output; no ambient credential location.
    pub fn publish_file(&self, path: &Path, data: Option<&Path>) -> Result<()> {
        if let Some(data) = data {
            binding::check_binding(data, &self.binding.public)?;
        }
        fs_safe::plain(path)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs_safe::plain(parent)?;
        if !fs::metadata(parent)?.is_dir() {
            return Err(Error::Validation("Connection output requires a directory"));
        }
        let temporary = parent.join(format!(
            ".memento-publish-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let cleanup = fs_safe::RemoveFile(temporary.clone());
        let mut file = fs_safe::create(&temporary)?;
        file.write_all(&serde_json::to_vec(&self.private_json())?)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs_safe::sync_dir(parent)?;
        drop(cleanup);
        Ok(())
    }
    pub fn load(data: &Path) -> Result<Self> {
        Self::load_file(&data.join("drive-oauth.json"), Some(data))
    }
    /// Explicit owner restore file; never discovers ambient credentials.
    pub fn load_file(path: &Path, data: Option<&Path>) -> Result<Self> {
        let value: Value = serde_json::from_slice(&fs_safe::read_private(path, 65536)?)?;
        let grant = Self::from_value(&value)?;
        if let Some(data) = data {
            binding::check_binding(data, &grant.binding.public)?;
        }
        Ok(grant)
    }
    pub(crate) fn token(
        &mut self,
        transport: &dyn Transport,
        data: Option<&Path>,
    ) -> Result<String> {
        if !self.access_token.is_empty()
            && self.expires_at > Utc::now() + chrono::Duration::seconds(30)
        {
            return Ok(self.access_token.clone());
        }
        let request = Request {
            method: "POST",
            url: TOKEN_URI.into(),
            headers: vec![(
                "Content-Type".into(),
                "application/x-www-form-urlencoded".into(),
            )],
            body: transport::form(&[
                ("client_id", &self.binding.client.id),
                ("client_secret", &self.binding.client.secret),
                ("refresh_token", &self.refresh_token),
                ("grant_type", "refresh_token"),
            ]),
        };
        let (response, value) = transport::json_response(transport, request)?;
        if response.status == 400
            && ["invalid_grant", "invalid_rapt"].contains(&value["error"].as_str().unwrap_or(""))
        {
            return Err(Error::Unavailable("Drive reauthorization required"));
        }
        if response.status != 200 {
            return Err(Error::Unavailable("Drive token refresh failed"));
        }
        if value.get("scope").is_some_and(|s| {
            s.as_str().map(|v| v.split_whitespace().collect::<Vec<_>>()) != Some(vec![SCOPE])
        }) {
            return Err(Error::Validation("Drive refresh scope changed"));
        }
        let access = value["access_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Unavailable("Drive token refresh returned no grant"))?;
        let seconds = value["expires_in"]
            .as_i64()
            .filter(|s| *s > 0 && *s <= 31536000)
            .ok_or(Error::Unavailable("Drive token refresh returned no expiry"))?;
        self.access_token = access.into();
        self.expires_at = Utc::now() + chrono::Duration::seconds(seconds);
        if let Some(data) = data {
            self.publish(data)?;
        }
        Ok(self.access_token.clone())
    }
}
pub fn load_bindings(data: &Path) -> Result<Binding> {
    let mut bindings = Vec::new();
    let bootstrap = data.join("drive-client.json");
    let installed = data.join("drive-oauth.json");
    if bootstrap.try_exists()? || std::fs::symlink_metadata(&bootstrap).is_ok() {
        let value: Value = serde_json::from_slice(&fs_safe::read_private(&bootstrap, 65536)?)?;
        let client = Client::from_desktop(&value)?;
        let public = PublicBinding {
            client_id: client.id.clone(),
            account: value["memento"]["account"].as_str().unwrap_or("").into(),
            folder_id: value["memento"]["folder_id"].as_str().unwrap_or("").into(),
        };
        public.validate()?;
        bindings.push(Binding { public, client });
    }
    if installed.try_exists()? || std::fs::symlink_metadata(&installed).is_ok() {
        bindings.push(ConnectionGrant::load(data)?.binding);
    }
    let first = bindings
        .first()
        .ok_or(Error::Unavailable("Drive authorization is not configured"))?
        .clone();
    if bindings.iter().any(|b| !first.matches(b)) {
        return Err(Error::Conflict(
            "Original Drive connection identities disagree",
        ));
    }
    binding::check_binding(data, &first.public)?;
    Ok(first)
}
pub fn configure_client(
    client_file: &Path,
    account: &str,
    folder: &str,
    data: &Path,
) -> Result<()> {
    fs_safe::private_dir(data)?;
    let value: Value = serde_json::from_slice(&fs_safe::read_private(client_file, 65536)?)?;
    let client = Client::from_desktop(&value)?;
    let public = PublicBinding {
        client_id: client.id.clone(),
        account: account.into(),
        folder_id: folder.into(),
    };
    public.validate()?;
    binding::check_binding(data, &public)?;
    if value.get("memento").is_some()
        && (value["memento"]["account"] != account || value["memento"]["folder_id"] != folder)
    {
        return Err(Error::Conflict("Declared original Drive binding differs"));
    }
    let bound = Binding { public, client };
    if ["drive-client.json", "drive-oauth.json"]
        .iter()
        .any(|name| std::fs::symlink_metadata(data.join(name)).is_ok())
        && !bound.matches(&load_bindings(data)?)
    {
        return Err(Error::Conflict(
            "Reconnect must preserve the original Drive binding",
        ));
    }
    let mut published = bound.client.desktop();
    published["memento"] = json!({"account":account,"folder_id":folder});
    fs_safe::atomic_private(
        &data.join("drive-client.json"),
        &serde_json::to_vec(&published)?,
    )
}
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct AuthorizationError {
    pub status: u16,
    pub code: &'static str,
}
impl std::fmt::Display for AuthorizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.code)
    }
}
impl std::error::Error for AuthorizationError {}
type AuthResult<T> = std::result::Result<T, AuthorizationError>;
pub fn requires_reauthorization(error: &Error) -> bool {
    matches!(error, Error::Unavailable("Drive reauthorization required"))
}
fn auth(status: u16) -> AuthorizationError {
    AuthorizationError {
        status,
        code: match status {
            400 => "drive_authorization_rejected",
            409 => "drive_authorization_conflict",
            410 => "drive_authorization_expired",
            503 => "drive_authorization_unavailable",
            _ => "drive_authorization_failed",
        },
    }
}
#[derive(serde::Serialize)]
pub struct Begin {
    pub authorization_url: String,
    pub state: String,
    pub expires_in: u64,
}
impl std::fmt::Debug for Begin {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Begin")
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}
struct Pending {
    state: String,
    verifier: String,
    redirect: String,
    deadline: Instant,
    binding: Binding,
    reconnect: bool,
}
struct State {
    pending: Option<Pending>,
    closed: bool,
}
pub type Install =
    dyn Fn(Arc<crate::drive::DriveColdStore>, ConnectionGrant, bool) -> Result<()> + Send + Sync;
pub struct DriveAuthorization {
    data: PathBuf,
    transport: Arc<dyn Transport>,
    install: Arc<Install>,
    state: Mutex<State>,
}
impl DriveAuthorization {
    pub fn new(data: PathBuf, transport: Arc<dyn Transport>, install: Arc<Install>) -> Self {
        Self {
            data,
            transport,
            install,
            state: Mutex::new(State {
                pending: None,
                closed: false,
            }),
        }
    }
    pub fn begin(&self, callback_port: u16, reconnect: bool) -> AuthResult<Begin> {
        if callback_port == 0 {
            return Err(auth(400));
        }
        let mut state = self.state.lock().map_err(|_| auth(503))?;
        if state.closed {
            return Err(auth(503));
        }
        if state
            .pending
            .as_ref()
            .is_some_and(|p| Instant::now() < p.deadline)
        {
            return Err(auth(409));
        }
        state.pending = None;
        let binding = load_bindings(&self.data).map_err(|e| {
            auth(if matches!(e, Error::Conflict(_)) {
                409
            } else {
                503
            })
        })?;
        let secret = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let verifier = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let redirect = format!("http://127.0.0.1:{callback_port}/");
        let query = String::from_utf8(transport::form(&[
            ("client_id", &binding.client.id),
            ("redirect_uri", &redirect),
            ("response_type", "code"),
            ("scope", SCOPE),
            ("state", &secret),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("access_type", "offline"),
            ("prompt", "consent"),
            ("login_hint", &binding.public.account),
            ("include_granted_scopes", "false"),
        ]))
        .unwrap();
        state.pending = Some(Pending {
            state: secret.clone(),
            verifier,
            redirect,
            deadline: Instant::now() + Duration::from_secs(600),
            binding,
            reconnect,
        });
        Ok(Begin {
            authorization_url: format!("{AUTH_URI}?{query}"),
            state: secret,
            expires_in: 600,
        })
    }
    pub fn finish(
        &self,
        provided: &str,
        code: Option<&str>,
        error: Option<&str>,
    ) -> AuthResult<Value> {
        if !(16..=512).contains(&provided.len()) || !provided.is_ascii() {
            return Err(auth(400));
        }
        let mut state = self.state.lock().map_err(|_| auth(503))?;
        if state.closed {
            return Err(auth(503));
        }
        let pending = state.pending.as_ref().ok_or(auth(410))?;
        if Instant::now() >= pending.deadline {
            state.pending = None;
            return Err(auth(410));
        }
        let difference = provided
            .bytes()
            .zip(pending.state.bytes())
            .fold(provided.len() ^ pending.state.len(), |v, (a, b)| {
                v | usize::from(a ^ b)
            });
        if difference != 0 {
            return Err(auth(400));
        }
        let pending = state.pending.take().unwrap();
        // Genuine state consumes the flow, including malformed/cancelled responses.
        if code.is_some() == error.is_some()
            || error.is_some()
            || code.is_none_or(|v| v.trim().is_empty() || v.len() > 4096)
        {
            return Err(auth(400));
        }
        let check = || -> AuthResult<()> {
            let current = load_bindings(&self.data).map_err(|_| auth(409))?;
            if !pending.binding.matches(&current) {
                return Err(auth(409));
            }
            Ok(())
        };
        check()?;
        let body = transport::form(&[
            ("client_id", &pending.binding.client.id),
            ("client_secret", &pending.binding.client.secret),
            ("code", code.unwrap()),
            ("code_verifier", &pending.verifier),
            ("redirect_uri", &pending.redirect),
            ("grant_type", "authorization_code"),
        ]);
        let (response, value) = transport::json_response(
            self.transport.as_ref(),
            Request {
                method: "POST",
                url: TOKEN_URI.into(),
                headers: vec![(
                    "Content-Type".into(),
                    "application/x-www-form-urlencoded".into(),
                )],
                body,
            },
        )
        .map_err(|_| auth(502))?;
        if response.status != 200 {
            return Err(auth(if response.status == 400 { 400 } else { 502 }));
        }
        if value.get("scope").is_some_and(|s| {
            s.as_str().map(|s| s.split_whitespace().collect::<Vec<_>>()) != Some(vec![SCOPE])
        }) || value["token_type"]
            .as_str()
            .is_some_and(|s| !s.eq_ignore_ascii_case("bearer"))
        {
            return Err(auth(400));
        }
        let access = value["access_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(auth(400))?;
        let refresh = value["refresh_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(auth(400))?;
        let expires = value["expires_in"]
            .as_i64()
            .filter(|s| *s > 0 && *s <= 31536000)
            .ok_or(auth(400))?;
        let grant = ConnectionGrant {
            binding: pending.binding.clone(),
            access_token: access.into(),
            refresh_token: refresh.into(),
            expires_at: Utc::now() + chrono::Duration::seconds(expires),
        };
        let backend = crate::drive::DriveColdStore::verified(
            grant.clone_secret(),
            self.transport.clone(),
            Some(self.data.clone()),
        )
        .map_err(|e| {
            auth(if matches!(e, Error::Validation(_)) {
                400
            } else {
                502
            })
        })?;
        check()?;
        (self.install)(backend, grant, pending.reconnect).map_err(|e| {
            auth(if matches!(e, Error::Conflict(_)) {
                409
            } else {
                502
            })
        })?;
        Ok(json!({"status":"connected"}))
    }
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.pending = None;
        }
    }
}
impl ConnectionGrant {
    pub(crate) fn clone_secret(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            access_token: self.access_token.clone(),
            refresh_token: self.refresh_token.clone(),
            expires_at: self.expires_at,
        }
    }
}
