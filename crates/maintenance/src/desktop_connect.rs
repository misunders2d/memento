//! Explicit owner Desktop grant. No account discovery, alternate identity,
//! scheduler, Python runtime or service activation is performed here.
use crate::{
    binding::{self, PublicBinding},
    drive::DriveColdStore,
    drive_auth::{AUTH_URI, Binding, Client, ConnectionGrant, SCOPE, TOKEN_URI},
    fs_safe,
    transport::{self, Request, Transport},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use memento_core::{Error, Result, raw};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
#[derive(Clone)]
pub struct ConnectOptions {
    pub client_json: PathBuf,
    pub account: String,
    pub output: PathBuf,
    pub port: u16,
    pub folder_id: Option<String>,
    pub data_dir: Option<PathBuf>,
}
pub struct PendingConnect {
    options: ConnectOptions,
    client: Client,
    account: String,
    folder: Option<String>,
    state: String,
    verifier: String,
    url: String,
    redirect: String,
    deadline: Instant,
    consumed: bool,
    client_sha: String,
    previous_sha: Option<String>,
}
#[derive(serde::Serialize)]
pub struct Connected {
    pub output: PathBuf,
    pub binding: PublicBinding,
}
fn account_eq(a: &str, b: &str) -> bool {
    memento_domain::unicode::casefold(a) == memento_domain::unicode::casefold(b)
}
fn private_value(path: &Path) -> Result<(Value, String)> {
    let bytes = fs_safe::read_private(path, 65536)?;
    Ok((serde_json::from_slice(&bytes)?, raw::sha256(&bytes)))
}
fn client_value(path: &Path) -> Result<(Value, String)> {
    // Desktop client downloads need not be chmodded before explicit use. Only
    // the resulting refresh-token connection is required to be owner-private.
    let mut bytes = Vec::new();
    fs_safe::regular(path)?
        .take(65537)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(Error::Validation("Desktop OAuth client exceeds size limit"));
    }
    Ok((serde_json::from_slice(&bytes)?, raw::sha256(&bytes)))
}
fn output_sha(path: &Path) -> Result<Option<String>> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(Some(private_value(path)?.1)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
impl PendingConnect {
    pub fn begin(mut options: ConnectOptions) -> Result<Self> {
        if options.port == 0 {
            return Err(Error::Validation(
                "An explicit account and valid loopback port are required",
            ));
        }
        options.account = options.account.trim().to_owned();
        if options.account.is_empty()
            || options.account.chars().count() > 320
            || options.account.chars().any(|c| (c as u32) < 32)
        {
            return Err(Error::Validation(
                "An explicit personal Drive account is required",
            ));
        }
        if let Some(folder) = &options.folder_id {
            binding::object_id(folder)?;
        }
        fs_safe::plain(&options.output)?;
        let previous_sha = output_sha(&options.output)?;
        let previous = if previous_sha.is_some() {
            Some(ConnectionGrant::load_file(
                &options.output,
                options.data_dir.as_deref(),
            )?)
        } else {
            None
        };
        if previous
            .as_ref()
            .is_some_and(|g| !account_eq(&g.binding.public.account, &options.account))
        {
            return Err(Error::Conflict(
                "Reconnect must use the previously verified account",
            ));
        }
        if let (Some(previous), Some(folder)) = (&previous, &options.folder_id) {
            if previous.binding.public.folder_id != *folder {
                return Err(Error::Conflict(
                    "Reconnect must use the previously verified folder",
                ));
            }
        }
        let (value, client_sha) = client_value(&options.client_json)?;
        let client = Client::from_desktop(&value)?;
        if previous
            .as_ref()
            .is_some_and(|g| g.binding.client.id != client.id)
        {
            return Err(Error::Conflict(
                "Reconnect must use the original OAuth client",
            ));
        }
        let pin = options
            .data_dir
            .as_deref()
            .map(binding::read_binding)
            .transpose()?
            .flatten();
        let mut folder = options.folder_id.clone().or_else(|| {
            previous
                .as_ref()
                .map(|g| g.binding.public.folder_id.clone())
        });
        if folder.is_none() {
            folder = pin.as_ref().map(|p| p.folder_id.clone());
        }
        if let Some(pin) = &pin {
            let supplied = PublicBinding {
                client_id: client.id.clone(),
                account: options.account.clone(),
                folder_id: folder.clone().unwrap_or_default(),
            };
            if !pin.matches(&supplied) {
                return Err(Error::Conflict(
                    "Drive binding differs from the restored instance",
                ));
            }
        }
        let state = format!(
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
        let redirect = format!("http://127.0.0.1:{}/", options.port);
        let query = transport::form(&[
            ("client_id", &client.id),
            ("redirect_uri", &redirect),
            ("response_type", "code"),
            ("scope", SCOPE),
            ("state", &state),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("access_type", "offline"),
            ("prompt", "consent"),
            ("login_hint", &options.account),
            ("include_granted_scopes", "false"),
        ]);
        let url = format!("{AUTH_URI}?{}", String::from_utf8(query).unwrap());
        let account = options.account.clone();
        Ok(Self {
            options,
            client,
            account,
            folder,
            state,
            verifier,
            url,
            redirect,
            deadline: Instant::now() + Duration::from_secs(600),
            consumed: false,
            client_sha,
            previous_sha,
        })
    }
    pub fn authorization_url(&self) -> &str {
        &self.url
    }
    pub fn port(&self) -> u16 {
        self.options.port
    }
    pub fn expires_at(&self) -> Instant {
        self.deadline
    }
    pub fn accepts_state(&self, state: &str) -> bool {
        if !(16..=512).contains(&state.len()) || !state.is_ascii() {
            return false;
        }
        state
            .bytes()
            .zip(self.state.bytes())
            .fold(state.len() ^ self.state.len(), |v, (a, b)| {
                v | usize::from(a ^ b)
            })
            == 0
    }
    fn unchanged(&self) -> Result<()> {
        if client_value(&self.options.client_json)?.1 != self.client_sha
            || output_sha(&self.options.output)? != self.previous_sha
        {
            return Err(Error::Conflict(
                "Original Drive connection changed during consent",
            ));
        }
        if let Some(data) = &self.options.data_dir {
            if let Some(pin) = binding::read_binding(data)? {
                let current = PublicBinding {
                    client_id: self.client.id.clone(),
                    account: self.account.clone(),
                    folder_id: self.folder.clone().unwrap_or_default(),
                };
                if !pin.matches(&current) {
                    return Err(Error::Conflict(
                        "Original Drive binding changed during consent",
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn finish(
        &mut self,
        state: &str,
        code: Option<&str>,
        error: Option<&str>,
        provider: Arc<dyn Transport>,
    ) -> Result<Connected> {
        if self.consumed || Instant::now() >= self.deadline {
            return Err(Error::Conflict(
                "Drive authorization expired or was already used",
            ));
        }
        if !self.accepts_state(state) {
            return Err(Error::Validation("Drive authorization state differs"));
        }
        self.consumed = true;
        if code.is_some() == error.is_some()
            || error.is_some()
            || code.is_none_or(|c| c.trim().is_empty() || c.len() > 4096)
        {
            return Err(Error::Validation("Drive authorization was rejected"));
        }
        self.unchanged()?;
        let body = transport::form(&[
            ("client_id", &self.client.id),
            ("client_secret", &self.client.secret),
            ("code", code.unwrap()),
            ("code_verifier", &self.verifier),
            ("redirect_uri", &self.redirect),
            ("grant_type", "authorization_code"),
        ]);
        let (response, value) = transport::json_response(
            provider.as_ref(),
            Request {
                method: "POST",
                url: TOKEN_URI.into(),
                headers: vec![(
                    "Content-Type".into(),
                    "application/x-www-form-urlencoded".into(),
                )],
                body,
            },
        )?;
        if response.status != 200 {
            return Err(Error::Unavailable(
                "Drive authorization could not be completed",
            ));
        }
        if value.get("scope").is_some_and(|v| {
            v.as_str().map(|s| s.split_whitespace().collect::<Vec<_>>()) != Some(vec![SCOPE])
        }) || value["token_type"]
            .as_str()
            .is_some_and(|s| !s.eq_ignore_ascii_case("bearer"))
        {
            return Err(Error::Validation(
                "A renewable drive.file-only grant is required",
            ));
        }
        let access = value["access_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Validation(
                "A renewable drive.file-only grant is required",
            ))?;
        let refresh = value["refresh_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Validation(
                "A renewable drive.file-only grant is required",
            ))?;
        let seconds = value["expires_in"]
            .as_i64()
            .filter(|s| *s > 0 && *s <= 31536000)
            .ok_or(Error::Validation("Invalid Drive grant expiry"))?;
        // Account verification always precedes optional first-folder creation.
        let headers = vec![("Authorization".into(), format!("Bearer {access}"))];
        let (response, about) = transport::json_response(
            provider.as_ref(),
            Request {
                method: "GET",
                url: "https://www.googleapis.com/drive/v3/about?fields=user%28emailAddress%29"
                    .into(),
                headers: headers.clone(),
                body: vec![],
            },
        )?;
        if response.status != 200 {
            return Err(Error::Unavailable("Drive account verification failed"));
        }
        if !account_eq(
            about["user"]["emailAddress"].as_str().unwrap_or(""),
            &self.account,
        ) {
            return Err(Error::Validation(
                "Drive OAuth account differs from the explicitly configured personal account",
            ));
        }
        self.unchanged()?;
        if self.folder.is_none() {
            let (response, folder) = transport::json_response(
                provider.as_ref(),
                Request {
                    method: "POST",
                    url: "https://www.googleapis.com/drive/v3/files?fields=id".into(),
                    headers: headers
                        .into_iter()
                        .chain(std::iter::once((
                            "Content-Type".into(),
                            "application/json".into(),
                        )))
                        .collect(),
                    body: serde_json::to_vec(
                        &json!({"name":"Memento","mimeType":"application/vnd.google-apps.folder","appProperties":{"memento":"cold-storage"}}),
                    )?,
                },
            )?;
            if response.status != 200 && response.status != 201 {
                return Err(Error::Unavailable(
                    "Initial dedicated Drive folder creation failed",
                ));
            }
            let id = folder["id"].as_str().ok_or(Error::Unavailable(
                "Initial dedicated Drive folder was not returned",
            ))?;
            binding::object_id(id)?;
            self.folder = Some(id.into());
        }
        let binding = PublicBinding {
            client_id: self.client.id.clone(),
            account: self.account.clone(),
            folder_id: self.folder.clone().unwrap(),
        };
        binding.validate()?;
        let grant = ConnectionGrant {
            binding: Binding {
                client: self.client.clone(),
                public: binding.clone(),
            },
            access_token: access.into(),
            refresh_token: refresh.into(),
            expires_at: Utc::now() + chrono::Duration::seconds(seconds),
        };
        // The shared backend verifies the same account and owned/untrashed folder.
        let _verified = DriveColdStore::verified(
            grant.clone_secret(),
            provider,
            self.options.data_dir.clone(),
        )?;
        self.unchanged()?;
        let parent = self
            .options
            .output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs_safe::plain(parent)?;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
        grant.publish_file(&self.options.output, self.options.data_dir.as_deref())?;
        Ok(Connected {
            output: self.options.output.clone(),
            binding,
        })
    }
}
pub struct Callback {
    pub state: String,
    pub code: Option<String>,
    pub error: Option<String>,
}
#[cfg(test)]
#[path = "desktop_connect_tests.rs"]
mod tests;
fn decode(input: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let data = input.as_bytes();
    let mut i = 0;
    while i < data.len() {
        match data[i] {
            b'+' => bytes.push(b' '),
            b'%' => {
                if i + 2 >= data.len() {
                    return Err(Error::Validation("Invalid authorization callback"));
                }
                let hex = |v: u8| (v as char).to_digit(16);
                bytes.push(
                    ((hex(data[i + 1])
                        .ok_or(Error::Validation("Invalid authorization callback"))?
                        << 4)
                        | hex(data[i + 2])
                            .ok_or(Error::Validation("Invalid authorization callback"))?)
                        as u8,
                );
                i += 2;
            }
            value => bytes.push(value),
        }
        i += 1;
    }
    String::from_utf8(bytes).map_err(|_| Error::Validation("Invalid authorization callback"))
}
pub fn callback(target: &str) -> Result<Callback> {
    let (path, query) = target
        .split_once('?')
        .ok_or(Error::Validation("Invalid authorization callback"))?;
    if path != "/" || target.len() > 16384 {
        return Err(Error::Validation("Invalid authorization callback"));
    }
    let mut fields = BTreeMap::new();
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode(k)?;
        if fields.insert(key, decode(v)?).is_some() {
            return Err(Error::Validation("Duplicate authorization callback field"));
        }
    }
    let state = fields.remove("state").ok_or(Error::Validation(
        "Authorization callback state is required",
    ))?;
    Ok(Callback {
        state,
        code: fields.remove("code"),
        error: fields.remove("error"),
    })
}
