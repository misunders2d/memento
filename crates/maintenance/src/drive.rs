//! Google Drive v3 transport with resumable uploads and end-to-end checksums.
//! Only explicit original-account credentials are accepted; no ambient auth.
use crate::{
    binding::{self, PublicBinding},
    cold::{ColdObject, ColdStore, RemoteBackup},
    drive_auth::ConnectionGrant,
    fs_safe,
    portable::sha_valid,
    transport::{self, Request, Response, Transport},
};
use memento_core::{Error, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
};
const API: &str = "https://www.googleapis.com/drive/v3";
const UPLOAD: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,size,md5Checksum,sha256Checksum";
const CHUNK: usize = 8 * 1024 * 1024;
pub struct DriveColdStore {
    grant: Mutex<ConnectionGrant>,
    transport: Arc<dyn Transport>,
    data: Option<PathBuf>,
    binding: PublicBinding,
    sessions: Mutex<HashMap<String, String>>,
}
fn unavailable() -> Error {
    Error::Unavailable("Drive operation failed; original data retained")
}
fn mutex<T>(value: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    value.lock().map_err(|_| unavailable())
}
fn remote_size(value: &Value) -> Option<u64> {
    value
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| value.as_u64())
}
pub fn hashes(path: &Path) -> Result<(String, String, u64)> {
    let mut file = fs_safe::regular(path)?;
    let mut sha = Sha256::new();
    let mut md5 = <md5::Md5 as md5::Digest>::new();
    let mut size = 0_u64;
    let mut buf = vec![0; 1024 * 1024];
    loop {
        let count = file.read(&mut buf)?;
        if count == 0 {
            break;
        }
        sha.update(&buf[..count]);
        md5::Digest::update(&mut md5, &buf[..count]);
        size += count as u64;
    }
    Ok((
        sha.finalize().iter().map(|b| format!("{b:02x}")).collect(),
        md5::Digest::finalize(md5)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        size,
    ))
}
struct HashSink {
    sha: Sha256,
    size: u64,
}
impl Write for HashSink {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.sha.update(b);
        self.size += b.len() as u64;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl DriveColdStore {
    pub fn from_file(data: PathBuf, transport: Arc<dyn Transport>) -> Result<Arc<Self>> {
        Self::from_connection_file(&data.join("drive-oauth.json"), Some(&data), transport)
    }
    pub fn from_connection_file(
        path: &Path,
        data: Option<&Path>,
        transport: Arc<dyn Transport>,
    ) -> Result<Arc<Self>> {
        let grant = ConnectionGrant::load_file(path, data)?;
        Self::verified(grant, transport, data.map(Path::to_path_buf))
    }
    pub fn verified(
        mut grant: ConnectionGrant,
        transport: Arc<dyn Transport>,
        data: Option<PathBuf>,
    ) -> Result<Arc<Self>> {
        // No new credentials are published during verification; old grant remains
        // installed until the caller's explicit install callback succeeds.
        if let Some(data) = &data {
            binding::check_binding(data, grant.public_binding())?;
        }
        let token = grant.token(transport.as_ref(), None)?;
        let public = grant.public_binding().clone();
        let send = |url: String| {
            transport::json_response(
                transport.as_ref(),
                Request {
                    method: "GET",
                    url,
                    headers: vec![("Authorization".into(), format!("Bearer {token}"))],
                    body: vec![],
                },
            )
        };
        let (response, about) = send(format!("{API}/about?fields=user%28emailAddress%29"))?;
        if response.status != 200 {
            return Err(unavailable());
        }
        if memento_domain::unicode::casefold(about["user"]["emailAddress"].as_str().unwrap_or(""))
            != memento_domain::unicode::casefold(&public.account)
        {
            return Err(Error::Validation(
                "OAuth account differs from the explicitly configured personal account",
            ));
        }
        let (response, folder) = send(format!(
            "{API}/files/{}?fields=id,mimeType,trashed,ownedByMe",
            public.folder_id
        ))?;
        if response.status != 200 {
            return Err(unavailable());
        }
        if folder["id"] != public.folder_id
            || folder["mimeType"] != "application/vnd.google-apps.folder"
            || folder["trashed"] != false
            || folder["ownedByMe"] != true
        {
            return Err(Error::Validation(
                "Cold storage requires the original owned untrashed folder",
            ));
        }
        Ok(Arc::new(Self {
            grant: Mutex::new(grant),
            transport,
            data,
            binding: public,
            sessions: Mutex::new(HashMap::new()),
        }))
    }
    fn request(
        &self,
        method: &'static str,
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<Request> {
        let token = mutex(&self.grant)?.token(self.transport.as_ref(), self.data.as_deref())?;
        let mut all = vec![("Authorization".into(), format!("Bearer {token}"))];
        all.extend(headers);
        Ok(Request {
            method,
            url,
            headers: all,
            body,
        })
    }
    fn json(
        &self,
        method: &'static str,
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(Response, Value)> {
        let result = transport::json_response(
            self.transport.as_ref(),
            self.request(method, url.clone(), headers.clone(), body.clone())?,
        )?;
        if result.0.status != 401 {
            return Ok(result);
        }
        mutex(&self.grant)?.expires_at = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
        transport::json_response(
            self.transport.as_ref(),
            self.request(method, url, headers, body)?,
        )
    }
    fn session_url(url: &str) -> Result<String> {
        let uri: ureq::http::Uri = url
            .parse()
            .map_err(|_| Error::Validation("Invalid Drive resumable session"))?;
        if uri.scheme_str() != Some("https")
            || uri
                .authority()
                .is_none_or(|a| a.as_str() != "www.googleapis.com")
            || uri.path() != "/upload/drive/v3/files"
            || uri.query().is_none()
        {
            return Err(Error::Validation(
                "Drive resumable session escaped the official endpoint",
            ));
        }
        Ok(url.into())
    }
    fn download_to(&self, id: &str, target: &mut dyn Write, size: u64) -> Result<()> {
        let mut position = 0_u64;
        let mut empty = size == 0;
        while position < size || empty {
            empty = false;
            let length = (size - position).min(CHUNK as u64);
            let headers = if size == 0 {
                vec![]
            } else {
                vec![(
                    "Range".into(),
                    format!("bytes={position}-{}", position + length - 1),
                )]
            };
            let mut bytes = Vec::with_capacity(length as usize);
            let mut response = self.transport.send(
                self.request(
                    "GET",
                    format!("{API}/files/{id}?alt=media"),
                    headers.clone(),
                    vec![],
                )?,
                &mut bytes,
                length.max(65536),
            )?;
            if response.status == 401 {
                mutex(&self.grant)?.expires_at = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
                bytes.clear();
                response = self.transport.send(
                    self.request(
                        "GET",
                        format!("{API}/files/{id}?alt=media"),
                        headers,
                        vec![],
                    )?,
                    &mut bytes,
                    length.max(65536),
                )?;
            }
            if bytes.len() as u64 != length {
                return Err(Error::Unavailable(
                    "Drive original range has the wrong length",
                ));
            }
            match response.status {
                206 if size > 0
                    && response.headers.get("content-range")
                        == Some(&format!(
                            "bytes {position}-{}/{size}",
                            position + length - 1
                        )) =>
                {
                    ()
                }
                200 if position == 0 && length == size => (),
                _ => {
                    return Err(Error::Unavailable(
                        "Drive original range could not be verified",
                    ));
                }
            }
            target.write_all(&bytes)?;
            position += length;
        }
        Ok(())
    }
    fn verify_object(&self, id: &str, sha: &str, md5: &str, size: u64) -> Result<ColdObject> {
        binding::object_id(id)?;
        let (response, value) = self.json(
            "GET",
            format!("{API}/files/{id}?fields=id,size,md5Checksum,sha256Checksum"),
            vec![],
            vec![],
        )?;
        if response.status != 200
            || value["id"] != id
            || remote_size(&value["size"]) != Some(size)
            || value["md5Checksum"] != md5
        {
            return Err(Error::Validation(
                "Drive upload identity size or MD5 verification failed",
            ));
        }
        if let Some(remote_sha) = value["sha256Checksum"].as_str() {
            if remote_sha != sha {
                return Err(Error::Validation(
                    "Drive upload SHA-256 verification failed",
                ));
            }
        } else {
            let mut sink = HashSink {
                sha: Sha256::new(),
                size: 0,
            };
            self.download_to(id, &mut sink, size)?;
            let actual: String = sink
                .sha
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            if sink.size != size || actual != sha {
                return Err(Error::Validation(
                    "Drive upload downloaded SHA-256 verification failed",
                ));
            }
        }
        Ok(ColdObject {
            id: id.into(),
            size,
            sha256: sha.into(),
            md5: md5.into(),
        })
    }
    fn position(response: &Response, size: u64) -> Result<u64> {
        let Some(range) = response.headers.get("range") else {
            return Ok(0);
        };
        let end = range
            .strip_prefix("bytes=0-")
            .and_then(|v| v.parse::<u64>().ok())
            .and_then(|v| v.checked_add(1))
            .ok_or(Error::Validation("Invalid Drive upload acknowledgement"))?;
        if end > size {
            return Err(Error::Validation(
                "Drive upload acknowledgement exceeds original size",
            ));
        }
        Ok(end)
    }
}
impl ColdStore for DriveColdStore {
    fn binding(&self) -> &PublicBinding {
        &self.binding
    }
    fn upload(
        &self,
        path: &Path,
        name: &str,
        mime: &str,
        checkpoint: &Value,
        save: &mut dyn FnMut(Value) -> Result<()>,
    ) -> Result<ColdObject> {
        let (sha, md5, size) = hashes(path)?;
        if checkpoint.as_object().is_some_and(|m| !m.is_empty())
            && (checkpoint["sha256"] != sha || checkpoint["size"].as_u64() != Some(size))
        {
            return Err(Error::Validation(
                "Upload checkpoint differs from immutable original",
            ));
        }
        if let Some(id) = checkpoint["object_id"].as_str() {
            return self.verify_object(id, &sha, &md5, size);
        }
        let mut sessions = mutex(&self.sessions)?;
        let mut position = 0_u64;
        let mut completed = None;
        if let Some(session) = sessions.get(&sha).cloned() {
            let (response, value) = self.json(
                "PUT",
                Self::session_url(&session)?,
                vec![
                    ("Content-Length".into(), "0".into()),
                    ("Content-Range".into(), format!("bytes */{size}")),
                ],
                vec![],
            )?;
            match response.status {
                200 | 201 => completed = Some(value),
                308 => position = Self::position(&response, size)?,
                404 | 410 => {
                    sessions.remove(&sha);
                }
                _ => return Err(unavailable()),
            }
        }
        if completed.is_none() && !sessions.contains_key(&sha) {
            let(response,_)=self.json("POST",UPLOAD.into(),vec![("Content-Type".into(),"application/json; charset=UTF-8".into()),("X-Upload-Content-Type".into(),mime.into()),("X-Upload-Content-Length".into(),size.to_string())],serde_json::to_vec(&json!({"name":name,"parents":[self.binding.folder_id],"appProperties":{"sha256":sha}}))?)?;
            if ![200, 201].contains(&response.status) {
                return Err(unavailable());
            }
            let session = Self::session_url(
                response
                    .headers
                    .get("location")
                    .ok_or(Error::Unavailable("Drive upload session was not returned"))?,
            )?;
            sessions.insert(sha.clone(), session);
        }
        save(json!({"sha256":sha,"size":size}))?;
        if completed.is_none() {
            let session = sessions.get(&sha).unwrap().clone();
            let mut source = fs_safe::regular(path)?;
            source.seek(SeekFrom::Start(position))?;
            let mut zero = size == 0;
            while position < size || zero {
                zero = false;
                let length = (size - position).min(CHUNK as u64) as usize;
                let mut body = vec![0; length];
                source.read_exact(&mut body)?;
                let range = if size == 0 {
                    "bytes */0".to_owned()
                } else {
                    format!("bytes {}-{}/{size}", position, position + length as u64 - 1)
                };
                let (response, value) = self.json(
                    "PUT",
                    session.clone(),
                    vec![
                        ("Content-Length".into(), length.to_string()),
                        ("Content-Type".into(), mime.into()),
                        ("Content-Range".into(), range),
                    ],
                    body,
                )?;
                match response.status {
                    200 | 201 => {
                        completed = Some(value);
                        break;
                    }
                    308 => {
                        let next = Self::position(&response, size)?;
                        if next <= position || next >= size {
                            return Err(unavailable());
                        }
                        position = next;
                        source.seek(SeekFrom::Start(position))?;
                    }
                    404 | 410 => {
                        sessions.remove(&sha);
                        return Err(unavailable());
                    }
                    _ => return Err(unavailable()),
                }
            }
        }
        let value = completed.ok_or_else(unavailable)?;
        let id = value["id"].as_str().ok_or_else(unavailable)?;
        let object = self.verify_object(id, &sha, &md5, size)?;
        sessions.remove(&sha);
        save(json!({"object_id":object.id,"sha256":sha,"size":size}))?;
        Ok(object)
    }
    fn download(&self, id: &str, destination: &Path, sha: &str, size: u64) -> Result<()> {
        binding::object_id(id)?;
        if !sha_valid(sha) {
            return Err(Error::Validation("Invalid original artifact checksum"));
        }
        let parent = fs_safe::private_dir(destination.parent().ok_or(Error::Validation(
            "Download needs an existing private parent",
        ))?)?;
        fs_safe::plain(destination)?;
        let temp = parent.join(format!(".cold-{}", uuid::Uuid::new_v4().simple()));
        let _cleanup = fs_safe::RemoveFile(temp.clone());
        let mut file = fs_safe::create(&temp)?;
        self.download_to(id, &mut file, size)?;
        file.sync_all()?;
        drop(file);
        if crate::hash_file(&temp)? != (sha.to_owned(), size) {
            return Err(Error::Validation("Cold artifact SHA-256 or size mismatch"));
        }
        fs::rename(&temp, destination)?;
        fs_safe::sync_dir(&parent)
    }
    fn list_backups(&self) -> Result<Vec<RemoteBackup>> {
        static PATTERN: LazyLock<regex::Regex> = LazyLock::new(|| {
            regex::Regex::new(r"\Amemory-(?:incomplete-(?P<gaps>[1-9][0-9]{0,9})-)?(?P<id>[0-9]{8}T[0-9]{6}-[a-f0-9]{12})-(?P<sha>[a-f0-9]{64})\.sqlite\z").unwrap()
        });
        let mut result = Vec::new();
        let mut token = String::new();
        let mut tokens = std::collections::HashSet::new();
        loop {
            let q = format!(
                "'{}' in parents and trashed=false and name contains 'memory-'",
                self.binding.folder_id
            );
            let url = format!(
                "{API}/files?q={}&fields=nextPageToken,files%28id,name,size,sha256Checksum%29&pageSize=1000&pageToken={}",
                transport::encode(&q),
                transport::encode(&token)
            );
            let (response, value) = self.json("GET", url, vec![], vec![])?;
            if response.status != 200 {
                return Err(unavailable());
            }
            for item in value["files"].as_array().ok_or_else(unavailable)? {
                let Some(captures) = item["name"].as_str().and_then(|s| PATTERN.captures(s)) else {
                    continue;
                };
                let sha = &captures["sha"];
                if item["sha256Checksum"].as_str().is_some_and(|s| s != sha) {
                    return Err(Error::Validation(
                        "Remote backup name and checksum disagree",
                    ));
                }
                let id = item["id"].as_str().ok_or_else(unavailable)?;
                binding::object_id(id)?;
                let gaps = captures
                    .name("gaps")
                    .map(|s| s.as_str().parse::<u64>())
                    .transpose()
                    .map_err(|_| unavailable())?
                    .unwrap_or(0);
                result.push(RemoteBackup {
                    id: captures["id"].into(),
                    remote_id: id.into(),
                    sha256: sha.into(),
                    size: remote_size(&item["size"]).ok_or_else(unavailable)?,
                    gap_count: gaps,
                    complete: gaps == 0,
                });
            }
            token = value["nextPageToken"].as_str().unwrap_or("").into();
            if token.is_empty() {
                break;
            }
            if !tokens.insert(token.clone()) {
                return Err(Error::Unavailable("Drive repeated a backup listing page"));
            }
        }
        result.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(result)
    }
}
