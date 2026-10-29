//! Human OS-admitted owner transport. Never mounted on HTTP or MCP.
use memento_core::{Error, Result};
use memento_domain::{Graph, dto, processing::Processing};
use memento_maintenance::{cold::ColdManager, fs_safe, portable};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::{
            ffi::OsStrExt,
            fs::{FileTypeExt, MetadataExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
const REQUEST_LIMIT: usize = 1024 * 1024;
const RESPONSE_LIMIT: usize = 4 * 1024 * 1024;
pub type ColdSupplier = dyn Fn() -> Option<Arc<ColdManager>> + Send + Sync;
pub type Compact = dyn Fn() -> Result<Value> + Send + Sync;
pub struct AdminServices {
    pub graph: Graph,
    pub processing: Arc<Processing>,
    pub cold: Arc<ColdSupplier>,
    pub compact: Arc<Compact>,
}
impl AdminServices {
    pub fn new(
        graph: Graph,
        processing: Arc<Processing>,
        cold: Arc<ColdSupplier>,
        compact: Arc<Compact>,
    ) -> Result<Arc<Self>> {
        if !Arc::ptr_eq(&graph.store, &processing.graph.store) {
            return Err(Error::Validation(
                "Owner services must share one authority store",
            ));
        }
        Ok(Arc::new(Self {
            graph,
            processing,
            cold,
            compact,
        }))
    }
    fn cold(&self) -> Result<Option<Arc<ColdManager>>> {
        let value = (self.cold)();
        if value
            .as_ref()
            .is_some_and(|m| !Arc::ptr_eq(&m.store, &self.graph.store))
        {
            return Err(Error::Unavailable(
                "Owner backend does not match authority storage",
            ));
        }
        Ok(value)
    }
    fn dispatch(&self, uid: u32, request: &Value) -> Result<Value> {
        let operation = text(request, "operation")?;
        let actor = format!("human:uid:{uid}");
        match operation {
            "list" => Ok(json!(self.graph.pending_reviews()?)),
            "backup" => Ok(serde_json::to_value(
                self.cold()?
                    .ok_or(Error::Unavailable("Drive cold storage is not configured"))?
                    .backup()?,
            )?),
            "export" => {
                let output = Path::new(text(request, "output")?);
                let report = if let Some(cold) = self.cold()? {
                    cold.export_archive(output)?
                } else {
                    portable::export_archive(self.graph.store.data_dir(), output)?
                };
                Ok(serde_json::to_value(report)?)
            }
            "compact" => (self.compact)(),
            "history_status" => self.processing.history_status(),
            "history_park" => self.processing.park_history(),
            "history_release-candidates" => self.processing.release_history_candidates(),
            "approve" | "reject" => self.graph.decide_review(dto::Review {
                command_id: text(request, "command_id")?.into(),
                actor,
                why: text(request, "why")?.into(),
                proposal_id: text(request, "proposal_id")?.into(),
                decision: operation.into(),
            }),
            "inventory" => self.graph.merge_inventory(),
            "merge_preview" => self.graph.merge_preview(
                text(request, "cluster")?,
                optional_text(request, "into")?,
                &list(request, "exclude")?,
                &list(request, "include")?,
            ),
            "merge" => self.graph.merge(dto::Merge {
                command_id: text(request, "command_id")?.into(),
                actor,
                why: text(request, "why")?.into(),
                cluster: text(request, "cluster")?.into(),
                into: text(request, "into")?.into(),
                exclude: list_required(request, "exclude")?,
                token: text(request, "token")?.into(),
                include: list(request, "include")?,
            }),
            "revision_plan" => self.graph.revision_recovery_plan(
                limit(request)?,
                &list(request, "exclude")?,
                &list(request, "protected")?,
            ),
            "recover_revisions" => self.graph.recover_revisions(dto::RecoverRevisions {
                command_id: text(request, "command_id")?.into(),
                actor,
                why: text(request, "why")?.into(),
                limit: limit(request)?,
                exclude: list_required(request, "exclude")?,
                protected: list_required(request, "protected")?,
                token: text(request, "token")?.into(),
            }),
            "forget_preview" => self.graph.forget_preview(text(request, "target")?),
            "forget" => self.graph.forget(dto::Forget {
                command_id: text(request, "command_id")?.into(),
                actor,
                why: text(request, "why")?.into(),
                target: text(request, "target")?.into(),
                token: text(request, "token")?.into(),
            }),
            _ => Err(Error::Validation("Unknown administrative operation")),
        }
    }
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or(Error::Validation("Invalid administrative request"))
}
fn optional_text<'a>(v: &'a Value, key: &str) -> Result<Option<&'a str>> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        _ => Err(Error::Validation("Invalid administrative request")),
    }
}
fn list(v: &Value, key: &str) -> Result<Vec<String>> {
    match v.get(key) {
        None => Ok(Vec::new()),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|_| Error::Validation("Invalid administrative request")),
    }
}
fn limit(v: &Value) -> Result<usize> {
    v.get("limit")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .ok_or(Error::Validation("Invalid administrative request"))
}
fn list_required(v: &Value, key: &str) -> Result<Vec<String>> {
    if v.get(key).is_none() {
        return Err(Error::Validation("Invalid administrative request"));
    }
    list(v, key)
}
fn admitted(uid: u32, service_uid: u32) -> bool {
    uid == 0 || uid == service_uid
}
fn peer_uid(stream: &UnixStream) -> Result<u32> {
    let mut value: libc::ucred = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let code = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut value as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    if code != 0 || length as usize != std::mem::size_of::<libc::ucred>() {
        return Err(Error::Unavailable(
            "Administrative peer credentials are unavailable",
        ));
    }
    Ok(value.uid)
}
pub fn ascii_json(value: &Value) -> Result<Vec<u8>> {
    let encoded = serde_json::to_string(value)?;
    let mut out = String::with_capacity(encoded.len());
    for c in encoded.chars() {
        if c.is_ascii() {
            out.push(c)
        } else {
            let mut units = [0; 2];
            for u in c.encode_utf16(&mut units) {
                out.push_str(&format!("\\u{u:04x}"));
            }
        }
    }
    Ok(out.into_bytes())
}
fn read_line(
    stream: &mut UnixStream,
    maximum: usize,
    deadline: Option<Instant>,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Unavailable("Administrative request timed out"));
            }
            stream.set_read_timeout(Some(remaining))?;
        }
        let length = stream.read(&mut buffer).map_err(|error| {
            if deadline.is_some()
                && [std::io::ErrorKind::TimedOut, std::io::ErrorKind::WouldBlock]
                    .contains(&error.kind())
            {
                Error::Unavailable("Administrative request timed out")
            } else {
                error.into()
            }
        })?;
        if length == 0 {
            return if bytes.is_empty() {
                Err(Error::Validation("Invalid administrative request"))
            } else {
                Ok(bytes)
            };
        }
        let part = &buffer[..length];
        let end = part
            .iter()
            .position(|b| *b == b'\n')
            .map(|n| n + 1)
            .unwrap_or(length);
        if bytes.len() + end > maximum {
            return Err(Error::Validation(
                "Administrative message exceeds size limit",
            ));
        }
        bytes.extend_from_slice(&part[..end]);
        if end < length || bytes.last() == Some(&b'\n') {
            return Ok(bytes);
        }
    }
}
fn handle(mut stream: UnixStream, services: Arc<AdminServices>, service_uid: u32) {
    let result = (|| {
        let uid = peer_uid(&stream)?;
        if !admitted(uid, service_uid) {
            return Err(Error::Validation("Administrative identity required"));
        }
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(30)))?;
        let bytes = read_line(
            &mut stream,
            REQUEST_LIMIT,
            Some(Instant::now() + Duration::from_secs(10)),
        )?;
        let request: Value = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Validation("Invalid administrative request"))?;
        services.dispatch(uid, &request)
    })();
    let response = match result {
        Ok(value) => json!({"result":value}),
        Err(error) => json!({"error":error.to_string()}),
    };
    if let Ok(mut bytes) = ascii_json(&response) {
        bytes.push(b'\n');
        let _ = stream.write_all(&bytes);
    }
    let _ = stream.shutdown(std::net::Shutdown::Both);
}
struct State {
    stopping: AtomicBool,
    workers: Mutex<Vec<JoinHandle<()>>>,
}
pub struct OwnerServer {
    state: Arc<State>,
    accept: Mutex<Option<JoinHandle<()>>>,
    path: PathBuf,
    identity: (u64, u64),
}
impl OwnerServer {
    pub fn start(services: Arc<AdminServices>) -> Result<Self> {
        let data = fs_safe::private_dir(services.graph.store.data_dir())?;
        let path = data.join("admin.sock");
        match fs::symlink_metadata(&path) {
            Ok(info) => {
                if !info.file_type().is_socket() {
                    return Err(Error::Conflict(
                        "Refusing to replace a non-socket admin path",
                    ));
                }
                fs::remove_file(&path)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let listener = UnixListener::bind(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let info = fs::symlink_metadata(&path)?;
        let identity = (info.dev(), info.ino());
        let state = Arc::new(State {
            stopping: AtomicBool::new(false),
            workers: Mutex::new(Vec::new()),
        });
        let background = state.clone();
        let service_uid = unsafe { libc::getuid() };
        let accept = thread::Builder::new()
            .name("memento-owner".into())
            .spawn(move || {
                while !background.stopping.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if background.stopping.load(Ordering::Acquire) {
                                break;
                            }
                            let services = services.clone();
                            match thread::Builder::new()
                                .name("memento-owner-request".into())
                                .spawn(move || handle(stream, services, service_uid))
                            {
                                Ok(worker) => {
                                    if let Ok(mut workers) = background.workers.lock() {
                                        workers.push(worker);
                                    } else {
                                        break;
                                    }
                                }
                                Err(_) => {
                                    log::warn!("Owner request worker unavailable");
                                    break;
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let mut fds = libc::pollfd {
                                fd: listener.as_raw_fd(),
                                events: libc::POLLIN,
                                revents: 0,
                            };
                            unsafe {
                                libc::poll(&mut fds, 1, 100);
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => (),
                        Err(_) => {
                            log::warn!("Owner socket accept failed");
                            break;
                        }
                    }
                    if let Ok(mut workers) = background.workers.lock() {
                        let mut i = 0;
                        while i < workers.len() {
                            if workers[i].is_finished() {
                                let worker = workers.swap_remove(i);
                                let _ = worker.join();
                            } else {
                                i += 1
                            }
                        }
                    }
                }
            })?;
        Ok(Self {
            state,
            accept: Mutex::new(Some(accept)),
            path,
            identity,
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn close(&self) -> Result<()> {
        self.state.stopping.store(true, Ordering::Release);
        if let Some(accept) = self
            .accept
            .lock()
            .map_err(|_| Error::Unavailable("Owner shutdown lock unavailable"))?
            .take()
        {
            accept
                .join()
                .map_err(|_| Error::Unavailable("Owner listener shutdown failed"))?;
        }
        let workers = std::mem::take(
            &mut *self
                .state
                .workers
                .lock()
                .map_err(|_| Error::Unavailable("Owner shutdown lock unavailable"))?,
        );
        for worker in workers {
            worker
                .join()
                .map_err(|_| Error::Unavailable("Owner request shutdown failed"))?;
        }
        match fs::symlink_metadata(&self.path) {
            Ok(info)
                if info.file_type().is_socket() && (info.dev(), info.ino()) == self.identity =>
            {
                fs::remove_file(&self.path)?;
            }
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}
impl Drop for OwnerServer {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
fn connect(path: &Path, timeout: Duration) -> Result<UnixStream> {
    let bytes = path.as_os_str().as_bytes();
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.len() >= address.sun_path.len() || bytes.contains(&0) {
        return Err(Error::Validation("Administrative socket path is invalid"));
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (i, b) in bytes.iter().enumerate() {
        address.sun_path[i] = *b as libc::c_char;
    }
    let fd = unsafe {
        libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let result = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&address as *const libc::sockaddr_un).cast(),
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if ![Some(libc::EINPROGRESS), Some(libc::EAGAIN)].contains(&error.raw_os_error()) {
            return Err(error.into());
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remain = deadline.saturating_duration_since(Instant::now());
            if remain.is_zero() {
                return Err(Error::Unavailable("Administrative connection timed out"));
            }
            let mut poll = libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            };
            let status = unsafe {
                libc::poll(
                    &mut poll,
                    1,
                    remain.as_millis().min(i32::MAX as u128) as i32,
                )
            };
            if status < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
                continue;
            }
            if status <= 0 {
                return Err(Error::Unavailable("Administrative connection timed out"));
            }
            let mut error: libc::c_int = 0;
            let mut length = std::mem::size_of_val(&error) as libc::socklen_t;
            if unsafe {
                libc::getsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_ERROR,
                    (&mut error as *mut libc::c_int).cast(),
                    &mut length,
                )
            } != 0
            {
                return Err(std::io::Error::last_os_error().into());
            }
            if error != 0 {
                return Err(std::io::Error::from_raw_os_error(error).into());
            }
            break;
        }
    }
    let stream = UnixStream::from(fd);
    stream.set_nonblocking(false)?;
    Ok(stream)
}
pub fn request(data: &Path, value: &Value) -> Result<Value> {
    let path = data.join("admin.sock");
    let mut stream = connect(&path, Duration::from_secs(30))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let mut bytes = ascii_json(value)?;
    bytes.push(b'\n');
    if bytes.len() > REQUEST_LIMIT {
        return Err(Error::Validation(
            "Administrative message exceeds size limit",
        ));
    }
    stream.write_all(&bytes)?;
    if ["backup", "export", "compact", "history_park", "history_release-candidates", "recover_revisions"]
        .contains(&value["operation"].as_str().unwrap_or(""))
    {
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(None)?;
    }
    let line = read_line(&mut stream, RESPONSE_LIMIT, None)?;
    serde_json::from_slice(&line).map_err(|_| Error::Unavailable("Invalid administrative response"))
}
#[cfg(test)]
pub(crate) fn real_peer_uid(stream: &UnixStream) -> Result<u32> {
    peer_uid(stream)
}
#[cfg(test)]
pub(crate) fn peer_admitted(uid: u32, service_uid: u32) -> bool {
    admitted(uid, service_uid)
}
#[cfg(test)]
pub(crate) fn serve_test_peer(stream: UnixStream, services: Arc<AdminServices>, service_uid: u32) {
    handle(stream, services, service_uid)
}
#[cfg(test)]
pub(crate) fn read_test_deadline(stream: &mut UnixStream, timeout: Duration) -> Result<Vec<u8>> {
    read_line(stream, REQUEST_LIMIT, Some(Instant::now() + timeout))
}
