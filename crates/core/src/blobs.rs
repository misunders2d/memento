//! Immutable content-addressed bytes, durable before SQLite registration.
use crate::{Error, Result, WriteTxn, now_utc};
use regex::Regex;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobInfo {
    pub sha256: String,
    pub size: u64,
    pub tier: String,
}
pub struct BlobStore {
    root: PathBuf,
    readers: Mutex<HashMap<String, usize>>,
}
pub struct BlobReader {
    file: File,
    path: PathBuf,
    sha: String,
    store: Arc<BlobStore>,
}
impl Read for BlobReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buffer)
    }
}
impl Seek for BlobReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(position)
    }
}
impl BlobReader {
    pub fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for BlobReader {
    fn drop(&mut self) {
        if let Ok(mut readers) = self.store.readers.lock() {
            if let Some(count) = readers.get_mut(&self.sha) {
                *count -= 1;
                if *count == 0 {
                    readers.remove(&self.sha);
                }
            }
        }
    }
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn unavailable() -> Error {
    Error::Unavailable("Original artifact is unavailable or corrupt")
}
fn hash(reader: &mut impl Read) -> Result<(String, u64)> {
    let mut digest = Sha256::new();
    let mut size = 0;
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let length = reader.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        size += length as u64;
        digest.update(&buffer[..length]);
    }
    Ok((
        digest
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        size,
    ))
}
impl BlobStore {
    pub fn new(root: PathBuf) -> Result<Arc<Self>> {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)?;
        if std::fs::symlink_metadata(&root)?.file_type().is_symlink() {
            return Err(Error::Validation(
                "Artifact storage cannot be a symbolic link",
            ));
        }
        Ok(Arc::new(Self {
            root,
            readers: Mutex::new(HashMap::new()),
        }))
    }
    pub fn path(&self, sha: &str) -> Result<PathBuf> {
        if sha.len() != 64
            || !sha
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::Validation("Expected a lowercase SHA-256 digest"));
        }
        Ok(self.root.join(&sha[..2]).join(&sha[2..]))
    }
    pub fn verify(&self, sha: &str) -> Result<PathBuf> {
        let path = self.path(sha)?;
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(unavailable());
        }
        let mut file = File::open(&path)?;
        if hash(&mut file)?.0 != sha {
            return Err(unavailable());
        }
        Ok(path)
    }
    /// Reader lease protects the published path from this service's cache eviction.
    pub fn reader(self: &Arc<Self>, sha: &str) -> Result<BlobReader> {
        let path = self.path(sha)?;
        let mut readers = self.readers.lock().map_err(|_| unavailable())?;
        let file = File::open(&path)?;
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(unavailable());
        }
        *readers.entry(sha.into()).or_default() += 1;
        drop(readers);
        let mut reader = BlobReader {
            file,
            path,
            sha: sha.into(),
            store: self.clone(),
        };
        if hash(&mut reader)?.0 != sha {
            return Err(unavailable());
        }
        reader.seek(SeekFrom::Start(0))?;
        Ok(reader)
    }
    pub fn in_use(&self, sha: &str) -> Result<bool> {
        Ok(self
            .readers
            .lock()
            .map_err(|_| unavailable())?
            .get(sha)
            .copied()
            .unwrap_or(0)
            > 0)
    }
    /// The caller must have durably verified the external copy before eviction.
    pub fn evict_if_unused(&self, sha: &str) -> Result<bool> {
        let readers = self.readers.lock().map_err(|_| unavailable())?;
        if readers.get(sha).copied().unwrap_or(0) > 0 {
            return Ok(false);
        }
        let path = self.path(sha)?;
        match std::fs::remove_file(&path) {
            Ok(()) => {
                File::open(path.parent().unwrap())?.sync_all()?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e.into()),
        }
    }
    pub fn put(&self, source: &mut impl Read, maximum: u64) -> Result<BlobInfo> {
        let temp = Temporary(
            self.root
                .join(format!(".ingest-{}", uuid::Uuid::new_v4().simple())),
        );
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp.0)?;
        let mut digest = Sha256::new();
        let mut size = 0u64;
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let length = source.read(&mut buffer)?;
            if length == 0 {
                break;
            }
            size = size
                .checked_add(length as u64)
                .ok_or(Error::Validation("Artifact exceeds endpoint size limit"))?;
            if size > maximum {
                return Err(Error::Validation("Artifact exceeds endpoint size limit"));
            }
            digest.update(&buffer[..length]);
            target.write_all(&buffer[..length])?;
        }
        target.sync_all()?;
        drop(target);
        let sha: String = digest
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let path = self.path(&sha)?;
        let parent = path.parent().unwrap();
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
        if std::fs::symlink_metadata(parent)?.file_type().is_symlink() {
            return Err(Error::Validation(
                "Artifact storage cannot be a symbolic link",
            ));
        }
        match std::fs::hard_link(&temp.0, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if self.verify(&sha).is_err() {
                    std::fs::rename(&temp.0, &path)?;
                }
            }
            Err(e) => return Err(e.into()),
        }
        File::open(parent)?.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(BlobInfo {
            sha256: sha,
            size,
            tier: "hot".into(),
        })
    }
    pub fn put_path(&self, path: &Path, maximum: u64) -> Result<BlobInfo> {
        self.put(&mut File::open(path)?, maximum)
    }
}
pub fn normalize_mime(value: &str) -> Result<String> {
    static MIME: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\A[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+\z").unwrap());
    let mime = value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if !MIME.is_match(&mime) {
        return Err(Error::Validation("Invalid MIME type"));
    }
    Ok(mime)
}
impl WriteTxn<'_, '_> {
    pub fn register_blob(&self, info: &BlobInfo, mime: &str) -> Result<serde_json::Value> {
        let mime = normalize_mime(mime)?;
        if info.sha256.len() != 64
            || !info
                .sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::Validation("Expected a lowercase SHA-256 digest"));
        }
        let old = self
            .sql()
            .query_row(
                "SELECT size FROM blobs WHERE sha256=?",
                [&info.sha256],
                |r| r.get::<_, u64>(0),
            )
            .optional()?;
        if old.is_some_and(|size| size != info.size) {
            return Err(Error::Validation(
                "Artifact size differs from its immutable reference",
            ));
        }
        if old.is_none() {
            self.sql().execute(
                "INSERT INTO blobs VALUES(?,?,?,'hot',NULL,?)",
                params![info.sha256, info.size, mime, now_utc()],
            )?;
        }
        self.sql().execute(
            "UPDATE captured_media SET state='ready' WHERE sha256=? AND state='waiting'",
            [&info.sha256],
        )?;
        Ok(self.sql().query_row("SELECT sha256,size,mime,tier,remote_id,created_at FROM blobs WHERE sha256=?",[&info.sha256],|r|Ok(serde_json::json!({"sha256":r.get::<_,String>(0)?,"size":r.get::<_,u64>(1)?,"mime":r.get::<_,String>(2)?,"tier":r.get::<_,String>(3)?,"remote_id":r.get::<_,Option<String>>(4)?,"created_at":r.get::<_,String>(5)?})))?)
    }
}
