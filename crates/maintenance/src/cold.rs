//! Bounded cold maintenance. Network/hash/copy work precedes short writer
//! transactions; a verified remote object is durable before hot eviction.
use crate::{
    binding::{self, PublicBinding},
    fs_safe::{self, Destination, Stage},
    hash_file,
    portable::{self, ArchiveReport},
};
use memento_core::{
    Error, Result, Store, WriteTxn,
    blobs::{BlobReader, BlobStore},
    now_utc,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
#[derive(Clone, Debug, Serialize)]
pub struct ColdObject {
    pub id: String,
    pub size: u64,
    pub sha256: String,
    pub md5: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct RemoteBackup {
    pub id: String,
    pub remote_id: String,
    pub sha256: String,
    pub size: u64,
    pub gap_count: u64,
    pub complete: bool,
}
pub trait ColdStore: Send + Sync {
    fn binding(&self) -> &PublicBinding;
    fn upload(
        &self,
        path: &Path,
        name: &str,
        mime: &str,
        checkpoint: &Value,
        save: &mut dyn FnMut(Value) -> Result<()>,
    ) -> Result<ColdObject>;
    fn download(&self, id: &str, destination: &Path, sha: &str, size: u64) -> Result<()>;
    fn list_backups(&self) -> Result<Vec<RemoteBackup>>;
}
#[derive(Clone, Serialize)]
pub struct BlobRecord {
    pub sha256: String,
    pub size: u64,
    pub mime: String,
    pub tier: String,
    pub remote_id: Option<String>,
    pub created_at: String,
}
#[derive(Serialize)]
pub struct Sweep {
    pub promoted: u64,
    pub failure_count: u64,
    pub batch_full: bool,
}
#[derive(Serialize)]
pub struct Trim {
    pub bytes: u64,
    pub evicted: u64,
}
#[derive(Serialize)]
pub struct BackupReport {
    pub id: String,
    pub remote_id: Option<String>,
    pub sha256: String,
    pub size: u64,
    pub complete: bool,
    pub gap_count: u64,
}
#[derive(Serialize)]
pub struct RestoreReport {
    pub path: String,
    pub id: String,
    pub complete: bool,
    pub gap_count: u64,
    pub destination_header: i64,
}
pub struct ColdManager {
    pub store: Arc<Store>,
    pub backend: Arc<dyn ColdStore>,
    cache: Arc<BlobStore>,
    operation: Mutex<()>,
    hydrate: Mutex<()>,
    pub max_importance: f64,
}
fn locked<T>(m: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    m.lock()
        .map_err(|_| Error::Unavailable("Cold maintenance lock is unavailable"))
}
fn seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn record(db: &Connection, sha: &str) -> Result<Option<BlobRecord>> {
    Ok(db
        .query_row(
            "SELECT sha256,size,mime,tier,remote_id,created_at FROM blobs WHERE sha256=?",
            [sha],
            |r| {
                Ok(BlobRecord {
                    sha256: r.get(0)?,
                    size: r.get(1)?,
                    mime: r.get(2)?,
                    tier: r.get(3)?,
                    remote_id: r.get(4)?,
                    created_at: r.get(5)?,
                })
            },
        )
        .optional()?)
}
fn pending_media(db: &Connection, sha: &str) -> Result<bool> {
    Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM jobs WHERE kind='media' AND state IN ('pending','leased') AND json_extract(payload,'$.sha256')=?1) OR EXISTS(SELECT 1 FROM media_stage_units u JOIN jobs j ON j.id=u.job_id WHERE j.kind='media' AND j.state IN ('pending','leased') AND (json_extract(u.metadata,'$.artifact_sha256')=?1 OR json_extract(u.metadata,'$.source_sha256')=?1))",[sha],|r|r.get(0))?)
}
fn phase(tx: &WriteTxn, sha: &str, name: &str, remote: &str) -> Result<()> {
    let now = now_utc();
    tx.sql().execute(
        "UPDATE cold_transfers SET status=?,remote_id=?,updated_at=? WHERE sha256=?",
        params![name, remote, now, sha],
    )?;
    tx.sql().execute(
        "INSERT INTO blob_moves(sha256,phase,remote_id,at) VALUES(?,?,?,?)",
        params![sha, name, remote, now],
    )?;
    if name == "evicted" {
        tx.sql()
            .execute("DELETE FROM cold_failures WHERE sha256=?", [sha])?;
    }
    Ok(())
}
impl ColdManager {
    pub fn new(store: Arc<Store>, backend: Arc<dyn ColdStore>) -> Result<Arc<Self>> {
        backend.binding().validate()?;
        binding::check_binding(store.data_dir(), backend.binding())?;
        // The verified backend pins the same public identity for portable recovery.
        fs_safe::atomic_private(
            &store.data_dir().join("drive-binding.json"),
            &serde_json::to_vec(backend.binding())?,
        )?;
        let cache = BlobStore::new(store.data_dir().join("cold-cache"))?;
        Ok(Arc::new(Self {
            store,
            backend,
            cache,
            operation: Mutex::new(()),
            hydrate: Mutex::new(()),
            max_importance: 0.75,
        }))
    }
    pub fn info(&self, sha: &str) -> Result<BlobRecord> {
        self.store.blobs().path(sha)?;
        self.store.read(|tx| {
            record(tx.sql(), sha)?.ok_or(Error::NotFound("Unknown registered original artifact"))
        })
    }
    /// A hot/cache lease protects bytes from eviction for the reader's lifetime.
    /// Hashing and download never hold the SQLite writer or snapshot open.
    pub fn reader(&self, sha: &str) -> Result<BlobReader> {
        let mut info = self.info(sha)?;
        if let Ok(reader) = self.store.blobs().reader(sha) {
            if fs::metadata(reader.path())?.len() == info.size {
                return Ok(reader);
            }
        }
        if info.remote_id.is_none() {
            info = self.info(sha)?;
        }
        let remote = info.remote_id.as_ref().ok_or(Error::Unavailable(
            "Original artifact is missing or corrupt",
        ))?;
        if let Ok(reader) = self.cache.reader(sha) {
            if fs::metadata(reader.path())?.len() == info.size {
                File::open(reader.path())?.set_modified(SystemTime::now())?;
                return Ok(reader);
            }
        }
        let _hydrating = locked(&self.hydrate)?;
        if let Ok(reader) = self.cache.reader(sha) {
            if fs::metadata(reader.path())?.len() == info.size {
                return Ok(reader);
            }
        }
        let path = self.cache.path(sha)?;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path.parent().unwrap())?;
        self.backend.download(remote, &path, sha, info.size)?;
        if hash_file(&path)? != (sha.to_owned(), info.size) {
            return Err(Error::Unavailable(
                "Fetched original failed checksum verification",
            ));
        }
        let reader = self.cache.reader(sha)?;
        File::open(reader.path())?.set_modified(SystemTime::now())?;
        Ok(reader)
    }
    fn copy(
        &self,
        info: &BlobRecord,
        path: &Path,
        purpose: &str,
        name: &str,
    ) -> Result<ColdObject> {
        if hash_file(path)? != (info.sha256.clone(), info.size) {
            return Err(Error::Unavailable(
                "Original artifact is missing or corrupt",
            ));
        }
        let checkpoint: Value = self.store.read(|tx| {
            let text: Option<String> = tx
                .sql()
                .query_row(
                    "SELECT checkpoint FROM cold_transfers WHERE sha256=?",
                    [&info.sha256],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(text
                .map(|s| serde_json::from_str(&s))
                .transpose()?
                .unwrap_or_else(|| json!({})))
        })?;
        let mut save = |checkpoint: Value| -> Result<()> {
            let status = if checkpoint.get("object_id").is_some() {
                if purpose == "backup" {
                    "backup_verified"
                } else {
                    "verified"
                }
            } else if purpose == "backup" {
                "backup_copying"
            } else {
                "copying"
            };
            self.store.write(|tx|{tx.sql().execute("INSERT INTO cold_transfers(sha256,checkpoint,status,remote_id,updated_at) VALUES(?,?,?,NULL,?) ON CONFLICT(sha256) DO UPDATE SET checkpoint=excluded.checkpoint,status=excluded.status,updated_at=excluded.updated_at",params![info.sha256,serde_json::to_string(&checkpoint)?,status,now_utc()])?;Ok(())})
        };
        let object = self
            .backend
            .upload(path, name, &info.mime, &checkpoint, &mut save)?;
        binding::object_id(&object.id)?;
        if object.sha256 != info.sha256 || object.size != info.size {
            return Err(Error::Validation(
                "Verified remote object differs from immutable original",
            ));
        }
        Ok(object)
    }
    fn evict_hot(&self, sha: &str, remote: &str) -> Result<bool> {
        self.store.write(|tx| {
            let info = record(tx.sql(), sha)?
                .ok_or(Error::NotFound("Unknown registered original artifact"))?;
            if info.tier != "cold" || info.remote_id.as_deref() != Some(remote) {
                return Err(Error::Conflict("Artifact location changed before eviction"));
            }
            if pending_media(tx.sql(), sha)? {
                return Ok(false);
            }
            if self.store.blobs().evict_if_unused(sha)? {
                phase(tx, sha, "evicted", remote)?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }
    pub fn promote(&self, sha: &str) -> Result<BlobRecord> {
        let _operation = locked(&self.operation)?;
        self.promote_inner(sha)
    }
    fn promote_inner(&self, sha: &str) -> Result<BlobRecord> {
        let info = self.info(sha)?;
        if info.tier == "cold" {
            let remote = info.remote_id.as_ref().ok_or(Error::Unavailable(
                "Cold artifact has no verified remote reference",
            ))?;
            self.evict_hot(sha, remote)?;
            return Ok(info);
        }
        let reader = self.store.blobs().reader(sha)?;
        let object = self.copy(&info, reader.path(), "archive", sha)?;
        self.store.write(|tx| {
            let current = record(tx.sql(), sha)?
                .ok_or(Error::NotFound("Unknown registered original artifact"))?;
            if current.size != info.size {
                return Err(Error::Conflict("Original artifact changed during transfer"));
            }
            if current
                .remote_id
                .as_ref()
                .is_some_and(|id| *id != object.id)
            {
                return Err(Error::Conflict(
                    "Original remote reference changed during transfer",
                ));
            }
            tx.sql().execute(
                "UPDATE blobs SET tier='cold',remote_id=? WHERE sha256=?",
                params![object.id, sha],
            )?;
            tx.touch_media(sha)?;
            phase(tx, sha, "committed", &object.id)
        })?;
        drop(reader);
        self.evict_hot(sha, &object.id)?;
        self.info(sha)
    }
    pub fn trim_cache(&self, max_bytes: u64, ttl_seconds: u64) -> Result<Trim> {
        let _operation = locked(&self.operation)?;
        let rows = self.store.read(|tx| {
            let mut q = tx
                .sql()
                .prepare("SELECT sha256,tier FROM blobs WHERE remote_id IS NOT NULL")?;
            Ok(
                q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )
        })?;
        let mut files = Vec::new();
        for (sha, tier) in rows {
            for hot in [false, true] {
                if hot && tier != "cold" {
                    continue;
                }
                let storage = if hot { self.store.blobs() } else { &self.cache };
                let path = storage.path(&sha)?;
                match fs::metadata(&path) {
                    Ok(info) => files.push((
                        info.modified()?
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs_f64(),
                        info.len(),
                        sha.clone(),
                        hot,
                    )),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        files.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.2.cmp(&b.2)));
        let mut total = files.iter().map(|r| r.1).sum::<u64>();
        let cutoff = seconds() - ttl_seconds as f64;
        let mut evicted = 0;
        for (time, size, sha, hot) in files {
            if time <= cutoff || total > max_bytes {
                let removed = self.store.write(|tx| {
                    if pending_media(tx.sql(), &sha)? {
                        return Ok(false);
                    }
                    let info = record(tx.sql(), &sha)?
                        .ok_or(Error::NotFound("Unknown registered original artifact"))?;
                    let remote = info
                        .remote_id
                        .ok_or(Error::Unavailable("Cache has no verified remote original"))?;
                    if hot && info.tier != "cold" {
                        return Ok(false);
                    }
                    let storage = if hot { self.store.blobs() } else { &self.cache };
                    let removed = storage.evict_if_unused(&sha)?;
                    if removed {
                        phase(tx, &sha, "evicted", &remote)?;
                    }
                    Ok(removed)
                })?;
                if removed {
                    total = total.saturating_sub(size);
                    evicted += 1;
                }
            }
        }
        Ok(Trim {
            bytes: total,
            evicted,
        })
    }
    pub fn sweep(&self, after_days: u64, min_bytes: u64, limit: usize) -> Result<Sweep> {
        if after_days == 0 || min_bytes == 0 || limit == 0 || limit > 100 {
            return Err(Error::Validation("Invalid cold retention policy"));
        }
        let _operation = locked(&self.operation)?;
        let cutoff = (chrono::Utc::now()
            - chrono::Duration::days(
                i64::try_from(after_days)
                    .map_err(|_| Error::Validation("Invalid cold retention age"))?,
            ))
        .to_rfc3339();
        let now = seconds();
        let ids=self.store.read(|tx|{
            let sql="SELECT b.sha256 FROM blobs b LEFT JOIN cold_failures f USING(sha256) WHERE b.tier='hot' AND (f.next_attempt IS NULL OR f.next_attempt<=?1) AND (b.size>=?2 OR b.mime LIKE 'image/%' OR b.mime LIKE 'audio/%' OR b.mime LIKE 'video/%' OR b.mime IN ('application/pdf','application/x-memento-raw-run') OR EXISTS(SELECT 1 FROM media_files WHERE sha256=b.sha256) OR EXISTS(SELECT 1 FROM segments WHERE artifact_sha256=b.sha256 OR source_sha256=b.sha256) OR EXISTS(SELECT 1 FROM jobs WHERE kind='media' AND json_extract(payload,'$.sha256')=b.sha256) OR (b.created_at<?3 AND NOT EXISTS(SELECT 1 FROM node_versions n LEFT JOIN node_stats s ON s.node_id=n.id WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND n.state='active' AND (n.pinned=1 OR coalesce(s.importance,0.5)>=?4) AND n.id IN (SELECT node_id FROM media_files WHERE sha256=b.sha256 UNION SELECT node_id FROM segments WHERE artifact_sha256=b.sha256 OR source_sha256=b.sha256))) OR EXISTS(SELECT 1 FROM cold_transfers t WHERE t.sha256=b.sha256 AND t.status IN ('copying','verified','committed'))) ORDER BY coalesce(f.attempts,0),b.created_at,b.sha256 LIMIT ?5";
            let mut q=tx.sql().prepare(sql)?;Ok(q.query_map(params![now,min_bytes,cutoff,self.max_importance,limit],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?)
        })?;
        let mut promoted = 0;
        for sha in &ids {
            match self.promote_inner(sha) {
                Ok(_) => promoted += 1,
                Err(_) => {
                    self.store.write(|tx|{let attempts:i64=tx.sql().query_row("SELECT attempts FROM cold_failures WHERE sha256=?",[sha],|r|r.get(0)).optional()?.unwrap_or(0)+1;let delay=3600_f64.min(60_f64*2_f64.powi((attempts-1).min(6)as i32));tx.sql().execute("INSERT INTO cold_failures VALUES(?,?,'transfer_failed',?) ON CONFLICT(sha256) DO UPDATE SET attempts=excluded.attempts,error=excluded.error,next_attempt=excluded.next_attempt",params![sha,attempts,seconds()+delay])?;Ok(())})?;
                }
            }
        }
        let failure_count=self.store.read(|tx|Ok(tx.sql().query_row("SELECT count(*) FROM cold_failures f JOIN blobs b USING(sha256) WHERE b.tier='hot'",[],|r|r.get(0))?))?;
        Ok(Sweep {
            promoted,
            failure_count,
            batch_full: ids.len() == limit,
        })
    }
    pub fn export_archive(&self, output: &Path) -> Result<ArchiveReport> {
        let _operation = locked(&self.operation)?;
        portable::export_archive(self.store.data_dir(), output)
    }
    /// Owner operation only. ColdRuntime never calls backup/retry_backups.
    pub fn backup(&self) -> Result<BackupReport> {
        let _operation = locked(&self.operation)?;
        let root = self.store.data_dir().join("backups");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)?;
        fs_safe::private_dir(&root)?;
        let id = format!(
            "{}-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%S"),
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let path = root.join(format!("{id}.sqlite"));
        let cleanup = fs_safe::RemoveFile(path.clone());
        portable::snapshot(self.store.data_dir(), &path)?;
        let db = Connection::open(&path)?;
        if portable::header(&db)? != 5 {
            return Err(Error::Validation("Native backups require native authority"));
        }
        let rows = portable::registered(&db)?;
        for row in rows.iter().filter(|r| r.remote.is_none()) {
            let info = record(&db, &row.sha)?.unwrap();
            let copied = self
                .store
                .blobs()
                .reader(&row.sha)
                .and_then(|reader| self.copy(&info, reader.path(), "backup", &row.sha));
            match copied {
                Ok(object) => {
                    db.execute(
                        "UPDATE blobs SET remote_id=? WHERE sha256=?",
                        params![object.id, row.sha],
                    )?;
                    self.store.write(|tx| {
                        tx.sql().execute(
                            "UPDATE blobs SET remote_id=coalesce(remote_id,?) WHERE sha256=?",
                            params![object.id, row.sha],
                        )?;
                        phase(tx, &row.sha, "backed_up", &object.id)
                    })?;
                }
                Err(_) => {
                    db.execute(
                        "INSERT INTO backup_gaps VALUES(?,?,'original_unavailable',?)",
                        params![id, row.sha, now_utc()],
                    )?;
                }
            }
        }
        let gaps = {
            let mut q = db.prepare("SELECT sha256 FROM backup_gaps WHERE backup_id=?")?;
            q.query_map([&id], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()?
        };
        portable::validate_native_with_gaps(&db, self.store.data_dir(), &gaps)?;
        let gap_rows = {
            let mut q =
                db.prepare("SELECT sha256,error,recorded_at FROM backup_gaps WHERE backup_id=?")?;
            q.query_map([&id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        db.execute_batch("PRAGMA journal_mode=DELETE")?;
        drop(db);
        File::open(&path)?.sync_all()?;
        fs_safe::sync_dir(&root)?;
        let (sha, size) = hash_file(&path)?;
        self.store.write(|tx| {
            tx.sql().execute(
                "INSERT INTO backups VALUES(?,?,?,?,NULL,'{}',?)",
                params![id, path.to_string_lossy(), sha, size, now_utc()],
            )?;
            for (a, b, c) in &gap_rows {
                tx.sql().execute(
                    "INSERT INTO backup_gaps VALUES(?,?,?,?)",
                    params![id, a, b, c],
                )?;
            }
            Ok(())
        })?;
        std::mem::forget(cleanup);
        self.upload_backup(&id)
    }
    fn upload_backup(&self, id: &str) -> Result<BackupReport> {
        let (path, sha, size, checkpoint, remote, gaps) = self.store.read(|tx| {
            let row = tx.sql().query_row(
                "SELECT path,sha256,size,checkpoint,remote_id FROM backups WHERE id=?",
                [id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, u64>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, Option<String>>(4)?,
                    ))
                },
            )?;
            let gaps = tx.sql().query_row(
                "SELECT count(*) FROM backup_gaps WHERE backup_id=?",
                [id],
                |r| r.get::<_, u64>(0),
            )?;
            Ok((
                PathBuf::from(row.0),
                row.1,
                row.2,
                serde_json::from_str::<Value>(&row.3)?,
                row.4,
                gaps,
            ))
        })?;
        if let Some(remote) = remote {
            return Ok(BackupReport {
                id: id.into(),
                remote_id: Some(remote),
                sha256: sha,
                size,
                complete: gaps == 0,
                gap_count: gaps,
            });
        }
        if hash_file(&path)? != (sha.clone(), size) {
            return Err(Error::Unavailable("Backup snapshot is missing or corrupt"));
        }
        let name = if gaps > 0 {
            format!("memory-incomplete-{gaps}-{id}-{sha}.sqlite")
        } else {
            format!("memory-{id}-{sha}.sqlite")
        };
        let mut save = |checkpoint: Value| {
            self.store.write(|tx| {
                tx.sql().execute(
                    "UPDATE backups SET checkpoint=? WHERE id=?",
                    params![serde_json::to_string(&checkpoint)?, id],
                )?;
                Ok(())
            })
        };
        let object = self.backend.upload(
            &path,
            &name,
            "application/vnd.sqlite3",
            &checkpoint,
            &mut save,
        )?;
        if object.sha256 != sha || object.size != size {
            return Err(Error::Validation(
                "Remote backup differs from immutable snapshot",
            ));
        }
        binding::object_id(&object.id)?;
        self.store.write(|tx| {
            tx.sql().execute(
                "UPDATE backups SET remote_id=? WHERE id=?",
                params![object.id, id],
            )?;
            Ok(())
        })?;
        fs::remove_file(&path)?;
        fs_safe::sync_dir(path.parent().unwrap())?;
        Ok(BackupReport {
            id: id.into(),
            remote_id: Some(object.id),
            sha256: sha,
            size,
            complete: gaps == 0,
            gap_count: gaps,
        })
    }
    pub fn retry_backups(&self) -> Result<Vec<BackupReport>> {
        let _operation = locked(&self.operation)?;
        let ids = self.store.read(|tx| {
            let mut q = tx
                .sql()
                .prepare("SELECT id FROM backups WHERE remote_id IS NULL ORDER BY created_at,id")?;
            Ok(q.query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?)
        })?;
        ids.iter().map(|id| self.upload_backup(id)).collect()
    }
    pub fn latest_backup_gap_count(&self) -> Result<u64> {
        self.store.read(|tx|Ok(tx.sql().query_row("SELECT count(*) FROM backup_gaps WHERE backup_id=(SELECT id FROM backups ORDER BY created_at DESC,id DESC LIMIT 1)",[],|r|r.get(0))?))
    }
    pub fn restore_from(
        backend: &dyn ColdStore,
        id: &str,
        destination: &Path,
        allow_incomplete: bool,
    ) -> Result<RestoreReport> {
        backend.binding().validate()?;
        let selected = backend
            .list_backups()?
            .into_iter()
            .filter(|b| b.id == id)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Err(Error::NotFound("Verified backup not found"));
        }
        if selected.len() != 1 {
            return Err(Error::Conflict("Backup identity is ambiguous"));
        }
        let backup = &selected[0];
        if backup.gap_count > 0 && !allow_incomplete {
            return Err(Error::Validation(
                "Incomplete backup requires explicit owner recovery",
            ));
        }
        let destination = fs_safe::plain(destination)?;
        let parent = fs_safe::private_dir(
            destination
                .parent()
                .ok_or(Error::Validation("Restore needs a destination parent"))?,
        )?;
        let identity = Destination::inspect(&destination)?;
        let stage = Stage::new(&parent, "memento-drive-restore")?;
        let snapshot = stage.0.join("memory.sqlite");
        backend.download(&backup.remote_id, &snapshot, &backup.sha256, backup.size)?;
        if hash_file(&snapshot)? != (backup.sha256.clone(), backup.size) {
            return Err(Error::Validation("Downloaded backup checksum mismatch"));
        }
        let db = Connection::open(&snapshot)?;
        let header = portable::header(&db)?;
        let rows = portable::registered(&db)?;
        let gaps = {
            let mut q = db.prepare("SELECT sha256 FROM backup_gaps WHERE backup_id=?")?;
            q.query_map([id], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()?
        };
        if gaps.len() as u64 != backup.gap_count {
            return Err(Error::Validation(
                "Backup gap declaration differs from authority",
            ));
        }
        for row in &rows {
            let tier: String =
                db.query_row("SELECT tier FROM blobs WHERE sha256=?", [&row.sha], |r| {
                    r.get(0)
                })?;
            if tier != "hot" {
                continue;
            }
            let Some(remote) = &row.remote else {
                if allow_incomplete && gaps.contains(&row.sha) {
                    continue;
                }
                return Err(Error::Unavailable("Backup omits a required local original"));
            };
            let path = stage.0.join(portable::blob_name(&row.sha));
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(path.parent().unwrap())?;
            backend.download(remote, &path, &row.sha, row.size)?;
            if hash_file(&path)? != (row.sha.clone(), row.size) {
                return Err(Error::Validation("Restored original checksum mismatch"));
            }
        }
        fs_safe::atomic_private(
            &stage.0.join("drive-binding.json"),
            &serde_json::to_vec(backend.binding())?,
        )?;
        let original_authority = portable::authority_fingerprint(&db)?;
        db.execute_batch(
            "DELETE FROM cold_transfers; DELETE FROM cold_failures; DELETE FROM backups;",
        )?;
        if original_authority != portable::authority_fingerprint(&db)? {
            return Err(Error::Validation(
                "Backup authority changed outside permitted restore rebasing",
            ));
        }
        drop(db);
        // Legacy backup conversion uses the same native importer as portable v1/v2.
        let converted: Option<Stage>;
        if header != 5 {
            #[cfg(feature = "legacy-import")]
            {
                converted = Some(Stage::new(&parent, "memento-drive-native")?);
                let target = &converted.as_ref().unwrap().0;
                fs::remove_dir(target)?;
                if gaps.is_empty() {
                    crate::legacy::import(&snapshot, target)?;
                } else {
                    crate::legacy::import_with_allowed_gaps(&snapshot, target, &gaps)?;
                }
            }
            #[cfg(not(feature = "legacy-import"))]
            {
                return Err(Error::Unavailable(
                    "Legacy backup requires native legacy-import feature",
                ));
            }
        } else {
            converted = None;
        }
        let publish = converted
            .as_ref()
            .map(|s| s.0.as_path())
            .unwrap_or(&stage.0);
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(publish.join("blobs"))?;
        let db = portable::open_read(&publish.join("memory.sqlite"))?;
        portable::validate_native_with_gaps(&db, publish, &gaps)?;
        drop(db);
        fs_safe::sync_tree(publish)?;
        identity.publish(publish, &destination)?;
        Ok(RestoreReport {
            path: destination.join("memory.sqlite").display().to_string(),
            id: id.into(),
            complete: gaps.is_empty(),
            gap_count: gaps.len() as u64,
            destination_header: 5,
        })
    }
}
