use crate::{
    binding::PublicBinding,
    cold::{ColdManager, ColdObject, ColdStore, RemoteBackup},
    cold_runtime::{ColdPolicy, ColdRuntime},
    drive, fs_safe, portable,
};
use memento_core::{Error, Result, Store};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
pub(super) fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
pub(super) struct Directory {
    pub(super) root: PathBuf,
    binding: PublicBinding,
    pub(super) names: Mutex<BTreeMap<String, RemoteBackup>>,
    interrupt: AtomicBool,
    block: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}
impl Directory {
    pub(super) fn new(root: PathBuf) -> Arc<Self> {
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        Arc::new(Self {
            root,
            binding: PublicBinding {
                client_id: "synthetic".into(),
                account: "owner@example.test".into(),
                folder_id: "original_folder".into(),
            },
            names: Mutex::new(BTreeMap::new()),
            interrupt: AtomicBool::new(false),
            block: Mutex::new(None),
        })
    }
}
impl ColdStore for Directory {
    fn binding(&self) -> &PublicBinding {
        &self.binding
    }
    fn upload(
        &self,
        path: &Path,
        name: &str,
        _mime: &str,
        _checkpoint: &Value,
        save: &mut dyn FnMut(Value) -> Result<()>,
    ) -> Result<ColdObject> {
        let (sha, md5, size) = drive::hashes(path)?;
        let target = self.root.join(&sha);
        if !target.exists() {
            crate::copy_verified(path, &target, &sha, size)?;
        }
        save(json!({"object_id":sha,"sha256":sha,"size":size}))?;
        if let Some((started, release)) = self.block.lock().unwrap().take() {
            started.send(()).unwrap();
            release
                .recv_timeout(std::time::Duration::from_secs(5))
                .map_err(|_| Error::Unavailable("Controlled provider deadline"))?;
        }
        if self.interrupt.swap(false, Ordering::SeqCst) {
            return Err(Error::Unavailable(
                "Controlled interruption after remote copy",
            ));
        }
        if name.starts_with("memory-") {
            let mut stem = name.strip_prefix("memory-").unwrap();
            let mut gaps = 0;
            if let Some(rest) = stem.strip_prefix("incomplete-") {
                let (count, next) = rest.split_once('-').unwrap();
                gaps = count.parse().unwrap();
                stem = next;
            }
            let (id, _) = stem.rsplit_once('-').unwrap();
            let backup = RemoteBackup {
                id: id.into(),
                remote_id: sha.clone(),
                sha256: sha.clone(),
                size,
                gap_count: gaps,
                complete: gaps == 0,
            };
            self.names.lock().unwrap().insert(id.into(), backup);
        }
        Ok(ColdObject {
            id: sha.clone(),
            sha256: sha,
            size,
            md5,
        })
    }
    fn download(&self, id: &str, destination: &Path, sha: &str, size: u64) -> Result<()> {
        let tmp =
            destination.with_file_name(format!(".controlled-{}", uuid::Uuid::new_v4().simple()));
        let _cleanup = fs_safe::RemoveFile(tmp.clone());
        crate::copy_verified(&self.root.join(id), &tmp, sha, size)?;
        fs::rename(&tmp, destination)?;
        fs_safe::sync_dir(destination.parent().unwrap())
    }
    fn list_backups(&self) -> Result<Vec<RemoteBackup>> {
        Ok(self.names.lock().unwrap().values().cloned().collect())
    }
}
fn setup(parent: &Path) -> (Arc<Store>, Arc<ColdManager>, Arc<Directory>) {
    let data = parent.join("data");
    fs::create_dir(&data).unwrap();
    fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
    let store = Store::open(&data).unwrap();
    let backend = Directory::new(parent.join("remote"));
    let manager = ColdManager::new(store.clone(), backend.clone()).unwrap();
    (store, manager, backend)
}
fn blob(store: &Store, mut content: &[u8], mime: &str) -> String {
    let maximum = content.len() as u64;
    let info = store.blobs().put(&mut content, maximum).unwrap();
    store
        .write(|tx| {
            tx.register_blob(&info, mime)?;
            Ok(())
        })
        .unwrap();
    info.sha256
}
#[test]
fn interrupted_copy_keeps_hot_original_retry_commits_before_eviction_and_reader_fetches() {
    let temp = private();
    let (store, manager, backend) = setup(temp.path());
    let bytes = b"original raw artifact\0\xff".repeat(1024);
    let sha = blob(&store, &bytes, "image/png");
    backend.interrupt.store(true, Ordering::SeqCst);
    assert!(manager.promote(&sha).is_err());
    assert_eq!(manager.info(&sha).unwrap().tier, "hot");
    assert_eq!(fs::read(store.blobs().path(&sha).unwrap()).unwrap(), bytes);
    assert_eq!(fs::read(backend.root.join(&sha)).unwrap(), bytes);
    let record = manager.promote(&sha).unwrap();
    assert_eq!(record.tier, "cold");
    assert_eq!(record.remote_id.as_deref(), Some(sha.as_str()));
    assert!(!store.blobs().path(&sha).unwrap().exists());
    let mut reader = manager.reader(&sha).unwrap();
    let mut restored = Vec::new();
    reader.read_to_end(&mut restored).unwrap();
    assert_eq!(restored, bytes);
    store
        .read(|tx| {
            let mut q = tx
                .sql()
                .prepare("SELECT phase FROM blob_moves ORDER BY id")?;
            let phases = q
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            assert_eq!(phases, ["committed", "evicted"]);
            Ok(())
        })
        .unwrap();
}
#[test]
fn active_original_and_cache_readers_fence_eviction_pending_media_keeps_hot_until_done() {
    let temp = private();
    let (store, manager, _) = setup(temp.path());
    let sha = blob(&store, b"image original", "image/png");
    let hot = manager.reader(&sha).unwrap();
    manager.promote(&sha).unwrap();
    assert!(store.blobs().path(&sha).unwrap().exists());
    assert_eq!(manager.trim_cache(0, 0).unwrap().evicted, 0);
    drop(hot);
    store.write(|tx|{tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts) VALUES('media','media','media',?,'pending',0,0)",[json!({"sha256":sha}).to_string()])?;Ok(())}).unwrap();
    manager.promote(&sha).unwrap();
    assert!(store.blobs().path(&sha).unwrap().exists());
    store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE jobs SET state='done' WHERE id='media'", [])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(manager.trim_cache(0, 0).unwrap().evicted, 1);
    assert!(!store.blobs().path(&sha).unwrap().exists());
    let reader = manager.reader(&sha).unwrap();
    assert_eq!(manager.trim_cache(0, 0).unwrap().evicted, 0);
    assert!(reader.path().exists());
    drop(reader);
    assert_eq!(manager.trim_cache(0, 0).unwrap().evicted, 1);
    assert_eq!(manager.trim_cache(0, 0).unwrap().bytes, 0);
    assert!(manager.reader(&sha).is_ok());
}
#[test]
fn sweep_failure_backoff_does_not_starve_other_originals_and_fresh_generic_waits() {
    let temp = private();
    let (store, manager, _) = setup(temp.path());
    let bad = blob(&store, b"bad media", "image/png");
    let good = blob(&store, b"good media", "audio/wav");
    let generic = blob(&store, b"fresh ordinary", "application/octet-stream");
    fs::write(store.blobs().path(&bad).unwrap(), b"corrupt").unwrap();
    let result = manager.sweep(90, 10 * 1024 * 1024, 10).unwrap();
    assert_eq!(result.promoted, 1);
    assert_eq!(result.failure_count, 1);
    assert_eq!(manager.info(&good).unwrap().tier, "cold");
    assert_eq!(manager.info(&bad).unwrap().tier, "hot");
    assert_eq!(manager.info(&generic).unwrap().tier, "hot");
    assert_eq!(manager.sweep(90, 10 * 1024 * 1024, 10).unwrap().promoted, 0);
    store
        .read(|tx| {
            let (attempts, next): (i64, f64) = tx.sql().query_row(
                "SELECT attempts,next_attempt FROM cold_failures WHERE sha256=?",
                [&bad],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!(attempts, 1);
            assert!(next > chrono::Utc::now().timestamp() as f64 + 50.);
            Ok(())
        })
        .unwrap();
}
#[test]
fn media_offload_preserves_pin_manual_loop_and_importance_history() {
    let temp = private();
    let (store, manager, _) = setup(temp.path());
    let sha = blob(&store, b"important pinned photo", "image/png");
    store.write(|tx|{tx.sql().execute("INSERT INTO graph_commits VALUES('commit','command','{}','{}','human','explicit','2026')",[])?;tx.sql().execute("INSERT INTO node_versions VALUES('node',1,'commit','knowledge','original','active',NULL,NULL,NULL,1,'[\"personal\"]')",[])?;tx.sql().execute("INSERT INTO node_stats(node_id,importance) VALUES('node',1)",[])?;tx.sql().execute("INSERT INTO media_files VALUES('node',?,'original.png','image','commit')",[&sha])?;tx.sql().execute("INSERT INTO loop_events VALUES('manual','node','commit','open',NULL,NULL)",[])?;Ok(())}).unwrap();
    assert_eq!(manager.sweep(90, 10 * 1024 * 1024, 10).unwrap().promoted, 1);
    assert!(manager.reader(&sha).is_ok());
    store
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("SELECT pinned FROM node_versions", [], |r| r
                        .get::<_, i64>(0))?,
                1
            );
            assert_eq!(
                tx.sql()
                    .query_row("SELECT state FROM loop_events", [], |r| r
                        .get::<_, String>(0))?,
                "open"
            );
            assert_eq!(
                tx.sql()
                    .query_row("SELECT importance FROM node_stats", [], |r| r
                        .get::<_, f64>(0))?,
                1.
            );
            Ok(())
        })
        .unwrap();
}
#[test]
fn complete_disk_loss_restore_needs_only_remote_backup_and_pinned_binding() {
    let temp = private();
    let (store, manager, backend) = setup(temp.path());
    let content = b"exact original image\0\xff";
    let sha = blob(&store, content, "image/png");
    let captured=store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"source","conversation_id":"thread","kind":"user","text":"The copper key stays in the blue tin. Ω"})).unwrap()).unwrap();
    let backup = manager.backup().unwrap();
    assert!(backup.complete);
    assert_eq!(manager.info(&sha).unwrap().tier, "hot");
    assert!(manager.info(&sha).unwrap().remote_id.is_some());
    let source = store.data_dir().to_owned();
    drop(manager);
    drop(store);
    fs::remove_dir_all(source).unwrap();
    let dest = temp.path().join("restored");
    let report = ColdManager::restore_from(backend.as_ref(), &backup.id, &dest, false).unwrap();
    assert!(report.complete);
    assert_eq!(
        fs::read(dest.join(portable::blob_name(&sha))).unwrap(),
        content
    );
    let recovered = Store::open(&dest).unwrap();
    assert_eq!(
        recovered
            .read(|tx| tx.original_text(&captured.id))
            .unwrap()
            .unwrap(),
        "The copper key stays in the blue tin. Ω"
    );
    assert_eq!(
        crate::binding::read_binding(&dest).unwrap(),
        Some(backend.binding.clone())
    );
}
#[test]
fn incomplete_backup_gap_is_explicit_and_default_restore_never_publishes() {
    let temp = private();
    let (store, manager, backend) = setup(temp.path());
    let missing = blob(&store, b"unavailable original", "image/png");
    let good = blob(&store, b"available original", "audio/wav");
    fs::remove_file(store.blobs().path(&missing).unwrap()).unwrap();
    let backup = manager.backup().unwrap();
    assert!(!backup.complete);
    assert_eq!(backup.gap_count, 1);
    let dest = temp.path().join("refused");
    assert!(ColdManager::restore_from(backend.as_ref(), &backup.id, &dest, false).is_err());
    assert!(!dest.exists());
    let dest = temp.path().join("explicit-partial");
    let report = ColdManager::restore_from(backend.as_ref(), &backup.id, &dest, true).unwrap();
    assert!(!report.complete);
    assert_eq!(report.gap_count, 1);
    assert!(dest.join(portable::blob_name(&good)).exists());
    assert!(!dest.join(portable::blob_name(&missing)).exists());
}
#[test]
fn blocked_backup_upload_does_not_block_hot_reader_or_capture_writer() {
    let temp = private();
    let (store, manager, backend) = setup(temp.path());
    let sha = blob(&store, b"read while Drive stalls", "image/png");
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *backend.block.lock().unwrap() = Some((started_tx, release_rx));
    let worker = manager.clone();
    let handle = std::thread::spawn(move || worker.backup());
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let mut reader = manager.reader(&sha).unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"read while Drive stalls");
    store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"during-backup","conversation_id":"thread","kind":"user","text":"Capture stays available."})).unwrap()).unwrap();
    release_tx.send(()).unwrap();
    assert!(handle.join().unwrap().unwrap().complete);
}
#[test]
fn runtime_drains_bounded_batches_stops_and_never_creates_backup() {
    let temp = private();
    let (store, manager, _) = setup(temp.path());
    for content in [b"one", b"two", b"six"] {
        blob(&store, content, "image/png");
    }
    let runtime = ColdRuntime::new(
        manager,
        ColdPolicy {
            batch_limit: 1,
            ..ColdPolicy::default()
        },
    )
    .unwrap();
    assert!(runtime.run_once().unwrap());
    assert!(runtime.run_once().unwrap());
    assert!(runtime.run_once().unwrap());
    assert!(!runtime.run_once().unwrap());
    runtime.close().unwrap();
    assert!(!runtime.run_once().unwrap());
    assert!(runtime.status().unwrap().last_error.is_none());
    store
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("SELECT count(*) FROM backups", [], |r| r.get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();
}
#[test]
fn pending_derived_media_stage_retains_its_registered_artifact_until_job_finishes() {
    let temp = private();
    let (store, manager, _) = setup(temp.path());
    let original = blob(&store, b"original video", "video/mp4");
    let derived = blob(&store, b"derived preview", "image/png");
    store.write(|tx|{tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts) VALUES('media-stage','media-stage','media',?,'pending',0,0)",[json!({"sha256":original}).to_string()])?;tx.sql().execute("INSERT INTO media_stage_units VALUES('media-stage',0,'fixed',?,?)",rusqlite::params![json!({"artifact_sha256":derived,"source_sha256":original}).to_string(),vec![0_u8;3072]])?;Ok(())}).unwrap();
    manager.promote(&derived).unwrap();
    assert!(store.blobs().path(&derived).unwrap().exists());
    store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE jobs SET state='done' WHERE id='media-stage'", [])?;
            tx.sql().execute(
                "DELETE FROM media_stage_units WHERE job_id='media-stage'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    manager.promote(&derived).unwrap();
    assert!(!store.blobs().path(&derived).unwrap().exists());
    assert!(manager.reader(&derived).is_ok());
}
