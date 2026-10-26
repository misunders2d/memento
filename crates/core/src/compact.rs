//! Explicit owner-only physical maintenance in a fresh native process.
use crate::{Error, FORMAT_VERSION, Result, Store, raw};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};
use std::{
    fs::File,
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::Instant,
};
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn sizes(database: &Path) -> Result<Value> {
    let mut wal = database.as_os_str().to_owned();
    wal.push("-wal");
    Ok(
        json!({"database":std::fs::metadata(database)?.len(),"wal":std::fs::metadata(wal).map(|m|m.len()).unwrap_or(0)}),
    )
}
impl Store {
    pub fn compact_with_helper(&self, executable: &Path) -> Result<Value> {
        self.with_writer_exclusive(|| {
            let scratch = Scratch(self.data_dir().join(format!(
                ".memento-compact-{}",
                uuid::Uuid::new_v4().simple()
            )));
            std::fs::DirBuilder::new().mode(0o700).create(&scratch.0)?;
            let output = Command::new(executable)
                .arg("--compact-worker")
                .arg(self.data_dir())
                .env("SQLITE_TMPDIR", &scratch.0)
                .output()?;
            if !output.status.success() {
                return Err(Error::Unavailable(
                    "Database compaction failed; logical data retained",
                ));
            }
            let report: Value = serde_json::from_slice(&output.stdout)?;
            if !report["complete"].is_boolean() || !report["checkpoint_busy"].is_i64() {
                return Err(Error::Unavailable(
                    "Native compaction worker returned an invalid result",
                ));
            }
            Ok(report)
        })
    }
    pub fn health(&self) -> Result<()> {
        let probe = Connection::open_with_flags(
            self.data_dir().join("memory.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        probe.busy_timeout(std::time::Duration::from_millis(250))?;
        probe.execute_batch("BEGIN IMMEDIATE; ROLLBACK;")?;
        Ok(())
    }
}
/// Private hidden CLI entry: parent holds its Store writer mutex and supplies disk scratch.
/// Dispatch BEFORE opening another Store or loading any embedding model.
pub fn compact_worker(data_dir: &Path) -> Result<Value> {
    let uid = unsafe { libc::getuid() };
    let info = std::fs::symlink_metadata(data_dir)?;
    if !info.is_dir()
        || info.file_type().is_symlink()
        || (uid != 0 && info.uid() != uid)
        || info.mode() & 0o077 != 0
    {
        return Err(Error::Validation(
            "Compaction requires the private service data directory",
        ));
    }
    let scratch = std::env::var_os("SQLITE_TMPDIR").ok_or(Error::Validation(
        "Compaction requires private disk-backed scratch",
    ))?;
    let scratch = Path::new(&scratch).canonicalize()?;
    if scratch.parent() != Some(data_dir.canonicalize()?.as_path())
        || !scratch
            .file_name()
            .is_some_and(|n| n.as_encoded_bytes().starts_with(b".memento-compact-"))
    {
        return Err(Error::Validation(
            "Compaction scratch must be inside service storage",
        ));
    }
    let database = data_dir.join("memory.sqlite");
    if std::fs::symlink_metadata(&database)?
        .file_type()
        .is_symlink()
    {
        return Err(Error::Validation("Database cannot be a symbolic link"));
    }
    let guard = Connection::open_with_flags(&database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let header: i64 = guard.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if header != FORMAT_VERSION {
        return Err(Error::UnsupportedHeader(header));
    }
    drop(guard);
    let started = Instant::now();
    let before = sizes(&database)?;
    let db = Connection::open_with_flags(&database, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    raw::register(&db, Arc::new(Mutex::new(Default::default())))?;
    db.busy_timeout(std::time::Duration::from_secs(3))?;
    db.execute_batch("PRAGMA synchronous=FULL; PRAGMA temp_store=FILE;")?;
    let free_before: i64 = db.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    db.execute_batch("VACUUM")?;
    let busy: i64 = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    let free_after: i64 = db.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    File::open(&database)?.sync_all()?;
    Ok(
        json!({"complete":busy==0,"before":before,"after":sizes(&database)?,"freelist_before":free_before,"freelist_after":free_after,"checkpoint_busy":busy,"elapsed_seconds":started.elapsed().as_secs_f64()}),
    )
}
