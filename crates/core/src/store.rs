use crate::{
    CommandContext, DirtyEvent, Error, EventFence, FORMAT_VERSION, Result, SourceCut, Tier,
    VectorWrite, canonical_json, now_utc, raw,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Params, Transaction, TransactionBehavior, params,
};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    collections::BTreeSet,
    fs::{File, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use uuid::Uuid;
fn nonce() -> String {
    Uuid::new_v4().simple().to_string()
}
fn header(db: &Connection) -> Result<i64> {
    Ok(db.query_row("PRAGMA user_version", [], |r| r.get(0))?)
}
fn acceptable(db: &Connection) -> Result<bool> {
    let version = header(db)?;
    if version == FORMAT_VERSION {
        return Ok(false);
    }
    if version != 0 {
        return Err(Error::UnsupportedHeader(version));
    }
    if db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%')",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        return Err(Error::Validation(
            "Refusing to initialize an unversioned, nonempty database",
        ));
    }
    Ok(true)
}
pub struct Store {
    directory: PathBuf,
    writer: Mutex<Connection>,
    _lease: File,
    permits: raw::FramePermits,
}
pub struct ReadTxn<'a> {
    sql: &'a Connection,
    directory: &'a Path,
    source: SourceCut,
}
pub struct WriteTxn<'tx, 'conn> {
    sql: &'tx Transaction<'conn>,
    directory: &'tx Path,
    generation: Cell<Option<i64>>,
    permits: &'tx raw::FramePermits,
}
struct ClearPermits(raw::FramePermits);
impl Drop for ClearPermits {
    fn drop(&mut self) {
        if let Ok(mut p) = self.0.lock() {
            p.clear()
        }
    }
}
impl Store {
    pub fn open(directory: impl AsRef<Path>) -> Result<Arc<Self>> {
        let input = directory.as_ref();
        let database = input.join("memory.sqlite");
        if database.exists() {
            let read = Connection::open_with_flags(&database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            acceptable(&read)?;
        }
        let uid = unsafe { libc::getuid() };
        if std::env::var("MEMENTO_AGENT_UID")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            == Some(uid)
        {
            return Err(Error::Validation(
                "Memory service and agent must use distinct operating-system UIDs",
            ));
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(input)?;
        let info = std::fs::symlink_metadata(input)?;
        if !info.is_dir()
            || info.file_type().is_symlink()
            || info.uid() != uid
            || info.mode() & 0o077 != 0
        {
            return Err(Error::Validation(
                "Data directory must be owned by the service UID and private (0700)",
            ));
        }
        let directory = input.canonicalize()?;
        let database = directory.join("memory.sqlite");
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("writer.lock"))?;
        lease
            .try_lock()
            .map_err(|_| Error::Conflict("Another memento writer owns this data directory"))?;
        let mut db = Connection::open(&database)?;
        let fresh = acceptable(&db)?; // Recheck after exclusive lease, before DB mutations.
        std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o600))?;
        let permits = Arc::new(Mutex::new(Default::default()));
        raw::register(&db, Arc::clone(&permits))?;
        let mode: String = db.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if mode != "wal" {
            return Err(Error::Unavailable(
                "Evidence storage requires SQLite WAL mode",
            ));
        }
        db.execute_batch("PRAGMA synchronous=FULL; PRAGMA busy_timeout=3000;")?;
        if fresh {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(include_str!("schema.sql"))?;
            tx.execute("INSERT INTO runtime_state VALUES(1,?,0)", [nonce()])?;
            for tier in [Tier::Hot, Tier::Cold] {
                tx.execute(
                    "INSERT INTO projection_ack VALUES(?,0,?,0)",
                    params![tier.as_str(), nonce()],
                )?;
            }
            tx.pragma_update(None, "user_version", FORMAT_VERSION)?;
            tx.commit()?;
        }
        db.query_row(
            "SELECT db_uuid,generation FROM runtime_state WHERE singleton=1",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )?;
        Ok(Arc::new(Self {
            directory,
            writer: Mutex::new(db),
            _lease: lease,
            permits,
        }))
    }
    pub fn data_dir(&self) -> &Path {
        &self.directory
    }
    pub fn read<T>(&self, apply: impl FnOnce(&ReadTxn<'_>) -> Result<T>) -> Result<T> {
        let db = Connection::open_with_flags(
            self.directory.join("memory.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        raw::register(&db, Arc::new(Mutex::new(Default::default())))?;
        db.execute_batch("PRAGMA query_only=ON; PRAGMA busy_timeout=2000; BEGIN;")?;
        // The first actual SELECT pins this request's WAL cut.
        let source = db.query_row(
            "SELECT db_uuid,generation FROM runtime_state WHERE singleton=1",
            [],
            |r| {
                Ok(SourceCut {
                    db_uuid: r.get(0)?,
                    generation: r.get(1)?,
                })
            },
        )?;
        let result = apply(&ReadTxn {
            sql: &db,
            directory: &self.directory,
            source,
        });
        db.execute_batch("ROLLBACK")?;
        result
    }
    pub fn write<T>(&self, apply: impl FnOnce(&WriteTxn<'_, '_>) -> Result<T>) -> Result<T> {
        let mut db = self
            .writer
            .lock()
            .map_err(|_| Error::Unavailable("Evidence writer requires recovery"))?;
        let _clear = ClearPermits(Arc::clone(&self.permits));
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = apply(&WriteTxn {
            sql: &tx,
            directory: &self.directory,
            generation: Cell::new(None),
            permits: &self.permits,
        })?;
        tx.commit()?;
        Ok(result)
    }
    pub fn command(
        &self,
        context: &CommandContext,
        request: &Value,
        apply: impl FnOnce(&WriteTxn<'_, '_>, &str) -> Result<Value>,
    ) -> Result<Value> {
        if context.command_id.is_empty()
            || context.actor.is_empty()
            || context.why.trim().is_empty()
        {
            return Err(Error::Validation("command_id, actor and why are required"));
        }
        let serialized =
            canonical_json(&json!({"actor":context.actor,"why":context.why,"command":request}))?;
        self.write(|tx| {
            let prior = tx
                .sql
                .query_row(
                    "SELECT request,result FROM graph_commits WHERE command_id=?",
                    [&context.command_id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()?;
            if let Some((old, result)) = prior {
                if old != serialized {
                    return Err(Error::Conflict(
                        "Command identifier already contains a different command",
                    ));
                }
                return Ok(serde_json::from_str(&result)?);
            }
            let id = nonce();
            let result = apply(tx, &id)?;
            tx.sql.execute(
                "INSERT INTO graph_commits VALUES(?,?,?,?,?,?,?)",
                params![
                    id,
                    context.command_id,
                    serialized,
                    serde_json::to_string(&result)?,
                    context.actor,
                    context.why,
                    now_utc()
                ],
            )?;
            Ok(result)
        })
    }
}
impl ReadTxn<'_> {
    pub fn sql(&self) -> &Connection {
        self.sql
    }
    pub fn data_dir(&self) -> &Path {
        self.directory
    }
    pub fn source_cut(&self) -> &SourceCut {
        &self.source
    }
    pub fn original_text(&self, id: &str) -> Result<Option<String>> {
        raw::read_original(self.sql, self.directory, id)
    }
    pub fn fence(&self, tier: Tier) -> Result<EventFence> {
        Ok(self.sql.query_row("SELECT id,nonce,generation FROM projection_events WHERE tier=? ORDER BY id DESC LIMIT 1",[tier.as_str()],|r|Ok(EventFence{id:r.get(0)?,nonce:r.get(1)?,generation:r.get(2)?})).optional()?.unwrap_or(self.sql.query_row("SELECT id,nonce,generation FROM projection_ack WHERE tier=?",[tier.as_str()],|r|Ok(EventFence{id:r.get(0)?,nonce:r.get(1)?,generation:r.get(2)?}))?))
    }
    pub fn dirty(&self, tier: Tier, through: i64) -> Result<Vec<DirtyEvent>> {
        let mut s=self.sql.prepare("SELECT id,nonce,generation,vector_seq FROM projection_events WHERE tier=? AND id<=? ORDER BY id")?;
        Ok(s.query_map(params![tier.as_str(), through], |r| {
            Ok(DirtyEvent {
                fence: EventFence {
                    id: r.get(0)?,
                    nonce: r.get(1)?,
                    generation: r.get(2)?,
                },
                vector_seq: r.get(3)?,
                tier,
            })
        })?
        .collect::<std::result::Result<_, _>>()?)
    }
}
impl<'tx, 'conn> WriteTxn<'tx, 'conn> {
    pub fn sql(&self) -> &Transaction<'conn> {
        self.sql
    }
    pub fn data_dir(&self) -> &Path {
        self.directory
    }
    pub fn original_text(&self, id: &str) -> Result<Option<String>> {
        raw::read_original(self.sql, self.directory, id)
    }
    pub fn generation(&self) -> Result<i64> {
        if let Some(value) = self.generation.get() {
            return Ok(value);
        }
        let value=self.sql.query_row("UPDATE runtime_state SET generation=generation+1 WHERE singleton=1 RETURNING generation",[],|r|r.get(0))?;
        self.generation.set(Some(value));
        Ok(value)
    }
    pub fn authorize_frame(&self, seq: i64, refs: &str, sha: &str) -> Result<()> {
        self.permits
            .lock()
            .map_err(|_| Error::Unavailable("Frame writer requires recovery"))?
            .insert(seq, (refs.into(), sha.into()));
        Ok(())
    }
    pub fn touch_vector(&self, seq: i64, tier: Tier) -> Result<EventFence> {
        let generation = self.generation()?;
        let nonce = nonce();
        self.sql.execute(
            "UPDATE vector_blobs SET generation=? WHERE seq=?",
            params![generation, seq],
        )?;
        self.sql.execute(
            "INSERT INTO projection_events(vector_seq,tier,generation,nonce) VALUES(?,?,?,?)",
            params![seq, tier.as_str(), generation, nonce],
        )?;
        Ok(EventFence {
            id: self.sql.last_insert_rowid(),
            nonce,
            generation,
        })
    }
    pub(crate) fn touch_selected<P: Params>(
        &self,
        select: &str,
        parameters: P,
    ) -> Result<Vec<EventFence>> {
        let ids = {
            let mut query = self.sql.prepare(select)?;
            query
                .query_map(parameters, |r| r.get::<_, i64>(0))?
                .collect::<std::result::Result<BTreeSet<_>, _>>()?
        };
        let mut result = Vec::new();
        for seq in ids {
            for tier in [Tier::Hot, Tier::Cold] {
                result.push(self.touch_vector(seq, tier)?);
            }
        }
        Ok(result)
    }
    pub fn put_vector(&self, value: &VectorWrite) -> Result<EventFence> {
        let previous = self
            .sql
            .query_row(
                "SELECT tier FROM vector_blobs WHERE seq=?",
                [value.seq],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        self.sql.execute("INSERT INTO vector_items VALUES(?,?,?,?) ON CONFLICT(seq) DO UPDATE SET target=excluded.target,kind=excluded.kind,version=excluded.version",params![value.seq,value.target,value.kind.as_str(),value.version])?;
        self.sql.execute("INSERT INTO vector_blobs VALUES(?,?,?,?,?) ON CONFLICT(seq) DO UPDATE SET embedding=excluded.embedding,current=excluded.current,generation=excluded.generation,tier=excluded.tier",params![value.seq,value.embedding.as_bytes(),value.current,self.generation()?,value.tier.as_str()])?;
        if previous.is_some_and(|p| p != value.tier.as_str()) {
            self.touch_vector(
                value.seq,
                if value.tier == Tier::Hot {
                    Tier::Cold
                } else {
                    Tier::Hot
                },
            )?;
        }
        self.touch_vector(value.seq, value.tier)
    }
    /// Coordinator precondition: Edge flush and external checkpoint are durable first.
    pub fn acknowledge(&self, tier: Tier, fence: &EventFence) -> Result<()> {
        let known:bool=self.sql.query_row("SELECT EXISTS(SELECT 1 FROM projection_events WHERE tier=?1 AND id=?2 AND nonce=?3 AND generation=?4) OR EXISTS(SELECT 1 FROM projection_ack WHERE tier=?1 AND id=?2 AND nonce=?3 AND generation=?4)",params![tier.as_str(),fence.id,fence.nonce,fence.generation],|r|r.get(0))?;
        if !known {
            return Err(Error::Conflict(
                "Projection fence differs from the source timeline",
            ));
        }
        self.sql.execute(
            "UPDATE projection_ack SET id=?,nonce=?,generation=? WHERE tier=? AND id<=?",
            params![
                fence.id,
                fence.nonce,
                fence.generation,
                tier.as_str(),
                fence.id
            ],
        )?;
        self.sql.execute(
            "DELETE FROM projection_events WHERE tier=? AND id<=?",
            params![tier.as_str(), fence.id],
        )?;
        Ok(())
    }
}
