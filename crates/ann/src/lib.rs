//! Disposable native hot/cold vector indexes. SQLite remains the sole authority.
pub mod discovery;
pub mod eligibility;
use memento_core::{Error, EventFence, F32Blob, ModelSignature, ReadTxn, Result, Store, Tier};
use qdrant_edge::{
    BoolIndexParams, CreateIndex, DEFAULT_VECTOR_NAME, Distance, EdgeConfig, EdgeConfigBuilder,
    EdgeOptimizersConfig, EdgeShard, EdgeVectorParamsBuilder, FieldIndexOperations, Filter,
    HnswIndexConfig, KeywordIndexParams, Memory, NamedQuery, Payload, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType, PointId,
    PointInsertOperations, PointOperations, PointStructPersisted, QueryEnum, QueryRequestBuilder,
    ResourceBudget, ScoringQuery, SearchParams, UpdateOperation, VectorInternal,
    VectorStorageDatatype, VectorStructPersisted, WalOptions,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
const BATCH: usize = 256;
const FOREGROUND_EVENTS: usize = 512;
// Disposable profile fence: older caches retain uncapped appendable/Pinned indexes.
const PROJECTION_FORMAT: u32 = 3;
fn unavailable() -> Error {
    Error::Unavailable("Semantic search is not ready")
}
fn edge<T, E>(value: std::result::Result<T, E>) -> Result<T> {
    value.map_err(|_| unavailable())
}
fn config() -> EdgeConfig {
    EdgeConfigBuilder::new()
        .vector(
            DEFAULT_VECTOR_NAME,
            EdgeVectorParamsBuilder::new(768, Distance::Cosine)
                .memory(Memory::Cold)
                .datatype(VectorStorageDatatype::Float32)
                .build(),
        )
        .payload_memory(Memory::Cold)
        .id_tracker_memory(Memory::Cold)
        .hnsw_config(HnswIndexConfig {
            // SearchView always requests exact FP32 search; neither graph is used.
            m: 0,
            payload_m: Some(0),
            ef_construct: 200,
            full_scan_threshold: 10000,
            max_indexing_threads: 1,
            memory: Some(Memory::Cold),
            ..Default::default()
        })
        .optimizers(EdgeOptimizersConfig {
            // Vendor units are KiB of vectors, not point counts. Positive indexing
            // converts Cold storage to nonappendable mmap even with both graphs off.
            indexing_threshold: Some(10_000),
            max_segment_size: Some(256_000),
            default_segment_number: Some(2),
            prevent_unoptimized: Some(false),
            ..Default::default()
        })
        .max_search_threads(1)
        .wal_enabled(true)
        .wal_options(WalOptions {
            segment_capacity: 1024 * 1024,
            ..Default::default()
        })
        .build()
}
#[derive(Clone, Debug)]
pub struct ProjectionRow {
    pub seq: i64,
    pub target: String,
    pub kind: String,
    pub version: i64,
    pub embedding: F32Blob,
    pub current: bool,
    pub generation: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    format: u32,
    db_uuid: String,
    signature: ModelSignature,
    tier: Tier,
    cursor: EventFence,
    pending: Option<EventFence>,
    building: bool,
}
struct State {
    shard: Option<Arc<EdgeShard>>,
    checkpoint: Option<Checkpoint>,
}
struct Shard {
    // Background-only ownership of the absolute Edge path through optimization
    // and rebuild publication. Foreground reads never acquire this permit.
    maintenance: Mutex<()>,
    state: Mutex<State>,
    ready: AtomicBool,
    path: PathBuf,
}
pub struct Projection {
    store: Arc<Store>,
    signature: ModelSignature,
    hot: Shard,
    cold: Shard,
}
#[derive(Clone, Copy, Debug)]
pub enum Pool {
    Ordinary,
    Automatic,
    Historical,
    RawFallback,
    ParkedCandidate,
    Entity,
}
impl Pool {
    fn field(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Automatic => "automatic",
            Self::Historical => "historical",
            Self::RawFallback => "raw_fallback",
            Self::ParkedCandidate => "parked_candidate",
            Self::Entity => "entity",
        }
    }
}
#[derive(Clone, Debug)]
pub struct SearchHit {
    pub seq: i64,
    pub target: String,
    pub kind: String,
    pub version: i64,
    pub cosine: f32,
}
pub struct SearchView<'a> {
    hot: Option<&'a EdgeShard>,
    cold: Option<&'a EdgeShard>,
    deadline: Instant,
}
/// Single-use Cold query from the same pinned authority cut as owned Hot hits.
/// The guard is released before the owned result reaches hydration/scoring.
pub struct ColdQuery<'a> {
    guard: Option<MutexGuard<'a, State>>,
    deadline: Instant,
}
impl ColdQuery<'_> {
    pub fn search(
        self,
        vector: &[f32; 768],
        limit: usize,
        kind: &str,
        pool: Pool,
        scope: Option<&str>,
        memory_type: Option<&str>,
    ) -> Result<Vec<SearchHit>> {
        let result = {
            let view = SearchView {
                hot: None,
                cold: self.guard.as_ref().and_then(|state| state.shard.as_deref()),
                deadline: self.deadline,
            };
            view.search(Tier::Cold, vector, limit, kind, pool, scope, memory_type)
        };
        drop(self);
        result
    }
}
fn check(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|v| Instant::now() >= v) {
        Err(unavailable())
    } else {
        Ok(())
    }
}
fn lock_until<T>(mutex: &Mutex<T>, deadline: Instant) -> Result<MutexGuard<'_, T>> {
    loop {
        match mutex.try_lock() {
            Ok(g) => return Ok(g),
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(unavailable()),
            Err(_) => {
                check(Some(deadline))?;
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
}
fn lock_ready_until(shard: &Shard, deadline: Instant) -> Result<MutexGuard<'_, State>> {
    loop {
        match shard.state.try_lock() {
            // A free loaded cache may validate/catch up on its first foreground
            // read before background readiness is set; preserve that contract.
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(unavailable()),
            Err(_) => {
                // Rebuild owns State while publishing no usable Cold view. Do
                // not wait its entire build/request deadline before reporting it.
                if !shard.ready.load(Ordering::Acquire) { return Err(unavailable()); }
                check(Some(deadline))?;
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
}
fn load_checkpoint(path: &Path) -> Result<Option<Checkpoint>> {
    match std::fs::read(path.join("checkpoint.json")) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn persist(path: &Path, checkpoint: &Checkpoint) -> Result<()> {
    let temporary = path.join("checkpoint.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(checkpoint)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path.join("checkpoint.json"))?;
    File::open(path)?.sync_all()?;
    Ok(())
}
fn shard(path: PathBuf) -> Result<Shard> {
    let checkpoint = load_checkpoint(&path)?;
    let loaded = if checkpoint
        .as_ref()
        .is_some_and(|c| c.format == PROJECTION_FORMAT)
    {
        EdgeShard::load(&path.join("edge"), None).ok().map(Arc::new)
    } else {
        None
    };
    Ok(Shard {
        maintenance: Mutex::new(()),
        state: Mutex::new(State {
            shard: loaded,
            checkpoint,
        }),
        ready: AtomicBool::new(false),
        path,
    })
}
fn known(tx: &ReadTxn<'_>, tier: Tier, fence: &EventFence) -> Result<bool> {
    Ok(tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM projection_events WHERE tier=?1 AND id=?2 AND nonce=?3 AND generation=?4) OR EXISTS(SELECT 1 FROM projection_ack WHERE tier=?1 AND id=?2 AND nonce=?3 AND generation=?4)",params![tier.as_str(),fence.id,fence.nonce,fence.generation],|r|r.get(0))?)
}
fn rebuild_page(after: Option<i64>) -> (&'static str, i64) {
    match after {
        Some(seq) => (
            "SELECT i.seq,i.target,i.kind,i.version,b.embedding,b.current,b.generation FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE b.tier=?1 AND i.seq>?2 ORDER BY i.seq LIMIT ?3",
            seq,
        ),
        // An inclusive bound keeps the first page seekable and includes i64::MIN.
        // Later pages use the cursor directly, without overflowing i64::MAX.
        None => (
            "SELECT i.seq,i.target,i.kind,i.version,b.embedding,b.current,b.generation FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE b.tier=?1 AND i.seq>=?2 ORDER BY i.seq LIMIT ?3",
            i64::MIN,
        ),
    }
}
fn rows(
    tx: &ReadTxn<'_>,
    tier: Tier,
    ids: Option<&BTreeSet<i64>>,
    after: Option<i64>,
) -> Result<Vec<ProjectionRow>> {
    let mut result = Vec::new();
    if let Some(ids) = ids {
        // Point-keyed reads retain every i64 ID, including negative and zero.
        let mut one=tx.sql().prepare("SELECT i.seq,i.target,i.kind,i.version,b.embedding,b.current,b.generation FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE b.tier=? AND i.seq=?")?;
        for id in ids {
            if let Some(row) = one
                .query_row(params![tier.as_str(), id], decode_row)
                .optional()?
            {
                result.push(row?);
            }
        }
    } else {
        let (sql, lower) = rebuild_page(after);
        let mut query = tx.sql().prepare(sql)?;
        for row in query.query_map(params![tier.as_str(), lower, BATCH], decode_row)? {
            result.push(row??);
        }
    }
    Ok(result)
}
fn decode_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Result<ProjectionRow>> {
    let seq = r.get(0)?;
    let target = r.get(1)?;
    let kind = r.get(2)?;
    let version = r.get(3)?;
    let bytes = r.get(4)?;
    let current = r.get(5)?;
    let generation = r.get(6)?;
    Ok(F32Blob::from_bytes(bytes).map(|embedding| ProjectionRow {
        seq,
        target,
        kind,
        version,
        embedding,
        current,
        generation,
    }))
}

fn upsert(shard: &EdgeShard, tx: &ReadTxn<'_>, rows: &[ProjectionRow]) -> Result<()> {
    let mut points = Vec::with_capacity(rows.len());
    for row in rows {
        let vector = row.embedding.values();
        let norm: f64 = vector.iter().map(|v| f64::from(*v).powi(2)).sum();
        if !norm.is_finite() || norm <= 0.0 {
            return Err(Error::Unavailable("Stored vector cannot be searched"));
        }
        let payload = eligibility::payload(tx.sql(), row)?;
        points.push(PointStructPersisted {
            id: PointId::NumId(row.seq as u64),
            vector: VectorStructPersisted::Single(vector),
            payload: Some(Payload::from(payload.as_object().unwrap().clone())),
        });
    }
    if !points.is_empty() {
        edge(shard.update(UpdateOperation::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::PointsList(points)),
        )))?;
    }
    Ok(())
}
fn remove(shard: &EdgeShard, ids: Vec<PointId>) -> Result<()> {
    if !ids.is_empty() {
        edge(shard.update(UpdateOperation::PointOperation(
            PointOperations::DeletePoints { ids },
        )))?;
    }
    Ok(())
}
fn indices(shard: &EdgeShard) -> Result<()> {
    for (name, kind) in [
        ("kind", PayloadSchemaType::Keyword),
        ("type", PayloadSchemaType::Keyword),
        ("current", PayloadSchemaType::Bool),
        ("ordinary", PayloadSchemaType::Bool),
        ("automatic", PayloadSchemaType::Bool),
        ("historical", PayloadSchemaType::Bool),
        ("raw_fallback", PayloadSchemaType::Bool),
        ("parked_candidate", PayloadSchemaType::Bool),
        ("entity", PayloadSchemaType::Bool),
        ("personal", PayloadSchemaType::Bool),
        ("professional", PayloadSchemaType::Bool),
    ] {
        let schema = match kind {
            PayloadSchemaType::Keyword => PayloadSchemaParams::Keyword(KeywordIndexParams {
                memory: Some(Memory::Cold), ..Default::default()
            }),
            // The pinned vendor accepts Cold but implements Bool as heap/Pinned
            // RoaringFlags<MmapFile>. The lifecycle test retains this limitation.
            PayloadSchemaType::Bool => PayloadSchemaParams::Bool(BoolIndexParams {
                memory: Some(Memory::Cold), ..Default::default()
            }),
            _ => unreachable!(),
        };
        edge(shard.update(UpdateOperation::FieldIndexOperation(
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: name.parse().unwrap(),
                field_schema: Some(PayloadFieldSchema::FieldParams(schema)),
            }),
        )))?;
    }
    Ok(())
}
fn optimize(shard: &EdgeShard) -> Result<()> {
    if edge(shard.optimize_with_budget(ResourceBudget::new(1, 1)))? {
        edge(shard.flush())?;
    }
    Ok(())
}
fn finish_build(shard: &EdgeShard, path: &Path, checkpoint: &mut Checkpoint) -> Result<()> {
    optimize(shard)?;
    // An unpublished below-threshold batch need not have changed any optimizer.
    // Its vectors must still be flushed BEFORE the durable building=false fence.
    edge(shard.flush())?;
    checkpoint.building = false;
    persist(path, checkpoint)
}
fn reconcile_raw_tiers(store: &Store) -> Result<()> {
    let mut next = i64::MIN;
    loop {
        let last = store.write(|tx| {
            let mut query = tx.sql().prepare("SELECT i.seq FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.kind='episode' AND i.seq>=?1 AND b.tier='hot' ORDER BY i.seq LIMIT ?2")?;
            let ids = query.query_map(params![next, BATCH], |r| r.get::<_, i64>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(query);
            for id in &ids {
                tx.sql().execute("UPDATE vector_blobs SET tier='cold' WHERE seq=?", [id])?;
                tx.touch_vector(*id, Tier::Hot)?;
                tx.touch_vector(*id, Tier::Cold)?;
            }
            Ok(ids.last().copied())
        })?;
        match last.and_then(|seq| seq.checked_add(1)) {
            Some(after) => next = after,
            None => return Ok(()),
        }
    }
}
impl Projection {
    pub fn open(store: Arc<Store>, signature: ModelSignature) -> Result<Arc<Self>> {
        let root = store.data_dir().join("search");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)?;
        for path in [&root, &root.join("hot"), &root.join("cold")] {
            if path.exists() && std::fs::symlink_metadata(path)?.file_type().is_symlink() {
                return Err(Error::Validation(
                    "Search storage cannot be a symbolic link",
                ));
            }
        }
        store.write(|tx|{
            let stored=tx.sql().query_row("SELECT model_id,revision,dimensions,text_encoding FROM vector_model WHERE singleton=1",[],|r|Ok(ModelSignature{model_id:r.get(0)?,revision:r.get(1)?,dimensions:r.get(2)?,text_encoding:r.get(3)?})).optional()?;
            if let Some(stored)=stored{if stored!=signature{return Err(Error::Unavailable("Stored vectors use a different model signature"))}}
            else{let count:i64=tx.sql().query_row("SELECT count(*) FROM vector_blobs",[],|r|r.get(0))?;if count!=0{return Err(Error::Unavailable("Stored vectors have no model signature"))}tx.sql().execute("INSERT INTO vector_model VALUES(1,?,?,?,?)",params![signature.model_id,signature.revision,signature.dimensions,signature.text_encoding])?;}Ok(())
        })?;
        // Idempotent bounded transactions: legacy Hot raw points move through the
        // existing two-tier dirty log without changing source text or vector bytes.
        reconcile_raw_tiers(&store)?;
        let hot = shard(root.join("hot"))?;
        let cold = shard(root.join("cold"))?;
        Ok(Arc::new(Self {
            store,
            signature,
            hot,
            cold,
        }))
    }
    fn tier(&self, tier: Tier) -> &Shard {
        match tier {
            Tier::Hot => &self.hot,
            Tier::Cold => &self.cold,
        }
    }
    pub fn ready(&self, tier: Tier) -> bool {
        self.tier(tier).ready.load(Ordering::Acquire)
    }
    fn valid(&self, tx: &ReadTxn<'_>, tier: Tier, checkpoint: &Checkpoint) -> Result<bool> {
        Ok(checkpoint.format == PROJECTION_FORMAT
            && !checkpoint.building
            && checkpoint.db_uuid == tx.source_cut().db_uuid
            && checkpoint.signature == self.signature
            && checkpoint.tier == tier
            && known(tx, tier, &checkpoint.cursor)?
            && match &checkpoint.pending {
                None => true,
                Some(pending) => {
                    known(tx, tier, pending)?
                        && pending.id >= checkpoint.cursor.id
                        && pending.id <= tx.fence(tier)?.id
                }
            })
    }
    fn sync(
        &self,
        tx: &ReadTxn<'_>,
        tier: Tier,
        state: &mut State,
        path: &Path,
        deadline: Option<Instant>,
        foreground: bool,
        ack: bool,
    ) -> Result<bool> {
        check(deadline)?;
        let shard = state.shard.as_ref().ok_or_else(unavailable)?;
        let mut checkpoint = state.checkpoint.clone().ok_or_else(unavailable)?;
        if !self.valid(tx, tier, &checkpoint)? {
            return Err(unavailable());
        }
        let cut = tx.fence(tier)?;
        if checkpoint.cursor == cut && checkpoint.pending.is_none() {
            if ack {
                let acknowledged = tx.sql().query_row(
                    "SELECT id,nonce,generation FROM projection_ack WHERE tier=?",
                    [tier.as_str()],
                    |r| {
                        Ok(EventFence {
                            id: r.get(0)?,
                            nonce: r.get(1)?,
                            generation: r.get(2)?,
                        })
                    },
                )?;
                // Skip the writer on healthy read-only recall. A durable index
                // cursor whose SQL acknowledgment lagged a crash still recovers.
                if acknowledged != cut {
                    self.store.write(|writer| writer.acknowledge(tier, &cut))?;
                }
            }
            return Ok(true);
        }
        // Background pages yield to optimization every 256 points. Foreground
        // retains its existing bounded 512-event admission and never optimizes.
        let limit = if foreground { FOREGROUND_EVENTS } else { BATCH };
        let mut query=tx.sql().prepare("SELECT id,nonce,generation,vector_seq FROM projection_events WHERE tier=? AND id>? AND id<=? ORDER BY id LIMIT ?")?;
        let events = query
            .query_map(
                params![
                    tier.as_str(),
                    checkpoint.cursor.id,
                    cut.id,
                    limit + 1
                ],
                |r| {
                    Ok((
                        EventFence {
                            id: r.get(0)?,
                            nonce: r.get(1)?,
                            generation: r.get(2)?,
                        },
                        r.get::<_, i64>(3)?,
                    ))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if events.is_empty() {
            return Err(unavailable());
        }
        if foreground && events.len() > FOREGROUND_EVENTS {
            return Err(unavailable());
        }
        let events = &events[..events.len().min(limit)];
        let through = events.last().unwrap().0.clone();
        checkpoint.pending = Some(through.clone());
        persist(path, &checkpoint)?;
        state.checkpoint = Some(checkpoint.clone()); // durable intent BEFORE any point mutation
        let ids = events.iter().map(|v| v.1).collect::<BTreeSet<_>>();
        for chunk in ids.iter().copied().collect::<Vec<_>>().chunks(BATCH) {
            check(deadline)?;
            let batch = chunk.iter().copied().collect::<BTreeSet<_>>();
            let rows = rows(tx, tier, Some(&batch), None)?;
            let present = rows.iter().map(|r| r.seq).collect::<BTreeSet<_>>();
            remove(
                shard,
                batch
                    .difference(&present)
                    .map(|v| PointId::NumId(*v as u64))
                    .collect(),
            )?;
            upsert(shard, tx, &rows)?;
        }
        edge(shard.flush())?; // Edge update acknowledgment alone is NOT durable.
        checkpoint.cursor = through.clone();
        checkpoint.pending = None;
        persist(path, &checkpoint)?;
        state.checkpoint = Some(checkpoint);
        if ack {
            self.store
                .write(|writer| writer.acknowledge(tier, &through))?;
        }
        check(deadline)?;
        Ok(through == cut)
    }
    /// One short background step. Large backlogs are never reported healthy-empty.
    pub fn synchronize(&self, tier: Tier) -> Result<bool> {
        self.synchronize_with(tier, optimize)
    }
    fn synchronize_with(
        &self,
        tier: Tier,
        maintenance: impl FnOnce(&EdgeShard) -> Result<()>,
    ) -> Result<bool> {
        let shard = self.tier(tier);
        let _maintenance = shard.maintenance.lock().map_err(|_| unavailable())?;
        let (result, native) = {
            let mut state = shard.state.lock().map_err(|_| unavailable())?;
            let result = self.store.read(|tx| {
                self.sync(tx, tier, &mut state, &shard.path, None, false, true)
            });
            (result, state.shard.clone())
        };
        // Same-tier foreground reads/updates may use the native Arc during vendor
        // optimization. No SQL snapshot or State guard survives this boundary.
        // The maintenance permit prevents rebuild from reusing its absolute path.
        let result = result.and_then(|ready| {
            maintenance(native.as_deref().ok_or_else(unavailable)?)?;
            Ok(ready)
        });
        shard
            .ready
            .store(matches!(result, Ok(true)), Ordering::Release);
        result
    }
    /// Rebuild only this app-owned derived tier, in bounded source snapshots.
    /// No source writer or long WAL snapshot is held during Edge optimization.
    pub fn rebuild(&self, tier: Tier, stop: &AtomicBool) -> Result<()> {
        let target = self.tier(tier);
        let _maintenance = target.maintenance.lock().map_err(|_| unavailable())?;
        let mut state = target.state.lock().map_err(|_| unavailable())?;
        target.ready.store(false, Ordering::Release);
        let staging = target.path.with_file_name(format!(
            "{}.building-{}",
            tier.as_str(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::DirBuilder::new().mode(0o700).create(&staging)?;
        let result = (|| {
            let (source, fence) = self
                .store
                .read(|tx| Ok((tx.source_cut().clone(), tx.fence(tier)?)))?;
            let shard = Arc::new(edge(EdgeShard::new(&staging.join("edge"), config()))?);
            indices(&shard)?;
            let mut checkpoint = Checkpoint {
                format: PROJECTION_FORMAT,
                db_uuid: source.db_uuid,
                signature: self.signature.clone(),
                tier,
                cursor: fence,
                pending: None,
                building: true,
            };
            persist(&staging, &checkpoint)?;
            let mut after = None;
            loop {
                if stop.load(Ordering::Acquire) {
                    return Err(unavailable());
                }
                let next = self.store.read(|tx| {
                    let batch = rows(tx, tier, None, after)?;
                    upsert(&shard, tx, &batch)?;
                    Ok(batch.last().map(|r| r.seq))
                })?;
                match next {
                    None => break,
                    Some(seq) => {
                        after = Some(seq);
                        if stop.load(Ordering::Acquire) { return Err(unavailable()); }
                        optimize(&shard)?;
                    }
                }
            }
            if stop.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            finish_build(&shard, &staging, &mut checkpoint)?;
            let mut staging_state = State {
                shard: Some(shard),
                checkpoint: Some(checkpoint),
            };
            loop {
                if stop.load(Ordering::Acquire) {
                    return Err(unavailable());
                }
                let synced = self.store.read(|tx| {
                    self.sync(tx, tier, &mut staging_state, &staging, None, false, false)
                })?;
                if stop.load(Ordering::Acquire) { return Err(unavailable()); }
                optimize(staging_state.shard.as_deref().ok_or_else(unavailable)?)?;
                if synced {
                    break;
                }
            }
            let final_checkpoint = staging_state.checkpoint.clone().unwrap();
            drop(staging_state);
            state.shard = None;
            let retired = target.path.with_file_name(format!(
                "{}.retired-{}",
                tier.as_str(),
                uuid::Uuid::new_v4().simple()
            ));
            if target.path.exists() {
                std::fs::rename(&target.path, &retired)?;
            }
            std::fs::rename(&staging, &target.path)?;
            File::open(target.path.parent().unwrap())?.sync_all()?;
            state.shard = Some(Arc::new(edge(EdgeShard::load(
                &target.path.join("edge"),
                None,
            ))?));
            state.checkpoint = Some(final_checkpoint.clone());
            self.store
                .write(|writer| writer.acknowledge(tier, &final_checkpoint.cursor))?;
            target.ready.store(true, Ordering::Release);
            if retired.exists() {
                std::fs::remove_dir_all(retired)?;
            }
            Ok(())
        })();
        if result.is_err() && staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        result
    }
    fn sync_snapshot(&self, tx: &ReadTxn<'_>, tier: Tier, state: &mut State, deadline: Instant) -> Result<()> {
        let shard = self.tier(tier);
        let synced = self.sync(tx, tier, state, &shard.path, Some(deadline), true, true);
        shard.ready.store(matches!(synced, Ok(true)), Ordering::Release);
        if !synced? { return Err(unavailable()); }
        Ok(())
    }
    /// Own every required Hot hit before releasing Hot. The same SQLite read
    /// transaction remains pinned through the optional single Cold query and finish.
    pub fn snapshot_hot_until<H, T>(
        &self,
        cold: bool,
        deadline: Instant,
        collect_hot: impl FnOnce(&ReadTxn<'_>, &SearchView<'_>) -> Result<H>,
        finish: impl FnOnce(&ReadTxn<'_>, H, ColdQuery<'_>) -> Result<T>,
    ) -> Result<T> {
        check(Some(deadline))?;
        // Preserve cold->hot order; waiting for a Cold build never holds Hot.
        let mut cold_guard = if cold { Some(lock_ready_until(&self.cold, deadline)?) } else { None };
        let mut hot_guard = lock_until(&self.hot.state, deadline)?;
        self.store.read(|tx| {
            if let Some(state) = cold_guard.as_mut() {
                self.sync_snapshot(tx, Tier::Cold, state, deadline)?;
            }
            self.sync_snapshot(tx, Tier::Hot, &mut hot_guard, deadline)?;
            let owned_hot = {
                let hot_view = SearchView {
                    hot: hot_guard.shard.as_deref(), cold: None, deadline,
                };
                collect_hot(tx, &hot_view)?
            };
            // The borrowed view is gone; no Hot handle escapes in the owned hits.
            drop(hot_guard);
            let value = finish(tx, owned_hot, ColdQuery { guard: cold_guard, deadline })?;
            check(Some(deadline))?;
            Ok(value)
        })
    }
    /// Encode query BEFORE calling this method. Hydration uses this same pinned cut.
    pub fn snapshot<T>(
        &self,
        hot: bool,
        cold: bool,
        apply: impl FnOnce(&ReadTxn<'_>, &SearchView<'_>) -> Result<T>,
    ) -> Result<T> {
        self.snapshot_until(hot, cold, Instant::now() + Duration::from_secs(2), apply)
    }
    /// The caller carries its existing request deadline, including encoding time.
    /// Different foreground/deep/archive paths must not inherit an artificial common ceiling.
    pub fn snapshot_until<T>(
        &self,
        hot: bool,
        cold: bool,
        deadline: Instant,
        apply: impl FnOnce(&ReadTxn<'_>, &SearchView<'_>) -> Result<T>,
    ) -> Result<T> {
        check(Some(deadline))?;
        // Fixed cold->hot ordering. A cold build never monopolizes the hot lock.
        let mut cold_guard = if cold {
            Some(lock_ready_until(&self.cold, deadline)?)
        } else {
            None
        };
        let mut hot_guard = if hot {
            Some(lock_until(&self.hot.state, deadline)?)
        } else {
            None
        };
        self.store.read(|tx| {
            if let Some(state) = cold_guard.as_mut() {
                self.sync_snapshot(tx, Tier::Cold, state, deadline)?;
            }
            if let Some(state) = hot_guard.as_mut() {
                self.sync_snapshot(tx, Tier::Hot, state, deadline)?;
            }
            let view = SearchView {
                hot: hot_guard.as_ref().and_then(|s| s.shard.as_deref()),
                cold: cold_guard.as_ref().and_then(|s| s.shard.as_deref()),
                deadline,
            };
            let value = apply(tx, &view)?;
            check(Some(deadline))?;
            Ok(value)
        })
    }
}
impl SearchView<'_> {
    pub fn search(
        &self,
        tier: Tier,
        vector: &[f32; 768],
        limit: usize,
        kind: &str,
        pool: Pool,
        scope: Option<&str>,
        memory_type: Option<&str>,
    ) -> Result<Vec<SearchHit>> {
        check(Some(self.deadline))?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        if !vector.iter().all(|v| v.is_finite()) || vector.iter().all(|v| *v == 0.0) {
            return Err(Error::Validation("Query vector must be finite and nonzero"));
        }
        let shard = match tier {
            Tier::Hot => self.hot,
            Tier::Cold => self.cold,
        }
        .ok_or_else(unavailable)?;
        let mut must = vec![
            json!({"key":"current","match":{"value":true}}),
            json!({"key":"kind","match":{"value":kind}}),
            json!({"key":pool.field(),"match":{"value":true}}),
        ];
        if let Some(scope) = scope {
            if !["personal", "professional"].contains(&scope) {
                return Err(Error::Validation("Invalid memory scope"));
            }
            must.push(json!({"key":scope,"match":{"value":true}}));
        }
        if let Some(kind) = memory_type {
            must.push(json!({"key":"type","match":{"value":kind}}));
        }
        let filter: Filter = serde_json::from_value(json!({"must":must}))?;
        let request = QueryRequestBuilder::new(limit)
            .filter(filter)
            .params(SearchParams {
                hnsw_ef: Some(2048),
                exact: true,
                ..Default::default()
            })
            .with_payload(true.into())
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::from(vector.to_vec()),
                DEFAULT_VECTOR_NAME.to_string(),
            ))))
            .build();
        let points = edge(shard.query(request))?;
        check(Some(self.deadline))?;
        points
            .into_iter()
            .map(|p| {
                let PointId::NumId(id) = p.id else {
                    return Err(unavailable());
                };
                let payload = p.payload.ok_or_else(unavailable)?;
                let value = serde_json::to_value(payload)?;
                Ok(SearchHit {
                    seq: id as i64,
                    target: value["target"].as_str().ok_or_else(unavailable)?.into(),
                    kind: value["kind"].as_str().ok_or_else(unavailable)?.into(),
                    version: value["version"].as_i64().ok_or_else(unavailable)?,
                    cosine: p.score,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod pagination_tests;

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use memento_core::{VectorKind, VectorWrite};
    use std::os::unix::fs::PermissionsExt;
    fn fixture() -> (tempfile::TempDir, Arc<Store>, Arc<Projection>) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = Store::open(dir.path()).unwrap();
        let projection = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
        (dir, store, projection)
    }
    fn add(store: &Store, id: &str, seq: i64, cos: f32) {
        store.write(|tx|{tx.sql().execute("INSERT INTO graph_commits SELECT 'test','test','{}','{}','test','test','2026' WHERE NOT EXISTS(SELECT 1 FROM graph_commits WHERE id='test')",[])?;tx.sql().execute("INSERT INTO node_versions VALUES(?,1,'test','person','A recorded person','active',NULL,NULL,NULL,0,'[\"personal\"]')",[id])?;let mut v=[0.;768];v[0]=cos;v[1]=(1.-cos*cos).sqrt();tx.put_vector(&VectorWrite{seq,target:id.into(),kind:VectorKind::Node,version:1,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&v)?})?;Ok(())}).unwrap()
    }
    fn top(index: &Projection) -> Result<String> {
        index.snapshot(true, false, |_, view| {
            let mut q = [0.; 768];
            q[0] = 1.;
            Ok(view.search(
                Tier::Hot,
                &q,
                1,
                "node",
                Pool::Ordinary,
                Some("personal"),
                None,
            )?[0]
                .target
                .clone())
        })
    }
    fn accepted_but_unpublished(index: &Projection) {
        let mut state = index.hot.state.lock().unwrap();
        index
            .store
            .read(|tx| {
                let through = tx.fence(Tier::Hot)?;
                let mut checkpoint = state.checkpoint.clone().unwrap();
                checkpoint.pending = Some(through);
                persist(&index.hot.path, &checkpoint)?;
                state.checkpoint = Some(checkpoint);
                let all = rows(tx, Tier::Hot, None, None)?;
                upsert(state.shard.as_ref().unwrap(), tx, &all)?;
                edge(state.shard.as_ref().unwrap().flush())?;
                Ok(())
            })
            .unwrap()
    }
    #[test]
    fn accepted_mutation_with_durable_intent_replays_after_reopen() {
        let (_dir, store, index) = fixture();
        add(&store, "old", 1, 0.7);
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        add(&store, "new", 2, 0.9);
        accepted_but_unpublished(&index);
        drop(index);
        let index = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
        assert_eq!(top(&index).unwrap(), "new");
        let cp = load_checkpoint(&index.hot.path).unwrap().unwrap();
        assert!(cp.pending.is_none());
        assert!(
            store
                .read(|tx| Ok(tx.dirty(Tier::Hot, i64::MAX)?.is_empty()))
                .unwrap()
        );
    }
    #[test]
    fn older_sqlite_restore_rejects_unknown_pending_timeline_and_rebuilds() {
        let (dir, store, index) = fixture();
        add(&store, "old", 1, 0.7);
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        let backup = dir.path().join("before.sqlite");
        store
            .read(|tx| {
                tx.sql().backup("main", &backup, None)?;
                Ok(())
            })
            .unwrap();
        add(&store, "future", 2, 0.9);
        accepted_but_unpublished(&index);
        drop(index);
        drop(store);
        for suffix in ["-wal", "-shm"] {
            let p = dir.path().join(format!("memory.sqlite{suffix}"));
            if p.exists() {
                std::fs::remove_file(p).unwrap()
            }
        }
        std::fs::rename(&backup, dir.path().join("memory.sqlite")).unwrap();
        let store = Store::open(dir.path()).unwrap();
        let index = Projection::open(store, ModelSignature::pinned()).unwrap();
        assert!(top(&index).is_err());
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        assert_eq!(top(&index).unwrap(), "old");
    }
    #[test]
    fn durable_index_cursor_before_sql_ack_is_reconciled_without_empty_result() {
        let (_dir, store, index) = fixture();
        add(&store, "old", 1, 0.7);
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        add(&store, "new", 2, 0.9);
        {
            let mut state = index.hot.state.lock().unwrap();
            store
                .read(|tx| {
                    assert!(index.sync(
                        tx,
                        Tier::Hot,
                        &mut state,
                        &index.hot.path,
                        None,
                        false,
                        false
                    )?);
                    Ok(())
                })
                .unwrap();
        }
        assert!(
            !store
                .read(|tx| Ok(tx.dirty(Tier::Hot, i64::MAX)?.is_empty()))
                .unwrap()
        );
        drop(index);
        let index = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
        assert_eq!(top(&index).unwrap(), "new");
        assert!(
            store
                .read(|tx| Ok(tx.dirty(Tier::Hot, i64::MAX)?.is_empty()))
                .unwrap()
        );
    }
    #[test]
    fn legacy_profile_checkpoint_rebuild_preserves_authority_and_fp32() {
        let (_dir, store, index) = fixture();
        add(&store, "old", 1, 0.7);
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        let before = store.read(|tx| Ok((tx.source_cut().clone(), tx.sql().query_row(
            "SELECT embedding,current,generation FROM vector_blobs WHERE seq=1", [],
            |r| Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?)),
        )?))).unwrap();
        let mut legacy = load_checkpoint(&index.hot.path).unwrap().unwrap();
        legacy.format = 2;
        persist(&index.hot.path, &legacy).unwrap();
        drop(index);
        let index = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
        assert!(index.hot.state.lock().unwrap().shard.is_none());
        assert!(!index.ready(Tier::Hot));
        assert!(top(&index).is_err(), "legacy cache was falsely ready");
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        assert_eq!(top(&index).unwrap(), "old");
        assert_eq!(load_checkpoint(&index.hot.path).unwrap().unwrap().format, 3);
        let after = store.read(|tx| Ok((tx.source_cut().clone(), tx.sql().query_row(
            "SELECT embedding,current,generation FROM vector_blobs WHERE seq=1", [],
            |r| Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?)),
        )?))).unwrap();
        assert_eq!(after, before, "derived profile upgrade changed authority");
    }
    #[test]
    fn background_optimization_releases_foreground_and_serializes_rebuild_path() {
        use std::sync::mpsc;
        let (_dir, store, index) = fixture();
        add(&store, "old", 1, 0.7);
        index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
        let native = index.hot.state.lock().unwrap().shard.clone().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let background = {
            let index = index.clone();
            std::thread::spawn(move || index.synchronize_with(Tier::Hot, |shard| {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                optimize(shard)
            }))
        };
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(index.hot.maintenance.try_lock().is_err());
        assert!(index.hot.state.try_lock().is_ok(), "State held during native maintenance");
        assert!(index.cold.maintenance.try_lock().is_ok(), "maintenance crossed tiers");
        // A fresh authoritative mutation and foreground fence/query must proceed
        // while maintenance is paused; no SQL read transaction/State guard remains.
        add(&store, "new", 2, 0.9);
        assert_eq!(top(&index).unwrap(), "new");
        let (started_tx, started_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let rebuilding = {
            let index = index.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let result = index.rebuild(Tier::Hot, &AtomicBool::new(false));
                finished_tx.send(()).unwrap();
                result
            })
        };
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(finished_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)), "rebuild reused path during old-Arc maintenance");
        assert!(Arc::ptr_eq(index.hot.state.lock().unwrap().shard.as_ref().unwrap(), &native));
        release_tx.send(()).unwrap();
        assert!(background.join().unwrap().unwrap());
        rebuilding.join().unwrap().unwrap();
        assert!(!Arc::ptr_eq(index.hot.state.lock().unwrap().shard.as_ref().unwrap(), &native));
        assert_eq!(top(&index).unwrap(), "new");
    }
    #[test]
    fn below_threshold_bulk_build_flushes_before_ready_checkpoint_without_wal_replay() {
        fn disk_version(path: &Path) -> u64 {
            let versions:Vec<_> = std::fs::read_dir(path.join("edge/segments")).unwrap()
                .map(|entry|entry.unwrap().path())
                .filter(|path|path.join("version.info").is_file())
                .map(|path|serde_json::from_slice::<qdrant_edge::SegmentState>(
                    &std::fs::read(path.join("segment.json")).unwrap()).unwrap().version.unwrap())
                .collect();
            assert_eq!(versions.len(),1,"tiny fixture must remain appendable");
            versions[0]
        }
        let (_dir, store, index) = fixture();
        add(&store,"durable",1,0.7);
        let path=index.hot.path.with_file_name("unpublished-flush-proof");
        std::fs::create_dir(&path).unwrap();
        let native=EdgeShard::new(&path.join("edge"),config()).unwrap();
        indices(&native).unwrap();
        native.flush().unwrap();
        let before=disk_version(&path);
        let mut checkpoint=store.read(|tx| {
            let checkpoint=Checkpoint {format:PROJECTION_FORMAT,db_uuid:tx.source_cut().db_uuid.clone(),
                signature:ModelSignature::pinned(),tier:Tier::Hot,cursor:tx.fence(Tier::Hot)?,
                pending:None,building:true};
            persist(&path,&checkpoint)?;
            upsert(&native,tx,&rows(tx,Tier::Hot,None,None)?)?;
            Ok(checkpoint)
        }).unwrap();
        optimize(&native).unwrap();
        assert_eq!(disk_version(&path),before,"unchanged optimizer unexpectedly force-flushed");
        assert!(load_checkpoint(&path).unwrap().unwrap().building);
        finish_build(&native,&path,&mut checkpoint).unwrap();
        // Check actual persisted segment state BEFORE load can replay/flush WAL.
        assert!(disk_version(&path)>before,"ready checkpoint preceded bulk data flush");
        assert!(!load_checkpoint(&path).unwrap().unwrap().building);
        drop(native);
        let native=EdgeShard::load(&path.join("edge"),None).unwrap();
        let mut query=[0.;768];query[0]=1.;
        let view=SearchView {hot:Some(&native),cold:None,deadline:Instant::now()+Duration::from_secs(2)};
        let hits=view.search(Tier::Hot,&query,1,"node",Pool::Ordinary,Some("personal"),None).unwrap();
        assert_eq!(hits[0].target,"durable");
        assert!((hits[0].cosine-0.7).abs()<1e-6);
    }
    #[test]
    fn cold_rebuild_unavailable_returns_without_waiting_or_blocking_hot() {
        use std::sync::mpsc;
        let (_dir, store, index) = fixture();
        add(&store,"curated",1,0.9);
        for tier in [Tier::Hot,Tier::Cold] {
            index.rebuild(tier,&AtomicBool::new(false)).unwrap();
        }
        // Pause the exact Cold rebuild ownership boundary; no Hot lock is held.
        let maintenance=index.cold.maintenance.lock().unwrap();
        let cold=index.cold.state.lock().unwrap();
        let (started_tx,started_rx)=mpsc::channel();
        let (result_tx,result_rx)=mpsc::channel();
        let callbacks=Arc::new(AtomicBool::new(false));
        let workers:Vec<_>=[false,true].into_iter().map(|owned| {
            let index=index.clone();let started=started_tx.clone();
            let result=result_tx.clone();let callbacks=callbacks.clone();
            std::thread::spawn(move || {
                started.send(()).unwrap();
                let deadline=Instant::now()+Duration::from_secs(10);
                let value=if owned {
                    index.snapshot_hot_until(true,deadline,|_,_| {
                        callbacks.store(true,Ordering::Release);Ok(())
                    },|_,(),_|Ok(()))
                } else {
                    index.snapshot_until(true,true,deadline,|_,_| {
                        callbacks.store(true,Ordering::Release);Ok(())
                    })
                };
                result.send(matches!(value,Err(Error::Unavailable("Semantic search is not ready")))).unwrap();
            })
        }).collect();
        for _ in 0..2 {started_rx.recv_timeout(Duration::from_secs(5)).unwrap();}
        // Covers readiness becoming false while the lock is contended, as well
        // as a request arriving after rebuild has already announced unavailability.
        index.cold.ready.store(false,Ordering::Release);
        let first=result_rx.recv_timeout(Duration::from_secs(1));
        let second=result_rx.recv_timeout(Duration::from_secs(1));
        let hot=top(&index);
        drop(cold);drop(maintenance);
        index.cold.ready.store(true,Ordering::Release);
        for worker in workers {worker.join().unwrap();}
        assert_eq!(first.unwrap(),true,"Cold request waited for rebuild State");
        assert_eq!(second.unwrap(),true,"Cold request waited for rebuild State");
        assert!(!callbacks.load(Ordering::Acquire),"unavailable Cold invoked snapshot callbacks");
        assert_eq!(hot.unwrap(),"curated","Cold rebuild blocked available Hot results");
    }
}

#[cfg(test)]
mod owned_snapshot_tests;

#[cfg(test)]
mod lifecycle_tests;
