//! Disposable native hot/cold vector indexes. SQLite remains the sole authority.
pub mod discovery;
pub mod eligibility;
use memento_core::{Error, EventFence, F32Blob, ModelSignature, ReadTxn, Result, Store, Tier};
use qdrant_edge::{
    CreateIndex, DEFAULT_VECTOR_NAME, Distance, EdgeConfig, EdgeConfigBuilder,
    EdgeOptimizersConfig, EdgeShard, EdgeVectorParamsBuilder, FieldIndexOperations, Filter,
    HnswIndexConfig, Memory, NamedQuery, Payload, PayloadFieldSchema, PayloadSchemaType, PointId,
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
            m: 16,
            ef_construct: 200,
            full_scan_threshold: 10000,
            max_indexing_threads: 1,
            memory: Some(Memory::Cold),
            ..Default::default()
        })
        .optimizers(EdgeOptimizersConfig {
            indexing_threshold: Some(10000),
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
    ParkedCandidate,
    Entity,
}
impl Pool {
    fn field(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Automatic => "automatic",
            Self::Historical => "historical",
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
    let loaded = if checkpoint.is_some() {
        EdgeShard::load(&path.join("edge"), None).ok().map(Arc::new)
    } else {
        None
    };
    Ok(Shard {
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
fn rows(
    tx: &ReadTxn<'_>,
    tier: Tier,
    ids: Option<&BTreeSet<i64>>,
    after: Option<i64>,
) -> Result<Vec<ProjectionRow>> {
    let mut query=tx.sql().prepare("SELECT i.seq,i.target,i.kind,i.version,b.embedding,b.current,b.generation FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE b.tier=?1 AND (?2 IS NULL OR i.seq>?2) ORDER BY i.seq LIMIT ?3")?;
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
        for row in query.query_map(params![tier.as_str(), after, BATCH], decode_row)? {
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
        ("parked_candidate", PayloadSchemaType::Bool),
        ("entity", PayloadSchemaType::Bool),
        ("personal", PayloadSchemaType::Bool),
        ("professional", PayloadSchemaType::Bool),
    ] {
        edge(shard.update(UpdateOperation::FieldIndexOperation(
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: name.parse().unwrap(),
                field_schema: Some(PayloadFieldSchema::FieldType(kind)),
            }),
        )))?;
    }
    Ok(())
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
        Ok(checkpoint.format == 1
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
        let mut query=tx.sql().prepare("SELECT id,nonce,generation,vector_seq FROM projection_events WHERE tier=? AND id>? AND id<=? ORDER BY id LIMIT ?")?;
        let events = query
            .query_map(
                params![
                    tier.as_str(),
                    checkpoint.cursor.id,
                    cut.id,
                    FOREGROUND_EVENTS + 1
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
        let events = &events[..events.len().min(FOREGROUND_EVENTS)];
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
        let shard = self.tier(tier);
        let mut state = shard.state.lock().map_err(|_| unavailable())?;
        let result = self
            .store
            .read(|tx| self.sync(tx, tier, &mut state, &shard.path, None, false, true));
        shard
            .ready
            .store(matches!(result, Ok(true)), Ordering::Release);
        result
    }
    /// Rebuild only this app-owned derived tier, in bounded source snapshots.
    /// No source writer or long WAL snapshot is held during Edge optimization.
    pub fn rebuild(&self, tier: Tier, stop: &AtomicBool) -> Result<()> {
        let target = self.tier(tier);
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
                format: 1,
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
                    Some(seq) => after = Some(seq),
                }
            }
            if stop.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            edge(shard.optimize_with_budget(ResourceBudget::new(1, 1)))?;
            edge(shard.flush())?;
            checkpoint.building = false;
            persist(&staging, &checkpoint)?;
            let mut staging_state = State {
                shard: Some(shard),
                checkpoint: Some(checkpoint),
            };
            loop {
                if stop.load(Ordering::Acquire) {
                    return Err(unavailable());
                }
                if self.store.read(|tx| {
                    self.sync(tx, tier, &mut staging_state, &staging, None, false, false)
                })? {
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
            Some(lock_until(&self.cold.state, deadline)?)
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
                let synced = self.sync(
                    tx,
                    Tier::Cold,
                    state,
                    &self.cold.path,
                    Some(deadline),
                    true,
                    true,
                );
                self.cold
                    .ready
                    .store(matches!(synced, Ok(true)), Ordering::Release);
                if !synced? {
                    return Err(unavailable());
                }
            }
            if let Some(state) = hot_guard.as_mut() {
                let synced = self.sync(
                    tx,
                    Tier::Hot,
                    state,
                    &self.hot.path,
                    Some(deadline),
                    true,
                    true,
                );
                self.hot
                    .ready
                    .store(matches!(synced, Ok(true)), Ordering::Release);
                if !synced? {
                    return Err(unavailable());
                }
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
}
