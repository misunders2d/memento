//! Bounded background text indexing. Model and ANN hint work never hold the writer.
use crate::records;
use memento_ann::{Pool, Projection, eligibility::ACTIVE_CANDIDATE};
use memento_core::{CharSpan, Error, F32Blob, Result, Store, Tier, VectorKind, VectorWrite, raw};
use memento_domain::{self as domain, jobs::timestamp};
use memento_embedding::EmbeddingModel;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
const NODE: &str = "SELECT n.id,'node' AS kind,n.version,n.text,n.type,NULL AS episode_id,NULL AS start,NULL AS end,n.rowid AS position FROM node_versions n WHERE n.state='active' AND n.type<>'media' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)";
fn candidate_source() -> String {
    format!(
        "SELECT c.id,'candidate' AS kind,1 AS version,c.text,c.type,NULL AS episode_id,NULL AS start,NULL AS end,c.rowid AS position FROM candidates c WHERE {ACTIVE_CANDIDATE}"
    )
}
const EPISODE: &str = "SELECT x.id,'episode' AS kind,1 AS version,NULL AS text,NULL AS type,x.episode_id,x.start,x.end,x.rowid AS position FROM episode_chunks x JOIN episodes e ON e.id=x.episode_id WHERE coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip' AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)";
pub(crate) fn sources() -> [(String, &'static str); 3] {
    [
        (NODE.into(), "node_versions"),
        (candidate_source(), "candidates"),
        (EPISODE.into(), "episode_chunks"),
    ]
}
pub struct Indexer {
    store: Arc<Store>,
    projection: Arc<Projection>,
    model: EmbeddingModel,
    cursors: Mutex<([i64; 3], i64)>,
    pub stop: Arc<AtomicBool>,
}
impl Indexer {
    pub fn new(
        store: Arc<Store>,
        projection: Arc<Projection>,
        model: EmbeddingModel,
        stop: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            projection,
            model,
            cursors: Mutex::new(([0; 3], 0)),
            stop,
        })
    }
    fn prepare_episode(&self) -> Result<bool> {
        let mut cursor = self
            .cursors
            .lock()
            .map_err(|_| Error::Unavailable("Index cursor is unavailable"))?;
        let (through,episode)=self.store.read(|tx|{let through=tx.sql().query_row("SELECT coalesce(max(seq),0) FROM episodes",[],|r|r.get::<_,i64>(0))?;let row=records::one(tx.sql(),"SELECT e.id,e.seq,e.source,e.event_id,e.text FROM episodes e WHERE e.seq>? AND e.seq<=? AND coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip' AND NOT EXISTS(SELECT 1 FROM episode_chunks x WHERE x.episode_id=e.id) AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) ORDER BY e.seq LIMIT 1",params![cursor.1,through])?;Ok((through,row))})?;
        let Some(row) = episode else {
            cursor.1 = through;
            return Ok(false);
        };
        // Full original hydration and tokenization happen outside the writer transaction.
        let text = self
            .store
            .read(|tx| tx.original_text(row["id"].as_str().unwrap()))?
            .ok_or(Error::Unavailable("Index source is unavailable"))?;
        let mut spans = Vec::new();
        for (left, right) in domain::source_text_ranges(&text) {
            let part = CharSpan {
                start: left,
                end: right,
            }
            .slice(&text)?;
            for chunk in self
                .model
                .chunk_text(&part)
                .map_err(|_| Error::Unavailable("Passage tokenization failed"))?
            {
                spans.push((left + chunk.start, left + chunk.end));
            }
        }
        if spans.is_empty() {
            spans.push((0, 0));
        }
        self.store.write(|tx|{let exists:bool=tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM episode_chunks WHERE episode_id=?1) OR EXISTS(SELECT 1 FROM episodes e JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE e.id=?1)",[row["id"].as_str()],|r|r.get(0))?;if !exists{for(start,end)in &spans{let id=format!("e_{}",&raw::sha256(format!("{}:{start}:{end}",row["id"].as_str().unwrap()).as_bytes())[..32]);tx.sql().execute("INSERT INTO episode_chunks VALUES(?,?,?,?)",params![id,row["id"].as_str(),start,end])?;}}Ok(())})?;
        cursor.1 = row["seq"].as_i64().unwrap();
        Ok(true)
    }
    fn pending(&self) -> Result<Option<Value>> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| Error::Unavailable("Index cursor is unavailable"))?;
        self.store.read(|tx|{
            for(source,_)in sources(){if let Some(row)=records::one(tx.sql(),&format!("SELECT a.* FROM ({source}) a JOIN index_failures f ON f.target=a.id AND f.version=a.version WHERE f.resolved=0 AND f.next_attempt<=? AND NOT EXISTS(SELECT 1 FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=a.id AND i.kind=a.kind AND i.version=a.version) ORDER BY f.next_attempt LIMIT 1"),[timestamp()])?{return Ok(Some(row));}}
            for(index,(source,table))in sources().iter().enumerate(){let row=records::one(tx.sql(),&format!("SELECT a.* FROM ({source}) a WHERE a.position>? AND NOT EXISTS(SELECT 1 FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=a.id AND i.kind=a.kind AND i.version=a.version) AND NOT EXISTS(SELECT 1 FROM index_failures f WHERE f.target=a.id AND f.version=a.version AND f.resolved=0) ORDER BY a.position LIMIT 1"),[cursors.0[index]])?;if let Some(row)=row{return Ok(Some(row));}cursors.0[index]=tx.sql().query_row(&format!("SELECT coalesce(max(rowid),0) FROM {table}"),[],|r|r.get(0))?;}
            Ok(None)
        })
    }
    pub fn run_once(&self) -> Result<bool> {
        if self.stop.load(Ordering::Acquire) || !self.projection.ready(Tier::Hot) {
            return Ok(false);
        }
        resolve_due_failures(&self.store)?;
        let prepared = self.prepare_episode()?;
        let Some(mut row) = self.pending()? else {
            return Ok(prepared);
        };
        if row["kind"] == "episode" {
            let original = self
                .store
                .read(|tx| tx.original_text(row["episode_id"].as_str().unwrap()))?
                .ok_or(Error::Unavailable("Index source is unavailable"))?;
            row["text"] = json!(
                CharSpan {
                    start: row["start"].as_u64().unwrap() as usize,
                    end: row["end"].as_u64().unwrap() as usize
                }
                .slice(&original)?
            );
        }
        if self.stop.load(Ordering::Acquire) {
            return Ok(prepared);
        }
        let vector = self
            .model
            .background()
            .encode_document(row["text"].as_str().unwrap(), None)
            .map_err(|_| Error::Unavailable("Native background encoding failed"));
        match vector {
            Ok(vector) => {
                let norm = vector
                    .iter()
                    .map(|v| (*v as f64).powi(2))
                    .sum::<f64>()
                    .sqrt();
                if !vector.iter().all(|v| v.is_finite()) || (norm - 1.).abs() > 0.001 {
                    self.failed(&row)?;
                    return Ok(true);
                }
                let hints = if row["kind"] == "candidate" {
                    self.entity_hints(&row, &vector)?
                } else {
                    Vec::new()
                };
                self.commit(&row, &vector, &hints)?;
            }
            Err(_) => self.failed(&row)?,
        }
        if let Some(index) = match row["kind"].as_str() {
            Some("node") => Some(0),
            Some("candidate") => Some(1),
            Some("episode") => Some(2),
            _ => None,
        } {
            let mut cursor = self
                .cursors
                .lock()
                .map_err(|_| Error::Unavailable("Index cursor is unavailable"))?;
            cursor.0[index] = cursor.0[index].max(row["position"].as_i64().unwrap_or(0));
        }
        Ok(true)
    }
    fn failed(&self, row: &Value) -> Result<()> {
        self.store.write(|tx|{let attempts=tx.sql().query_row("SELECT attempts FROM index_failures WHERE target=? AND version=?",params![row["id"].as_str(),row["version"].as_i64()],|r|r.get::<_,i64>(0)).optional()?.unwrap_or(0)+1;let next=timestamp()+(5.*2f64.powi((attempts-1).min(10)as i32)).min(3600.);tx.sql().execute("INSERT INTO index_failures VALUES(?,?,?,?,?,0) ON CONFLICT(target,version) DO UPDATE SET attempts=excluded.attempts,error=excluded.error,next_attempt=excluded.next_attempt,resolved=0",params![row["id"].as_str(),row["version"].as_i64(),attempts,"NativeEncodingError",next])?;Ok(())})
    }
    fn entity_hints(&self, row: &Value, vector: &[f32; 768]) -> Result<Vec<Value>> {
        let eligible=self.store.read(|tx|Ok(tx.sql().query_row(&format!("SELECT EXISTS(SELECT 1 FROM candidates c WHERE c.id=? AND c.kind='entity' AND {ACTIVE_CANDIDATE})"),[row["id"].as_str()],|r|r.get::<_,bool>(0))?))?;
        if !eligible {
            return Ok(Vec::new());
        }
        let result=self.projection.snapshot_until(true,false,Instant::now()+Duration::from_secs(2),|tx,view|{
            let mut found=Vec::new();for node in domain::recall_keywords(tx.sql(),row["text"].as_str().unwrap(),20,None)?{if node["type"]!=row["type"] {continue;}let bytes:Option<Vec<u8>>=tx.sql().query_row("SELECT b.embedding FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=? AND i.kind='node' AND i.version=? AND b.current=1",params![node["id"].as_str(),node["version"].as_i64()],|r|r.get(0)).optional()?;let cosine=bytes.map(F32Blob::from_bytes).transpose()?.map(|b|b.values().iter().zip(vector).map(|(a,b)|*a as f64* *b as f64).sum::<f64>().clamp(-1.,1.));found.push(json!({"target":node["id"],"version":node["version"],"cosine":cosine,"name_match":1}));}
            for hit in view.search(Tier::Hot,vector,5,"node",Pool::Entity,None,row["type"].as_str())? {if hit.cosine>=0.78&&!found.iter().any(|v|v["target"]==hit.target){found.push(json!({"target":hit.target,"version":hit.version,"cosine":hit.cosine,"name_match":0}));}}
            found.sort_by(|a,b|b["name_match"].as_i64().cmp(&a["name_match"].as_i64()).then_with(||b["cosine"].as_f64().unwrap_or(-1.).total_cmp(&a["cosine"].as_f64().unwrap_or(-1.))).then_with(||a["target"].as_str().cmp(&b["target"].as_str())));found.truncate(5);Ok(found)
        });
        match result {
            Err(Error::Unavailable(_)) => Ok(Vec::new()),
            other => other,
        }
    }
    fn commit(&self, row: &Value, vector: &[f32; 768], hints: &[Value]) -> Result<()> {
        commit_index(&self.store, row, vector, hints)
    }
}
fn commit_index(store: &Store, row: &Value, vector: &[f32; 768], hints: &[Value]) -> Result<()> {
    store.write(|tx|{
            if !still_indexable(tx.sql(),row)? {return Ok(());}
            let exists:bool=tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=? AND i.kind=? AND i.version=?)",params![row["id"].as_str(),row["kind"].as_str(),row["version"].as_i64()],|r|r.get(0))?;
            if !exists {let existing:Option<i64>=tx.sql().query_row("SELECT seq FROM vector_items WHERE target=? AND kind=? AND version=?",params![row["id"].as_str(),row["kind"].as_str(),row["version"].as_i64()],|r|r.get(0)).optional()?;let seq=match existing{Some(seq)=>seq,None=>{tx.sql().execute("INSERT INTO vector_items(target,kind,version) VALUES(?,?,?)",params![row["id"].as_str(),row["kind"].as_str(),row["version"].as_i64()])?;tx.sql().last_insert_rowid()}};
                let tier=if row["kind"]=="episode"{Tier::Cold}else{Tier::Hot};
                tx.put_vector(&VectorWrite{seq,target:row["id"].as_str().unwrap().into(),kind:match row["kind"].as_str(){Some("node")=>VectorKind::Node,Some("candidate")=>VectorKind::Candidate,_=>VectorKind::Episode},version:row["version"].as_i64().unwrap(),embedding:F32Blob::from_values(vector)?,current:true,tier})?;
            }
            if row["kind"]=="candidate" {for hint in hints{let valid:bool=tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM node_versions n WHERE n.id=? AND n.version=? AND n.type=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id))",params![hint["target"].as_str(),hint["version"].as_i64(),row["type"].as_str()],|r|r.get(0))?;if valid{tx.sql().execute("INSERT OR IGNORE INTO candidate_entity_matches VALUES(?,?,?,?,?,?,?,?)",params![row["id"].as_str(),hint["target"].as_str(),hint["version"].as_i64(),hint["cosine"].as_f64(),hint["name_match"].as_i64(),format!("native-indexer/{}@{}",memento_core::MODEL_ID,memento_core::MODEL_REVISION),"Tentative name/vector association only; no identity merge or primary assertion",memento_core::now_utc()])?;}}}
            tx.sql().execute("UPDATE index_failures SET resolved=1 WHERE target=? AND version=?",params![row["id"].as_str(),row["version"].as_i64()])?;Ok(())
        })
}
fn still_indexable(db: &Connection, row: &Value) -> Result<bool> {
    for (source, _) in sources() {
        let eligible=db.query_row(&format!("SELECT EXISTS(SELECT 1 FROM ({source}) a WHERE a.id=? AND a.kind=? AND a.version=? AND (a.text IS NULL OR a.text=?))"),params![row["id"].as_str(),row["kind"].as_str(),row["version"].as_i64(),row["text"].as_str()],|r|r.get::<_,bool>(0))?;
        if eligible {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use memento_domain::Graph;
    fn fixture() -> (tempfile::TempDir, Arc<Store>, Graph, String) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        let source=store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"one","conversation_id":"thread","kind":"user","text":"Original exact source for revision tests","metadata":{}})).unwrap()).unwrap();
        let graph = Graph::new(store.clone());
        (dir, store, graph, source.id)
    }
    #[test]
    fn raw_pool_new_unparked_index_unit_is_cold_without_reembedding_or_parking() {
        let (_dir,store,_graph,episode)=fixture();
        store.write(|tx|{tx.sql().execute("INSERT INTO episode_chunks VALUES('fresh-chunk',?,0,8)",[&episode])?;Ok(())}).unwrap();
        let mut vector=[0.;768];vector[0]=1.;
        commit_index(&store,&json!({"id":"fresh-chunk","kind":"episode","version":1,"text":"Original"}),&vector,&[]).unwrap();
        store.read(|tx|{
            let (tier,bytes):(String,Vec<u8>)=tx.sql().query_row("SELECT b.tier,b.embedding FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target='fresh-chunk'",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
            assert_eq!(tier,"cold");assert_eq!(bytes,F32Blob::from_values(&vector)?.as_bytes());
            assert_eq!(tx.sql().query_row("SELECT count(*) FROM historical_parking",[],|r|r.get::<_,i64>(0))?,0);
            Ok(())
        }).unwrap();
    }
    #[test]
    fn native_index_commit_is_idempotent_and_revalidates_old_version_after_encoding() {
        let (_dir, store, graph, episode) = fixture();
        let saved=graph.remember(serde_json::from_value(json!({"command_id":"save","actor":"owner","why":"Grounded source","type":"knowledge","text":"Original assertion","scope":["personal"],"evidence":[{"episode_id":episode,"quote":"Original exact source"}]})).unwrap()).unwrap();
        let row = json!({"id":saved["id"],"kind":"node","version":1,"text":"Original assertion"});
        let mut vector = [0f32; 768];
        vector[0] = 1.;
        commit_index(&store, &row, &vector, &[]).unwrap();
        commit_index(&store, &row, &vector, &[]).unwrap();
        let bytes = store
            .read(|tx| {
                assert_eq!(
                    tx.sql()
                        .query_row("SELECT count(*) FROM vector_blobs", [], |r| r
                            .get::<_, i64>(0))?,
                    1
                );
                Ok(tx
                    .sql()
                    .query_row("SELECT embedding FROM vector_blobs", [], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })?)
            })
            .unwrap();
        assert_eq!(bytes, F32Blob::from_values(&vector).unwrap().as_bytes());
        graph.supersede(serde_json::from_value(json!({"command_id":"correct","actor":"owner","why":"Exact supported correction","old":saved["id"],"kind":"correction","new":{"type":"knowledge","text":"Corrected assertion","scope":["personal"]},"evidence":[{"episode_id":episode,"quote":"revision tests"}]})).unwrap()).unwrap();
        // A vector encoded before the replacement cannot overwrite/recreate a stale unit.
        let stale = json!({"id":saved["id"],"kind":"node","version":1,"text":"Original assertion"});
        let mut changed = [0f32; 768];
        changed[1] = 1.;
        commit_index(&store, &stale, &changed, &[]).unwrap();
        assert_eq!(
            store
                .read(|tx| Ok(tx.sql().query_row(
                    "SELECT embedding FROM vector_blobs",
                    [],
                    |r| r.get::<_, Vec<u8>>(0)
                )?))
                .unwrap(),
            bytes
        );
    }
    #[test]
    fn forgotten_source_before_index_commit_excludes_candidate_without_creating_a_vector() {
        let (_dir, store, graph, episode) = fixture();
        store.command(&domain::context("extract".into(),"model-fixture".into(),"Synthetic candidate".into()),&json!({"kind":"fixture"}),|tx,commit|{tx.sql().execute("INSERT INTO extraction_runs VALUES('run',?,?,'extract-v2','native','[]','{}',?)",params![commit,episode,memento_core::now_utc()])?;tx.sql().execute("INSERT INTO candidates(id,run_id,kind,type,text,confidence,why,scope) VALUES('candidate','run','memory','knowledge','Tentative source fact',0.8,'Exact synthetic source','[\"personal\"]')",[])?;Ok(json!({}))}).unwrap();
        let row = json!({"id":"candidate","kind":"candidate","version":1,"text":"Tentative source fact","type":"knowledge"});
        let carrier=graph.remember(serde_json::from_value(json!({"command_id":"carrier","actor":"owner","why":"Exact carrier evidence","type":"knowledge","text":"Source carrier assertion","scope":["personal"],"evidence":[{"episode_id":episode,"quote":"Original exact source"}]})).unwrap()).unwrap();
        let preview = graph
            .forget_preview(carrier["id"].as_str().unwrap())
            .unwrap();
        graph.forget(serde_json::from_value(json!({"command_id":"forget","actor":"owner","why":"Explicit synthetic privacy fence","target":carrier["id"],"token":preview["token"]})).unwrap()).unwrap();
        let mut vector = [0f32; 768];
        vector[0] = 1.;
        commit_index(&store, &row, &vector, &[]).unwrap();
        assert_eq!(
            store
                .read(|tx| Ok(tx.sql().query_row(
                    "SELECT count(*) FROM vector_blobs",
                    [],
                    |r| r.get::<_, i64>(0)
                )?))
                .unwrap(),
            0
        );
    }
}

/// Exact current index-unit counts. Reads metadata/covering indexes, not source bodies.
pub fn status(
    db: &Connection,
    source: Option<&str>,
    error: bool,
    hot_ready: bool,
    cold_ready: bool,
) -> Result<Value> {
    // Source membership intentionally accepts evidence from any version. The
    // episode branch is qualified before chunk expansion, while segment source
    // membership belongs to its parent node. DISTINCT keeps unit counts exact.
    let prefix = if source.is_some() {
        "source_targets AS MATERIALIZED (SELECT DISTINCT g.target FROM episodes e INDEXED BY episodes_status_cover_v1 CROSS JOIN graph_evidence g WHERE e.source=?1 AND g.episode_id=e.id)"
    } else {
        "forgotten_episode_ids AS MATERIALIZED (SELECT DISTINCT e.id FROM forgotten_sources f CROSS JOIN episodes e WHERE e.source=f.source AND e.event_id=f.event_id)"
    };
    let membership = |target: &str| {
        if source.is_some() {
            format!("{target} IN (SELECT target FROM source_targets)")
        } else {
            "1".to_owned()
        }
    };
    let (episode_index, episode_filter) = if source.is_some() {
        (
            "episodes_status_cover_v1",
            "e.source=?1 AND CAST(coalesce(json_extract(e.metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip' AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)",
        )
    } else {
        (
            "episodes_status_indexable_v1",
            "?1 IS NULL AND CAST(coalesce(json_extract(e.metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip' AND NOT EXISTS(SELECT 1 FROM forgotten_episode_ids f WHERE f.id=e.id)",
        )
    };
    // All three joined keys are unique. A no-chunk episode contributes only to
    // unprepared_episodes; absent/current=0 embeddings remain pending or failed.
    // No intermediate per-unit state table or repeated episode anti-join is needed.
    let sql = format!(
        "WITH {prefix},indexable AS (SELECT n.id,'node' AS kind,n.version,n.id AS rank_target FROM node_versions n WHERE n.state='active' AND n.type<>'media' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {nodes} UNION ALL SELECT c.id,'candidate',1,c.id FROM candidates c WHERE {ACTIVE_CANDIDATE} AND {candidates} UNION ALL SELECT x.id,'episode',1,x.id FROM episodes e INDEXED BY {episode_index} LEFT JOIN episode_chunks x ON x.episode_id=e.id WHERE {episode_filter} UNION ALL SELECT s.id,'segment',1,n.id FROM segments s JOIN node_versions n ON n.id=s.node_id WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {visible} AND {nodes}) SELECT count(a.id) AS total,coalesce(sum(b.seq IS NOT NULL),0) AS indexed,coalesce(sum(a.id IS NOT NULL AND b.seq IS NULL AND f.target IS NULL),0) AS pending,coalesce(sum(a.id IS NOT NULL AND b.seq IS NULL AND f.target IS NOT NULL),0) AS failed,coalesce(sum(a.id IS NULL),0) AS unprepared_episodes FROM indexable a LEFT JOIN vector_items i ON i.target=a.id AND i.kind=a.kind AND i.version=a.version LEFT JOIN vector_blobs b INDEXED BY vector_blobs_status_current_v1 ON b.seq=i.seq AND b.current=1 LEFT JOIN index_failures f ON f.target=a.id AND f.version=a.version AND f.resolved=0",
        nodes = membership("n.id"),
        candidates = membership("c.id"),
        visible = memento_ann::eligibility::visible_sql("n.id")
    );
    let mut result = records::one(db, &sql, [source])?
        .ok_or(Error::Unavailable("Index counts are unavailable"))?;
    records::merge(
        &mut result,
        json!({"available":true,"error":if error {Some("NativeIndexingError")}else{None},"search":{"engine":"qdrant-edge","ready":hot_ready,"cold_ready":cold_ready,"error":if hot_ready {None}else{Some("not_ready")}}}),
    );
    Ok(result)
}
#[cfg(test)]
mod status_tests {
    use super::*;
    use memento_domain::Graph;
    #[test]
    fn index_status_counts_current_units_and_unprepared_sources_without_body_hydration() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        let graph = Graph::new(store.clone());
        let source=store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"status","conversation_id":"thread","kind":"user","text":"Known source quote","metadata":{}})).unwrap()).unwrap();
        let mut saved = Vec::new();
        for key in ["indexed", "pending"] {
            saved.push(graph.remember(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Exact status fixture","type":"knowledge","text":format!("{key} source assertion"),"scope":["personal"],"evidence":[{"episode_id":source.id,"quote":"Known source quote"}]})).unwrap()).unwrap());
        }
        let mut vector = [0f32; 768];
        vector[0] = 1.;
        commit_index(&store,&json!({"id":saved[0]["id"],"kind":"node","version":1,"text":"indexed source assertion"}),&vector,&[]).unwrap();
        store
            .read(|tx| {
                let actual = status(tx.sql(), Some("synthetic"), false, true, false)?;
                assert_eq!(actual["total"], 2);
                assert_eq!(actual["indexed"], 1);
                assert_eq!(actual["pending"], 1);
                assert_eq!(actual["failed"], 0);
                assert_eq!(actual["unprepared_episodes"], 1);
                assert_eq!(actual["search"]["ready"], true);
                assert_eq!(actual["search"]["cold_ready"], false);
                let other = status(tx.sql(), Some("other-source"), false, true, false)?;
                assert_eq!(other["total"], 0);
                assert_eq!(other["unprepared_episodes"], 0);
                Ok(())
            })
            .unwrap();
    }
}

// The ordinary worker alone resolves stale/already-complete failures. Metadata-only
// vectors remain due until their actual embedding is durably committed.
pub(crate) fn resolve_due_failures(store: &Store) -> Result<()> {
    let due=store.read(|tx|{let mut q=tx.sql().prepare("SELECT target,version FROM index_failures WHERE resolved=0 AND next_attempt<=? ORDER BY next_attempt LIMIT 16")?;Ok(q.query_map([timestamp()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()? )})?;
    if due.is_empty() {
        return Ok(());
    }
    store.write(|tx|{
 for(target,version)in due{let mut eligible=false;for(source,_)in sources(){if tx.sql().query_row(&format!("SELECT EXISTS(SELECT 1 FROM ({source}) a WHERE a.id=? AND a.version=?)"),params![target,version],|r|r.get::<_,bool>(0))?{eligible=true;break;}}
 let complete:bool=tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=? AND i.version=?)",params![target,version],|r|r.get(0))?;if !eligible||complete{tx.sql().execute("UPDATE index_failures SET resolved=1 WHERE target=? AND version=?",params![target,version])?;}}
 Ok(())})
}
