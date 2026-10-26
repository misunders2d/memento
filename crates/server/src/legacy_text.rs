//! Selective compatibility for legacy text encodings. Authority/history never changes.
use memento_core::{
    CharSpan, Error, F32Blob, ModelSignature, Result, Store, Tier, VectorKind, VectorWrite,
};
use memento_embedding::EmbeddingModel;
use rusqlite::{OptionalExtension, params};
use std::sync::atomic::{AtomicBool, Ordering};
// Literal spellings from the pinned source tokenizer_config.json SHA256 17bd5d6e9364ca49a534e1502076593317c298d4a663623091ed45388f004874.
// Mirrors named + extra + model-specific special tokens; this is model vocabulary, not query heuristics.
const CONTROL_TOKENS: &[&str] = &[
    "<bos>",
    "<eos>",
    "<unk>",
    "<pad>",
    "<mask>",
    "<|video|>",
    "<|audio|>",
    "<|audio>",
    "<|image>",
    "<audio|>",
    "<channel|>",
    "<image|>",
    "<turn|>",
    "<|\"|>",
    "<tool_call|>",
    "<tool|>",
    "<tool_response|>",
    "<|image|>",
    "<|channel>",
    "<|turn>",
    "<|tool_call>",
    "<|tool>",
    "<|tool_response>",
    "<|think|>",
];
struct Unit {
    seq: i64,
    target: String,
    kind: String,
    version: i64,
    bytes: Vec<u8>,
    text: Option<String>,
    episode: Option<String>,
    start: Option<usize>,
    end: Option<usize>,
}
fn signature(store: &Store) -> Result<Option<ModelSignature>> {
    store.read(|tx|Ok(tx.sql().query_row("SELECT model_id,revision,dimensions,text_encoding FROM vector_model WHERE singleton=1",[],|r|Ok(ModelSignature{model_id:r.get(0)?,revision:r.get(1)?,dimensions:r.get(2)?,text_encoding:r.get(3)?})).optional()?))
}
fn check(stop: &AtomicBool) -> Result<()> {
    if stop.load(Ordering::Acquire) {
        Err(Error::Unavailable("Literal text upgrade stopped"))
    } else {
        Ok(())
    }
}
fn contains(text: &str) -> bool {
    CONTROL_TOKENS.iter().any(|token| text.contains(token))
}
pub fn upgrade(store: &Store, model: &EmbeddingModel, stop: &AtomicBool) -> Result<usize> {
    upgrade_with(store, stop, |text| {
        model
            .background()
            .encode_document(text, None)
            .map_err(|_| Error::Unavailable("Literal text upgrade encoding failed"))
    })
}
fn upgrade_with(
    store: &Store,
    stop: &AtomicBool,
    mut encode: impl FnMut(&str) -> Result<[f32; 768]>,
) -> Result<usize> {
    let Some(stored) = signature(store)? else {
        return Ok(0);
    };
    let pinned = ModelSignature::pinned();
    if stored == pinned {
        return Ok(0);
    }
    if stored.model_id != pinned.model_id
        || stored.revision != pinned.revision
        || stored.dimensions != pinned.dimensions
        || stored.text_encoding != 0
    {
        return Err(Error::Unavailable(
            "Stored vectors use a different model signature",
        ));
    }
    let mut after = None::<i64>;
    let mut changed = 0;
    loop {
        check(stop)?;
        let units=store.read(|tx|{let mut query=tx.sql().prepare("SELECT i.seq,i.target,i.kind,i.version,b.embedding,CASE i.kind WHEN 'node' THEN n.text WHEN 'candidate' THEN c.text WHEN 'segment' THEN s.text END,x.episode_id,x.start,x.end FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq LEFT JOIN node_versions n ON i.kind='node' AND n.id=i.target AND n.version=i.version LEFT JOIN candidates c ON i.kind='candidate' AND c.id=i.target LEFT JOIN segments s ON i.kind='segment' AND s.id=i.target LEFT JOIN episode_chunks x ON i.kind='episode' AND x.id=i.target WHERE (?1 IS NULL OR i.seq>?1) AND (i.kind<>'segment' OR s.modality='text') ORDER BY i.seq LIMIT 16")?;Ok(query.query_map([after],|r|Ok(Unit{seq:r.get(0)?,target:r.get(1)?,kind:r.get(2)?,version:r.get(3)?,bytes:r.get(4)?,text:r.get(5)?,episode:r.get(6)?,start:r.get(7)?,end:r.get(8)?}))?.collect::<std::result::Result<Vec<_>,_>>()?)})?;
        if units.is_empty() {
            break;
        }
        for unit in units {
            check(stop)?;
            after = Some(unit.seq);
            let text = if unit.kind == "episode" {
                match (unit.episode.as_deref(), unit.start, unit.end) {
                    (Some(id), Some(start), Some(end)) => {
                        let original = store
                            .read(|tx| tx.original_text(id))?
                            .ok_or(Error::Unavailable("Literal text source is unavailable"))?;
                        Some(CharSpan { start, end }.slice(&original)?)
                    }
                    _ => None,
                }
            } else {
                unit.text.clone()
            };
            let Some(text) = text else { continue };
            if !contains(&text) {
                continue;
            }
            let vector = encode(&text)?;
            let norm = vector
                .iter()
                .map(|v| (*v as f64).powi(2))
                .sum::<f64>()
                .sqrt();
            if !vector.iter().all(|v| v.is_finite()) || (norm - 1.).abs() > 0.001 {
                return Err(Error::Unavailable(
                    "Literal text upgrade produced an invalid vector",
                ));
            }
            store.write(|tx|{let current=tx.sql().query_row("SELECT i.target,i.kind,i.version,b.embedding,b.current,b.tier FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.seq=?",[unit.seq],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,bool>(4)?,r.get::<_,String>(5)?))).optional()?;
    let Some((target,kind,version,bytes,current,tier))=current else{return Err(Error::Conflict("Literal text vector changed during upgrade"))};if target!=unit.target||kind!=unit.kind||version!=unit.version||bytes!=unit.bytes{return Err(Error::Conflict("Literal text vector changed during upgrade"));}
    // Latest eligibility/tier may have changed during inference; never revive it.
    tx.put_vector(&VectorWrite{seq:unit.seq,target,kind:match kind.as_str(){"node"=>VectorKind::Node,"candidate"=>VectorKind::Candidate,"episode"=>VectorKind::Episode,"segment"=>VectorKind::Segment,_=>return Err(Error::Unavailable("Stored vector kind is unsupported"))},version,embedding:F32Blob::from_values(&vector)?,current,tier:match tier.as_str(){"hot"=>Tier::Hot,"cold"=>Tier::Cold,_=>return Err(Error::Unavailable("Stored vector tier is unsupported"))}})?;Ok(())})?;
            changed += 1;
        }
    }
    check(stop)?;
    // Retry only the previously failed literal-control inputs. Keep attempts/error
    // rows and unresolved state; the ordinary worker still owns resolution.
    let mut failed_after = None::<(String, i64)>;
    loop {
        let failures=store.read(|tx|{let mut q=tx.sql().prepare("SELECT target,version FROM index_failures WHERE resolved=0 AND (?1 IS NULL OR (target,version)>(?1,?2)) ORDER BY target,version LIMIT 16")?;Ok(q.query_map(params![failed_after.as_ref().map(|x|x.0.as_str()),failed_after.as_ref().map(|x|x.1)],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?)})?;
        if failures.is_empty() {
            break;
        }
        let mut due = Vec::new();
        for (target, version) in failures {
            check(stop)?;
            failed_after = Some((target.clone(), version));
            let found = store.read(|tx| {
                for (source, _) in crate::indexing::sources() {
                    if let Some(row) = crate::records::one(
                        tx.sql(),
                        &format!(
                            "SELECT a.* FROM ({source}) a WHERE a.id=? AND a.version=? LIMIT 1"
                        ),
                        params![target, version],
                    )? {
                        return Ok(Some(row));
                    }
                }
                Ok(None)
            })?;
            if let Some(row) = found {
                let text = if row["kind"] == "episode" {
                    let id = row["episode_id"]
                        .as_str()
                        .ok_or(Error::Unavailable("Literal failure source is unavailable"))?;
                    let original = store
                        .read(|tx| tx.original_text(id))?
                        .ok_or(Error::Unavailable("Literal failure source is unavailable"))?;
                    CharSpan {
                        start: row["start"].as_u64().unwrap() as usize,
                        end: row["end"].as_u64().unwrap() as usize,
                    }
                    .slice(&original)?
                } else {
                    row["text"].as_str().unwrap_or("").to_owned()
                };
                if contains(&text) {
                    due.push((target, version));
                }
            }
        }
        store.write(|tx|{for(target,version)in due{tx.sql().execute("UPDATE index_failures SET next_attempt=0 WHERE target=? AND version=? AND resolved=0",params![target,version])?;}Ok(())})?;
    }
    // The marker stays0 through all bounded units. A crash/rejection remains fail-closed;
    // replay may encode an affected unit again, never serve a half-upgraded signature.
    store.write(|tx| {
        let stored = tx.sql().query_row(
            "SELECT model_id,revision,dimensions,text_encoding FROM vector_model WHERE singleton=1",
            [],
            |r| {
                Ok(ModelSignature {
                    model_id: r.get(0)?,
                    revision: r.get(1)?,
                    dimensions: r.get(2)?,
                    text_encoding: r.get(3)?,
                })
            },
        )?;
        if stored.text_encoding != 0
            || stored.model_id != pinned.model_id
            || stored.revision != pinned.revision
            || stored.dimensions != pinned.dimensions
        {
            return Err(Error::Conflict(
                "Vector model signature changed during upgrade",
            ));
        }
        tx.sql().execute(
            "UPDATE vector_model SET text_encoding=1 WHERE singleton=1",
            [],
        )?;
        Ok(())
    })?;
    Ok(changed)
}
#[cfg(test)]
mod tests {
    use super::*;
    use memento_core::raw;
    use std::sync::Arc;
    fn fixture() -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        store
            .write(|tx| {
                tx.sql().execute(
                    "INSERT INTO vector_model VALUES(1,?,?,768,0)",
                    params![memento_core::MODEL_ID, memento_core::MODEL_REVISION],
                )?;
                Ok(())
            })
            .unwrap();
        (dir, store)
    }
    fn add(store: &Store, id: &str, seq: i64, text: &str, current: bool) {
        store.command(&memento_domain::context(id.into(),"owner".into(),"Literal synthetic source".into()),&serde_json::json!({"fixture":id}),|tx,commit|{tx.sql().execute("INSERT INTO node_versions(id,version,type,text,state,pinned,scope,commit_id) VALUES(?,1,'knowledge',?,'active',0,'[\"personal\"]',?)",params![id,text,commit])?;let mut vector=[0f32;768];vector[0]=1.;tx.put_vector(&VectorWrite{seq,target:id.into(),kind:VectorKind::Node,version:1,embedding:F32Blob::from_values(&vector)?,current,tier:Tier::Hot})?;Ok(serde_json::json!({}))}).unwrap();
    }
    #[test]
    fn selective_upgrade_keeps_ids_flags_unaffected_bytes_and_all_authority() {
        let (_dir, store) = fixture();
        add(&store, "plain", -1, "Ordinary literal text", true);
        add(&store, "control", 0, "User wrote <bos> literally", false);
        let before = store
            .read(|tx| {
                Ok((
                    tx.sql().query_row(
                        "SELECT embedding FROM vector_blobs WHERE seq=-1",
                        [],
                        |r| r.get::<_, Vec<u8>>(0),
                    )?,
                    tx.sql()
                        .query_row("SELECT count(*) FROM graph_commits", [], |r| {
                            r.get::<_, i64>(0)
                        })?,
                ))
            })
            .unwrap();
        store.write(|tx|{tx.sql().execute("INSERT INTO index_failures VALUES('control',1,3,'LegacyTextEncode',9999999999,0)",[])?;Ok(())}).unwrap();
        let mut calls = 0;
        assert_eq!(
            upgrade_with(&store, &AtomicBool::new(false), |text| {
                assert!(text.contains("<bos>"));
                calls += 1;
                let mut v = [0f32; 768];
                v[1] = 1.;
                Ok(v)
            })
            .unwrap(),
            1
        );
        assert_eq!(calls, 1);
        store
            .read(|tx| {
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT embedding FROM vector_blobs WHERE seq=-1",
                        [],
                        |r| r.get::<_, Vec<u8>>(0)
                    )?,
                    before.0
                );
                assert!(!tx.sql().query_row(
                    "SELECT current FROM vector_blobs WHERE seq=0",
                    [],
                    |r| r.get::<_, bool>(0)
                )?);
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT text FROM node_versions WHERE id='control'",
                        [],
                        |r| r.get::<_, String>(0)
                    )?,
                    "User wrote <bos> literally"
                );
                assert_eq!(
                    tx.sql()
                        .query_row("SELECT count(*) FROM graph_commits", [], |r| r
                            .get::<_, i64>(0))?,
                    before.1
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(signature(&store).unwrap().unwrap().text_encoding, 1);
        store.read(|tx|{let row:(i64,f64,bool)=tx.sql().query_row("SELECT attempts,next_attempt,resolved FROM index_failures WHERE target='control'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;assert_eq!(row,(3,0.0,false));Ok(())}).unwrap();
        crate::indexing::resolve_due_failures(&store).unwrap();
        assert!(
            store
                .read(|tx| Ok(tx.sql().query_row(
                    "SELECT resolved FROM index_failures WHERE target='control'",
                    [],
                    |r| r.get::<_, bool>(0)
                )?))
                .unwrap()
        );
        assert_eq!(
            upgrade_with(&store, &AtomicBool::new(false), |_| panic!(
                "already upgraded"
            ))
            .unwrap(),
            0
        );
    }
    #[test]
    fn interrupted_or_failed_upgrade_does_not_publish_model_marker_and_replay_finishes() {
        let (_dir, store) = fixture();
        add(&store, "one", 1, "Literal <bos>", true);
        add(&store, "two", 2, "Literal <eos>", true);
        let mut calls = 0;
        assert!(
            upgrade_with(&store, &AtomicBool::new(false), |_| {
                calls += 1;
                if calls == 2 {
                    return Err(Error::Unavailable("Synthetic encoder failure"));
                }
                let mut v = [0f32; 768];
                v[1] = 1.;
                Ok(v)
            })
            .is_err()
        );
        assert_eq!(signature(&store).unwrap().unwrap().text_encoding, 0);
        assert_eq!(
            upgrade_with(&store, &AtomicBool::new(false), |_| {
                let mut v = [0f32; 768];
                v[1] = 1.;
                Ok(v)
            })
            .unwrap(),
            2
        );
        assert_eq!(signature(&store).unwrap().unwrap().text_encoding, 1);
        let _ = raw::sha256(b"synthetic control selection");
    }
    #[test]
    fn legacy_upgrade_uses_codepoint_passages_and_leaves_nontext_media_vectors_exact() {
        let (_dir, store) = fixture();
        let text = "🧭 Literal <bos> boundary remains";
        let episode=store.capture(serde_json::from_value(serde_json::json!({"source":"synthetic","event_id":"literal-kinds","conversation_id":"literal-kinds","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap();
        store.command(&memento_domain::context("literal-kinds".into(),"owner".into(),"Exact legacy kind fixture".into()),&serde_json::json!({"fixture":"literal-kinds"}),|tx,commit|{
            tx.sql().execute("INSERT INTO extraction_runs VALUES('literal-run',?,?,'fixture','fixture','[]','{}','2020-01-01T00:00:00Z')",params![commit,episode.id])?;
            tx.sql().execute(r#"INSERT INTO candidates(id,run_id,kind,type,text,why,confidence,scope) VALUES('literal-candidate','literal-run','memory','knowledge','Candidate <eos> statement','Exact fixture',1,'["personal"]')"#,[])?;
            for(id,modality,text)in[("literal-text","text","Document <mask> note"),("literal-image","image","Image depicts <bos> marker")]{
                tx.sql().execute("INSERT INTO segments(id,node_id,modality,text,artifact_sha256) VALUES(?,'literal-media',?,?,?)",params![id,modality,text,"0".repeat(64)])?;
            }
            tx.sql().execute("INSERT INTO episode_chunks VALUES('literal-chunk',?,2,?)",params![episode.id,text.chars().count() as i64])?;
            let mut vector=[0f32;768];vector[0]=1.;
            for(seq,target,kind,current,tier)in [(10,"literal-candidate",VectorKind::Candidate,true,Tier::Hot),(11,"literal-text",VectorKind::Segment,true,Tier::Hot),(12,"literal-image",VectorKind::Segment,true,Tier::Hot),(13,"literal-chunk",VectorKind::Episode,false,Tier::Cold)] {
                tx.put_vector(&VectorWrite{seq,target:target.into(),kind,version:1,embedding:F32Blob::from_values(&vector)?,current,tier})?;
            }
            Ok(serde_json::json!({}))
        }).unwrap();
        let before = store
            .read(|tx| {
                Ok(tx.sql().query_row(
                    "SELECT embedding FROM vector_blobs WHERE seq=12",
                    [],
                    |r| r.get::<_, Vec<u8>>(0),
                )?)
            })
            .unwrap();
        let mut calls = Vec::new();
        assert_eq!(
            upgrade_with(&store, &AtomicBool::new(false), |text| {
                calls.push(text.to_owned());
                let mut v = [0f32; 768];
                v[1] = 1.;
                Ok(v)
            })
            .unwrap(),
            3
        );
        assert_eq!(
            calls,
            [
                "Candidate <eos> statement",
                "Document <mask> note",
                "Literal <bos> boundary remains"
            ]
        );
        store
            .read(|tx| {
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT embedding FROM vector_blobs WHERE seq=12",
                        [],
                        |r| r.get::<_, Vec<u8>>(0)
                    )?,
                    before
                );
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT current,tier FROM vector_blobs WHERE seq=13",
                        [],
                        |r| Ok((r.get::<_, bool>(0)?, r.get::<_, String>(1)?))
                    )?,
                    (false, "cold".into())
                );
                assert_eq!(tx.original_text(&episode.id)?.as_deref(), Some(text));
                assert_eq!(
                    tx.sql()
                        .query_row("SELECT count(*) FROM graph_commits", [], |r| r
                            .get::<_, i64>(0))?,
                    1
                );
                Ok(())
            })
            .unwrap();
    }
}
