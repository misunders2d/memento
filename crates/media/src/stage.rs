//! Derived progress has no visible graph node until atomic final publication.
use crate::{Error, Result};
use memento_core::{
    F32Blob, Store, Tier, VectorKind, VectorWrite, blobs::BlobInfo, canonical_json,
};
use memento_domain::{TxGraph, context, jobs::Job};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub modality: String,
    pub text: String,
    pub artifact_sha256: String,
    pub start_s: Option<f64>,
    pub end_s: Option<f64>,
    pub page: Option<usize>,
    pub char_start: Option<usize>,
    pub char_end: Option<usize>,
    pub source_sha256: Option<String>,
}
pub struct Unit {
    pub metadata: Metadata,
    pub vector: [f32; 768],
    pub artifacts: Vec<(BlobInfo, String)>,
}
pub(crate) fn string<'a>(value: &'a Value, key: &str) -> memento_core::Result<&'a str> {
    value[key].as_str().ok_or(memento_core::Error::Validation(
        "Media job field is missing",
    ))
}
pub(crate) fn active_source(db: &rusqlite::Connection, sha: &str) -> memento_core::Result<Value> {
    db.query_row("SELECT r.episode_id,r.quote FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id WHERE r.sha256=? AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) AND NOT EXISTS(SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=r.sha256) ORDER BY e.seq DESC LIMIT 1",[sha],|r|Ok(json!({"episode_id":r.get::<_,String>(0)?,"quote":r.get::<_,String>(1)?,"occurrence":0}))).optional()?.ok_or(memento_core::Error::Conflict("Captured media no longer has an active source"))
}
pub(crate) fn check_source(db: &rusqlite::Connection, job: &Job) -> memento_core::Result<()> {
    if job.payload["captured"] == true {
        active_source(db, string(&job.payload, "sha256")?)?;
    }
    Ok(())
}
pub(crate) fn representation(
    store: &Store,
    kind: &str,
    filename: &str,
) -> memento_core::Result<String> {
    store.read(|tx| representation_db(tx.sql(), kind, filename))
}
pub(crate) fn representation_db(
    db: &rusqlite::Connection,
    kind: &str,
    filename: &str,
) -> memento_core::Result<String> {
    let model=db.query_row("SELECT model_id,revision,dimensions,text_encoding FROM vector_model WHERE singleton=1",[],|r|Ok(json!({"model_id":r.get::<_,String>(0)?,"revision":r.get::<_,String>(1)?,"dimensions":r.get::<_,i64>(2)?,"text_encoding":r.get::<_,i64>(3)?})))?;
    if model["model_id"] != memento_embedding::MODEL_ID
        || model["revision"] != memento_embedding::MODEL_REVISION
        || model["dimensions"] != 768
        || model["text_encoding"] != 1
    {
        return Err(memento_core::Error::Validation(
            "Media model signature differs",
        ));
    }
    let suffix = if kind == "document" {
        std::path::Path::new(filename)
            .extension()
            .and_then(|x| x.to_str())
            .map(|x| format!(".{}", x.to_lowercase()))
            .unwrap_or_default()
    } else {
        String::new()
    };
    canonical_json(
        &json!({"decoder":2,"implementation":"native-pillow12.3-ffmpeg9","kind":kind,"suffix":suffix,"model":model}),
    )
}
pub(crate) fn request(job: &Job) -> memento_core::Result<Value> {
    let p = &job.payload;
    let captured = p["captured"] == true;
    let mut value = json!({"op":"media_ingest","sha256":string(p,"sha256")?,"kind":string(p,"kind")?,"filename":string(p,"filename")?,"evidence":if captured{json!([])}else{p["evidence"].clone()},"scope":if captured{json!([])}else{p["scope"].clone()}});
    if captured {
        value["captured"] = json!(true);
    }
    Ok(value)
}
pub(crate) fn previous(store: &Store, job: &Job) -> memento_core::Result<Option<Value>> {
    let request = request(job)?;
    let encoded = canonical_json(
        &json!({"actor":string(&job.payload,"actor")?,"why":string(&job.payload,"why")?,"command":request}),
    )?;
    store.read(|tx| {
        let row = tx
            .sql()
            .query_row(
                "SELECT request,result FROM graph_commits WHERE command_id=?",
                [format!("job/{}", job.id)],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        row.map(|(prior, result)| {
            if prior != encoded {
                return Err(memento_core::Error::Conflict(
                    "Command identifier already contains a different command",
                ));
            }
            Ok(serde_json::from_str(&result)?)
        })
        .transpose()
    })
}
pub(crate) fn cursor(store: &Store, job: &Job, identity: &str) -> memento_core::Result<usize> {
    store.read(|tx|{job.check_in(tx.sql())?;check_source(tx.sql(),job)?;
  let(count,last,min_identity,max_identity):(i64,Option<i64>,Option<String>,Option<String>)=tx.sql().query_row("SELECT count(*),max(ordinal),min(identity),max(identity) FROM media_stage_units WHERE job_id=?",[&job.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
  if last!=if count==0{None}else{Some(count-1)}||min_identity.as_deref().is_some_and(|s|s!=identity)||max_identity.as_deref().is_some_and(|s|s!=identity){return Err(memento_core::Error::Conflict("Media stage identity or cursor differs"));}
  let context:Option<String>=tx.sql().query_row("SELECT context FROM jobs WHERE id=?",[&job.id],|r|r.get(0))?;
  if let Some(text)=context{let value:Value=serde_json::from_str(&text)?;if value["version"]!=1||value["identity"]!=identity||value["cursor"].as_i64()!=Some(count){return Err(memento_core::Error::Conflict("Media stage context differs"));}}
  else if count!=0{return Err(memento_core::Error::Conflict("Media stage context is missing"));}
  Ok(count as usize)
 })
}
pub(crate) fn persist(
    store: &Store,
    job: &Job,
    identity: &str,
    ordinal: usize,
    unit: Unit,
) -> Result<()> {
    // Validate without a second normalization: exact encoder F32 bytes survive.
    memento_embedding::validation::cosine_error(&unit.vector, &unit.vector)?;
    let meta = serde_json::to_string(&unit.metadata)?;
    let bytes: Vec<u8> = unit.vector.iter().flat_map(|x| x.to_le_bytes()).collect();
    store.write(|tx|{job.check_in(tx.sql())?;check_source(tx.sql(),job)?;
  if representation_db(tx.sql(),string(&job.payload,"kind")?,string(&job.payload,"filename")?)?!=identity{return Err(memento_core::Error::Conflict("Media model changed during encoding"));}
  let count:i64=tx.sql().query_row("SELECT count(*) FROM media_stage_units WHERE job_id=?",[&job.id],|r|r.get(0))?;
  if count!=ordinal as i64{return Err(memento_core::Error::Conflict("Media stage changed during encoding"));}
  for(info,mime)in &unit.artifacts{tx.register_blob(info,mime)?;}
  tx.sql().execute("INSERT INTO media_stage_units(job_id,ordinal,identity,metadata,embedding) VALUES (?,?,?,?,?)",params![job.id,ordinal as i64,identity,meta,bytes])?;
  tx.sql().execute("UPDATE jobs SET context=? WHERE id=?",params![canonical_json(&json!({"version":1,"identity":identity,"cursor":ordinal+1}))?,job.id])?;Ok(())
 })?;
    Ok(())
}
pub(crate) fn artifact_refs(store: &Store, job: &Job) -> memento_core::Result<Vec<String>> {
    store.read(|tx|{job.check_in(tx.sql())?;let mut stmt=tx.sql().prepare("SELECT json_extract(metadata,'$.artifact_sha256') AS sha FROM media_stage_units WHERE job_id=?1 UNION SELECT json_extract(metadata,'$.source_sha256') FROM media_stage_units WHERE job_id=?1")?;Ok(stmt.query_map([&job.id],|r|r.get::<_,Option<String>>(0))?.collect::<std::result::Result<Vec<_>,_>>()?.into_iter().flatten().collect())})
}
pub(crate) fn finish(store: &Arc<Store>, job: &Job, identity: &str, count: usize) -> Result<Value> {
    if count == 0 {
        return Err(Error::Decode(
            "No indexable media units; original artifact remains stored",
        ));
    }
    let ctx = context(
        format!("job/{}", job.id),
        string(&job.payload, "actor")?.into(),
        string(&job.payload, "why")?.into(),
    );
    let request = request(job)?;
    Ok(store.command(&ctx,&request,|tx,commit|{
  job.check_in(tx.sql())?;check_source(tx.sql(),job)?;
  if representation_db(tx.sql(),string(&job.payload,"kind")?,string(&job.payload,"filename")?)?!=identity{return Err(memento_core::Error::Conflict("Media model changed before completion"));}
  let sources=if job.payload["captured"]==true{json!([active_source(tx.sql(),string(&job.payload,"sha256")?)?])}else{job.payload["evidence"].clone()};
  let node=TxGraph::insert_node(tx,commit,&json!({"type":"media","text":string(&job.payload,"filename")?,"evidence":sources,"scope":request["scope"]}))?;let node_id=string(&node,"id")?;
  tx.sql().execute("INSERT INTO media_files(node_id,sha256,filename,kind,commit_id) VALUES (?,?,?,?,?)",params![node_id,string(&job.payload,"sha256")?,string(&job.payload,"filename")?,string(&job.payload,"kind")?,commit])?;
  let mut segments=Vec::new();
  {
   let mut stmt=tx.sql().prepare("SELECT ordinal,identity,metadata,embedding FROM media_stage_units WHERE job_id=? ORDER BY ordinal")?;let mut rows=stmt.query([&job.id])?;
   while let Some(row)=rows.next()?{
    let ordinal:usize=row.get(0)?;let stored_identity:String=row.get(1)?;if ordinal!=segments.len()||stored_identity!=identity{return Err(memento_core::Error::Conflict("Media stage changed before completion"));}
    let meta:Metadata=serde_json::from_str(&row.get::<_,String>(2)?)?;let embedding=F32Blob::from_bytes(row.get(3)?)?;
    let id=format!("{:x}",Sha256::digest(format!("media/{}/{ordinal}",job.id).as_bytes()))[..16].to_owned();
    tx.sql().execute("INSERT INTO segments(id,node_id,modality,text,artifact_sha256,start_s,end_s,page,char_start,char_end,source_sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?)",params![id,node_id,meta.modality,meta.text,meta.artifact_sha256,meta.start_s,meta.end_s,meta.page.map(|x|x as i64),meta.char_start.map(|x|x as i64),meta.char_end.map(|x|x as i64),meta.source_sha256])?;
    tx.sql().execute("INSERT INTO vector_items(target,kind,version) VALUES (?,'segment',1)",[&id])?;let seq=tx.sql().last_insert_rowid();
    tx.put_vector(&VectorWrite{seq,target:id.clone(),kind:VectorKind::Segment,version:1,current:true,tier:Tier::Hot,embedding})?;
    segments.push(json!({"id":id,"start_s":meta.start_s,"end_s":meta.end_s,"page":meta.page,"char_start":meta.char_start,"char_end":meta.char_end,"source_text_url":meta.source_sha256.as_ref().map(|s|format!("/blobs/{s}"))}));
   }
  }
  if segments.len()!=count{return Err(memento_core::Error::Conflict("Media stage count differs before completion"));}
  tx.sql().execute("INSERT INTO media_representations(node_id,identity) VALUES (?,?)",params![node_id,identity])?;
  if job.payload["captured"]==true{tx.sql().execute("UPDATE captured_media SET node_id=?,state='indexed',error=NULL WHERE sha256=?",params![node_id,string(&job.payload,"sha256")?])?;}
  tx.sql().execute("DELETE FROM media_stage_units WHERE job_id=?",[&job.id])?;tx.sql().execute("UPDATE jobs SET context=NULL WHERE id=?",[&job.id])?;
  Ok(json!({"id":node_id,"blob_url":format!("/blobs/{}",string(&job.payload,"sha256")?),"segments":segments}))
 })?)
}
