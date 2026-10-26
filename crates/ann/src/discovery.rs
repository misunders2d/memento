//! Existing consolidation's bounded semantic hints, outside its authority writer.
use crate::{Pool, Projection};
use memento_core::{Error, F32Blob, Result, Tier, raw};
use memento_domain::consolidation::DiscoveryProvider;
use rusqlite::OptionalExtension;
use serde_json::{Map, Value, json};
use std::sync::Arc;
pub struct Discovery {
    projection: Arc<Projection>,
}
impl Discovery {
    pub fn new(projection: Arc<Projection>) -> Arc<Self> {
        Arc::new(Self { projection })
    }
}
impl DiscoveryProvider for Discovery {
    fn discover(&self, candidate_ids: &[String]) -> Result<Value> {
        let result=self.projection.snapshot(true,false,|tx,view|{
            let mut candidates=Map::new();
            for id in candidate_ids{
                let row=tx.sql().query_row("SELECT c.kind,c.type,c.text,c.scope,b.embedding FROM candidates c JOIN vector_items i ON i.kind='candidate' AND i.target=c.id AND i.version=1 JOIN vector_blobs b ON b.seq=i.seq AND b.current=1 WHERE c.id=?",[id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,Vec<u8>>(4)?))).optional()?;
                let Some((kind,memory_type,text,scope,bytes))=row else{continue};
                let source=json!([kind,memory_type,text,serde_json::from_str::<Value>(&scope)?]);let fingerprint=raw::sha256(&bytes);let vector:[f32;768]=F32Blob::from_bytes(bytes)?.values().try_into().map_err(|_|Error::Unavailable("Stored vector has an invalid shape"))?;
                let typed=view.search(Tier::Hot,&vector,2,"node",Pool::Entity,None,Some(&memory_type))?.into_iter().map(|p|json!({"target":p.target,"version":p.version})).collect::<Vec<_>>();
                let broad=view.search(Tier::Hot,&vector,8,"node",Pool::Ordinary,None,None)?.into_iter().map(|p|json!({"target":p.target,"version":p.version})).collect::<Vec<_>>();
                candidates.insert(id.clone(),json!({"source":source,"vector":fingerprint,"typed":typed,"broad":broad}));
            }Ok(json!({"candidates":candidates,"pending":false}))
        });
        match result {
            Err(Error::Unavailable(_)) => Ok(json!({"candidates":{},"pending":true})),
            other => other,
        }
    }
}
