//! Lossless physical raw-frame packing. Reads/encoding precede the short writer transaction.
use crate::{Error, Result, Store, raw};
use flate2::{Compression, write::ZlibEncoder};
use rusqlite::{MAIN_DB, params};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
const FRAME: usize = 262144;
struct Row {
    seq: i64,
    source: String,
    conversation: String,
    checksum: Option<String>,
    size: usize,
}
struct Frame {
    offset: u64,
    length: usize,
    size: usize,
    count: usize,
    sha: String,
}
struct Record {
    seq: i64,
    refs: Vec<[usize; 3]>,
    sha: String,
}
fn flush(
    file: &mut std::fs::File,
    payload: &mut Vec<u8>,
    count: &mut usize,
    frames: &mut Vec<Frame>,
) -> Result<()> {
    if payload.is_empty() && *count == 0 {
        return Ok(());
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(payload)?;
    let packed = encoder.finish()?;
    let offset = file.stream_position()?;
    file.write_all(&packed)?;
    frames.push(Frame {
        offset,
        length: packed.len(),
        size: payload.len(),
        count: *count,
        sha: raw::sha256(payload),
    });
    payload.clear();
    *count = 0;
    Ok(())
}
impl Store {
    /// One64-row scan / one conversation /256KiB frame group. A single legacy large
    /// record may span disk-backed frames; no allocation follows its declared size.
    pub fn compress_raw_step(&self) -> Result<bool> {
        let mut cursor = self
            .raw_cursor
            .lock()
            .map_err(|_| Error::Unavailable("Raw compression cursor is unavailable"))?;
        let mut packed = tempfile::tempfile_in(self.data_dir())?;
        let mut frames = Vec::new();
        let mut records = Vec::new();
        let (mut through, mut origin) = (*cursor, None::<(String, String)>);
        self.read(|tx|{
   let mut selected=Vec::new();let mut total=0usize;let mut query=tx.sql().prepare("SELECT seq,source,conversation_id,text_sha256,CASE WHEN kind IN ('native_record','native_live_record','native_hook') THEN text_runs IS NULL AND raw_frame IS NULL AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')='skip' ELSE 0 END,length(CAST(text AS BLOB)) FROM episodes WHERE seq>? ORDER BY seq LIMIT 64")?;
   let mut rows=query.query([*cursor])?;while let Some(row)=rows.next()?{let seq=row.get::<_,i64>(0)?;if row.get::<_,bool>(4)?{let source=row.get::<_,String>(1)?;let conversation=row.get::<_,String>(2)?;let current=(source.clone(),conversation.clone());if origin.as_ref().is_some_and(|o|*o!=current){break;}let size=row.get::<_,usize>(5)?;if !selected.is_empty()&&total.saturating_add(size)>FRAME{break;}origin=Some(current);selected.push(Row{seq,source,conversation,checksum:row.get(3)?,size});total=total.saturating_add(size);}through=seq;if total>=FRAME{break;}}
   drop(rows);drop(query);
   let mut payload=Vec::with_capacity(FRAME);let mut count=0;
   for row in selected {
    if payload.len()+row.size>FRAME&&(!payload.is_empty()||count>0){flush(&mut packed,&mut payload,&mut count,&mut frames)?;}
    let mut source=tx.sql().blob_open(MAIN_DB,"episodes","text",row.seq,true)?;if source.len()!=row.size{return Err(Error::Unavailable("Raw source storage changed during compression"));}
    let mut remaining=row.size;let mut first=true;let mut refs=Vec::new();let mut digest=Sha256::new();
    while remaining>0||first{let length=remaining.min(FRAME-payload.len());let start=payload.len();payload.resize(start+length,0);source.read_exact(&mut payload[start..])?;refs.push([frames.len(),start,length]);count+=usize::from(first);digest.update(&payload[start..]);remaining-=length;first=false;if payload.len()==FRAME{flush(&mut packed,&mut payload,&mut count,&mut frames)?;}}
    let checksum:String=digest.finalize().iter().map(|b|format!("{b:02x}")).collect();if row.checksum.as_ref().is_some_and(|old|*old!=checksum){return Err(Error::Unavailable("Raw source checksum differs during compression"));}
    if origin.as_ref()!=Some(&(row.source,row.conversation)){return Err(Error::Unavailable("Raw frame conversation differs"));}
    records.push(Record{seq:row.seq,refs,sha:checksum});
   }
   flush(&mut packed,&mut payload,&mut count,&mut frames)?;Ok(())
  })?;
        if records.is_empty() {
            let advanced = through != *cursor;
            *cursor = through;
            return Ok(advanced);
        }
        let (source, conversation) =
            origin.ok_or(Error::Unavailable("Raw compression source is unavailable"))?;
        self.write(|tx|{let mut identities=Vec::new();for frame in &frames{packed.seek(SeekFrom::Start(frame.offset))?;let mut data=vec![0;frame.length];packed.read_exact(&mut data)?;tx.sql().execute("INSERT INTO raw_frames(source,conversation_id,raw_size,record_count,sha256,data) VALUES(?,?,?,?,?,?)",params![source,conversation,frame.size,frame.count,frame.sha,data])?;identities.push(tx.sql().last_insert_rowid());}
   for record in &records {let refs=record.refs.iter().map(|[index,offset,length]|[identities[*index],*offset as i64,*length as i64]).collect::<Vec<_>>();let refs=serde_json::to_string(&refs)?;tx.authorize_frame(record.seq,&refs,&record.sha)?;let changed=tx.sql().execute("UPDATE episodes SET text='',raw_frame=?,text_sha256=? WHERE seq=? AND raw_frame IS NULL AND text_runs IS NULL",params![refs,record.sha,record.seq])?;if changed!=1{return Err(Error::Conflict("Raw source storage changed during compression"));}}
   Ok(())
  })?;
        *cursor = through;
        Ok(true)
    }
}
