//! Optional statically linked vec0 compatibility is confined to this offline importer.
//! It never registers an automatic extension or reads undocumented vec0 shadow layouts.
use crate::{copy_verified, digest_rows, hash_file, quote};
use memento_core::{Error, Result, Store, raw};
use rusqlite::{Connection, OpenFlags, ffi, params};
use serde::Serialize;
use std::{
    ffi::{c_char, c_int},
    fs::File,
    os::unix::fs::DirBuilderExt,
    path::Path,
};
#[derive(Debug, Serialize)]
pub struct ImportReport {
    pub source_header: i64,
    pub source_sha256: String,
    pub tables: u64,
    pub authority_rows: u64,
    pub vectors: u64,
    pub local_blobs: u64,
    pub remote_blobs: u64,
    pub destination_header: i64,
}
fn init(db: &Connection) -> Result<()> {
    // Exact sqlite-vec0.1.9 shipped C ABI, SQLITE_CORE -> this bundled SQLite.
    type Init = unsafe extern "C" fn(
        *mut ffi::sqlite3,
        *mut *mut c_char,
        *const ffi::sqlite3_api_routines,
    ) -> c_int;
    let initialize: Init =
        unsafe { std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ()) };
    let mut message = std::ptr::null_mut();
    let code = unsafe { initialize(db.handle(), &mut message, std::ptr::null()) };
    if !message.is_null() {
        unsafe { ffi::sqlite3_free(message.cast()) }
    }
    if code != ffi::SQLITE_OK {
        return Err(Error::Unavailable(
            "Legacy vector reader failed to initialize",
        ));
    }
    let version: String = db.query_row("SELECT vec_version()", [], |r| r.get(0))?;
    if version != "v0.1.9" {
        return Err(Error::Unavailable("Legacy vector reader version differs"));
    }
    Ok(())
}
fn columns(db: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
    let mut q = db.prepare(&format!(
        "PRAGMA {}.table_info({})",
        quote(schema),
        quote(table)
    ))?;
    Ok(q.query_map([], |r| r.get(1))?
        .collect::<std::result::Result<_, _>>()?)
}
fn plain_file(path: &Path) -> Result<()> {
    let info = std::fs::symlink_metadata(path)?;
    if !info.is_file() || info.file_type().is_symlink() {
        return Err(Error::Validation(
            "Source must be a regular, nonlinked SQLite file",
        ));
    }
    Ok(())
}
// Only explicitly offline, no-WAL sources reach this URI. Never use it on live authority.
fn source_uri(source: &Path) -> String {
    let mut uri = String::from("file:");
    for byte in source.as_os_str().as_encoded_bytes() {
        if byte.is_ascii_alphanumeric() || b"/-_.~".contains(byte) {
            uri.push(*byte as char)
        } else {
            uri.push_str(&format!("%{byte:02X}"))
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    uri
}
fn source_guard(source: &Path) -> Result<(Connection, i64)> {
    plain_file(source)?;
    let mut wal = source.as_os_str().to_owned();
    wal.push("-wal");
    if Path::new(&wal).exists() && std::fs::metadata(&wal)?.len() != 0 {
        return Err(Error::Validation(
            "Legacy import requires an explicitly checkpointed offline source copy",
        ));
    }
    let db = Connection::open_with_flags(
        source_uri(source),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let header: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if ![1, 2, 3].contains(&header) {
        return Err(Error::UnsupportedHeader(header));
    }
    db.execute_batch("PRAGMA query_only=ON; BEGIN;")?;
    init(&db)?;
    Ok((db, header))
}
fn blobs(source_dir: &Path, stage: &Path, db: &Connection) -> Result<(u64, u64)> {
    let mut query = db.prepare("SELECT sha256,size,remote_id FROM blobs ORDER BY sha256")?;
    let mut local = 0;
    let mut remote = 0;
    for row in query.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, u64>(1)?,
            r.get::<_, Option<String>>(2)?,
        ))
    })? {
        let (sha, size, remote_id) = row?;
        if sha.len() != 64
            || !sha
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::Validation("Invalid registered blob checksum"));
        }
        let input = source_dir.join("blobs").join(&sha[..2]).join(&sha[2..]);
        if !input.exists() {
            if remote_id.is_some() {
                remote += 1;
                continue;
            } else {
                return Err(Error::Unavailable(
                    "Complete source import requires all locally registered blob artifacts",
                ));
            }
        }
        let parent = stage.join("blobs").join(&sha[..2]);
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&parent)?;
        copy_verified(&input, &parent.join(&sha[2..]), &sha, size)?;
        File::open(parent)?.sync_all()?;
        local += 1;
    }
    if remote > 0 {
        let binding = source_dir.join("drive-binding.json");
        if binding.exists() {
            plain_file(&binding)?;
            let (sha, size) = hash_file(&binding)?;
            copy_verified(&binding, &stage.join("drive-binding.json"), &sha, size)?;
        } else {
            return Err(Error::Unavailable(
                "Remote blob references require the original Drive binding",
            ));
        }
    }
    Ok((local, remote))
}
pub fn import(source: &Path, destination: &Path) -> Result<ImportReport> {
    plain_file(source)?;
    let source = source.canonicalize()?;
    let lease_path = source.parent().unwrap().join("writer.lock");
    let _source_lease = if lease_path.exists() {
        plain_file(&lease_path)?;
        let lease = File::open(lease_path)?;
        lease.try_lock_shared().map_err(|_| {
            Error::Conflict(
                "Legacy source has an active writer; import a checkpointed offline copy",
            )
        })?;
        Some(lease)
    } else {
        None
    };
    let (source_db, header) = source_guard(&source)?; // Unknown headers fail BEFORE any destination mutation.
    let (source_hash, _) = hash_file(&source)?;
    if destination.exists() {
        return Err(Error::Conflict("Import destination already exists"));
    }
    let parent = destination
        .parent()
        .ok_or(Error::Validation("Import needs a destination parent"))?
        .canonicalize()?;
    let final_path = parent.join(
        destination
            .file_name()
            .ok_or(Error::Validation("Invalid import destination"))?,
    );
    if final_path == source.parent().unwrap() {
        return Err(Error::Validation("Import cannot overwrite source storage"));
    }
    let stage = parent.join(format!(".memento-legacy-{}", uuid::Uuid::new_v4().simple()));
    std::fs::DirBuilder::new().mode(0o700).create(&stage)?;
    let result = (|| {
        let (local_blobs, remote_blobs) = blobs(source.parent().unwrap(), &stage, &source_db)?;
        let store = Store::open(&stage)?;
        let (tables,authority_rows,vectors)=store.write(|tx|{
            init(tx.sql())?;
            // SQLite encodes this as a read-only ATTACH, independently of source permissions.
            let uri=source_uri(&source);
            tx.sql().execute("ATTACH DATABASE ? AS legacy",[uri])?;
            if unsafe{ffi::sqlite3_db_readonly(tx.sql().handle(),c"legacy".as_ptr())}!=1{return Err(Error::Unavailable("Legacy source attachment is not read-only"))}
            let triggers={let mut q=tx.sql().prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger'")?;q.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?};
            for (name,_) in &triggers{tx.sql().execute_batch(&format!("DROP TRIGGER {}",quote(name)))?;}
            let source_tables={let mut q=tx.sql().prepare("PRAGMA legacy.table_list")?;q.query_map([],|r|Ok((r.get::<_,String>(1)?,r.get::<_,String>(2)?)))?.collect::<std::result::Result<Vec<_>,_>>()?};
            let mut copied=Vec::new();let mut table_count=0;let mut row_count=0;
            for (table,kind) in source_tables{
                if kind=="shadow"||table.starts_with("sqlite_")||["memory_vectors", "memory_vectors_vector_chunks00","search_state","search_outbox","episodes_fts"].contains(&table.as_str()){continue}
                if !["table","virtual"].contains(&kind.as_str()){continue}
                let from=columns(tx.sql(),"legacy",&table)?;let to=columns(tx.sql(),"main",&table)?;
                if to.is_empty()||from.iter().any(|c|!to.contains(c)){return Err(Error::Validation("Legacy authority table or column is unsupported; no source data was discarded"))}
                let names=from.iter().map(|s|quote(s)).collect::<Vec<_>>().join(",");
                tx.sql().execute_batch(&format!("DELETE FROM main.{table}; INSERT INTO main.{table}(rowid,{names}) SELECT rowid,{names} FROM legacy.{table} ORDER BY rowid",table=quote(&table)))?;
                let original=digest_rows(tx.sql(),"legacy",&table,&from)?;let imported=digest_rows(tx.sql(),"main",&table,&from)?;if original!=imported{return Err(Error::Unavailable("Imported authority differs from its original source"))}table_count+=1;row_count+=original.1;copied.push(table);
            }
            if !copied.iter().any(|t|t=="vector_items"){return Err(Error::Validation("Legacy vector authority is missing"))}
            // FTS5's external-content table enumerates ALL episodes, even rows never indexed.
            // Rebuild its derived postings with the exact capture predicate, not SELECT *.
            tx.sql().execute_batch("INSERT INTO episodes_fts(episodes_fts) VALUES('delete-all'); INSERT INTO episodes_fts(rowid,text) SELECT seq,text FROM episodes WHERE kind NOT IN ('native_record','native_live_record','native_hook') AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')<>'skip';")?;
            let mut vector_count=0;
            {let mut query=tx.sql().prepare("SELECT v.rowid,v.embedding,v.current,i.kind,i.target FROM legacy.memory_vectors v LEFT JOIN legacy.vector_items i ON i.seq=v.rowid ORDER BY v.rowid")?;let mut rows=query.query([])?;
                while let Some(row)=rows.next()?{let seq:i64=row.get(0)?;let bytes:Vec<u8>=row.get(1)?;if bytes.len()!=3072{return Err(Error::Validation("Legacy FP32 vector has the wrong byte length"))}let current:i64=row.get(2)?;let kind:Option<String>=row.get(3)?;let target:Option<String>=row.get(4)?;
                    let historical=if kind.as_deref()==Some("episode"){tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM episode_chunks x JOIN episodes e ON e.id=x.episode_id JOIN historical_parking h ON h.source=e.source AND h.conversation_id=e.conversation_id WHERE x.id=? AND e.seq<=h.parked_through)",[target],|r|r.get::<_,bool>(0))?}else if kind.as_deref()==Some("candidate"){tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM historical_parked_candidates WHERE candidate_id=? AND released_at IS NULL)",[target],|r|r.get(0))?}else{kind.is_none()};
                    tx.sql().execute("INSERT INTO vector_blobs VALUES(?,?,?,0,?)",params![seq,bytes,current,if historical{"cold"}else{"hot"}])?;vector_count+=1;
                }
            }
            let difference:bool=tx.sql().query_row("SELECT EXISTS(SELECT v.rowid FROM legacy.memory_vectors v LEFT JOIN vector_blobs b ON b.seq=v.rowid WHERE b.seq IS NULL OR b.embedding IS NOT v.embedding OR b.current IS NOT v.current) OR (SELECT count(*) FROM legacy.memory_vectors)<>(SELECT count(*) FROM vector_blobs)",[],|r|r.get(0))?;if difference{return Err(Error::Unavailable("Legacy vector IDs or bytes changed during import"))}
            for (_,sql) in &triggers{tx.sql().execute_batch(sql)?;}
            Ok((table_count,row_count,vector_count))
        })?;
        store.read(|tx|{let integrity:String=tx.sql().query_row("PRAGMA integrity_check",[],|r|r.get(0))?;if integrity!="ok"{return Err(Error::Unavailable("Imported authority integrity check failed"))}
            let refs="SELECT sha256 FROM media_files UNION SELECT artifact_sha256 FROM segments UNION SELECT source_sha256 FROM segments WHERE source_sha256 IS NOT NULL";if tx.sql().query_row(&format!("SELECT EXISTS(SELECT 1 FROM ({refs}) r WHERE NOT EXISTS(SELECT 1 FROM blobs b WHERE b.sha256=r.sha256))"),[],|r|r.get::<_,bool>(0))?{return Err(Error::Validation("Imported authority references unregistered artifacts"))}
            raw::validate_frames(tx.sql())?;
            let mut q=tx.sql().prepare("SELECT id FROM episodes WHERE raw_frame IS NOT NULL OR text_runs IS NOT NULL")?;
            for id in q.query_map([],|r|r.get::<_,String>(0))?{raw::read_original(tx.sql(),&stage,&id?)?;}
            Ok(())})?;
        drop(store);
        drop(source_db);
        if hash_file(&source)?.0 != source_hash {
            return Err(Error::Conflict(
                "Source SQLite changed while the offline import was running",
            ));
        }
        for name in ["memory.sqlite", "writer.lock"] {
            File::open(stage.join(name))?.sync_all()?;
        }
        File::open(&stage)?.sync_all()?;
        if final_path.exists() {
            return Err(Error::Conflict(
                "Import destination changed during validation",
            ));
        }
        std::fs::rename(&stage, &final_path)?;
        File::open(&parent)?.sync_all()?;
        Ok(ImportReport {
            source_header: header,
            source_sha256: source_hash,
            tables,
            authority_rows,
            vectors,
            local_blobs,
            remote_blobs,
            destination_header: 5,
        })
    })();
    if result.is_err() && stage.exists() {
        std::fs::remove_dir_all(stage)?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::ZlibEncoder};
    use memento_core::{F32Blob, Tier, VectorKind, VectorWrite};
    use serde_json::json;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    fn fixture(header: i64) -> tempfile::TempDir {
        let dir = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = Store::open(dir.path()).unwrap();
        let text = "a🧠б source evidence";
        let artifact = b"original tiny artifact";
        let sha = raw::sha256(artifact);
        let parent = dir.path().join("blobs").join(&sha[..2]);
        std::fs::create_dir_all(&parent).unwrap();
        std::fs::write(parent.join(&sha[2..]), artifact).unwrap();
        store.write(|tx|{
            tx.sql().execute("INSERT INTO graph_commits VALUES('commit','command','{\"actor\": \"Сергей\"}','{}','owner','source reason','2026-01-01T00:00:00+00:00')",[])?;
            tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES(1,'episode','fixture','event','thread','user',?,'2026-01-01T00:00:00+00:00','{}','2026-01-02T00:00:00+00:00')",[text])?;
            for version in [1,2]{tx.sql().execute("INSERT INTO node_versions VALUES('node',?,'commit','knowledge',?,'active',NULL,NULL,NULL,0,'[\"personal\"]')",params![version,format!("assertion{version}")])?;tx.sql().execute("INSERT INTO graph_evidence VALUES('node',?,'episode',1,3,'🧠б')",[version])?;}
            tx.sql().execute("INSERT INTO loop_events VALUES('loop','node','commit','open',NULL,NULL)",[])?;
            tx.sql().execute("INSERT INTO blobs VALUES(?,?,'text/plain','hot',NULL,'2026')",params![sha,artifact.len()])?;
            tx.sql().execute("INSERT INTO media_files VALUES('node',?,'fixture.txt','document','commit')",[&sha])?;
            if header>=2{let original="compressed raw Ω😀";let mut encoder=ZlibEncoder::new(Vec::new(),Compression::default());encoder.write_all(original.as_bytes())?;let compressed=encoder.finish()?;let hash=raw::sha256(original.as_bytes());tx.sql().execute("INSERT INTO raw_frames VALUES(7,'fixture','thread',?,1,?,?)",params![original.len(),hash,compressed])?;tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at,text_sha256,raw_frame) VALUES(2,'frame','fixture','native','thread','native_record','','{\"extraction_mode\":\"skip\"}','2026',?,?)",params![hash,json!([[7,0,original.len()]]).to_string()])?;}
            tx.sql().execute("INSERT INTO vector_model VALUES(1,'google/embeddinggemma-2','914f7f89142e33e77833254d9c9b90c3cef7303b',768,1)",[])?;
            for (seq,current) in [(i64::MIN,false),(-1,true),(0,true),(i64::MAX,true)]{let mut values=[0.0;768];values[0]=1.;values[1]=-0.;values[2]=f32::from_bits(1);tx.put_vector(&VectorWrite{seq,target:if seq==0{"node".into()}else{format!("target{seq}")},kind:VectorKind::Node,version:1,current,tier:Tier::Hot,embedding:F32Blob::from_values(&values)?})?;}
            tx.sql().execute("INSERT INTO vector_items VALUES(8,'metadata-only','candidate',1)",[])?;Ok(())
        }).unwrap();
        drop(store);
        let db = Connection::open(dir.path().join("memory.sqlite")).unwrap();
        init(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE memory_vectors USING vec0(embedding float[768] distance_metric=cosine,current boolean); INSERT INTO memory_vectors(rowid,embedding,current) SELECT seq,embedding,current FROM vector_blobs; DROP TABLE vector_blobs; DROP TABLE projection_events; DROP TABLE projection_ack; DROP TABLE runtime_state;").unwrap();
        let mut vector = vec![0_u8; 3072];
        vector[..4].copy_from_slice(&1f32.to_le_bytes());
        db.execute(
            "INSERT INTO memory_vectors(rowid,embedding,current) VALUES(99,?,1)",
            [vector],
        )
        .unwrap();
        if header == 1 {
            db.execute_batch("DROP TRIGGER episodes_no_update; DROP INDEX episodes_status_cover_v1; ALTER TABLE episodes DROP COLUMN text_runs; ALTER TABLE episodes DROP COLUMN text_sha256; ALTER TABLE episodes DROP COLUMN raw_frame; DROP TABLE raw_frames;").unwrap();
        }
        db.pragma_update(None, "user_version", header).unwrap();
        drop(db);
        dir
    }
    #[test]
    fn full_legacy_authority_and_vectors_import_losslessly_without_source_mutation() {
        for header in [1, 2, 3] {
            let source = fixture(header);
            let output = tempfile::tempdir().unwrap();
            let destination = output.path().join("imported");
            let original = hash_file(&source.path().join("memory.sqlite")).unwrap();
            let report = import(&source.path().join("memory.sqlite"), &destination).unwrap();
            assert_eq!(report.source_header, header);
            assert_eq!(report.vectors, 5);
            assert_eq!(report.local_blobs, 1);
            assert_eq!(
                hash_file(&source.path().join("memory.sqlite")).unwrap(),
                original
            );
            let store = Store::open(&destination).unwrap();
            store
                .read(|tx| {
                    assert_eq!(
                        tx.original_text("episode")?.unwrap(),
                        "a🧠б source evidence"
                    );
                    if header >= 2 {
                        assert_eq!(tx.original_text("frame")?.unwrap(), "compressed raw Ω😀")
                    };
                    assert_eq!(
                        tx.sql()
                            .query_row("SELECT count(*) FROM node_versions", [], |r| r
                                .get::<_, i64>(0))?,
                        2
                    );
                    assert_eq!(
                        tx.sql().query_row(
                            "SELECT quote FROM graph_evidence WHERE version=1",
                            [],
                            |r| r.get::<_, String>(0)
                        )?,
                        "🧠б"
                    );
                    assert_eq!(
                        tx.sql()
                            .query_row("SELECT target FROM loop_events", [], |r| r
                                .get::<_, String>(0))?,
                        "node"
                    );
                    assert_eq!(
                        tx.sql().query_row(
                            "SELECT count(*) FROM vector_items WHERE seq=8",
                            [],
                            |r| r.get::<_, i64>(0)
                        )?,
                        1
                    );
                    assert_eq!(
                        tx.sql().query_row(
                            "SELECT count(*) FROM vector_blobs WHERE seq=99",
                            [],
                            |r| r.get::<_, i64>(0)
                        )?,
                        1
                    );
                    assert!(
                        tx.sql()
                            .query_row("SELECT vec_version()", [], |r| r.get::<_, String>(0))
                            .is_err()
                    );
                    assert!(tx.sql().execute("DELETE FROM graph_evidence", []).is_err());
                    assert_eq!(
                        tx.sql().query_row(
                            "SELECT count(*) FROM episodes_fts WHERE episodes_fts MATCH 'evidence'",
                            [],
                            |r| r.get::<_, i64>(0)
                        )?,
                        1
                    );
                    Ok(())
                })
                .unwrap();
        }
    }
    #[test]
    fn unknown_header_and_missing_blob_fail_before_publication() {
        let source = fixture(3);
        let dbpath = source.path().join("memory.sqlite");
        let output = tempfile::tempdir().unwrap();
        let destination = output.path().join("imported");
        {
            let db = Connection::open(&dbpath).unwrap();
            db.pragma_update(None, "user_version", 4).unwrap();
        }
        let before = hash_file(&dbpath).unwrap();
        assert!(matches!(
            import(&dbpath, &destination),
            Err(Error::UnsupportedHeader(4))
        ));
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(output.path()).unwrap().count(), 0);
        assert_eq!(hash_file(&dbpath).unwrap(), before);
        {
            let db = Connection::open(&dbpath).unwrap();
            db.pragma_update(None, "user_version", 3).unwrap();
        }
        std::fs::remove_dir_all(source.path().join("blobs")).unwrap();
        assert!(import(&dbpath, &destination).is_err());
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(output.path()).unwrap().count(), 0);
    }
}
