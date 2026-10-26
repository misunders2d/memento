//! Strict uncompressed portable archive v1/v2. A manifest authenticates bytes;
//! the native authority validator authenticates references before publication.
use crate::{
    binding::{self, PublicBinding},
    copy_verified,
    fs_safe::{self, Destination, Stage},
    hash_file,
};
use memento_core::{Error, Result, raw};
use rusqlite::{Connection, OpenFlags};
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::DirBuilderExt,
    path::Path,
};
const MANIFEST_LIMIT: u64 = 64 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileRecord {
    pub sha256: String,
    pub size: u64,
}
#[derive(Serialize)]
pub struct ArchiveReport {
    pub path: String,
    pub format: &'static str,
    pub version: u64,
    pub blob_count: u64,
    pub complete: bool,
    pub included_blob_count: u64,
    pub remote_reference_count: u64,
    pub self_contained: bool,
    pub drive: Option<PublicBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_header: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_header: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebased: Option<Vec<&'static str>>,
}
#[derive(Clone)]
pub(crate) struct Registered {
    pub sha: String,
    pub size: u64,
    pub remote: Option<String>,
}
pub fn sha_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(crate) fn blob_name(sha: &str) -> String {
    format!("blobs/{}/{}", &sha[..2], &sha[2..])
}
fn allowed_name(name: &str) -> bool {
    name == "memory.sqlite"
        || name.is_ascii()
            && name.len() == 71
            && name.starts_with("blobs/")
            && &name[8..9] == "/"
            && sha_valid(&format!("{}{}", &name[6..8], &name[9..]))
}
pub(crate) fn header(db: &Connection) -> Result<i64> {
    let value: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if ![1, 2, 3, 5].contains(&value) {
        return Err(Error::UnsupportedHeader(value));
    }
    Ok(value)
}
pub(crate) fn open_read(path: &Path) -> Result<Connection> {
    fs_safe::regular(path)?;
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.execute_batch("PRAGMA query_only=ON")?;
    Ok(db)
}
pub(crate) fn registered(db: &Connection) -> Result<Vec<Registered>> {
    let mut q = db.prepare("SELECT sha256,size,remote_id FROM blobs ORDER BY sha256")?;
    let rows = q
        .query_map([], |r| {
            Ok(Registered {
                sha: r.get(0)?,
                size: r.get(1)?,
                remote: r.get(2)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for row in &rows {
        if !sha_valid(&row.sha) {
            return Err(Error::Validation("Invalid registered blob checksum"));
        }
        if let Some(remote) = &row.remote {
            binding::object_id(remote)?;
        }
    }
    Ok(rows)
}
pub(crate) fn integrity(db: &Connection) -> Result<()> {
    let mut q = db.prepare("PRAGMA integrity_check")?;
    let values = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if values != ["ok"] {
        return Err(Error::Unavailable(
            "Archive authority integrity check failed",
        ));
    }
    if db
        .prepare("PRAGMA foreign_key_check")?
        .query([])?
        .next()?
        .is_some()
    {
        return Err(Error::Validation(
            "Archive authority has broken foreign keys",
        ));
    }
    Ok(())
}
/// Rebase may change only host transfer state and blob locations. Compare exact
/// typed authority rows around those writes, including user-defined triggers.
pub(crate) fn authority_fingerprint(db: &Connection) -> Result<BTreeMap<String, (String, u64)>> {
    let mut q=db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")?;
    let tables = q
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?.unwrap_or_default(),
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut result = BTreeMap::new();
    for (name, sql) in tables {
        if ["cold_transfers", "cold_failures", "backups"].contains(&name.as_str())
            || [
                "memory_vectors",
                "episodes_fts",
                "node_fts",
                "segment_fts",
                "candidate_fts",
                "alias_fts",
            ]
            .iter()
            .any(|root| name == *root || name.starts_with(&format!("{root}_")))
        {
            continue;
        }
        if sql.to_ascii_uppercase().starts_with("CREATE VIRTUAL TABLE") {
            return Err(Error::Validation(
                "Unsupported archive authority virtual table",
            ));
        }
        let mut q = db.prepare(&format!("PRAGMA table_info({})", crate::quote(&name)))?;
        let mut columns = q
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if name == "blobs" {
            columns.retain(|c| !["tier", "remote_id"].contains(&c.as_str()));
        }
        result.insert(
            name.clone(),
            crate::digest_rows(db, "main", &name, &columns)?,
        );
    }
    Ok(result)
}
/// Checks originals locally when present; explicitly remote originals retain
/// their verified reference and binding without hydrating arbitrary remote TB.
pub(crate) fn validate_native(db: &Connection, data: &Path) -> Result<Vec<Registered>> {
    validate_native_with_gaps(db, data, &BTreeSet::new())
}
pub(crate) fn validate_native_with_gaps(
    db: &Connection,
    data: &Path,
    allowed_missing: &BTreeSet<String>,
) -> Result<Vec<Registered>> {
    if header(db)? != 5 {
        return Err(Error::Validation(
            "Legacy authority must be imported before native validation",
        ));
    }
    integrity(db)?;
    let rows = registered(db)?;
    let map: BTreeMap<_, _> = rows.iter().map(|r| (r.sha.as_str(), r)).collect();
    for row in &rows {
        if row.remote.is_none() && !allowed_missing.contains(&row.sha) {
            fs_safe::regular(&data.join(blob_name(&row.sha)))?;
        }
        if row.remote.is_none()
            && !allowed_missing.contains(&row.sha)
            && hash_file(&data.join(blob_name(&row.sha)))? != (row.sha.clone(), row.size)
        {
            return Err(Error::Unavailable(
                "Registered local original is missing or corrupt",
            ));
        }
    }
    if allowed_missing
        .iter()
        .any(|sha| !map.contains_key(sha.as_str()))
    {
        return Err(Error::Validation(
            "Backup gap references an unregistered original",
        ));
    }
    let mut refs = "SELECT sha256 FROM media_files UNION SELECT artifact_sha256 FROM segments UNION SELECT source_sha256 FROM segments WHERE source_sha256 IS NOT NULL UNION SELECT json_extract(payload,'$.sha256') FROM jobs WHERE kind='media' AND json_extract(payload,'$.sha256') IS NOT NULL".to_owned();
    if db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='media_stage_units')",[],|r|r.get::<_,bool>(0))?{refs.push_str(" UNION SELECT json_extract(u.metadata,'$.artifact_sha256') FROM media_stage_units u JOIN jobs j ON j.id=u.job_id WHERE j.state IN ('pending','leased') AND json_extract(u.metadata,'$.artifact_sha256') IS NOT NULL UNION SELECT json_extract(u.metadata,'$.source_sha256') FROM media_stage_units u JOIN jobs j ON j.id=u.job_id WHERE j.state IN ('pending','leased') AND json_extract(u.metadata,'$.source_sha256') IS NOT NULL");}
    let mut q = db.prepare(&refs)?;
    for sha in q.query_map([], |r| r.get::<_, String>(0))? {
        if !map.contains_key(sha?.as_str()) {
            return Err(Error::Validation(
                "Archive references an unregistered original artifact",
            ));
        }
    }
    raw::validate_frames(db)?;
    let mut q=db.prepare("SELECT id,text_runs,raw_frame,text_sha256 FROM episodes WHERE text_runs IS NOT NULL OR raw_frame IS NOT NULL")?;
    for row in q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
        ))
    })? {
        let (id, runs, frame, sha) = row?;
        if runs.is_some() && frame.is_some() || sha.as_ref().is_none_or(|v| !sha_valid(v)) {
            return Err(Error::Validation("Invalid raw source references"));
        }
        let mut all_local = true;
        if let Some(runs) = runs {
            let refs: Vec<(u64, String, u64)> = serde_json::from_str(&runs)?;
            if refs.is_empty() {
                return Err(Error::Validation("Empty raw source references"));
            }
            let mut end = 0;
            for (offset, sha, size) in refs {
                let row = map.get(sha.as_str()).ok_or(Error::Validation(
                    "Raw source references an unregistered original",
                ))?;
                if offset < end || size < 65536 || row.size != size {
                    return Err(Error::Validation(
                        "Raw source extent differs from registered original",
                    ));
                }
                end = offset
                    .checked_add(size)
                    .ok_or(Error::Validation("Invalid raw source extent"))?;
                if allowed_missing.contains(&sha) {
                    all_local = false;
                    continue;
                }
                if !data.join(blob_name(&sha)).try_exists()? {
                    if row.remote.is_none() {
                        return Err(Error::Unavailable("Raw source original is missing"));
                    }
                    all_local = false;
                }
            }
        }
        if all_local {
            raw::read_original(db, data, &id)?;
        }
    }
    Ok(rows)
}
pub(crate) fn snapshot(data: &Path, target: &Path) -> Result<i64> {
    let source = open_read(&data.join("memory.sqlite"))?;
    let value = header(&source)?;
    drop(fs_safe::create(target)?);
    source.backup("main", target, None)?;
    let db = Connection::open(target)?;
    db.execute_batch("PRAGMA journal_mode=DELETE")?;
    drop(db);
    File::open(target)?.sync_all()?;
    Ok(value)
}
fn normalize_legacy(stage: &Path) -> Result<Option<Stage>> {
    let source = open_read(&stage.join("memory.sqlite"))?;
    let value = header(&source)?;
    drop(source);
    if value == 5 {
        return Ok(None);
    }
    #[cfg(feature = "legacy-import")]
    {
        let converted = Stage::new(stage.parent().unwrap(), "memento-native")?;
        // Existing offline importer publishes its own directory atomically.
        fs::remove_dir(&converted.0)?;
        crate::legacy::import(&stage.join("memory.sqlite"), &converted.0)?;
        Ok(Some(converted))
    }
    #[cfg(not(feature = "legacy-import"))]
    {
        Err(Error::Unavailable(
            "Legacy archive support requires the native legacy-import feature",
        ))
    }
}
fn add(target: &mut File, name: &str, size: u64, source: &mut impl Read) -> Result<()> {
    let mut h = tar::Header::new_gnu();
    h.set_path(name)?;
    h.set_size(size);
    h.set_mode(0o600);
    h.set_uid(0);
    h.set_gid(0);
    h.set_mtime(0);
    h.set_entry_type(tar::EntryType::Regular);
    h.set_cksum();
    target.write_all(h.as_bytes())?;
    let copied = std::io::copy(&mut source.take(size), target)?;
    if copied != size {
        return Err(Error::Unavailable("Archive source changed while packing"));
    }
    target.write_all(&vec![0; ((512 - size % 512) % 512) as usize])?;
    Ok(())
}
pub fn export_archive(data: &Path, output: &Path) -> Result<ArchiveReport> {
    let data = fs_safe::private_dir(data)?;
    let output = fs_safe::plain(output)?;
    let parent = fs_safe::private_dir(
        output
            .parent()
            .ok_or(Error::Validation("Archive needs an output parent"))?,
    )?;
    if fs::symlink_metadata(&output).is_ok() {
        return Err(Error::Conflict("Archive output already exists"));
    }
    let stage = Stage::new(&parent, "memento-export")?;
    let original_header = snapshot(&data, &stage.0.join("memory.sqlite"))?;
    // For old live/offline sources, materialize verified local blobs plus public
    // binding before invoking the existing exact native vector importer.
    if original_header != 5 {
        let db = open_read(&stage.0.join("memory.sqlite"))?;
        let rows = registered(&db)?;
        drop(db);
        for r in &rows {
            let path = data.join(blob_name(&r.sha));
            if path.try_exists()? {
                let dst = stage.0.join(blob_name(&r.sha));
                fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(dst.parent().unwrap())?;
                copy_verified(&path, &dst, &r.sha, r.size)?;
            } else if r.remote.is_none() {
                return Err(Error::Unavailable(
                    "Archive requires every registered local original",
                ));
            }
        }
        if rows.iter().any(|r| r.remote.is_some()) {
            fs_safe::atomic_private(
                &stage.0.join("drive-binding.json"),
                &serde_json::to_vec(&binding::export_binding(&data)?)?,
            )?;
        }
    }
    let converted = normalize_legacy(&stage.0)?;
    let archive_data = converted
        .as_ref()
        .map(|v| v.0.as_path())
        .unwrap_or(&stage.0);
    let db = open_read(&archive_data.join("memory.sqlite"))?;
    // Source payloads for native snapshot still reside in the original data dir.
    let validation_data = if original_header == 5 {
        data.as_path()
    } else {
        archive_data
    };
    let rows = validate_native(&db, validation_data)?;
    drop(db);
    let remote = rows.iter().filter(|r| r.remote.is_some()).count() as u64;
    let drive = if remote > 0 {
        Some(binding::export_binding(&data)?)
    } else {
        None
    };
    let mut files = BTreeMap::new();
    let (sha, size) = hash_file(&archive_data.join("memory.sqlite"))?;
    files.insert(
        "memory.sqlite".to_string(),
        FileRecord { sha256: sha, size },
    );
    for row in &rows {
        if row.remote.is_none() {
            files.insert(
                blob_name(&row.sha),
                FileRecord {
                    sha256: row.sha.clone(),
                    size: row.size,
                },
            );
        }
    }
    let manifest = serde_json::to_vec(
        &json!({"format":"memento-portable","version":2,"files":files,"drive":drive}),
    )?;
    if manifest.len() as u64 > MANIFEST_LIMIT {
        return Err(Error::Validation("Archive manifest exceeds limit"));
    }
    let packed = stage.0.join("packed.tar");
    let mut target = fs_safe::create(&packed)?;
    add(
        &mut target,
        "manifest.json",
        manifest.len() as u64,
        &mut manifest.as_slice(),
    )?;
    for (name, info) in std::iter::once(files.get_key_value("memory.sqlite").unwrap()).chain(
        files
            .iter()
            .filter(|(name, _)| name.as_str() != "memory.sqlite"),
    ) {
        if name == "memory.sqlite" {
            add(
                &mut target,
                name,
                info.size,
                &mut fs_safe::regular(&archive_data.join(name))?,
            )?;
        } else {
            let src = if original_header == 5 {
                data.join(name)
            } else {
                archive_data.join(name)
            };
            let copied = stage.0.join("payload");
            copy_verified(&src, &copied, &info.sha256, info.size)?;
            add(
                &mut target,
                name,
                info.size,
                &mut fs_safe::regular(&copied)?,
            )?;
            fs::remove_file(copied)?;
        }
    }
    target.write_all(&[0; 1024])?;
    target.sync_all()?;
    drop(target);
    fs::hard_link(&packed, &output)?;
    fs_safe::sync_dir(&parent)?;
    Ok(ArchiveReport {
        path: output.display().to_string(),
        format: "memento-portable",
        version: 2,
        blob_count: rows.len() as u64,
        complete: true,
        included_blob_count: files.len() as u64 - 1,
        remote_reference_count: remote,
        self_contained: remote == 0,
        drive,
        source_header: Some(original_header),
        destination_header: None,
        rebased: None,
    })
}
// Recursive duplicate-key rejection: serde_json::Value alone silently keeps the
// final duplicate, including a substituted checksum or binding.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Unique;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("unique JSON object")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Unique, M::Error> {
                let mut result = serde_json::Map::new();
                while let Some((k, Unique(v))) = map.next_entry::<String, Unique>()? {
                    if result.insert(k, v).is_some() {
                        return Err(de::Error::custom("Duplicate archive key"));
                    }
                }
                Ok(Unique(Value::Object(result)))
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> std::result::Result<Unique, S::Error> {
                let mut out = Vec::new();
                while let Some(Unique(v)) = seq.next_element()? {
                    out.push(v)
                }
                Ok(Unique(Value::Array(out)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
        }
        d.deserialize_any(V)
    }
}
fn manifest(bytes: &[u8]) -> Result<(u64, BTreeMap<String, FileRecord>, Option<PublicBinding>)> {
    let Unique(value) = serde_json::from_slice(bytes)?;
    let object = value
        .as_object()
        .ok_or(Error::Validation("Invalid archive manifest"))?;
    let version = value["version"]
        .as_u64()
        .ok_or(Error::Validation("Invalid archive version"))?;
    let expected = if version == 1 {
        vec!["files", "format", "version"]
    } else if version == 2 {
        vec!["drive", "files", "format", "version"]
    } else {
        return Err(Error::Validation("Unsupported archive version"));
    };
    if value["format"] != "memento-portable"
        || object.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != expected.into_iter().collect()
    {
        return Err(Error::Validation("Invalid archive manifest fields"));
    }
    let files: BTreeMap<String, FileRecord> = serde_json::from_value(value["files"].clone())?;
    if !files.contains_key("memory.sqlite") {
        return Err(Error::Validation("Archive has no authority database"));
    }
    for (name, info) in &files {
        if !allowed_name(name)
            || !sha_valid(&info.sha256)
            || name != "memory.sqlite" && *name != blob_name(&info.sha256)
        {
            return Err(Error::Validation("Invalid archive payload declaration"));
        }
    }
    let drive = if version == 2 && !value["drive"].is_null() {
        let v: PublicBinding = serde_json::from_value(value["drive"].clone())?;
        v.validate()?;
        Some(v)
    } else {
        None
    };
    Ok((version, files, drive))
}
fn read_header(file: &mut File) -> Result<Option<(String, u64)>> {
    let mut bytes = [0; 512];
    file.read_exact(&mut bytes)?;
    if bytes.iter().all(|b| *b == 0) {
        let mut zeros = [0; 512];
        file.read_exact(&mut zeros)?;
        if zeros.iter().any(|b| *b != 0) {
            return Err(Error::Validation("Archive has malformed end padding"));
        }
        let mut n = 0;
        let mut tail = [0; 8192];
        loop {
            let count = file.read(&mut tail)?;
            if count == 0 {
                break;
            }
            if tail[..count].iter().any(|b| *b != 0) {
                return Err(Error::Validation("Archive has trailing payload"));
            }
            n += count;
        }
        if n % 512 != 0 {
            return Err(Error::Validation("Archive is not block aligned"));
        }
        return Ok(None);
    }
    let h = tar::Header::from_byte_slice(&bytes);
    let expected = h.cksum()?;
    let calculated: u32 = bytes
        .iter()
        .enumerate()
        .map(|(i, b)| {
            if (148..156).contains(&i) {
                32
            } else {
                u32::from(*b)
            }
        })
        .sum();
    if bytes[156] != b'0'
        || expected != calculated
        || h.entry_type() != tar::EntryType::Regular
        || h.link_name_bytes().is_some_and(|v| !v.is_empty())
    {
        return Err(Error::Validation(
            "Archive member is not a plain verified regular file",
        ));
    }
    // Extension/PAX/sparse/long-name records never reach payload parsing.
    if h.as_gnu().is_none() && h.as_ustar().is_none() && bytes[257..263].iter().any(|b| *b != 0) {
        return Err(Error::Validation("Unsupported archive header"));
    }
    let name = String::from_utf8(h.path_bytes().into_owned())
        .map_err(|_| Error::Validation("Invalid archive member name"))?;
    Ok(Some((name, h.size()?)))
}
fn padding(file: &mut File, size: u64) -> Result<()> {
    let mut bytes = vec![0; ((512 - size % 512) % 512) as usize];
    file.read_exact(&mut bytes)?;
    if bytes.iter().any(|b| *b != 0) {
        return Err(Error::Validation("Nonzero archive member padding"));
    }
    Ok(())
}
fn extract(file: &mut File, target: &Path, info: &FileRecord) -> Result<()> {
    let mut output = fs_safe::create(target)?;
    let mut hash = Sha256::new();
    let mut remaining = info.size;
    let mut buffer = vec![0; 1024 * 1024];
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..count])?;
        output.write_all(&buffer[..count])?;
        hash.update(&buffer[..count]);
        remaining -= count as u64;
    }
    let sha: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if sha != info.sha256 {
        return Err(Error::Validation("Archive payload checksum mismatch"));
    }
    output.sync_all()?;
    padding(file, info.size)
}
pub fn import_archive(archive: &Path, destination: &Path) -> Result<ArchiveReport> {
    let destination = fs_safe::plain(destination)?;
    let parent = fs_safe::private_dir(
        destination
            .parent()
            .ok_or(Error::Validation("Restore needs a destination parent"))?,
    )?;
    let identity = Destination::inspect(&destination)?;
    let mut input = fs_safe::regular(archive)?;
    let Some((name, size)) = read_header(&mut input)? else {
        return Err(Error::Validation("Archive is empty"));
    };
    if name != "manifest.json" || size > MANIFEST_LIMIT {
        return Err(Error::Validation(
            "Archive must begin with a bounded manifest",
        ));
    }
    let mut bytes = vec![0; size as usize];
    input.read_exact(&mut bytes)?;
    padding(&mut input, size)?;
    let (version, files, drive) = manifest(&bytes)?;
    let stage = Stage::new(&parent, "memento-import")?;
    let mut seen = BTreeSet::new();
    while let Some((name, size)) = read_header(&mut input)? {
        let info = files
            .get(&name)
            .ok_or(Error::Validation("Archive contains an undeclared payload"))?;
        if !seen.insert(name.clone()) || size != info.size {
            return Err(Error::Validation("Duplicate or mismatched archive payload"));
        }
        let target = stage.0.join(&name);
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(target.parent().unwrap())?;
        extract(&mut input, &target, info)?;
    }
    if seen != files.keys().cloned().collect() {
        return Err(Error::Validation("Archive is missing declared payloads"));
    }
    let db = Connection::open(stage.0.join("memory.sqlite"))?;
    let source_header = header(&db)?;
    // Validate exact declarations using ordinary authority tables even before
    // loading legacy vec0. No unknown header is changed or partially published.
    let rows = registered(&db)?;
    let mut expected = BTreeSet::from(["memory.sqlite".to_owned()]);
    let mut remote = 0;
    for row in &rows {
        if version == 1 || row.remote.is_none() {
            let name = blob_name(&row.sha);
            if files.get(&name)
                != Some(&FileRecord {
                    sha256: row.sha.clone(),
                    size: row.size,
                })
            {
                return Err(Error::Validation(
                    "Archive omits or misstates a registered local original",
                ));
            }
            expected.insert(name);
        } else {
            remote += 1;
        }
    }
    if expected != seen || version == 2 && (remote > 0) != drive.is_some() {
        return Err(Error::Validation(
            "Archive registration or Drive binding differs from its declared payloads",
        ));
    }
    if let Some(drive) = &drive {
        fs_safe::atomic_private(
            &stage.0.join("drive-binding.json"),
            &serde_json::to_vec(drive)?,
        )?;
    }
    let original_authority = authority_fingerprint(&db)?;
    db.execute_batch("BEGIN IMMEDIATE")?;
    if version == 1 {
        db.execute_batch("UPDATE blobs SET tier='hot',remote_id=NULL")?;
    } else {
        db.execute_batch(
            "UPDATE blobs SET tier=CASE WHEN remote_id IS NULL THEN 'hot' ELSE 'cold' END",
        )?;
    }
    db.execute_batch(
        "DELETE FROM cold_transfers; DELETE FROM cold_failures; DELETE FROM backups;",
    )?;
    let has_index:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='index' AND name='episodes_activity')",[],|r|r.get(0))?;
    if has_index {
        db.execute_batch("REINDEX episodes_activity")?;
    }
    db.execute_batch("COMMIT; PRAGMA journal_mode=DELETE;")?;
    if original_authority != authority_fingerprint(&db)? {
        return Err(Error::Validation(
            "Archive authority changed outside permitted restore rebasing",
        ));
    }
    drop(db);
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(stage.0.join("blobs"))?;
    let converted = normalize_legacy(&stage.0)?;
    let publish = converted
        .as_ref()
        .map(|v| v.0.as_path())
        .unwrap_or(&stage.0);
    let db = open_read(&publish.join("memory.sqlite"))?;
    validate_native(&db, publish)?;
    drop(db);
    fs_safe::sync_tree(publish)?;
    identity.publish(publish, &destination)?;
    Ok(ArchiveReport {
        path: destination.join("memory.sqlite").display().to_string(),
        format: "memento-portable",
        version,
        blob_count: rows.len() as u64,
        complete: true,
        included_blob_count: expected.len() as u64 - 1,
        remote_reference_count: remote,
        self_contained: remote == 0,
        drive,
        source_header: Some(source_header),
        destination_header: Some(5),
        rebased: Some(vec![
            "blobs.tier",
            "cold_transfers",
            "cold_failures",
            "backups",
        ]),
    })
}
