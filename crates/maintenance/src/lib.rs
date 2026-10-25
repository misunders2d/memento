//! Native offline authority migration and archive support. No Python authority fallback.
#[cfg(feature = "legacy-import")]
pub mod legacy;
use memento_core::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut size = 0;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        size += read as u64;
    }
    Ok((hex(&hash.finalize()), size))
}
pub fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
pub fn digest_rows(
    db: &rusqlite::Connection,
    schema: &str,
    table: &str,
    columns: &[String],
) -> Result<(String, u64)> {
    let mut statement = db.prepare(&format!(
        "SELECT rowid,{} FROM {}.{} ORDER BY rowid",
        columns
            .iter()
            .map(|s| quote(s))
            .collect::<Vec<_>>()
            .join(","),
        quote(schema),
        quote(table)
    ))?;
    let count = statement.column_count();
    let mut rows = statement.query([])?;
    let mut hash = Sha256::new();
    let mut n = 0;
    while let Some(row) = rows.next()? {
        n += 1;
        for i in 0..count {
            use rusqlite::types::ValueRef;
            match row.get_ref(i)? {
                ValueRef::Null => hash.update([0]),
                ValueRef::Integer(v) => {
                    hash.update([1]);
                    hash.update(v.to_le_bytes())
                }
                ValueRef::Real(v) => {
                    hash.update([2]);
                    hash.update(v.to_bits().to_le_bytes())
                }
                ValueRef::Text(v) => {
                    hash.update([3]);
                    hash.update((v.len() as u64).to_le_bytes());
                    hash.update(v)
                }
                ValueRef::Blob(v) => {
                    hash.update([4]);
                    hash.update((v.len() as u64).to_le_bytes());
                    hash.update(v)
                }
            }
        }
    }
    Ok((hex(&hash.finalize()), n))
}
pub fn copy_verified(source: &Path, target: &Path, sha: &str, size: u64) -> Result<()> {
    if std::fs::symlink_metadata(source)?.file_type().is_symlink() {
        return Err(Error::Validation("Blob cannot be a symbolic link"));
    }
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(target)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut length = 0;
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        length += read as u64;
        if length > size {
            return Err(Error::Validation(
                "Blob size differs from registered authority",
            ));
        }
        hash.update(&buffer[..read]);
        output.write_all(&buffer[..read])?;
    }
    if length != size || hex(&hash.finalize()) != sha {
        return Err(Error::Validation(
            "Blob checksum differs from registered authority",
        ));
    }
    output.sync_all()?;
    Ok(())
}
