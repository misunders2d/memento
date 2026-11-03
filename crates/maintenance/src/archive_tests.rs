use crate::{binding::PublicBinding, fs_safe, hash_file, portable::*};
use memento_core::{Store, raw};
use rusqlite::Connection;
use serde_json::json;
use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn fixture(root: &Path) -> std::sync::Arc<Store> {
    fs::create_dir(root).unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    let store = Store::open(root).unwrap();
    store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"e","conversation_id":"c","kind":"user","text":"Exact Ω😀\r\nsource\u{0000}history"})).unwrap()).unwrap();
    let info = store.blobs().put(&mut &b"original\0\xff"[..], 100).unwrap();
    store
        .write(|tx| {
            tx.register_blob(&info, "image/png")?;
            Ok(())
        })
        .unwrap();
    store
}
#[test]
fn native_roundtrip_preserves_raw_and_local_original_excludes_secrets_and_refuses_overwrite() {
    let parent = private();
    let data = parent.path().join("source");
    let store = fixture(&data);
    let tar = parent.path().join("archive.tar");
    fs_safe::atomic_private(&data.join("native-client.json"), b"PRIVATE NATIVE IDENTITY").unwrap();
    fs::create_dir(data.join("models")).unwrap();
    fs::write(data.join("models/secret"), b"PRIVATE MODEL CACHE").unwrap();
    let before = store
        .read(|tx| {
            Ok(tx
                .sql()
                .query_row("SELECT text FROM episodes", [], |r| r.get::<_, String>(0))?)
        })
        .unwrap();
    let report = export_archive(&data, &tar).unwrap();
    assert!(report.self_contained);
    assert_eq!(report.included_blob_count, 1);
    assert!(export_archive(&data, &tar).is_err());
    assert!(!fs::read(&tar).unwrap().windows(7).any(|s| s == b"PRIVATE"));
    let dest = parent.path().join("restored");
    let report = import_archive(&tar, &dest).unwrap();
    assert_eq!(report.destination_header, Some(5));
    let recovered = Store::open(&dest).unwrap();
    recovered
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("SELECT text FROM episodes", [], |r| r.get::<_, String>(0))?,
                before
            );
            let rows = registered(tx.sql())?;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                fs::read(dest.join(blob_name(&rows[0].sha)))?,
                b"original\0\xff"
            );
            Ok(())
        })
        .unwrap();
}
#[test]
fn remote_terabyte_reference_requires_binding_and_never_packs_cached_remote() {
    let parent = private();
    let data = parent.path().join("source");
    let store = fixture(&data);
    store
        .write(|tx| {
            tx.sql().execute(
                "UPDATE blobs SET remote_id='original_object',size=?",
                [4_i64 * 1024 * 1024 * 1024 * 1024],
            )?;
            Ok(())
        })
        .unwrap();
    let tar = parent.path().join("archive.tar");
    assert!(export_archive(&data, &tar).is_err());
    assert!(!tar.exists());
    let binding = PublicBinding {
        client_id: "original-client".into(),
        account: "owner@example.test".into(),
        folder_id: "original_folder".into(),
    };
    fs_safe::atomic_private(
        &data.join("drive-binding.json"),
        &serde_json::to_vec(&binding).unwrap(),
    )
    .unwrap();
    let report = export_archive(&data, &tar).unwrap();
    assert_eq!(report.remote_reference_count, 1);
    assert_eq!(report.included_blob_count, 0);
    assert!(!report.self_contained);
    let dest = parent.path().join("restored");
    import_archive(&tar, &dest).unwrap();
    let recovered = Store::open(&dest).unwrap();
    recovered
        .read(|tx| {
            let rows = registered(tx.sql())?;
            assert_eq!(rows[0].size, 4 * 1024_u64.pow(4));
            assert!(!dest.join(blob_name(&rows[0].sha)).exists());
            assert_eq!(
                tx.sql()
                    .query_row("SELECT tier FROM blobs", [], |r| r.get::<_, String>(0))?,
                "cold"
            );
            Ok(())
        })
        .unwrap();
}
fn rewrite(source: &Path, target: &Path, mutation: impl FnOnce(&mut Vec<(tar::Header, Vec<u8>)>)) {
    let mut archive = tar::Archive::new(fs::File::open(source).unwrap());
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        entries.push((entry.header().clone(), data));
    }
    mutation(&mut entries);
    let mut out = tar::Builder::new(fs::File::create(target).unwrap());
    for (mut h, data) in entries {
        h.set_cksum();
        out.append(&h, data.as_slice()).unwrap();
    }
    out.finish().unwrap();
}
#[test]
fn hostile_archives_never_publish_even_into_existing_empty_destination() {
    let parent = private();
    let data = parent.path().join("source");
    let _store = fixture(&data);
    let good = parent.path().join("good.tar");
    export_archive(&data, &good).unwrap();
    for kind in [
        "tamper",
        "missing",
        "extra",
        "traversal",
        "duplicate",
        "link",
        "pax",
        "unknown-header",
        "duplicate-json",
        "trailing",
        "truncated",
    ] {
        let bad = parent.path().join(format!("{kind}.tar"));
        rewrite(&good, &bad, |entries| match kind {
            "tamper" => entries.last_mut().unwrap().1[0] ^= 1,
            "missing" => {
                entries.pop();
            }
            "extra" | "traversal" => {
                let mut h = tar::Header::new_gnu();
                h.set_path("unexpected").unwrap();
                if kind == "traversal" {
                    h.as_mut_bytes()[..100].fill(0);
                    h.as_mut_bytes()[..19].copy_from_slice(b"blobs/aa/../escaped");
                }
                h.set_size(0);
                h.set_mode(0o600);
                entries.push((h, vec![]));
            }
            "duplicate" => entries.push(entries.last().unwrap().clone()),
            "link" => {
                let e = entries.last_mut().unwrap();
                e.0.set_entry_type(tar::EntryType::Symlink);
                e.0.set_link_name("/etc/passwd").unwrap();
                e.0.set_size(0);
                e.1.clear();
            }
            "pax" => entries[0].0.set_entry_type(tar::EntryType::XHeader),
            "duplicate-json" => {
                let e = &mut entries[0];
                let mut text = String::from_utf8(e.1.clone()).unwrap();
                text.insert_str(1, "\"version\":1,");
                e.1 = text.into_bytes();
                e.0.set_size(e.1.len() as u64);
            }
            "unknown-header" => {
                let e = &mut entries[1];
                e.1[60..64].copy_from_slice(&99_u32.to_be_bytes());
                let sha = raw::sha256(&e.1);
                let mut manifest: serde_json::Value =
                    serde_json::from_slice(&entries[0].1).unwrap();
                manifest["files"]["memory.sqlite"]["sha256"] = json!(sha);
                entries[0].1 = serde_json::to_vec(&manifest).unwrap();
                let length = entries[0].1.len() as u64;
                entries[0].0.set_size(length);
            }
            _ => (),
        });
        if kind == "trailing" {
            let bytes = fs::read(&good).unwrap();
            fs::OpenOptions::new()
                .append(true)
                .open(&bad)
                .unwrap()
                .write_all(&bytes)
                .unwrap();
        }
        if kind == "truncated" {
            let len = fs::metadata(&bad).unwrap().len();
            fs::OpenOptions::new()
                .write(true)
                .open(&bad)
                .unwrap()
                .set_len(len - 513)
                .unwrap();
        }
        for existing in [false, true] {
            let dest = parent.path().join(format!("{kind}-{existing}"));
            if existing {
                fs::create_dir(&dest).unwrap();
                fs::set_permissions(&dest, fs::Permissions::from_mode(0o700)).unwrap();
            }
            assert!(import_archive(&bad, &dest).is_err(), "{kind}");
            assert_eq!(dest.exists(), existing);
            if existing {
                assert!(fs::read_dir(&dest).unwrap().next().is_none());
            }
        }
    }
}
#[test]
fn independent_v1_archive_resets_remote_pointers_and_host_transfers() {
    let parent = private();
    let data = parent.path().join("source");
    let store = fixture(&data);
    store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE blobs SET remote_id='old_object',tier='cold'", [])?;
            tx.sql().execute(
                "INSERT INTO cold_failures SELECT sha256,2,'offline',12 FROM blobs",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let snapshot = parent.path().join("snapshot.sqlite");
    super::portable::snapshot(&data, &snapshot).unwrap();
    let db = Connection::open(&snapshot).unwrap();
    let rows = registered(&db).unwrap();
    drop(db);
    let mut payloads = vec![("memory.sqlite".to_owned(), fs::read(&snapshot).unwrap())];
    for row in rows {
        payloads.push((
            blob_name(&row.sha),
            fs::read(data.join(blob_name(&row.sha))).unwrap(),
        ));
    }
    let files: std::collections::BTreeMap<_, _> = payloads
        .iter()
        .map(|(n, b)| {
            (
                n.clone(),
                FileRecord {
                    sha256: raw::sha256(b),
                    size: b.len() as u64,
                },
            )
        })
        .collect();
    let manifest =
        serde_json::to_vec(&json!({"format":"memento-portable","version":1,"files":files}))
            .unwrap();
    let archive = parent.path().join("v1.tar");
    let mut builder = tar::Builder::new(fs::File::create(&archive).unwrap());
    for (name, bytes) in std::iter::once(("manifest.json".to_owned(), manifest)).chain(payloads) {
        let mut h = tar::Header::new_ustar();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_path(name).unwrap();
        h.set_size(bytes.len() as u64);
        h.set_mode(0o600);
        h.set_cksum();
        builder.append(&h, bytes.as_slice()).unwrap();
    }
    builder.finish().unwrap();
    drop(builder);
    let dest = parent.path().join("restore");
    let result = import_archive(&archive, &dest).unwrap();
    assert_eq!(result.version, 1);
    assert_eq!(result.remote_reference_count, 0);
    let recovered = Store::open(&dest).unwrap();
    recovered
        .read(|tx| {
            assert_eq!(
                tx.sql().query_row(
                    "SELECT count(*) FROM blobs WHERE remote_id IS NOT NULL OR tier!='hot'",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                0
            );
            assert_eq!(
                tx.sql()
                    .query_row("SELECT count(*) FROM cold_failures", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();
}
#[test]
fn restore_rejects_trigger_that_changes_authority_during_permitted_rebase() {
    let parent = private();
    let data = parent.path().join("source");
    let store = fixture(&data);
    store.write(|tx|{tx.sql().execute("INSERT INTO node_stats(node_id,importance) VALUES('synthetic-node',1)",[])?;tx.sql().execute_batch("CREATE TRIGGER malicious_restore AFTER UPDATE ON blobs BEGIN UPDATE node_stats SET importance=0; END;")?;Ok(())}).unwrap();
    let archive = parent.path().join("malicious.tar");
    export_archive(&data, &archive).unwrap();
    let before = hash_file(&archive).unwrap();
    let destination = parent.path().join("restore");
    assert!(import_archive(&archive, &destination).is_err());
    assert!(!destination.exists());
    assert_eq!(hash_file(&archive).unwrap(), before);
    store
        .read(|tx| {
            assert_eq!(
                tx.sql().query_row(
                    "SELECT importance FROM node_stats WHERE node_id='synthetic-node'",
                    [],
                    |r| r.get::<_, f64>(0)
                )?,
                1.
            );
            Ok(())
        })
        .unwrap();
}
#[test]
fn snapshot_finishes_while_another_connection_keeps_writing() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    use std::time::Duration;
    let parent = private();
    let data = parent.path().join("source");
    let _store = fixture(&data);
    let path = data.join("memory.sqlite");
    // Enough pages that a 100-page stepped copy needs thousands of steps.
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE filler(x BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<8000)
             INSERT INTO filler SELECT randomblob(4000) FROM n;",
        )
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let writer = {
        let (stop, writes, path) = (stop.clone(), writes.clone(), path.clone());
        std::thread::spawn(move || {
            let db = Connection::open(path).unwrap();
            db.busy_timeout(Duration::from_secs(5)).unwrap();
            db.execute_batch("PRAGMA synchronous=OFF").unwrap();
            while !stop.load(Ordering::Relaxed) {
                db.execute("INSERT INTO filler VALUES (randomblob(64))", []).unwrap();
                writes.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    while writes.load(Ordering::Relaxed) < 10 {
        std::thread::yield_now();
    }
    let before = writes.load(Ordering::Relaxed);
    let target = parent.path().join("copy.sqlite");
    let (done, finished) = std::sync::mpsc::channel();
    {
        let (data, target) = (data.clone(), target.clone());
        std::thread::spawn(move || {
            let _ = done.send(snapshot(&data, &target).map_err(|error| error.to_string()));
        });
    }
    let result = finished.recv_timeout(Duration::from_secs(60));
    let after = writes.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    result.expect("snapshot must finish while another connection writes").unwrap();
    assert!(after > before, "the writer kept committing during the snapshot");
    let copy = Connection::open(&target).unwrap();
    assert_eq!(copy.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0)).unwrap(), "ok");
    assert_eq!(
        copy.query_row("SELECT count(*) FROM filler WHERE length(x)=4000", [], |r| r.get::<_, i64>(0)).unwrap(),
        8000
    );
}
