//! Actual owner binaries against native authority on disposable synthetic data.
use memento_core::{F32Blob, Store, Tier, VectorKind, VectorWrite};
use memento_domain::{Graph, processing::Processing};
use memento_owner::owner::{AdminServices, OwnerServer};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    os::{fd::FromRawFd, unix::fs::PermissionsExt},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn admin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_memento-admin"))
}
fn services(graph: &Graph) -> Arc<AdminServices> {
    let store = graph.store.clone();
    let helper = admin();
    AdminServices::new(
        graph.clone(),
        Arc::new(Processing::new(graph.clone())),
        Arc::new(|| None),
        Arc::new(move || store.compact_with_helper(&helper)),
    )
    .unwrap()
}
#[test]
fn actual_compact_cli_reclaims_database_without_tty_and_preserves_authority_vectors_raw() {
    let dir = private();
    let store = Store::open(dir.path()).unwrap();
    let graph = Graph::new(store.clone());
    let raw_text = "Ω exact raw source\r\n".repeat(25_000);
    let captured=store.capture(serde_json::from_value(json!({"source":"owner-runtime","event_id":"raw","conversation_id":"thread","kind":"native_record","text":raw_text,"metadata":{"extraction_mode":"skip"}})).unwrap()).unwrap();
    let user=store.capture(serde_json::from_value(json!({"source":"owner-runtime","event_id":"projected","conversation_id":"thread","kind":"user","text":"Exact searchable source."})).unwrap()).unwrap();
    let node=graph.remember(serde_json::from_value(json!({"command_id":"saved","actor":"fixture","why":"Exact source","type":"knowledge","text":"Exact searchable source.","scope":["personal"],"evidence":[{"episode_id":user.id,"quote":"Exact searchable source."}]})).unwrap()).unwrap();
    let history = graph.history(node["id"].as_str().unwrap()).unwrap();
    let mut values = [0.; 768];
    values[0] = 1.;
    let vector = F32Blob::from_values(&values).unwrap();
    let bytes = vector.as_bytes().to_owned();
    store.write(|tx|{tx.put_vector(&VectorWrite{seq:101,target:node["id"].as_str().unwrap().into(),kind:VectorKind::Node,version:1,current:true,tier:Tier::Hot,embedding:vector})?;tx.sql().execute_batch("CREATE TABLE disposable(payload BLOB); INSERT INTO disposable VALUES(zeroblob(2097152)); DROP TABLE disposable;")?;Ok(())}).unwrap();
    let checkpoint = rusqlite::Connection::open(dir.path().join("memory.sqlite")).unwrap();
    checkpoint
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(checkpoint);
    let server = OwnerServer::start(services(&graph)).unwrap();
    let output = Command::new(admin())
        .args(["--data-dir", dir.path().to_str().unwrap(), "compact"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["result"]["complete"], true);
    assert_eq!(result["result"]["checkpoint_busy"], 0);
    let before = result["result"]["before"]["database"].as_u64().unwrap();
    let after = result["result"]["after"]["database"].as_u64().unwrap();
    assert!(after < before);
    assert_eq!(
        store
            .read(|tx| tx.original_text(&captured.id))
            .unwrap()
            .unwrap(),
        raw_text
    );
    assert_eq!(
        graph.history(node["id"].as_str().unwrap()).unwrap(),
        history
    );
    store
        .read(|tx| {
            assert_eq!(
                tx.sql().query_row(
                    "SELECT embedding FROM vector_blobs WHERE seq=101",
                    [],
                    |r| r.get::<_, Vec<u8>>(0)
                )?,
                bytes
            );
            assert!(
                tx.sql().query_row(
                    "SELECT count(*) FROM episodes_fts WHERE episodes_fts MATCH 'searchable'",
                    [],
                    |r| r.get::<_, i64>(0)
                )? > 0
            );
            Ok(())
        })
        .unwrap();
    assert!(!fs::read_dir(dir.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".memento-compact-")
    }));
    server.close().unwrap();
}
#[test]
fn actual_cli_nonterminal_decision_and_invalid_restore_fail_without_service_or_credentials() {
    let output = Command::new(admin())
        .args([
            "--data-dir",
            "/nonexistent-synthetic",
            "approve",
            "proposal",
            "--why",
            "SYNTHETIC PRIVATE REASON",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("interactive human terminal"));
    assert!(!error.contains("SYNTHETIC PRIVATE REASON"));
    assert!(output.stdout.is_empty());
    let output = Command::new(env!("CARGO_BIN_EXE_memento-restore"))
        .args([
            "--connection",
            "/nonexistent-synthetic",
            "--allow-incomplete",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("--backup-id")
    );
}
fn terminal() -> (File, File) {
    let (mut master, mut slave) = (-1, -1);
    let code = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert_eq!(code, 0);
    unsafe { (File::from_raw_fd(master), File::from_raw_fd(slave)) }
}
#[test]
fn actual_cli_tty_export_requires_exact_confirmation_before_native_archive_publication() {
    let dir = private();
    let store = Store::open(dir.path()).unwrap();
    let graph = Graph::new(store.clone());
    store.capture(serde_json::from_value(json!({"source":"owner-runtime","event_id":"one","conversation_id":"thread","kind":"user","text":"Synthetic exact export source Ω."})).unwrap()).unwrap();
    let server = OwnerServer::start(services(&graph)).unwrap();
    for accepted in [false, true] {
        let archive = dir.path().join(if accepted {
            "confirmed.tar"
        } else {
            "refused.tar"
        });
        let (mut master, slave) = terminal();
        let child = Command::new(admin())
            .args([
                "--data-dir",
                dir.path().to_str().unwrap(),
                "export",
                "--output",
                archive.to_str().unwrap(),
            ])
            .stdin(Stdio::from(slave))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let answer = if accepted {
            format!("export private archive to {}\n", archive.display())
        } else {
            "not confirmed\n".into()
        };
        master.write_all(answer.as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            output.status.success(),
            accepted,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(archive.exists(), accepted);
        if accepted {
            assert!(String::from_utf8_lossy(&output.stdout).contains("\"complete\": true"));
        } else {
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("Not confirmed; nothing changed.")
            );
        }
    }
    server.close().unwrap();
}

#[test]
fn installed_drive_binary_dispatches_connect_help_and_validation_without_provider_work() {
    let help = std::process::Command::new(env!("CARGO_BIN_EXE_memento-drive"))
        .args(["connect", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(
        String::from_utf8(help.stdout)
            .unwrap()
            .contains("memento-drive connect --client-json")
    );
    assert!(help.stderr.is_empty());
    let missing = std::process::Command::new(env!("CARGO_BIN_EXE_memento-drive"))
        .arg("connect")
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(2));
    assert!(!missing.stderr.is_empty());
}
