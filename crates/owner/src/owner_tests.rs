use crate::{
    owner::{self, AdminServices, OwnerServer},
    owner_cli::{self, Console},
    restore_cli,
};
use memento_core::{Error, Result, Store};
use memento_domain::{Graph, TxGraph, processing::Processing};
use memento_maintenance::{
    binding::PublicBinding,
    cold::{ColdManager, ColdObject, ColdStore, RemoteBackup},
    fs_safe, portable,
    transport::{Request, Response, Transport},
};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
const TEXT: &str =
    "Exact source Ω😀. Orion and orion and Orion warehouse refer to one project. Keep raw history.";
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
struct Fixture {
    dir: tempfile::TempDir,
    graph: Graph,
    episode: String,
    processing: Arc<Processing>,
}
impl Fixture {
    fn new() -> Self {
        let dir = private();
        let store = Store::open(dir.path()).unwrap();
        let captured=store.capture(serde_json::from_value(json!({"source":"owner-fixture","event_id":"one","conversation_id":"thread","kind":"user","text":TEXT})).unwrap()).unwrap();
        let graph = Graph::new(store);
        let processing = Arc::new(Processing::new(graph.clone()));
        Self {
            dir,
            graph,
            episode: captured.id,
            processing,
        }
    }
    fn services(&self) -> Arc<AdminServices> {
        AdminServices::new(
            self.graph.clone(),
            self.processing.clone(),
            Arc::new(|| None),
            Arc::new(|| {
                Err(Error::Unavailable(
                    "Compact helper is not used in this test",
                ))
            }),
        )
        .unwrap()
    }
    fn save(&self, key: &str, kind: &str, text: &str) -> Value {
        self.graph.remember(serde_json::from_value(json!({"command_id":key,"actor":"synthetic-agent","why":"Exact synthetic source","type":kind,"text":text,"scope":["professional"],"evidence":[{"episode_id":self.episode,"quote":"Exact source Ω😀."}]})).unwrap()).unwrap()
    }
    fn call(&self, value: Value) -> Value {
        owner::request(self.dir.path(), &value).unwrap()
    }
}
#[test]
fn real_socket_mode_wrappers_and_owner_review_actor_retry_conflict() {
    let f = Fixture::new();
    let proposal = f.save("goal", "goal", "Keep raw history");
    assert_eq!(proposal["status"], "pending_review");
    let server = OwnerServer::start(f.services()).unwrap();
    assert!(
        fs::symlink_metadata(server.path())
            .unwrap()
            .file_type()
            .is_socket()
    );
    assert_eq!(
        fs::metadata(server.path()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let listed = f.call(json!({"operation":"list"}));
    assert_eq!(listed["result"].as_array().unwrap().len(), 1);
    let request = json!({"operation":"approve","proposal_id":proposal["id"],"command_id":"owner-approval","why":"Human exact review","actor":"forged-client-actor","uid":0});
    let approved = f.call(request.clone());
    assert!(approved.get("error").is_none());
    let id = approved["result"]["id"].as_str().unwrap();
    let history = f.graph.history(id).unwrap();
    assert_eq!(
        history[0]["actor"],
        format!("human:uid:{}", unsafe { libc::getuid() })
    );
    assert_eq!(f.call(request.clone()), approved);
    let mut changed = request;
    changed["why"] = json!("Changed retry");
    assert!(f.call(changed).get("error").is_some());
    assert_eq!(f.graph.history(id).unwrap(), history);
    assert!(
        f.call(json!({"operation":"list"}))["result"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    server.close().unwrap();
    assert!(!server.path().exists());
}
#[test]
fn owner_merge_include_and_forget_preserve_original_raw_history() {
    let f = Fixture::new();
    let ids = ["Orion", "orion", "Orion warehouse"]
        .iter()
        .enumerate()
        .map(|(i, s)| {
            f.save(&format!("entity-{i}"), "project", s)["id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    let server = OwnerServer::start(f.services()).unwrap();
    let inventory = f.call(json!({"operation":"inventory"}));
    let cluster = inventory["result"]["clusters"][0]["cluster"].clone();
    let preview=f.call(json!({"operation":"merge_preview","cluster":cluster,"into":null,"exclude":[],"include":[ids[2]]}));
    assert_eq!(preview["result"]["included"], json!([ids[2]]));
    let survivor = preview["result"]["survivor"]["id"].clone();
    let result=f.call(json!({"operation":"merge","cluster":cluster,"into":survivor,"exclude":[],"include":[ids[2]],"token":preview["result"]["token"],"why":"Owner confirms identity","command_id":"reviewed-merge"}));
    assert!(result.get("error").is_none(), "{result}");
    let target = survivor.as_str().unwrap();
    let preview = f.call(json!({"operation":"forget_preview","target":target}));
    let invalid=f.call(json!({"operation":"forget","target":target,"token":"changed-token","why":"Owner request","command_id":"bad-forget"}));
    assert!(invalid.get("error").is_some());
    let valid=f.call(json!({"operation":"forget","target":target,"token":preview["result"]["token"],"why":"Owner request","command_id":"confirmed-forget"}));
    assert!(valid.get("error").is_none());
    assert_eq!(
        f.graph
            .store
            .read(|tx| tx.original_text(&f.episode))
            .unwrap()
            .unwrap(),
        TEXT
    );
    assert!(!f.graph.history(target).unwrap().is_empty());
    server.close().unwrap();
}
#[test]
fn owner_history_cutover_idempotent_and_export_is_complete_native_archive() {
    let f = Fixture::new();
    let server = OwnerServer::start(f.services()).unwrap();
    assert!(
        f.call(json!({"operation":"history_status"}))
            .get("result")
            .is_some()
    );
    let first = f.call(json!({"operation":"history_park"}));
    assert!(first.get("error").is_none());
    assert_eq!(f.call(json!({"operation":"history_park"})), first);
    f.graph.store.write(|tx| {
        tx.sql().execute("INSERT INTO historical_parked_candidates(candidate_id) VALUES ('preserved-candidate')",[])?;
        Ok(())
    }).unwrap();
    let released=f.call(json!({"operation":"history_release-candidates"}));
    assert_eq!(released["result"]["released_now"],1);
    assert_eq!(released["result"]["parked_candidates"],0);
    assert_eq!(released["result"]["parked_conversations"],first["result"]["parked_conversations"]);
    assert_eq!(f.call(json!({"operation":"history_release-candidates"}))["result"]["released_now"],0);
    let archive = f.dir.path().join("owner.tar");
    let result = f.call(json!({"operation":"export","output":archive}));
    assert_eq!(result["result"]["complete"], true);
    let destination = f.dir.path().join("restored");
    portable::import_archive(&archive, &destination).unwrap();
    let restored = Store::open(&destination).unwrap();
    assert_eq!(
        restored
            .read(|tx| tx.original_text(&f.episode))
            .unwrap()
            .unwrap(),
        TEXT
    );
    server.close().unwrap();
}
struct ColdFixture {
    root: PathBuf,
    binding: PublicBinding,
}
impl ColdStore for ColdFixture {
    fn binding(&self) -> &PublicBinding {
        &self.binding
    }
    fn upload(
        &self,
        path: &Path,
        _name: &str,
        _mime: &str,
        _checkpoint: &Value,
        save: &mut dyn FnMut(Value) -> Result<()>,
    ) -> Result<ColdObject> {
        let (sha, md5, size) = memento_maintenance::drive::hashes(path)?;
        let destination = self.root.join(&sha);
        if !destination.exists() {
            memento_maintenance::copy_verified(path, &destination, &sha, size)?;
        }
        save(json!({"object_id":sha,"sha256":sha,"size":size}))?;
        Ok(ColdObject {
            id: sha.clone(),
            sha256: sha,
            size,
            md5,
        })
    }
    fn download(&self, _id: &str, _dest: &Path, _sha: &str, _size: u64) -> Result<()> {
        Err(Error::Unavailable("Download unused"))
    }
    fn list_backups(&self) -> Result<Vec<RemoteBackup>> {
        Ok(Vec::new())
    }
}
#[test]
fn socket_observes_late_cold_install_without_restart() {
    let f = Fixture::new();
    let slot: Arc<Mutex<Option<Arc<ColdManager>>>> = Arc::new(Mutex::new(None));
    let current = slot.clone();
    let services = AdminServices::new(
        f.graph.clone(),
        f.processing.clone(),
        Arc::new(move || current.lock().unwrap().clone()),
        Arc::new(|| Err(Error::Unavailable("Unused"))),
    )
    .unwrap();
    let server = OwnerServer::start(services).unwrap();
    assert_eq!(
        f.call(json!({"operation":"backup"}))["error"],
        "Drive cold storage is not configured"
    );
    let remote = f.dir.path().join("remote");
    fs::create_dir(&remote).unwrap();
    let backend = Arc::new(ColdFixture {
        root: remote,
        binding: PublicBinding {
            client_id: "synthetic-client".into(),
            account: "owner@example.test".into(),
            folder_id: "original_folder".into(),
        },
    });
    *slot.lock().unwrap() = Some(ColdManager::new(f.graph.store.clone(), backend).unwrap());
    assert_eq!(
        f.call(json!({"operation":"backup"}))["result"]["complete"],
        true
    );
    server.close().unwrap();
}
#[test]
fn socket_refuses_regular_and_link_paths_and_preserves_replacement_on_shutdown() {
    let f = Fixture::new();
    let path = f.dir.path().join("admin.sock");
    fs::write(&path, b"keep original").unwrap();
    assert!(OwnerServer::start(f.services()).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"keep original");
    fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("unrelated", &path).unwrap();
    assert!(OwnerServer::start(f.services()).is_err());
    fs::remove_file(&path).unwrap();
    let stale = UnixListener::bind(&path).unwrap();
    drop(stale);
    let server = OwnerServer::start(f.services()).unwrap();
    fs::remove_file(&path).unwrap();
    fs::write(&path, b"new owner file").unwrap();
    server.close().unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"new owner file");
}
#[test]
fn real_peer_credentials_drive_gate_and_client_uid_fields_cannot_grant_authority() {
    let f = Fixture::new();
    let (mut client, server) = UnixStream::pair().unwrap();
    let uid = owner::real_peer_uid(&server).unwrap();
    assert_eq!(uid, unsafe { libc::getuid() });
    assert!(owner::peer_admitted(0, uid));
    assert!(owner::peer_admitted(uid, uid));
    assert!(!owner::peer_admitted(uid + 1, uid));
    if uid == 0 {
        return;
    } // Root is explicitly always admitted, independent of service UID.
    let services = f.services();
    let worker = std::thread::spawn(move || owner::serve_test_peer(server, services, uid + 1));
    client
        .write_all(b"{\"operation\":\"list\",\"uid\":0}\n")
        .unwrap();
    let mut bytes = Vec::new();
    let _ = client.read_to_end(&mut bytes);
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"], "Administrative identity required");
    worker.join().unwrap();
}
#[test]
fn malformed_requests_are_sanitized_and_one_connection_executes_one_line() {
    let f = Fixture::new();
    let server = OwnerServer::start(f.services()).unwrap();
    let mut stream = UnixStream::connect(server.path()).unwrap();
    stream.write_all(b"{secret:DO_NOT_REFLECT}\n").unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value, json!({"error":"Invalid administrative request"}));
    assert!(!String::from_utf8(bytes).unwrap().contains("DO_NOT_REFLECT"));
    let mut stream = UnixStream::connect(server.path()).unwrap();
    stream
        .write_all(b"{\"operation\":\"list\"}\n{\"operation\":\"unknown\"}\n")
        .unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 1);
    assert!(
        serde_json::from_slice::<Value>(&bytes)
            .unwrap()
            .get("result")
            .is_some()
    );
    server.close().unwrap();
}
#[derive(Default)]
struct FakeConsole {
    terminal: bool,
    answer: String,
    printed: String,
    prompts: Vec<String>,
}
impl Console for FakeConsole {
    fn interactive(&self) -> bool {
        self.terminal
    }
    fn confirm(&mut self, prompt: &str) -> std::io::Result<String> {
        self.prompts.push(prompt.into());
        Ok(self.answer.clone())
    }
    fn output(&mut self, text: &str) -> std::io::Result<()> {
        self.printed.push_str(text);
        Ok(())
    }
}
fn argv(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| s.to_string()).collect()
}
#[test]
fn cli_mutations_and_export_require_tty_before_preview_or_socket_request() {
    for args in [
        argv(&[
            "--data-dir",
            "synthetic",
            "approve",
            "proposal",
            "--why",
            "Exact review",
        ]),
        argv(&[
            "--data-dir",
            "synthetic",
            "merge",
            "cluster",
            "--why",
            "Exact review",
        ]),
        argv(&[
            "--data-dir",
            "synthetic",
            "forget",
            "node",
            "--why",
            "Exact review",
        ]),
        argv(&[
            "--data-dir",
            "synthetic",
            "export",
            "--output",
            "private.tar",
        ]),
        argv(&[
            "--data-dir",
            "synthetic",
            "recover-revisions",
            "--why",
            "Exact review",
        ]),
    ] {
        let calls = AtomicUsize::new(0);
        let error = owner_cli::run(&args, &mut FakeConsole::default(), &|_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({}))
        })
        .unwrap_err();
        assert_eq!(error.status, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
#[test]
fn cli_operational_commands_need_no_tty_and_compact_incomplete_is_nonzero() {
    for (op, expected) in [
        (vec!["list"], "list"),
        (vec!["inventory"], "inventory"),
        (vec!["backup"], "backup"),
        (vec!["history", "park"], "history_park"),
        (vec!["history", "status"], "history_status"),
        (vec!["history", "release-candidates"], "history_release-candidates"),
        (vec!["compact"], "compact"),
        (vec!["revisions"], "revision_plan"),
    ] {
        let mut values = vec!["--data-dir", "synthetic"];
        values.extend(op);
        let args = argv(&values);
        let mut console = FakeConsole::default();
        let status = owner_cli::run(&args, &mut console, &|_, v| {
            assert_eq!(v["operation"], expected);
            Ok(json!({"result":{"complete":false}}))
        })
        .unwrap();
        assert_eq!(status, if expected == "compact" { 1 } else { 0 });
        assert!(console.prompts.is_empty());
    }
}
#[test]
fn cli_preserves_option_target_intermix_and_repeated_merge_selection() {
    let parsed = owner_cli::parse(&argv(&[
        "--data-dir",
        "synthetic",
        "approve",
        "--why",
        "Human review",
        "proposal",
        "--command-id",
        "stable",
    ]))
    .unwrap();
    assert_eq!(parsed.positional.as_deref(), Some("proposal"));
    assert_eq!(parsed.why.as_deref(), Some("Human review"));
    assert_eq!(parsed.command_id.as_deref(), Some("stable"));
    let parsed = owner_cli::parse(&argv(&[
        "--data-dir=synthetic",
        "merge",
        "--into",
        "survivor",
        "--exclude",
        "a",
        "cluster",
        "--include",
        "b",
        "--exclude",
        "c",
        "--why=Exact review",
    ]))
    .unwrap();
    assert_eq!(parsed.positional.as_deref(), Some("cluster"));
    assert_eq!(parsed.into.as_deref(), Some("survivor"));
    assert_eq!(parsed.exclude, vec!["a", "c"]);
    assert_eq!(parsed.include, vec!["b"]);
    assert!(
        owner_cli::parse(&argv(&[
            "--data-dir",
            "synthetic",
            "approve",
            "--why",
            "review"
        ]))
        .is_err()
    );
}
#[test]
fn cli_merge_pins_preview_survivor_token_and_included_variants_before_confirmed_mutation() {
    let requests = RefCell::new(Vec::new());
    let mut console = FakeConsole {
        terminal: true,
        answer: "merge 2 into fixed-survivor".into(),
        ..Default::default()
    };
    let args = argv(&[
        "--data-dir",
        "synthetic",
        "merge",
        "cluster",
        "--include",
        "variant",
        "--exclude",
        "distinct",
        "--why",
        "Human choice",
        "--command-id",
        "stable-id",
    ]);
    let status=owner_cli::run(&args,&mut console,&|_,v|{requests.borrow_mut().push(v.clone());if v["operation"]=="merge_preview"{Ok(json!({"result":{"survivor":{"id":"fixed-survivor"},"absorbed":["a","b"],"token":"frozen-preview-token"}}))}else{Ok(json!({"result":{"id":"fixed-survivor"}}))}}).unwrap();
    assert_eq!(status, 0);
    let requests = requests.borrow();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1]["into"], "fixed-survivor");
    assert_eq!(requests[1]["token"], "frozen-preview-token");
    assert_eq!(requests[1]["include"], json!(["variant"]));
    assert_eq!(requests[1]["exclude"], json!(["distinct"]));
    assert_eq!(requests[1]["command_id"], "stable-id");
    assert!(requests[1].get("actor").is_none());
    assert!(console.printed.find("frozen-preview-token").is_some());
}
#[test]
fn cli_cancellation_performs_preview_only_and_confirmed_forget_uses_its_token() {
    for answer in ["wrong", "forget node from active memory"] {
        let count = AtomicUsize::new(0);
        let mut console = FakeConsole {
            terminal: true,
            answer: answer.into(),
            ..Default::default()
        };
        let status = owner_cli::run(
            &argv(&[
                "--data-dir",
                "synthetic",
                "forget",
                "node",
                "--why",
                "Human request",
            ]),
            &mut console,
            &|_, v| {
                count.fetch_add(1, Ordering::SeqCst);
                if v["operation"] == "forget_preview" {
                    Ok(json!({"result":{"token":"preview-token"}}))
                } else {
                    assert_eq!(v["token"], "preview-token");
                    assert_eq!(v["why"], "Human request");
                    assert_eq!(v["command_id"].as_str().unwrap().len(), 32);
                    Ok(json!({"result":{"id":"node"}}))
                }
            },
        )
        .unwrap();
        assert_eq!(status, if answer == "wrong" { 1 } else { 0 });
        assert_eq!(
            count.load(Ordering::SeqCst),
            if answer == "wrong" { 1 } else { 2 }
        );
    }
}
#[test]
fn cli_export_confirmation_names_exact_absolute_destination() {
    let data = private();
    let output = data.path().join("archive.tar");
    let expected = format!("export private archive to {}", output.display());
    let mut console = FakeConsole {
        terminal: true,
        answer: expected,
        ..Default::default()
    };
    let args = vec![
        "--data-dir".into(),
        data.path().to_string_lossy().into_owned(),
        "export".into(),
        "--output".into(),
        output.to_string_lossy().into_owned(),
    ];
    let status = owner_cli::run(&args, &mut console, &|_, v| {
        assert_eq!(v["output"], output.to_str().unwrap());
        Ok(json!({"result":{"complete":true}}))
    })
    .unwrap();
    assert_eq!(status, 0);
    assert_eq!(console.prompts.len(), 1);
}
struct NeverNetwork;
impl Transport for NeverNetwork {
    fn send(&self, _: Request, _: &mut dyn Write, _: u64) -> Result<Response> {
        panic!("Offline archive dispatch must not contact a provider")
    }
}
#[test]
fn restore_cli_archive_dispatch_is_native_offline_complete_and_options_fail_before_io() {
    let f = Fixture::new();
    let archive = f.dir.path().join("archive.tar");
    portable::export_archive(f.dir.path(), &archive).unwrap();
    let destination = f.dir.path().join("restored");
    let args = vec![
        "--archive".into(),
        archive.to_string_lossy().into_owned(),
        "--destination".into(),
        destination.to_string_lossy().into_owned(),
    ];
    let parsed = restore_cli::parse(&args).unwrap();
    let result = restore_cli::execute(&parsed, Arc::new(NeverNetwork)).unwrap();
    assert_eq!(result["complete"], true);
    let restored = Store::open(destination).unwrap();
    assert_eq!(
        restored
            .read(|tx| tx.original_text(&f.episode))
            .unwrap()
            .unwrap(),
        TEXT
    );
    for values in [
        vec![],
        vec!["--archive", "x"],
        vec!["--connection", "x", "--archive", "y", "--destination", "z"],
        vec!["--connection", "x", "--allow-incomplete"],
        vec!["--connection", "x", "--backup-id", "id"],
        vec!["--archive", "x", "--destination", "y", "--allow-incomplete"],
    ] {
        assert_eq!(restore_cli::parse(&argv(&values)).unwrap_err().status, 2);
    }
}
struct DriveProbe {
    calls: AtomicUsize,
}
impl Transport for DriveProbe {
    fn send(&self, request: Request, out: &mut dyn Write, _: u64) -> Result<Response> {
        assert!(
            request
                .url
                .starts_with("https://www.googleapis.com/drive/v3/")
        );
        let value = if request.url.contains("/about?") {
            json!({"user":{"emailAddress":"owner@example.test"}})
        } else if request.url.contains("/files/original_folder?") {
            json!({"id":"original_folder","mimeType":"application/vnd.google-apps.folder","ownedByMe":true,"trashed":false})
        } else {
            assert!(request.url.contains("/files?q="));
            json!({"files":[]})
        };
        self.calls.fetch_add(1, Ordering::SeqCst);
        out.write_all(&serde_json::to_vec(&value)?)?;
        Ok(Response {
            status: 200,
            headers: BTreeMap::new(),
        })
    }
}
#[test]
fn restore_cli_uses_only_explicit_private_original_connection_file() {
    let dir = private();
    let path = dir.path().join("chosen-original.json");
    let scope = "https://www.googleapis.com/auth/drive.file";
    let config = json!({"type":"authorized_user","client_id":"synthetic-client","client_secret":"SYNTHETIC SECRET","refresh_token":"SYNTHETIC REFRESH","token":"SYNTHETIC TOKEN","expiry":"2099-01-01T00:00:00+00:00","scopes":[scope],"memento":{"account":"owner@example.test","folder_id":"original_folder"}});
    fs_safe::atomic_private(&path, &serde_json::to_vec(&config).unwrap()).unwrap();
    fs_safe::atomic_private(
        &dir.path().join("drive-oauth.json"),
        b"invalid neighboring file must be ignored",
    )
    .unwrap();
    let parsed = restore_cli::parse(&vec![
        "--connection".into(),
        path.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let provider = Arc::new(DriveProbe {
        calls: AtomicUsize::new(0),
    });
    assert_eq!(
        restore_cli::execute(&parsed, provider.clone()).unwrap(),
        json!([])
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(restore_cli::execute(&parsed, Arc::new(NeverNetwork)).is_err());
}
#[test]
fn wire_json_is_ascii_with_exact_unicode_and_control_roundtrip() {
    let value = json!({"source":"Ω😀\r\n\0"});
    let bytes = owner::ascii_json(&value).unwrap();
    assert!(bytes.is_ascii());
    assert_eq!(
        bytes,
        b"{\"source\":\"\\u03a9\\ud83d\\ude00\\r\\n\\u0000\"}"
    );
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), value);
}
#[test]
fn total_request_deadline_and_size_bound_hold_before_dispatch() {
    let (mut writer, mut reader) = UnixStream::pair().unwrap();
    let feeder = std::thread::spawn(move || {
        for _ in 0..20 {
            if writer.write_all(b" ").is_err() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    let start = std::time::Instant::now();
    assert!(matches!(
        owner::read_test_deadline(&mut reader, std::time::Duration::from_millis(25)),
        Err(Error::Unavailable("Administrative request timed out"))
    ));
    assert!(start.elapsed() < std::time::Duration::from_millis(150));
    drop(reader);
    feeder.join().unwrap();
    let f = Fixture::new();
    let server = OwnerServer::start(f.services()).unwrap();
    let response = owner::request(
        f.dir.path(),
        &json!({"operation":"list","padding":"A".repeat(1024*1024)}),
    );
    assert!(matches!(
        response,
        Err(Error::Validation(
            "Administrative message exceeds size limit"
        ))
    ));
    assert_eq!(f.call(json!({"operation":"list"})), json!({"result":[]}));
    server.close().unwrap();
}
// A replacement written by the retired supersede path: new ID plus marker and
// supersedes edge in one commit.
fn legacy_replace(f: &Fixture, key: &str, old: &str, text: &str, pinned: bool) -> String {
    let evidence = json!([{"episode_id":f.episode,"quote":"Exact source Ω😀."}]);
    let context = memento_domain::context(key.into(), "legacy".into(), "Legacy fixture".into());
    let result = f.graph.store.command(&context, &json!({"legacy":key}), |tx, commit| {
        let (version, kind, prior, scope, pin): (i64, String, String, String, i64) = tx.sql().query_row(
            "SELECT version,type,text,scope,pinned FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",
            [old],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )?;
        let replacement = TxGraph::insert_node(tx, commit, &json!({"type":kind,"text":text,"scope":serde_json::from_str::<Value>(&scope)?,"valid_from":null,"valid_to":null,"evidence":evidence,"pinned":pinned}))?;
        let id = replacement["id"].as_str().unwrap();
        tx.sql().execute("INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)", rusqlite::params![old, version + 1, commit, kind, prior, "refined", None::<&str>, None::<&str>, id, pin, scope])?;
        TxGraph::evidence(tx, old, version + 1, &evidence)?;
        let edge = format!("edge-{key}");
        tx.sql().execute("INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)", rusqlite::params![edge, 1, commit, id, "supersedes", old, "active"])?;
        TxGraph::evidence(tx, &edge, 1, &evidence)?;
        Ok(replacement)
    });
    result.unwrap()["id"].as_str().unwrap().to_owned()
}
#[test]
fn owner_revision_recovery_applies_protected_chains_only_when_named() {
    let f = Fixture::new();
    let id = |value: Value| value["id"].as_str().unwrap().to_owned();
    let a = id(f.save("plain", "knowledge", "Atlas rollout is pending."));
    let d = id(f.save("guarded", "knowledge", "Atlas budget is draft."));
    let b = legacy_replace(&f, "legacy-a", &a, "Atlas rollout shipped.", false);
    let e = legacy_replace(&f, "legacy-d", &d, "Atlas budget is final.", true);
    let server = OwnerServer::start(f.services()).unwrap();
    let plan = f.call(json!({"operation":"revision_plan","limit":8,"exclude":[],"protected":[]}))["result"].clone();
    assert_eq!(plan["chains"].as_array().unwrap().iter().map(|c| c["root"].clone()).collect::<Vec<_>>(), vec![json!(a)]);
    assert_eq!(plan["protected"], json!([{"root":d,"members":[d,e],"reasons":["pinned"]}]));
    let recover = |key: &str, token: &Value, protected: Value| {
        f.call(json!({"operation":"recover_revisions","limit":8,"exclude":[],"protected":protected,"token":token,"why":"Owner reviewed plan","command_id":key}))
    };
    // Naming a protected root changes the plan, so the generic token cannot apply it.
    assert!(recover("stale", &plan["token"], json!([d])).get("error").is_some());
    let missing = f.call(json!({"operation":"recover_revisions","limit":8,"exclude":[],"token":plan["token"],"why":"Owner reviewed plan","command_id":"missing"}));
    assert!(missing.get("error").is_some());
    let generic = recover("generic", &plan["token"], json!([]));
    assert_eq!(generic["result"]["recovered"], json!([{"id":a,"version":3,"merged":[b],"requeued_candidates":[]}]), "{generic}");
    assert_eq!(recover("generic", &plan["token"], json!([])), generic);
    assert_eq!(f.graph.history(&d).unwrap().iter().filter(|v| v["id"] == d.as_str()).count(), 2);
    let named = f.call(json!({"operation":"revision_plan","limit":8,"exclude":[],"protected":[d]}))["result"].clone();
    assert_eq!(named["chains"][0]["protected_reasons"], json!(["pinned"]));
    let applied = recover("named", &named["token"], json!([d]));
    assert_eq!(applied["result"]["recovered"][0]["id"], d.as_str(), "{applied}");
    let history = f.graph.history(&a).unwrap();
    let current = history.last().unwrap();
    assert_eq!((current["id"].as_str(), current["text"].as_str()), (Some(a.as_str()), Some("Atlas rollout shipped.")));
    let origin = &current["revision"]["recovered_from"];
    assert_eq!((origin["id"].as_str(), origin["version"].as_i64(), origin["actor"].as_str(), origin["why"].as_str()), (Some(b.as_str()), Some(1), Some("legacy"), Some("Legacy fixture")));
    assert!(current["actor"].as_str().unwrap().starts_with("human:uid:"));
    assert_eq!(current["evidence"][0]["quote"], "Exact source Ω😀.");
    assert!(history.iter().any(|v| v["id"] == a.as_str() && v["text"] == "Atlas rollout is pending."));
    assert_eq!(f.graph.store.read(|tx| tx.original_text(&f.episode)).unwrap().unwrap(), TEXT);
    server.close().unwrap();
}
#[test]
fn cli_revision_recovery_requires_named_protected_roots_in_the_typed_confirmation() {
    let plan = json!({"result":{"chains":[{"root":"a","protected_reasons":[]},{"root":"d","protected_reasons":["pinned"]}],"token":"plan-token"}});
    for (answer, status) in [
        ("recover 2 revision chains", 1),
        ("recover 2 revision chains including protected d", 0),
    ] {
        let requests = RefCell::new(Vec::new());
        let mut console = FakeConsole {
            terminal: true,
            answer: answer.into(),
            ..Default::default()
        };
        let args = argv(&["--data-dir", "synthetic", "recover-revisions", "--limit", "8", "--protected", "d", "--exclude", "x", "--why", "Human choice", "--command-id", "stable-id"]);
        let code = owner_cli::run(&args, &mut console, &|_, v| {
            requests.borrow_mut().push(v.clone());
            Ok(if v["operation"] == "revision_plan" { plan.clone() } else { json!({"result":{"recovered":[]}}) })
        })
        .unwrap();
        assert_eq!(code, status);
        let requests = requests.borrow();
        assert_eq!(requests[0], json!({"limit":8,"exclude":["x"],"protected":["d"],"operation":"revision_plan"}));
        assert_eq!(requests.len(), if status == 0 { 2 } else { 1 });
        if status == 0 {
            assert_eq!(requests[1], json!({"limit":8,"exclude":["x"],"protected":["d"],"operation":"recover_revisions","token":"plan-token","why":"Human choice","command_id":"stable-id"}));
        }
        assert!(console.prompts[0].contains("including protected d"));
    }
    let mut console = FakeConsole { terminal: true, ..Default::default() };
    let code = owner_cli::run(&argv(&["--data-dir", "synthetic", "recover-revisions", "--why", "Human choice"]), &mut console, &|_, v| {
        assert_eq!(v["operation"], "revision_plan");
        Ok(json!({"result":{"chains":[],"token":"empty"}}))
    })
    .unwrap();
    assert_eq!(code, 0);
    assert!(console.prompts.is_empty());
    assert!(console.printed.contains("nothing changed"));
    for bad in [
        vec!["revisions", "--limit", "0"],
        vec!["revisions", "--limit", "257"],
        vec!["revisions", "--limit", "many"],
        vec!["revisions", "--why", "no apply"],
        vec!["recover-revisions"],
        vec!["approve", "proposal", "--why", "review", "--protected", "d"],
    ] {
        let mut values = vec!["--data-dir", "synthetic"];
        values.extend(bad);
        assert!(owner_cli::parse(&argv(&values)).is_err());
    }
}
