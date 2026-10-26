use crate::{
    binding::PublicBinding,
    cold::ColdStore,
    drive::DriveColdStore,
    drive_auth::{self, DriveAuthorization, SCOPE},
    fs_safe,
    transport::{Request, Response, Transport},
};
use memento_core::{Error, Result, raw};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn binding() -> PublicBinding {
    PublicBinding {
        client_id: "synthetic-client".into(),
        account: "owner@example.test".into(),
        folder_id: "original_folder".into(),
    }
}
fn connection(data: &Path) {
    let config = json!({"type":"authorized_user","client_id":"synthetic-client","client_secret":"SYNTHETIC CLIENT SECRET","token":"SYNTHETIC TOKEN","refresh_token":"SYNTHETIC REFRESH TOKEN","scopes":[SCOPE],"expiry":"2099-01-01T00:00:00+00:00","token_uri":drive_auth::TOKEN_URI,"memento":{"account":"owner@example.test","folder_id":"original_folder"}});
    fs_safe::atomic_private(
        &data.join("drive-oauth.json"),
        &serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
}
fn bootstrap(data: &Path) {
    let client = data.join("original-client.json");
    fs_safe::atomic_private(&client,&serde_json::to_vec(&json!({"installed":{"client_id":"synthetic-client","client_secret":"SYNTHETIC CLIENT SECRET","auth_uri":drive_auth::AUTH_URI,"token_uri":drive_auth::TOKEN_URI}})).unwrap()).unwrap();
    drive_auth::configure_client(&client, "owner@example.test", "original_folder", data).unwrap();
}
struct Step {
    method: &'static str,
    contains: &'static str,
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    check: Option<Box<dyn Fn(&Request) + Send>>,
}
impl Step {
    fn json(method: &'static str, contains: &'static str, status: u16, value: Value) -> Self {
        Self {
            method,
            contains,
            status,
            headers: BTreeMap::new(),
            body: serde_json::to_vec(&value).unwrap(),
            check: None,
        }
    }
}
struct Script {
    steps: Mutex<VecDeque<Step>>,
    calls: AtomicUsize,
}
impl Script {
    fn new(steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into()),
            calls: AtomicUsize::new(0),
        })
    }
    fn done(&self) {
        assert!(self.steps.lock().unwrap().is_empty());
    }
}
impl Transport for Script {
    fn send(&self, request: Request, out: &mut dyn Write, max: u64) -> Result<Response> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("Unexpected controlled provider call");
        assert_eq!(request.method, step.method);
        assert!(
            request.url.contains(step.contains),
            "Provider path contract"
        );
        if let Some(check) = step.check {
            check(&request)
        }
        if step.status == 0 {
            return Err(Error::Unavailable("Controlled interrupted transfer"));
        }
        assert!(step.body.len() as u64 <= max);
        out.write_all(&step.body)?;
        Ok(Response {
            status: step.status,
            headers: step.headers,
        })
    }
}
fn verification() -> Vec<Step> {
    vec![
        Step::json(
            "GET",
            "/about?",
            200,
            json!({"user":{"emailAddress":"OWNER@example.test"}}),
        ),
        Step::json(
            "GET",
            "/files/original_folder?",
            200,
            json!({"id":"original_folder","mimeType":"application/vnd.google-apps.folder","ownedByMe":true,"trashed":false}),
        ),
    ]
}
fn token() -> Step {
    Step::json(
        "POST",
        "oauth2.googleapis.com/token",
        200,
        json!({"access_token":"SYNTHETIC ACCESS","refresh_token":"SYNTHETIC REFRESH","expires_in":3600,"scope":SCOPE,"token_type":"Bearer"}),
    )
}
#[test]
fn oauth_pkce_wrong_state_single_use_and_original_binding_publication() {
    let data = private();
    bootstrap(data.path());
    let mut steps = vec![token()];
    steps.extend(verification());
    let provider = Script::new(steps);
    let installed = Arc::new(AtomicUsize::new(0));
    let count = installed.clone();
    let path = data.path().to_owned();
    let authorization = DriveAuthorization::new(
        data.path().to_owned(),
        provider.clone(),
        Arc::new(move |_backend, grant, reconnect| {
            assert!(!reconnect);
            grant.publish(&path)?;
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );
    let begun = authorization.begin(8766, false).unwrap();
    assert!(begun.authorization_url.starts_with(drive_auth::AUTH_URI));
    for expected in [
        "code_challenge_method=S256",
        "prompt=consent",
        "include_granted_scopes=false",
        "127.0.0.1%3A8766",
        "drive.file",
    ] {
        assert!(begun.authorization_url.contains(expected));
    }
    assert!(!begun.authorization_url.contains("SYNTHETIC CLIENT SECRET"));
    assert_eq!(authorization.begin(8766, false).unwrap_err().status, 409);
    assert_eq!(
        authorization
            .finish("wrong-state-but-valid-length", Some("synthetic-code"), None)
            .unwrap_err()
            .status,
        400
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        authorization
            .finish(&begun.state, Some("synthetic-code"), None)
            .unwrap()["status"],
        "connected"
    );
    assert_eq!(installed.load(Ordering::SeqCst), 1);
    assert_eq!(
        authorization
            .finish(&begun.state, Some("synthetic-code"), None)
            .unwrap_err()
            .status,
        410
    );
    provider.done();
    let grant = drive_auth::ConnectionGrant::load(data.path()).unwrap();
    assert!(grant.public_binding().matches(&binding()));
}
#[test]
fn broader_scope_wrong_account_changed_pin_and_provider_errors_never_install() {
    for kind in ["scope", "account", "folder", "provider", "binding"] {
        let data = private();
        bootstrap(data.path());
        connection(data.path());
        let before = fs::read(data.path().join("drive-oauth.json")).unwrap();
        let mut first = token();
        let mut steps = Vec::new();
        match kind{
            "scope"=>first.body=serde_json::to_vec(&json!({"access_token":"DO NOT REFLECT SECRET","refresh_token":"DO NOT REFLECT REFRESH","expires_in":3600,"scope":format!("{SCOPE} https://www.googleapis.com/auth/drive")})).unwrap(),
            "provider"=>{first.status=500;first.body=serde_json::to_vec(&json!({"error_description":"DO NOT REFLECT SECRET"})).unwrap();},
            "binding"=>{let path=data.path().to_owned();first.check=Some(Box::new(move|_|{let mut different=binding();different.folder_id="changed_folder".into();fs_safe::atomic_private(&path.join("drive-binding.json"),&serde_json::to_vec(&different).unwrap()).unwrap();}));},
            _=>(),
        }
        steps.push(first);
        if !["scope", "provider", "binding"].contains(&kind) {
            let mut verify = verification();
            if kind == "account" {
                verify[0].body =
                    serde_json::to_vec(&json!({"user":{"emailAddress":"other@example.test"}}))
                        .unwrap();
                verify.pop();
            }
            if kind == "folder" {
                verify[1].body=serde_json::to_vec(&json!({"id":"original_folder","mimeType":"application/vnd.google-apps.folder","ownedByMe":false,"trashed":false})).unwrap();
            }
            steps.extend(verify);
        }
        let provider = Script::new(steps);
        let installed = Arc::new(AtomicUsize::new(0));
        let count = installed.clone();
        let authorization = DriveAuthorization::new(
            data.path().to_owned(),
            provider.clone(),
            Arc::new(move |_, _, _| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        );
        let begun = authorization.begin(8766, true).unwrap();
        let result = authorization.finish(&begun.state, Some("synthetic-code"), None);
        assert!(result.is_err(), "{kind}");
        assert!(!format!("{:?}", result.err().unwrap()).contains("SECRET"));
        assert_eq!(installed.load(Ordering::SeqCst), 0);
        assert_eq!(
            fs::read(data.path().join("drive-oauth.json")).unwrap(),
            before
        );
        provider.done();
    }
}
#[test]
fn cancellation_consumes_state_close_disables_and_private_files_are_enforced() {
    let data = private();
    bootstrap(data.path());
    let provider = Script::new(vec![]);
    let authorization = DriveAuthorization::new(
        data.path().to_owned(),
        provider.clone(),
        Arc::new(|_, _, _| panic!("No installation")),
    );
    let begun = authorization.begin(8766, false).unwrap();
    assert_eq!(
        authorization
            .finish(&begun.state, None, Some("access_denied"))
            .unwrap_err()
            .status,
        400
    );
    assert_eq!(
        authorization
            .finish(&begun.state, None, Some("access_denied"))
            .unwrap_err()
            .status,
        410
    );
    authorization.close();
    assert_eq!(authorization.begin(8766, false).unwrap_err().status, 503);
    fs::set_permissions(
        data.path().join("drive-client.json"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(drive_auth::load_bindings(data.path()).is_err());
    provider.done();
}
#[test]
fn restored_pin_blocks_changed_identity_before_any_provider_call() {
    for field in ["client", "account", "folder"] {
        let data = private();
        bootstrap(data.path());
        let mut pin = binding();
        match field {
            "client" => pin.client_id = "different-client".into(),
            "account" => pin.account = "different@example.test".into(),
            _ => pin.folder_id = "different_folder".into(),
        };
        fs_safe::atomic_private(
            &data.path().join("drive-binding.json"),
            &serde_json::to_vec(&pin).unwrap(),
        )
        .unwrap();
        let provider = Script::new(vec![]);
        let authorization = DriveAuthorization::new(
            data.path().to_owned(),
            provider.clone(),
            Arc::new(|_, _, _| panic!()),
        );
        assert_eq!(authorization.begin(8766, false).unwrap_err().status, 409);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
}
#[test]
fn resumable_lost_ack_queries_server_and_verifies_before_checkpoint() {
    let data = private();
    connection(data.path());
    let content = vec![0x41; 300_000];
    let path = data.path().join("original.bin");
    fs::write(&path, &content).unwrap();
    let (sha, md5, size) = crate::drive::hashes(&path).unwrap();
    let mut steps = verification();
    let mut start = Step::json("POST", "uploadType=resumable", 200, Value::Null);
    start.headers.insert(
        "location".into(),
        "https://www.googleapis.com/upload/drive/v3/files?upload_id=synthetic".into(),
    );
    steps.push(start);
    steps.push(Step::json("PUT", "upload_id=synthetic", 0, Value::Null));
    let mut status = Step::json("PUT", "upload_id=synthetic", 308, Value::Null);
    status
        .headers
        .insert("range".into(), "bytes=0-262143".into());
    status.check = Some(Box::new(move |r| {
        assert!(r.body.is_empty());
        assert!(
            r.headers
                .iter()
                .any(|(k, v)| k == "Content-Range" && v == "bytes */300000")
        );
    }));
    steps.push(status);
    let mut finish = Step::json(
        "PUT",
        "upload_id=synthetic",
        200,
        json!({"id":"verified_object"}),
    );
    finish.check = Some(Box::new(|r| {
        assert_eq!(r.body.len(), 37_856);
        assert!(
            r.headers
                .iter()
                .any(|(k, v)| k == "Content-Range" && v == "bytes 262144-299999/300000")
        );
    }));
    steps.push(finish);
    steps.push(Step::json("GET","/files/verified_object?",200,json!({"id":"verified_object","size":size.to_string(),"md5Checksum":md5,"sha256Checksum":sha})));
    let provider = Script::new(steps);
    let drive = DriveColdStore::from_file(data.path().to_owned(), provider.clone()).unwrap();
    let mut checkpoint = json!({});
    assert!(
        drive
            .upload(
                &path,
                "original",
                "application/octet-stream",
                &checkpoint.clone(),
                &mut |v| {
                    checkpoint = v;
                    Ok(())
                }
            )
            .is_err()
    );
    assert!(checkpoint.get("object_id").is_none());
    let object = drive
        .upload(
            &path,
            "original",
            "application/octet-stream",
            &checkpoint.clone(),
            &mut |v| {
                checkpoint = v;
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(object.sha256, sha);
    assert_eq!(checkpoint["object_id"], "verified_object");
    provider.done();
}
#[test]
fn upload_checksum_mismatch_and_external_session_keep_unverified_checkpoint() {
    for kind in ["md5", "sha", "session"] {
        let data = private();
        connection(data.path());
        let path = data.path().join("original");
        fs::write(&path, b"original").unwrap();
        let (sha, md5, size) = crate::drive::hashes(&path).unwrap();
        let mut steps = verification();
        let mut start = Step::json("POST", "uploadType=resumable", 200, Value::Null);
        start.headers.insert(
            "location".into(),
            if kind == "session" {
                "https://attacker.example.test/stolen"
            } else {
                "https://www.googleapis.com/upload/drive/v3/files?upload_id=synthetic"
            }
            .into(),
        );
        steps.push(start);
        if kind != "session" {
            steps.push(Step::json(
                "PUT",
                "upload_id=synthetic",
                200,
                json!({"id":"uploaded"}),
            ));
            steps.push(Step::json("GET","/files/uploaded?",200,json!({"id":"uploaded","size":size.to_string(),"md5Checksum":if kind=="md5"{"bad"}else{&md5},"sha256Checksum":if kind=="sha"{"bad"}else{&sha}})));
        }
        let provider = Script::new(steps);
        let drive = DriveColdStore::from_file(data.path().to_owned(), provider.clone()).unwrap();
        let mut checkpoint = json!({});
        assert!(
            drive
                .upload(
                    &path,
                    "original",
                    "application/octet-stream",
                    &json!({}),
                    &mut |v| {
                        checkpoint = v;
                        Ok(())
                    }
                )
                .is_err()
        );
        assert!(checkpoint.get("object_id").is_none());
        provider.done();
    }
}
#[test]
fn downloaded_corruption_never_replaces_previous_destination() {
    let data = private();
    connection(data.path());
    let mut steps = verification();
    let mut download = Step::json("GET", "?alt=media", 200, Value::Null);
    download.body = b"corrupt!".to_vec();
    steps.push(download);
    let provider = Script::new(steps);
    let drive = DriveColdStore::from_file(data.path().to_owned(), provider.clone()).unwrap();
    let path = data.path().join("destination");
    fs::write(&path, b"original").unwrap();
    assert!(
        drive
            .download("original_object", &path, &raw::sha256(b"original"), 8)
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), b"original");
    provider.done();
}
#[test]
fn token_refresh_retains_exact_scope_and_only_structured_rejection_requests_reauthorization() {
    for kind in [
        "success",
        "invalid_grant",
        "invalid_rapt",
        "provider",
        "untyped-message",
        "broader-scope",
    ] {
        let data = private();
        connection(data.path());
        let config_path = data.path().join("drive-oauth.json");
        let mut config: Value = serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
        config["expiry"] = json!("2000-01-01T00:00:00+00:00");
        fs_safe::atomic_private(&config_path, &serde_json::to_vec(&config).unwrap()).unwrap();
        let before = fs::read(&config_path).unwrap();
        let mut step = Step::json(
            "POST",
            "oauth2.googleapis.com/token",
            200,
            json!({"access_token":"SYNTHETIC NEW ACCESS","expires_in":3600,"scope":SCOPE}),
        );
        match kind {
            "invalid_grant" | "invalid_rapt" => {
                step.status = 400;
                step.body = serde_json::to_vec(&json!({"error":kind})).unwrap();
            }
            "provider" => {
                step.status = 503;
                step.body = serde_json::to_vec(&json!({"error":"invalid_grant"})).unwrap();
            }
            "untyped-message" => {
                step.status = 400;
                step.body = serde_json::to_vec(
                    &json!({"error_description":"invalid_grant DO NOT REFLECT"}),
                )
                .unwrap();
            }
            "broader-scope" => {
                step.body=serde_json::to_vec(&json!({"access_token":"SYNTHETIC NEW ACCESS","expires_in":3600,"scope":format!("{SCOPE} another")})).unwrap();
            }
            _ => (),
        }
        step.check = Some(Box::new(|request| {
            let form = std::str::from_utf8(&request.body).unwrap();
            assert!(form.contains("grant_type=refresh_token"));
            assert!(!request.url.contains("SYNTHETIC"));
        }));
        let mut steps = vec![step];
        if kind == "success" {
            steps.extend(verification());
        }
        let provider = Script::new(steps);
        let result = DriveColdStore::from_file(data.path().to_owned(), provider.clone());
        if kind == "success" {
            let drive = result.unwrap();
            assert!(drive.binding().matches(&binding()));
        } else {
            let error = result.err().unwrap();
            assert_eq!(
                drive_auth::requires_reauthorization(&error),
                ["invalid_grant", "invalid_rapt"].contains(&kind)
            );
            assert!(!error.to_string().contains("SYNTHETIC"));
            assert_eq!(fs::read(&config_path).unwrap(), before);
        }
        provider.done();
    }
}
#[test]
fn ranged_download_is_bounded_and_exact_before_atomic_publication() {
    let data = private();
    connection(data.path());
    let mut content = vec![b'A'; 8 * 1024 * 1024 + 37];
    content[8 * 1024 * 1024..].fill(b'B');
    let sha = raw::sha256(&content);
    let mut steps = verification();
    for (start, end) in [(0, 8 * 1024 * 1024), (8 * 1024 * 1024, content.len())] {
        let mut step = Step::json("GET", "?alt=media", 206, Value::Null);
        step.body = content[start..end].to_vec();
        step.headers.insert(
            "content-range".into(),
            format!("bytes {start}-{}/{}", end - 1, content.len()),
        );
        step.check = Some(Box::new(move |request| {
            assert!(
                request
                    .headers
                    .iter()
                    .any(|(k, v)| k == "Range" && v == &format!("bytes={start}-{}", end - 1))
            );
        }));
        steps.push(step);
    }
    let provider = Script::new(steps);
    let drive = DriveColdStore::from_file(data.path().to_owned(), provider.clone()).unwrap();
    let destination = data.path().join("downloaded");
    drive
        .download("large_original", &destination, &sha, content.len() as u64)
        .unwrap();
    assert_eq!(fs::read(&destination).unwrap(), content);
    provider.done();
}
#[test]
fn missing_remote_sha_downloads_and_hashes_instead_of_trusting_md5_alone() {
    let data = private();
    connection(data.path());
    let original = b"original verified media";
    let path = data.path().join("original");
    fs::write(&path, original).unwrap();
    let (sha, md5, size) = crate::drive::hashes(&path).unwrap();
    let mut steps = verification();
    steps.push(Step::json(
        "GET",
        "/files/already_uploaded?",
        200,
        json!({"id":"already_uploaded","size":size.to_string(),"md5Checksum":md5}),
    ));
    let mut download = Step::json("GET", "?alt=media", 200, Value::Null);
    download.body = original.to_vec();
    steps.push(download);
    let provider = Script::new(steps);
    let drive = DriveColdStore::from_file(data.path().to_owned(), provider.clone()).unwrap();
    let object = drive
        .upload(
            &path,
            "original",
            "application/octet-stream",
            &json!({"object_id":"already_uploaded","sha256":sha,"size":size}),
            &mut |_| panic!("Existing verified checkpoint needs no new copy"),
        )
        .unwrap();
    assert_eq!(object.sha256, sha);
    provider.done();
}
#[test]
fn failed_install_preserves_previous_grant_and_mismatched_desktop_secret_blocks_export() {
    let data = private();
    bootstrap(data.path());
    connection(data.path());
    let old = fs::read(data.path().join("drive-oauth.json")).unwrap();
    let mut steps = vec![token()];
    steps.extend(verification());
    let provider = Script::new(steps);
    let authorization = DriveAuthorization::new(
        data.path().to_owned(),
        provider.clone(),
        Arc::new(|_, _, _| Err(Error::Unavailable("Controlled installation failure"))),
    );
    let begin = authorization.begin(8766, true).unwrap();
    assert_eq!(
        authorization
            .finish(&begin.state, Some("synthetic-code"), None)
            .unwrap_err()
            .status,
        502
    );
    assert_eq!(fs::read(data.path().join("drive-oauth.json")).unwrap(), old);
    provider.done();
    let file = data.path().join("drive-client.json");
    let mut client: Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
    client["installed"]["client_secret"] = json!("DIFFERENT SYNTHETIC SECRET");
    fs_safe::atomic_private(&file, &serde_json::to_vec(&client).unwrap()).unwrap();
    assert!(crate::binding::export_binding(data.path()).is_err());
}
#[test]
fn account_binding_keeps_python_unicode_casefold_and_exact_original_client_folder() {
    let mut original = binding();
    original.account = "Straße-Σ@example.test".into();
    let mut same = original.clone();
    same.account = "STRASSE-ς@example.test".into();
    assert!(original.matches(&same));
    same.folder_id = "another_folder".into();
    assert!(!original.matches(&same));
    same.folder_id = original.folder_id.clone();
    same.client_id = "another-client".into();
    assert!(!original.matches(&same));
}
