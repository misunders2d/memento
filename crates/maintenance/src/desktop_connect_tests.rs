use super::*;
use crate::transport::{Request, Response};
use std::{
    collections::{BTreeMap, VecDeque},
    io::Write,
    os::unix::fs::PermissionsExt,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn fixture() -> (tempfile::TempDir, ConnectOptions) {
    let dir = private();
    let client = dir.path().join("client.json");
    fs_safe::atomic_private(&client,&serde_json::to_vec(&json!({"installed":{"client_id":"original-client","client_secret":"SYNTHETIC CLIENT SECRET","auth_uri":AUTH_URI,"token_uri":TOKEN_URI}})).unwrap()).unwrap();
    let options = ConnectOptions {
        client_json: client,
        account: "owner@example.test".into(),
        output: dir.path().join("connection.json"),
        folder_id: None,
        data_dir: None,
        port: 8766,
    };
    (dir, options)
}
struct Step {
    method: &'static str,
    path: &'static str,
    status: u16,
    value: Value,
}
struct Provider {
    steps: Mutex<VecDeque<Step>>,
    calls: AtomicUsize,
}
impl Provider {
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
impl Transport for Provider {
    fn send(&self, request: Request, out: &mut dyn Write, _: u64) -> Result<Response> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("Unexpected controlled provider request");
        assert_eq!(request.method, step.method);
        assert!(request.url.contains(step.path));
        assert!(!request.url.contains("SYNTHETIC"));
        out.write_all(&serde_json::to_vec(&step.value)?)?;
        Ok(Response {
            status: step.status,
            headers: BTreeMap::new(),
        })
    }
}
fn step(method: &'static str, path: &'static str, value: Value) -> Step {
    Step {
        method,
        path,
        status: 200,
        value,
    }
}
fn token() -> Step {
    step(
        "POST",
        "oauth2.googleapis.com/token",
        json!({"access_token":"SYNTHETIC ACCESS","refresh_token":"SYNTHETIC REFRESH","expires_in":3600,"scope":SCOPE,"token_type":"Bearer"}),
    )
}
fn about() -> Step {
    step(
        "GET",
        "/about?",
        json!({"user":{"emailAddress":"OWNER@example.test"}}),
    )
}
fn folder() -> Step {
    step(
        "GET",
        "/files/original_folder?",
        json!({"id":"original_folder","mimeType":"application/vnd.google-apps.folder","ownedByMe":true,"trashed":false}),
    )
}
fn provider(create: bool) -> Arc<Provider> {
    let mut steps = vec![token(), about()];
    if create {
        steps.push(step(
            "POST",
            "/files?fields=id",
            json!({"id":"original_folder"}),
        ));
    }
    steps.extend([about(), folder()]);
    Provider::new(steps)
}
fn state(p: &PendingConnect) -> String {
    callback(&format!(
        "/?{}",
        p.authorization_url().split_once('?').unwrap().1
    ))
    .unwrap()
    .state
}
#[test]
fn explicit_downloaded_client_and_existing_public_parent_publish_private_connection() {
    let (dir, options) = fixture();
    fs::set_permissions(&options.client_json, fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let mut pending = PendingConnect::begin(options.clone()).unwrap();
    pending
        .finish(&state(&pending), Some("code"), None, provider(true))
        .unwrap();
    assert_eq!(
        fs::metadata(&options.output).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert!(ConnectionGrant::load_file(&options.output, None).is_ok());
}
#[test]
fn first_grant_creates_once_reconnect_reuses_same_folder_and_preserves_private_file() {
    let (dir, options) = fixture();
    let mut first = PendingConnect::begin(options.clone()).unwrap();
    assert!(
        first
            .authorization_url()
            .contains("code_challenge_method=S256")
    );
    assert!(
        first
            .authorization_url()
            .contains("include_granted_scopes=false")
    );
    assert!(!first.authorization_url().contains("SYNTHETIC"));
    let first_provider = provider(true);
    let result = first
        .finish(
            &state(&first),
            Some("synthetic-code"),
            None,
            first_provider.clone(),
        )
        .unwrap();
    assert_eq!(result.binding.folder_id, "original_folder");
    assert_eq!(
        fs::metadata(&result.output).unwrap().permissions().mode() & 0o777,
        0o600
    );
    first_provider.done();
    let previous = fs::read(&options.output).unwrap();
    let mut second = PendingConnect::begin(options.clone()).unwrap();
    assert_eq!(second.folder.as_deref(), Some("original_folder"));
    let reconnect = provider(false);
    second
        .finish(
            &state(&second),
            Some("synthetic-code"),
            None,
            reconnect.clone(),
        )
        .unwrap();
    reconnect.done();
    assert!(
        ConnectionGrant::load_file(&options.output, None)
            .unwrap()
            .binding
            .public
            .matches(&result.binding)
    );
    assert!(
        String::from_utf8(previous)
            .unwrap()
            .contains("SYNTHETIC REFRESH")
    );
    assert!(dir.path().exists());
}
#[test]
fn wrong_state_retains_pending_matching_cancel_or_malformed_consumes_without_network() {
    let (_dir, options) = fixture();
    let mut pending = PendingConnect::begin(options.clone()).unwrap();
    let provider = Provider::new(vec![]);
    assert!(
        pending
            .finish(
                "wrong-but-long-enough-state",
                Some("code"),
                None,
                provider.clone()
            )
            .is_err()
    );
    assert!(!pending.consumed);
    let matching_state = state(&pending);
    assert!(
        pending
            .finish(
                &matching_state,
                None,
                Some("access_denied"),
                provider.clone()
            )
            .is_err()
    );
    assert!(pending.consumed);
    assert!(
        pending
            .finish(&matching_state, Some("code"), None, provider.clone())
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let mut malformed = PendingConnect::begin(options).unwrap();
    assert!(
        malformed
            .finish(
                &state(&malformed),
                Some("code"),
                Some("access_denied"),
                provider.clone()
            )
            .is_err()
    );
    assert!(malformed.consumed);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}
#[test]
fn wrong_account_or_scope_never_creates_folder_or_publishes() {
    for kind in ["account", "scope", "refresh"] {
        let (_dir, options) = fixture();
        let mut pending = PendingConnect::begin(options.clone()).unwrap();
        let mut first = token();
        let steps = match kind {
            "account" => vec![
                first,
                step(
                    "GET",
                    "/about?",
                    json!({"user":{"emailAddress":"other@example.test"}}),
                ),
            ],
            "scope" => {
                first.value["scope"] =
                    json!(format!("{SCOPE} https://www.googleapis.com/auth/drive"));
                vec![first]
            }
            _ => {
                first.value.as_object_mut().unwrap().remove("refresh_token");
                vec![first]
            }
        };
        let provider = Provider::new(steps);
        assert!(
            pending
                .finish(
                    &state(&pending),
                    Some("synthetic-code"),
                    None,
                    provider.clone()
                )
                .is_err()
        );
        assert!(!options.output.exists());
        provider.done();
    }
}
#[test]
fn restored_pin_reuses_original_folder_and_rejects_new_binding_before_consent() {
    let (dir, mut options) = fixture();
    let pin = PublicBinding {
        client_id: "original-client".into(),
        account: "owner@example.test".into(),
        folder_id: "original_folder".into(),
    };
    fs_safe::atomic_private(
        &dir.path().join("drive-binding.json"),
        &serde_json::to_vec(&pin).unwrap(),
    )
    .unwrap();
    options.data_dir = Some(dir.path().to_path_buf());
    let mut pending = PendingConnect::begin(options.clone()).unwrap();
    assert_eq!(pending.folder.as_deref(), Some("original_folder"));
    let provider = provider(false);
    pending
        .finish(
            &state(&pending),
            Some("synthetic-code"),
            None,
            provider.clone(),
        )
        .unwrap();
    provider.done();
    for field in ["account", "folder", "client"] {
        let mut changed = options.clone();
        match field {
            "account" => changed.account = "other@example.test".into(),
            "folder" => changed.folder_id = Some("replacement_folder".into()),
            _ => {
                let path = dir.path().join("changed-client.json");
                fs_safe::atomic_private(&path,&serde_json::to_vec(&json!({"installed":{"client_id":"replacement-client","client_secret":"SYNTHETIC","auth_uri":AUTH_URI,"token_uri":TOKEN_URI}})).unwrap()).unwrap();
                changed.client_json = path;
            }
        }
        assert!(PendingConnect::begin(changed).is_err());
    }
}
#[test]
fn original_input_change_and_expiry_never_replace_previous_output() {
    let (_dir, options) = fixture();
    let mut first = PendingConnect::begin(options.clone()).unwrap();
    first
        .finish(&state(&first), Some("code"), None, provider(true))
        .unwrap();
    let previous = fs::read(&options.output).unwrap();
    let mut pending = PendingConnect::begin(options.clone()).unwrap();
    pending.deadline = Instant::now();
    assert!(
        pending
            .finish(&state(&pending), Some("code"), None, Provider::new(vec![]))
            .is_err()
    );
    assert_eq!(fs::read(&options.output).unwrap(), previous);
    let mut pending = PendingConnect::begin(options.clone()).unwrap();
    fs_safe::atomic_private(&options.client_json, b"changed configuration").unwrap();
    assert!(
        pending
            .finish(&state(&pending), Some("code"), None, Provider::new(vec![]))
            .is_err()
    );
    assert_eq!(fs::read(&options.output).unwrap(), previous);
}
#[test]
fn unsuitable_folder_never_replaces_working_grant() {
    let (_dir, options) = fixture();
    let mut first = PendingConnect::begin(options.clone()).unwrap();
    first
        .finish(&state(&first), Some("code"), None, provider(true))
        .unwrap();
    let before = fs::read(&options.output).unwrap();
    let mut pending = PendingConnect::begin(options.clone()).unwrap();
    let mut wrong = folder();
    wrong.value["ownedByMe"] = json!(false);
    let provider = Provider::new(vec![token(), about(), about(), wrong]);
    assert!(
        pending
            .finish(&state(&pending), Some("code"), None, provider.clone())
            .is_err()
    );
    assert_eq!(fs::read(&options.output).unwrap(), before);
    provider.done();
}
#[test]
fn callback_parser_rejects_duplicates_bad_encoding_and_wrong_path() {
    let value = callback("/?state=long-state-value&code=one%2Btwo%3Dthree").unwrap();
    assert_eq!(value.code.as_deref(), Some("one+two=three"));
    for input in [
        "/?state=a&state=b&code=c",
        "/else?state=a&code=b",
        "/?code=c",
        "/?state=a&code=%GG",
    ] {
        assert!(callback(input).is_err());
    }
}
