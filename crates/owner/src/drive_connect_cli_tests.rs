use super::*;
use memento_maintenance::{
    drive_auth::{AUTH_URI, SCOPE, TOKEN_URI},
    fs_safe,
    transport::{Request, Response},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

struct Provider {
    calls: AtomicUsize,
}
impl Provider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
        })
    }
}
impl Transport for Provider {
    fn send(&self, request: Request, out: &mut dyn Write, _: u64) -> Result<Response> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let value: Value = match call {
            0 => {
                assert_eq!(request.method, "POST");
                assert_eq!(request.url, TOKEN_URI);
                json!({"access_token":"SYNTHETIC ACCESS","refresh_token":"SYNTHETIC REFRESH","scope":SCOPE,"expires_in":3600,"token_type":"Bearer"})
            }
            1 | 2 => {
                assert_eq!(request.method, "GET");
                assert!(request.url.contains("/about?"));
                json!({"user":{"emailAddress":"owner@example.test"}})
            }
            3 => {
                assert_eq!(request.method, "GET");
                assert!(request.url.contains("/files/original_folder?"));
                json!({"id":"original_folder","mimeType":"application/vnd.google-apps.folder","ownedByMe":true,"trashed":false})
            }
            _ => panic!("Unexpected controlled provider call"),
        };
        out.write_all(&serde_json::to_vec(&value)?)?;
        Ok(Response {
            status: 200,
            headers: BTreeMap::new(),
        })
    }
}
fn fixture(no_browser: bool) -> (tempfile::TempDir, Arguments) {
    let dir = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let client = dir.path().join("client.json");
    fs_safe::atomic_private(&client,&serde_json::to_vec(&json!({"installed":{"client_id":"original-client","client_secret":"SYNTHETIC SECRET","auth_uri":AUTH_URI,"token_uri":TOKEN_URI}})).unwrap()).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let options = ConnectOptions {
        client_json: client,
        account: "owner@example.test".into(),
        output: dir.path().join("connection.json"),
        port,
        folder_id: Some("original_folder".into()),
        data_dir: None,
    };
    (
        dir,
        Arguments {
            options,
            no_browser,
        },
    )
}
fn request(port: u16, target: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(stream, "GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").unwrap();
    let mut output = String::new();
    stream.read_to_string(&mut output).unwrap();
    output
}
fn state(url: &str) -> String {
    desktop_connect::callback(&format!("/?{}", url.split_once('?').unwrap().1))
        .unwrap()
        .state
}
#[test]
fn parser_preserves_explicit_options_defaults_and_rejects_bad_ports() {
    let strings = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let args = strings(&[
        "--client-json=client.json",
        "--output",
        "connection.json",
        "--account",
        "owner@example.test",
        "--no-browser",
        "--folder-id",
        "original_folder",
        "--data-dir",
        "restored",
    ]);
    let parsed = parse(&args).unwrap();
    assert_eq!(parsed.options.port, 8766);
    assert!(parsed.no_browser);
    assert_eq!(parsed.options.folder_id.as_deref(), Some("original_folder"));
    assert_eq!(parsed.options.data_dir, Some(PathBuf::from("restored")));
    for port in ["0", "65536", "-1", "abc"] {
        let mut changed = args.clone();
        changed.extend(strings(&["--port", port]));
        assert!(parse(&changed).is_err());
    }
    assert!(parse(&strings(&["--account", "owner@example.test"])).is_err());
    assert!(parse(&strings(&["--unknown", "SYNTHETIC SECRET"])).is_err());
}
#[test]
fn loopback_rejects_wrong_state_then_accepts_cancel_without_provider_contact() {
    let (_dir, args) = fixture(false);
    let output = args.options.output.clone();
    let port = args.options.port;
    let provider = Provider::new();
    let browser = AtomicUsize::new(0);
    let handle = Mutex::new(None);
    let result = connect(
        args,
        provider.clone(),
        |url| {
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
            let state = state(url);
            *handle.lock().unwrap() = Some(thread::spawn(move || {
                let wrong = request(port, "/?state=unrelated-long-state&code=SYNTHETIC_CODE");
                assert!(wrong.starts_with("HTTP/1.1 400"));
                assert!(!wrong.contains("SYNTHETIC_CODE"));
                let wrong_path =
                    request(port, &format!("/other?state={state}&code=SYNTHETIC_CODE"));
                assert!(wrong_path.starts_with("HTTP/1.1 400"));
                let good = request(port, &format!("/?state={state}&error=access_denied"));
                assert!(good.starts_with("HTTP/1.1 200"));
                assert!(good.contains("Cache-Control: no-store"));
                assert!(!good.contains(&state));
            }));
            Ok(())
        },
        |_| {
            browser.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );
    handle.into_inner().unwrap().unwrap().join().unwrap();
    assert!(result.is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(browser.load(Ordering::SeqCst), 1);
    assert!(!output.exists());
}
#[test]
fn no_browser_success_requires_matching_human_callback_before_verified_publication() {
    let (_dir, args) = fixture(true);
    let port = args.options.port;
    let output = args.options.output.clone();
    let provider = Provider::new();
    let browser = AtomicUsize::new(0);
    let handle = Mutex::new(None);
    let result = connect(
        args,
        provider.clone(),
        |url| {
            assert!(url.starts_with(AUTH_URI));
            assert!(!url.contains("SYNTHETIC"));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
            assert!(!output.exists());
            let state = state(url);
            *handle.lock().unwrap() = Some(thread::spawn(move || {
                let response = request(port, &format!("/?state={state}&code=human-approved-code"));
                assert!(response.starts_with("HTTP/1.1 200"));
                assert!(!response.contains("human-approved-code"));
            }));
            Ok(())
        },
        |_| {
            browser.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .unwrap();
    handle.into_inner().unwrap().unwrap().join().unwrap();
    assert_eq!(result.binding.folder_id, "original_folder");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
    assert_eq!(browser.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::metadata(&output).unwrap().permissions().mode() & 0o777,
        0o600
    );
}
#[test]
fn occupied_loopback_port_and_notice_failure_never_invoke_browser_or_provider() {
    for occupied in [true, false] {
        let (_dir, args) = fixture(false);
        let listener = if occupied {
            Some(TcpListener::bind(("127.0.0.1", args.options.port)).unwrap())
        } else {
            None
        };
        let provider = Provider::new();
        let browser = AtomicUsize::new(0);
        let result = connect(
            args,
            provider.clone(),
            |_| Err(Error::Unavailable("Synthetic UI failure")),
            |_| {
                browser.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(browser.load(Ordering::SeqCst), 0);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        drop(listener);
    }
}
