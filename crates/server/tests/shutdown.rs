//! Actual signal + HTTP-drain boundary with a real native background unit.
use memento_core::Store;
use rusqlite::Connection;
use serde_json::json;
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}
fn frames(db: &Connection) -> i64 {
    db.query_row(
        "SELECT count(*) FROM episodes WHERE raw_frame IS NOT NULL",
        [],
        |r| r.get(0),
    )
    .unwrap()
}
fn health(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
    if write!(
        stream,
        "GET /health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .is_err()
    {
        return false;
    }
    let mut bytes = [0; 128];
    stream
        .read(&mut bytes)
        .is_ok_and(|n| bytes[..n].starts_with(b"HTTP/1.1 200"))
}
fn seed(path: &Path) {
    let store = Store::open(path).unwrap();
    for n in 0..512 {
        store.capture(serde_json::from_value(json!({"source":"native:synthetic:pi","event_id":format!("shutdown-{n}"),"conversation_id":format!("shutdown-{n}"),"kind":"native_record","text":format!("Synthetic native source {n}"),"metadata":{"extraction_mode":"skip"}})).unwrap()).unwrap();
    }
}
#[test]
fn sigterm_stops_new_background_units_before_existing_http_request_finishes() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("data");
    seed(&data);
    let reserved = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let binary = std::env::var_os("MEMENTO_SHUTDOWN_TEST_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_memento").into());
    let mut child = OwnedChild(
        Command::new(binary)
            .args([
                "--data-dir",
                data.to_str().unwrap(),
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ])
            .env("MEMENTO_EMBEDDINGS", "0")
            .env_remove("MEMENTO_AGENT_UID")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !health(port) {
        assert!(
            Instant::now() < deadline,
            "native daemon did not become healthy"
        );
        assert!(child.0.try_wait().unwrap().is_none());
        thread::sleep(Duration::from_millis(5));
    }
    let mut pending = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(pending,"POST /episodes HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: 100000\r\n\r\n{{").unwrap();
    pending.flush().unwrap();
    // A writer lock holds the CURRENT bounded compression unit at commit. This
    // changes no source row and leaves the HTTP request incomplete across SIGTERM.
    let db = Connection::open(data.join("memory.sqlite")).unwrap();
    db.busy_timeout(Duration::from_secs(2)).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = frames(&db);
    assert!(
        before < 510,
        "fixture was drained before the controlled boundary"
    );
    thread::sleep(Duration::from_millis(50));
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGTERM) }, 0);
    thread::sleep(Duration::from_millis(100));
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "existing HTTP request must still be draining"
    );
    db.execute_batch("ROLLBACK").unwrap();
    thread::sleep(Duration::from_millis(400));
    let after = frames(&db);
    assert_eq!(
        after,
        before + 1,
        "only the unit already in progress may commit while HTTP drains"
    );
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "join/exit must follow HTTP drain"
    );
    pending.shutdown(Shutdown::Both).unwrap();
    drop(pending);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "native shutdown failed to finish after drain"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !data.join("admin.sock").exists(),
        "owned socket cleanup must follow drain"
    );
}
