//! Fixed public model distribution. Original identities/FP32 pins are unchanged.
use memento_core::{Error, Result};
use memento_maintenance::transport::{Request, Transport};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};
const CHUNK: u64 = 8 * 1024 * 1024;
pub(crate) struct Asset<'a> {
    pub(crate) path: &'a str,
    pub(crate) url: &'a str,
    pub(crate) bytes: u64,
    pub(crate) sha: &'a str,
}
const ASSETS: &[Asset<'static>] = &[
    Asset {
        path: "onnx/model.onnx",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/onnx/model.onnx",
        bytes: 421421,
        sha: "bc47de15f81208a5c99e5ab10f746d5e33b51ea228b7dc0bef9c133a94f1c1c3",
    },
    Asset {
        path: "onnx/vision_encoder.onnx",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/onnx/vision_encoder.onnx",
        bytes: 121253,
        sha: "a3fb44f8b5b152cfee6afe16e2cd69e774b7e1d3b822d1db1190ddf7b08b575e",
    },
    Asset {
        path: "onnx/audio_encoder.onnx",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/onnx/audio_encoder.onnx",
        bytes: 204577,
        sha: "e0a62590c49949fe0b1410f5dc52c92a185abb4c4e69e2c50a4e316103fdbc09",
    },
    Asset {
        path: "onnx/model.onnx_data",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/onnx/model.onnx_data",
        bytes: 1084170240,
        sha: "9fd452bfc6916e5a92f080f8dada35598ca4512b21c65897d4c5974d5f9630a2",
    },
    Asset {
        path: "onnx/vision_encoder.onnx_data",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/onnx/vision_encoder.onnx_data",
        bytes: 671026176,
        sha: "ca8d28c99feb8129618f141978a27adae25090e0dadb68a30c235e191c01e37e",
    },
    Asset {
        path: "onnx/audio_encoder.onnx_data",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/onnx/audio_encoder.onnx_data",
        bytes: 1172750848,
        sha: "6943b910d1d1e9b5dffd4ccad63f61aa308ad10b70ed128bb61886b119da1e3a",
    },
    Asset {
        path: "tokenizer.json",
        url: "https://huggingface.co/google/embeddinggemma-2/resolve/914f7f89142e33e77833254d9c9b90c3cef7303b/tokenizer.json",
        bytes: 32170510,
        sha: "4d777ef5bdc1aa36227abdfb77c3e49e7b9c892d16e1b6bda41c393504828be4",
    },
    Asset {
        path: "MODEL_CARD.md",
        url: "https://huggingface.co/onnx-community/embeddinggemma-2-ONNX/resolve/daa72c51243991dfcaf9f9137d2c573d8f7790c0/README.md",
        bytes: 28720,
        sha: "4fe21bf1b86938ac62c7cd2d1511822e4ea6e8654e015e00b06a72676b5420e6",
    },
];
fn unavailable() -> Error {
    Error::Unavailable("Pinned model asset download is unavailable")
}
fn stopped(stop: &AtomicBool) -> Result<()> {
    if stop.load(Ordering::Acquire) {
        Err(Error::Unavailable("Model asset preparation stopped"))
    } else {
        Ok(())
    }
}
fn trusted(url: &str) -> bool {
    let Ok(uri) = url.parse::<ureq::http::Uri>() else {
        return false;
    };
    let Some(authority) = uri.authority() else {
        return false;
    };
    if uri.scheme_str() != Some("https")
        || authority.as_str().contains('@')
        || authority.port_u16().is_some_and(|p| p != 443)
    {
        return false;
    }
    let Some((host, port)) = crate::host_parts(authority.as_str()) else {
        return false;
    };
    if port.is_some_and(|p| p != 443) {
        return false;
    }
    host == "huggingface.co" || host.ends_with(".hf.co")
}
fn request(
    transport: &dyn Transport,
    asset: &Asset<'_>,
    start: u64,
    end: u64,
    stop: &AtomicBool,
) -> Result<Vec<u8>> {
    let mut url = asset.url.to_owned();
    for _ in 0..6 {
        stopped(stop)?;
        if !trusted(&url) {
            return Err(Error::Unavailable(
                "Model asset redirect is outside the pinned provider",
            ));
        }
        let mut bytes = Vec::new();
        let response = transport
            .send(
                Request {
                    method: "GET",
                    url: url.clone(),
                    headers: vec![
                        ("range".into(), format!("bytes={start}-{end}")),
                        ("accept-encoding".into(), "identity".into()),
                    ],
                    body: Vec::new(),
                },
                &mut bytes,
                CHUNK,
            )
            .map_err(|_| unavailable())?;
        if [401, 403].contains(&response.status) {
            return Err(Error::Unavailable(
                "Model asset access was denied; explicit provider access is required",
            ));
        }
        if [301, 302, 303, 307, 308].contains(&response.status) {
            let location = response.headers.get("location").ok_or_else(unavailable)?;
            url = if location.starts_with('/') && !location.starts_with("//") {
                let uri = url.parse::<ureq::http::Uri>().map_err(|_| unavailable())?;
                format!(
                    "https://{}{location}",
                    uri.authority().ok_or_else(unavailable)?
                )
            } else {
                location.clone()
            };
            continue;
        }
        if response.status == 206 {
            let expected = format!("bytes {start}-{end}/{}", asset.bytes);
            if response.headers.get("content-range") != Some(&expected) {
                return Err(Error::Unavailable(
                    "Model asset range differs from the pinned file",
                ));
            }
        } else if !(response.status == 200 && start == 0 && end + 1 == asset.bytes) {
            return Err(unavailable());
        }
        if bytes.len() as u64 != end - start + 1 {
            return Err(Error::Unavailable(
                "Model asset response size differs from the pinned file",
            ));
        }
        return Ok(bytes);
    }
    Err(Error::Unavailable("Model asset redirect limit exceeded"))
}
pub(crate) fn verify(path: &Path, asset: &Asset<'_>) -> Result<bool> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(Error::Validation("Model asset must be a regular file"));
    }
    if meta.len() != asset.bytes {
        return Err(Error::Unavailable("Pinned model asset size differs"));
    }
    let mut file = File::open(path)?;
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    let digest: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if digest != asset.sha {
        return Err(Error::Unavailable("Pinned model asset checksum differs"));
    }
    Ok(true)
}
pub(crate) fn ensure(
    transport: &dyn Transport,
    root: &Path,
    asset: &Asset<'_>,
    stop: &AtomicBool,
) -> Result<()> {
    stopped(stop)?;
    let target = root.join(asset.path);
    if verify(&target, asset)? {
        return Ok(());
    }
    let parent = target.parent().ok_or_else(unavailable)?;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)?;
    if std::fs::symlink_metadata(parent)?.file_type().is_symlink() {
        return Err(Error::Validation(
            "Model asset directory cannot be a symbolic link",
        ));
    }
    let partial = target.with_extension(format!(
        "{}.incomplete",
        target
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("asset")
    ));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&partial)?;
    let mut offset = file.metadata()?.len();
    if offset > asset.bytes {
        return Err(Error::Unavailable(
            "Partial model asset exceeds the pinned file",
        ));
    }
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;
    while offset < asset.bytes {
        stopped(stop)?;
        let end = (offset + CHUNK).min(asset.bytes) - 1;
        let mut failure = None;
        let mut next = None;
        for _ in 0..3 {
            match request(transport, asset, offset, end, stop) {
                Ok(bytes) => {
                    next = Some(bytes);
                    break;
                }
                Err(
                    e @ Error::Unavailable(
                        "Model asset access was denied; explicit provider access is required",
                    ),
                ) => return Err(e),
                Err(e) => failure = Some(e),
            }
        }
        let bytes = next.ok_or_else(|| failure.unwrap_or_else(unavailable))?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        offset += bytes.len() as u64;
    }
    file.sync_all()?;
    drop(file);
    if let Err(error) = verify(&partial, asset) {
        let _ = std::fs::remove_file(&partial);
        return Err(error);
    }
    stopped(stop)?;
    std::fs::set_permissions(&partial, std::fs::Permissions::from_mode(0o400))?;
    std::fs::hard_link(&partial, &target)?;
    std::fs::remove_file(&partial)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
/// Only the instance-owned default cache is bootstrapped. Explicit external
/// caches remain immutable and are verified by the model constructor.
pub fn prepare(transport: &dyn Transport, root: &Path, stop: &AtomicBool) -> Result<()> {
    if std::fs::symlink_metadata(root).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(Error::Validation("Model cache cannot be a symbolic link"));
    }
    for asset in ASSETS {
        ensure(transport, root, asset, stop)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use memento_core::raw;
    use memento_maintenance::transport::Response;
    use std::{collections::BTreeMap, sync::Mutex};
    struct Fake {
        requests: Mutex<Vec<Request>>,
        status: u16,
        bytes: Vec<u8>,
        redirect: Option<String>,
    }
    impl Transport for Fake {
        fn send(&self, r: Request, out: &mut dyn Write, _maximum: u64) -> Result<Response> {
            self.requests.lock().unwrap().push(r);
            out.write_all(&self.bytes)?;
            Ok(Response {
                status: self.status,
                headers: self
                    .redirect
                    .as_ref()
                    .map(|s| BTreeMap::from([("location".into(), s.clone())]))
                    .unwrap_or_default(),
            })
        }
    }
    fn fake(bytes: &[u8]) -> Fake {
        Fake {
            requests: Mutex::new(Vec::new()),
            status: 200,
            bytes: bytes.into(),
            redirect: None,
        }
    }
    #[test]
    fn exact_bytes_are_atomic_readonly_and_cached_without_network() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"synthetic pinned model bytes";
        let sha = raw::sha256(data);
        let asset = Asset {
            path: "onnx/tiny.onnx",
            url: "https://huggingface.co/public/pin/file",
            bytes: data.len() as u64,
            sha: &sha,
        };
        let transport = fake(data);
        ensure(&transport, dir.path(), &asset, &AtomicBool::new(false)).unwrap();
        assert_eq!(std::fs::read(dir.path().join(asset.path)).unwrap(), data);
        assert_eq!(
            std::fs::metadata(dir.path().join(asset.path))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
        ensure(&transport, dir.path(), &asset, &AtomicBool::new(false)).unwrap();
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }
    #[test]
    fn corrupt_response_never_publishes_and_auth_does_not_retry_or_follow_another_identity() {
        let dir = tempfile::tempdir().unwrap();
        let sha = raw::sha256(b"good");
        let asset = Asset {
            path: "tiny",
            url: "https://huggingface.co/public/pin/file",
            bytes: 4,
            sha: &sha,
        };
        assert!(ensure(&fake(b"evil"), dir.path(), &asset, &AtomicBool::new(false)).is_err());
        assert!(!dir.path().join("tiny").exists());
        let denied = Fake {
            status: 401,
            ..fake(b"nope")
        };
        assert!(ensure(&denied, dir.path(), &asset, &AtomicBool::new(false)).is_err());
        assert_eq!(denied.requests.lock().unwrap().len(), 1);
        assert!(!dir.path().join("tiny").exists());
    }
    #[test]
    fn partial_resume_requires_exact_range_and_cancellation_leaves_unpublished_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let sha = raw::sha256(b"complete");
        let asset = Asset {
            path: "tiny.bin",
            url: "https://huggingface.co/public/pin/file",
            bytes: 8,
            sha: &sha,
        };
        std::fs::write(dir.path().join("tiny.bin.incomplete"), b"comp").unwrap();
        struct Range;
        impl Transport for Range {
            fn send(&self, r: Request, out: &mut dyn Write, _: u64) -> Result<Response> {
                assert_eq!(r.headers[0].1, "bytes=4-7");
                out.write_all(b"lete")?;
                Ok(Response {
                    status: 206,
                    headers: BTreeMap::from([("content-range".into(), "bytes 4-7/8".into())]),
                })
            }
        }
        ensure(&Range, dir.path(), &asset, &AtomicBool::new(false)).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(asset.path)).unwrap(),
            b"complete"
        );
        let other = Asset {
            path: "stopped",
            ..asset
        };
        assert!(
            ensure(
                &fake(b"complete"),
                dir.path(),
                &other,
                &AtomicBool::new(true)
            )
            .is_err()
        );
        assert!(!dir.path().join("stopped").exists());
    }
    #[test]
    fn redirects_cannot_escape_https_provider_or_target_local_services() {
        for url in [
            "http://huggingface.co/file",
            "https://127.0.0.1/file",
            "https://huggingface.co@evil.invalid/file",
            "https://evil.hf.co.invalid/file",
        ] {
            assert!(!trusted(url));
        }
        assert!(trusted(
            "https://cas-bridge.xethub.hf.co/file?opaque=synthetic"
        ));
        let dir = tempfile::tempdir().unwrap();
        let sha = raw::sha256(b"data");
        let asset = Asset {
            path: "tiny",
            url: "https://huggingface.co/public/pin/file",
            bytes: 4,
            sha: &sha,
        };
        let redirect = Fake {
            status: 302,
            redirect: Some("https://127.0.0.1/secret".into()),
            ..fake(b"")
        };
        assert!(ensure(&redirect, dir.path(), &asset, &AtomicBool::new(false)).is_err());
        assert_eq!(redirect.requests.lock().unwrap().len(), 3);
        assert!(!dir.path().join("tiny").exists());
    }
}
