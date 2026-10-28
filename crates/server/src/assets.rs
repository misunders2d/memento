//! Wire-identical portable clients, revision stamping and static UI bytes.
use base64::{Engine, engine::general_purpose::STANDARD};
use memento_core::{Result, raw};
use serde_json::{Value, json};
use std::collections::BTreeMap;
const RUNNER: &[u8] = include_bytes!("../../../src/memento/runner.py");
const WORKER: &[u8] = include_bytes!("../../../src/memento/worker.py");
const NATIVE_WORKER: &[u8] = include_bytes!("../../../src/memento/native_worker.ts");
const BRIDGE: &[u8] = include_bytes!("../../../src/memento/bridge.py");
const PI: &[u8] = include_bytes!("../../../src/memento/native_pi.ts");
const OMP: &[u8] = include_bytes!("../../../src/memento/native_omp.ts");
const SHARED: &[u8] = include_bytes!("../../../src/memento/native_shared.ts");
pub struct ClientAssets {
    pub revision: String,
    pub bridge: Vec<u8>,
    pub release: Value,
}
impl ClientAssets {
    pub fn build() -> Result<Self> {
        let files = BTreeMap::from([
            ("runner.py", RUNNER),
            ("worker.py", WORKER),
            ("native_worker.ts", NATIVE_WORKER),
            ("bridge.py", BRIDGE),
            ("native_pi.ts", PI),
            ("native_omp.ts", OMP),
            ("native_shared.ts", SHARED),
        ]);
        let hashes = files
            .iter()
            .map(|(name, bytes)| (*name, raw::sha256(bytes)))
            .collect::<BTreeMap<_, _>>();
        let revision = raw::sha256(serde_json::to_string(&hashes)?.as_bytes());
        let marker = "CLIENT_REVISION = None";
        let source = std::str::from_utf8(BRIDGE).expect("shipped Python UTF8");
        assert_eq!(
            source.matches(marker).count(),
            1,
            "shipped revision marker must be unique"
        );
        let bridge = source
            .replacen(marker, &format!("CLIENT_REVISION = '{revision}'"), 1)
            .into_bytes();
        let mut records = files
            .into_iter()
            .map(|(name, original)| {
                let bytes = if name == "bridge.py" {
                    bridge.as_slice()
                } else {
                    original
                };
                (
                    name,
                    json!({"content":STANDARD.encode(bytes),"sha256":raw::sha256(bytes)}),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let processing = ["worker.py","native_worker.ts"].into_iter().map(|name|(name,records.remove(name).unwrap())).collect::<BTreeMap<_,_>>();
        let release = json!({"format":1,"revision":revision,"files":records,"processing_files":processing});
        Ok(Self {
            revision,
            bridge,
            release,
        })
    }
    pub fn raw(name: &str) -> Option<&'static [u8]> {
        match name {
            "runner" => Some(RUNNER),
            "pi" => Some(PI),
            "omp" => Some(OMP),
            "shared" => Some(SHARED),
            _ => None,
        }
    }
    pub fn ui(name: &str) -> Option<(&'static str, &'static [u8])> {
        match name {
            "index.html" => Some((
                "text/html; charset=utf-8",
                include_bytes!("../../../src/memento/ui/index.html"),
            )),
            "app.js" => Some((
                "text/javascript; charset=utf-8",
                include_bytes!("../../../src/memento/ui/app.js"),
            )),
            "app.css" => Some((
                "text/css; charset=utf-8",
                include_bytes!("../../../src/memento/ui/app.css"),
            )),
            "layout-worker.js" => Some((
                "text/javascript; charset=utf-8",
                include_bytes!("../../../src/memento/ui/layout-worker.js"),
            )),
            "vendor/forceatlas2.min.js" => Some((
                "text/javascript; charset=utf-8",
                include_bytes!("../../../src/memento/ui/vendor/forceatlas2.min.js"),
            )),
            "vendor/graphology.umd.min.js" => Some((
                "text/javascript; charset=utf-8",
                include_bytes!("../../../src/memento/ui/vendor/graphology.umd.min.js"),
            )),
            "vendor/sigma.min.js" => Some((
                "text/javascript; charset=utf-8",
                include_bytes!("../../../src/memento/ui/vendor/sigma.min.js"),
            )),
            _ => None,
        }
    }
}
