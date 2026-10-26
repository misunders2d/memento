//! Narrow synchronous transport. Bodies/URLs/headers deliberately have no Debug;
//! credentials never enter transport errors, proxy discovery, or redirect hops.
use memento_core::{Error, Result};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    time::Duration,
};
pub struct Request {
    pub method: &'static str,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}
pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
}
pub trait Transport: Send + Sync {
    fn send(&self, request: Request, output: &mut dyn Write, maximum: u64) -> Result<Response>;
}
pub struct NativeTransport {
    agent: ureq::Agent,
}
impl NativeTransport {
    pub fn new() -> Self {
        Self {
            agent: ureq::Agent::config_builder()
                .proxy(None)
                .max_redirects(0)
                .http_status_as_error(false)
                .timeout_global(Some(Duration::from_secs(60)))
                .build()
                .into(),
        }
    }
}
impl Default for NativeTransport {
    fn default() -> Self {
        Self::new()
    }
}
impl Transport for NativeTransport {
    fn send(&self, request: Request, output: &mut dyn Write, maximum: u64) -> Result<Response> {
        let mut builder = ureq::http::Request::builder()
            .method(request.method)
            .uri(&request.url);
        for (key, value) in request.headers {
            builder = builder.header(key, value);
        }
        let request = builder
            .body(request.body)
            .map_err(|_| Error::Validation("Invalid provider request"))?;
        let mut response = self
            .agent
            .run(request)
            .map_err(|_| Error::Unavailable("Drive transport failed"))?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v| (k.as_str().to_ascii_lowercase(), v.to_owned()))
            })
            .collect();
        let mut reader = response.body_mut().as_reader();
        let mut remaining = maximum;
        let mut buf = vec![0; 1024 * 1024];
        loop {
            let count = reader
                .read(&mut buf)
                .map_err(|_| Error::Unavailable("Drive response interrupted"))?;
            if count == 0 {
                break;
            }
            if count as u64 > remaining {
                return Err(Error::Unavailable("Drive response exceeds declared limit"));
            }
            remaining -= count as u64;
            output.write_all(&buf[..count])?;
        }
        Ok(Response { status, headers })
    }
}
pub fn encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char)
        } else {
            out.push_str(&format!("%{b:02X}"))
        }
    }
    out
}
pub fn form(values: &[(&str, &str)]) -> Vec<u8> {
    values
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
        .collect::<Vec<_>>()
        .join("&")
        .into_bytes()
}
pub fn json_response(
    transport: &dyn Transport,
    request: Request,
) -> Result<(Response, serde_json::Value)> {
    let mut bytes = Vec::new();
    let response = transport.send(request, &mut bytes, 1024 * 1024)?;
    let value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|_| Error::Unavailable("Drive returned an invalid structured response"))?
    };
    Ok((response, value))
}
#[cfg(test)]
mod privacy_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn wire_debug_and_trace_are_removed_at_compile_time() {
        assert!(log::STATIC_MAX_LEVEL <= log::LevelFilter::Info);
        struct Sensitive<'a>(&'a AtomicUsize);
        impl std::fmt::Debug for Sensitive<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                self.0.fetch_add(1, Ordering::SeqCst);
                f.write_str("SYNTHETIC TOKEN AND PRIVATE BODY")
            }
        }
        let formatted = AtomicUsize::new(0);
        log::trace!(target:"ureq_proto::util","{:?}",Sensitive(&formatted));
        log::debug!(target:"ureq::run","{:?}",Sensitive(&formatted));
        assert_eq!(formatted.load(Ordering::SeqCst), 0);
    }
}
