//! Native daemon entrypoint. Portable capture/runner clients remain separately shipped assets.
use memento_core::{Error, Result};
use memento_embedding::ModelConfig;
use memento_maintenance::cold_runtime::ColdPolicy;
use memento_server::{
    router,
    runtime::{Application, Config},
};
use std::{net::IpAddr, path::PathBuf};
fn value(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or(Error::Validation("Missing command argument"))
}
fn unsigned_env(name: &str, default: u64) -> Result<u64> {
    std::env::var(name)
        .ok()
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| Error::Validation("Invalid cold retention configuration"))
        })
        .unwrap_or(Ok(default))
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut data =
        PathBuf::from(std::env::var_os("MEMENTO_DATA_DIR").unwrap_or_else(|| "./data".into()));
    let mut host = "127.0.0.1".parse::<IpAddr>().unwrap();
    let mut port = 8765u16;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--health-check" => {
                use std::io::Read;
                let agent: ureq::Agent = ureq::Agent::config_builder()
                    .proxy(None)
                    .max_redirects(0)
                    .http_status_as_error(false)
                    .timeout_global(Some(std::time::Duration::from_secs(3)))
                    .build()
                    .into();
                let mut response = agent
                    .get("http://127.0.0.1:8765/health")
                    .call()
                    .map_err(|_| Error::Unavailable("Health check failed"))?;
                if response.status().as_u16() != 200 {
                    return Err(Error::Unavailable("Health check failed"));
                }
                let mut bytes = Vec::new();
                response
                    .body_mut()
                    .as_reader()
                    .take(65537)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > 65536
                    || serde_json::from_slice::<serde_json::Value>(&bytes)
                        .ok()
                        .is_none_or(|v| v["status"] != "ok")
                {
                    return Err(Error::Unavailable("Health check failed"));
                }
                return Ok(());
            }
            "--compact-worker" => {
                let path = PathBuf::from(value(&mut args)?);
                if args.next().is_some() {
                    return Err(Error::Validation("Invalid compact worker arguments"));
                }
                println!("{}", memento_core::compact_worker(&path)?);
                return Ok(());
            }
            "--data-dir" => data = value(&mut args)?.into(),
            "--host" => {
                host = value(&mut args)?
                    .parse()
                    .map_err(|_| Error::Validation("Invalid listen address"))?
            }
            "--port" => {
                port = value(&mut args)?
                    .parse()
                    .map_err(|_| Error::Validation("Invalid listen port"))?
            }
            "--help" | "-h" => {
                println!(
                    "memento --data-dir PATH [--host 127.0.0.1] [--port 8765]\nOwner operations: memento-admin; offline restore: memento-restore"
                );
                return Ok(());
            }
            _ => return Err(Error::Validation("Unknown native service argument")),
        }
    }
    let enabled = std::env::var("MEMENTO_EMBEDDINGS").unwrap_or_else(|_| "0".into());
    if !["0", "1"].contains(&enabled.as_str()) {
        return Err(Error::Validation("MEMENTO_EMBEDDINGS must be 0 or 1"));
    }
    let model = if enabled == "1" {
        let model_dir = std::env::var_os("MEMENTO_MODEL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| data.join("models"));
        let ort = std::env::var_os("MEMENTO_ORT_LIBRARY")
            .map(PathBuf::from)
            .ok_or(Error::Validation(
                "Pinned native ONNX runtime library is required",
            ))?;
        let mut config = ModelConfig::new(model_dir, ort);
        if let Some(path) = std::env::var_os("MEMENTO_TOKENIZER_PATH") {
            config.tokenizer_path = Some(path.into());
        }
        if let Some(path) = std::env::var_os("MEMENTO_GRAPH_DIR") {
            config.graph_dir = Some(path.into());
        }
        Some(config)
    } else {
        None
    };
    let pdfium = std::env::var_os("MEMENTO_PDFIUM_LIBRARY").map(PathBuf::from);
    if enabled == "1" && pdfium.is_none() {
        return Err(Error::Validation(
            "Pinned native PDFium library is required for multimodal ingestion",
        ));
    }
    let config = Config {
        data_dir: data,
        model,
        bootstrap_assets: std::env::var_os("MEMENTO_MODEL_DIR").is_none(),
        text_reranker_dir: std::env::var_os("MEMENTO_TEXT_RERANKER_DIR").map(PathBuf::from),
        pdfium,
        allowed_hosts: std::env::var("MEMENTO_ALLOWED_HOSTS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect(),
        compact_executable: std::env::current_exe()?,
        cold_policy: ColdPolicy {
            after_days: unsigned_env("MEMENTO_COLD_AFTER_DAYS", 90)?,
            min_bytes: unsigned_env("MEMENTO_COLD_MIN_BYTES", 10 * 1024 * 1024)?,
            cache_bytes: unsigned_env("MEMENTO_COLD_CACHE_BYTES", 1024 * 1024 * 1024)?,
            cache_ttl_seconds: unsigned_env("MEMENTO_COLD_CACHE_TTL_SECONDS", 3600)?,
            batch_limit: 10,
        },
        cold_max_importance: std::env::var("MEMENTO_COLD_MAX_IMPORTANCE")
            .ok()
            .map(|s| {
                s.parse::<f64>()
                    .map_err(|_| Error::Validation("Invalid cold importance configuration"))
            })
            .unwrap_or(Ok(0.75))?,
    };
    if !config.cold_max_importance.is_finite() || !(0.0..=1.0).contains(&config.cold_max_importance)
    {
        return Err(Error::Validation("Invalid cold importance configuration"));
    }
    // Bound both reactor and blocking work under the shared two-CPU/256-PID contract.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(32)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut app = Application::start(config)?;
        let listener = tokio::net::TcpListener::bind((host, port)).await?;
        let queue = app.state.processing.queue.clone();
        let services = app.services.clone();
        let shutdown = async move {
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = async {
                        if let Some(term) = term.as_mut() { term.recv().await; }
                        else { std::future::pending::<()>().await; }
                    } => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            // Stop between-unit background work at signal receipt, while existing
            // requests drain. Joins and owned socket cleanup follow the drain.
            queue.request_stop();
            services.request_stop();
        };
        let result = axum::serve(listener, router(app.state.clone()))
            .with_graceful_shutdown(shutdown)
            .await;
        let _ = tokio::task::spawn_blocking(move || app.close()).await;
        result.map_err(Error::Io)
    })
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
