//! Native memento-drive connect subcommand. Configure stays with the dispatcher.
use crate::owner_cli::{CliError, CliResult, expanded};
use memento_core::{Error, Result};
use memento_maintenance::{
    desktop_connect::{self, ConnectOptions, Connected, PendingConnect},
    transport::{NativeTransport, Transport},
};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
pub const HELP: &str = "Authorize the original Desktop app and personal Drive account\nUsage: memento-drive connect --client-json PATH --account EMAIL --output PATH [--folder-id EXISTING_FOLDER_ID] [--data-dir RESTORED_DATA_DIR] [--no-browser] [--port 8766]\n";
pub struct Arguments {
    pub options: ConnectOptions,
    pub no_browser: bool,
}
fn usage(message: &'static str) -> CliError {
    CliError { status: 2, message }
}
pub fn parse(args: &[String]) -> CliResult<Arguments> {
    let args = expanded(args);
    let (mut client, mut account, mut output, mut folder, mut data) =
        (None, None, None, None, None);
    let (mut no_browser, mut port) = (false, 8766);
    let mut i = 0;
    while i < args.len() {
        let key = &args[i];
        if key == "--no-browser" {
            no_browser = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or(usage("Drive connect option requires a value"))?;
        match key.as_str() {
            "--client-json" => client = Some(PathBuf::from(value)),
            "--account" => account = Some(value.clone()),
            "--output" => output = Some(PathBuf::from(value)),
            "--folder-id" => folder = Some(value.clone()),
            "--data-dir" => data = Some(PathBuf::from(value)),
            "--port" => {
                port = value
                    .parse::<u16>()
                    .ok()
                    .filter(|p| *p > 0)
                    .ok_or(usage("Drive callback port must be between 1 and 65535"))?
            }
            _ => return Err(usage("Unknown Drive connect option")),
        }
        i += 2;
    }
    Ok(Arguments {
        options: ConnectOptions {
            client_json: client.ok_or(usage("--client-json is required"))?,
            account: account.ok_or(usage("--account is required"))?,
            output: output.ok_or(usage("--output is required"))?,
            port,
            folder_id: folder,
            data_dir: data,
        },
        no_browser,
    })
}
fn respond(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    let message = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(message.as_bytes());
}
fn read_callback(stream: &mut TcpStream) -> Result<desktop_connect::Callback> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bytes = Vec::new();
    let mut buffer = [0; 2048];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::Unavailable("Authorization callback timed out"));
        }
        stream.set_read_timeout(Some(remaining))?;
        let n = stream.read(&mut buffer)?;
        if n == 0 {
            return Err(Error::Validation("Invalid authorization callback"));
        }
        if bytes.len() + n > 32768 {
            return Err(Error::Validation(
                "Authorization callback exceeds size limit",
            ));
        }
        bytes.extend_from_slice(&buffer[..n]);
        if bytes.windows(4).any(|b| b == b"\r\n\r\n") {
            break;
        }
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::Validation("Invalid authorization callback"))?;
    let line = text
        .lines()
        .next()
        .ok_or(Error::Validation("Invalid authorization callback"))?;
    let words = line.split_whitespace().collect::<Vec<_>>();
    if words.len() != 3 || words[0] != "GET" || !["HTTP/1.0", "HTTP/1.1"].contains(&words[2]) {
        return Err(Error::Validation("Invalid authorization callback"));
    }
    desktop_connect::callback(words[1])
}
pub fn receive(
    listener: &TcpListener,
    pending: &PendingConnect,
) -> Result<desktop_connect::Callback> {
    listener.set_nonblocking(true)?;
    while Instant::now() < pending.expires_at() {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_write_timeout(Some(Duration::from_secs(10)))?;
                match read_callback(&mut stream) {
                    Ok(callback) if pending.accepts_state(&callback.state) => {
                        respond(
                            &mut stream,
                            200,
                            "Authorization received. Return to memento for account and folder verification.\n",
                        );
                        return Ok(callback);
                    }
                    _ => respond(
                        &mut stream,
                        400,
                        "Authorization callback was invalid or rejected.\n",
                    ),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => (),
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::Unavailable("Drive authorization expired"))
}
/// Browser/notice callbacks are UI seams; provider remains the native transport.
/// Production main never substitutes endpoints, identity or callback validation.
pub fn connect(
    arguments: Arguments,
    provider: Arc<dyn Transport>,
    mut notice: impl FnMut(&str) -> Result<()>,
    mut browser: impl FnMut(&str) -> Result<()>,
) -> Result<Connected> {
    let mut pending = PendingConnect::begin(arguments.options)?;
    let listener = TcpListener::bind(("127.0.0.1", pending.port()))?;
    notice(pending.authorization_url())?;
    if !arguments.no_browser {
        browser(pending.authorization_url())?;
    }
    let callback = receive(&listener, &pending)?;
    pending.finish(
        &callback.state,
        callback.code.as_deref(),
        callback.error.as_deref(),
        provider,
    )
}
pub fn main_status(args: &[String]) -> i32 {
    if args.iter().any(|s| s == "--help" || s == "-h") {
        print!("{HELP}");
        return 0;
    }
    let arguments = match parse(args) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e.message);
            return e.status;
        }
    };
    let result = connect(
        arguments,
        Arc::new(NativeTransport::new()),
        |url| {
            println!("Please visit this URL to authorize this application: {url}");
            std::io::stdout().flush()?;
            Ok(())
        },
        |url| {
            let _ = Command::new("xdg-open")
                .arg(url)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn();
            Ok(())
        },
    );
    match result {
        Ok(_) => {
            println!("Drive account and dedicated folder verified; private connection saved.");
            0
        }
        Err(_) => {
            eprintln!("Drive connection failed; connection setup did not complete.");
            1
        }
    }
}
#[cfg(test)]
#[path = "drive_connect_cli_tests.rs"]
mod tests;
