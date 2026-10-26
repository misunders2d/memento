//! Existing human console behavior; this module is only a socket client.
use crate::owner;
use memento_core::{Error, Result};
use serde_json::{Value, json};
use std::{
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};
#[derive(Debug)]
pub struct CliError {
    pub status: i32,
    pub message: &'static str,
}
pub type CliResult<T> = std::result::Result<T, CliError>;
fn usage(message: &'static str) -> CliError {
    CliError { status: 2, message }
}
fn unavailable() -> CliError {
    CliError {
        status: 1,
        message: "Administrative operation failed",
    }
}
pub trait Console {
    fn interactive(&self) -> bool;
    fn confirm(&mut self, prompt: &str) -> io::Result<String>;
    fn output(&mut self, text: &str) -> io::Result<()>;
}
pub struct Stdio;
impl Console for Stdio {
    fn interactive(&self) -> bool {
        io::stdin().is_terminal()
    }
    fn confirm(&mut self, prompt: &str) -> io::Result<String> {
        print!("{prompt}");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }
        Ok(line)
    }
    fn output(&mut self, text: &str) -> io::Result<()> {
        io::stdout().write_all(text.as_bytes())
    }
}
pub type Requester<'a> = dyn Fn(&Path, &Value) -> Result<Value> + 'a;
pub const HELP: &str = "Human-only Memento review and portable export console\nUsage: memento-admin --data-dir PATH {list|inventory|backup|compact|history|export|approve|reject|merge|forget}\n  history {park|status}\n  export --output PATH\n  approve|reject PROPOSAL --why TEXT [--command-id ID]\n  merge CLUSTER [--into ID] [--exclude ID] [--include ID] --why TEXT [--command-id ID]\n  forget TARGET --why TEXT [--command-id ID]\n";
#[derive(Debug)]
pub struct AdminArgs {
    pub data: PathBuf,
    pub operation: String,
    pub positional: Option<String>,
    pub output: Option<PathBuf>,
    pub why: Option<String>,
    pub command_id: Option<String>,
    pub into: Option<String>,
    pub exclude: Vec<String>,
    pub include: Vec<String>,
}
pub(crate) fn expanded(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for arg in args {
        if arg.starts_with("--") && arg.contains('=') {
            let (a, b) = arg.split_once('=').unwrap();
            out.push(a.into());
            out.push(b.into());
        } else {
            out.push(arg.clone());
        }
    }
    out
}
pub fn parse(args: &[String]) -> CliResult<AdminArgs> {
    let args = expanded(args);
    let mut i = 0;
    let mut data = None;
    while args.get(i).is_some_and(|v| v == "--data-dir") {
        data = Some(PathBuf::from(
            args.get(i + 1).ok_or(usage("--data-dir requires a path"))?,
        ));
        i += 2;
    }
    let data = data.ok_or(usage("--data-dir is required"))?;
    let operation = args
        .get(i)
        .ok_or(usage("An administrative operation is required"))?
        .clone();
    i += 1;
    if ![
        "list",
        "inventory",
        "backup",
        "compact",
        "history",
        "export",
        "approve",
        "reject",
        "merge",
        "forget",
    ]
    .contains(&operation.as_str())
    {
        return Err(usage("Unknown administrative operation"));
    }
    let needs_target =
        ["history", "approve", "reject", "merge", "forget"].contains(&operation.as_str());
    let mut parsed = AdminArgs {
        data,
        operation,
        positional: None,
        output: None,
        why: None,
        command_id: None,
        into: None,
        exclude: Vec::new(),
        include: Vec::new(),
    };
    while i < args.len() {
        let key = &args[i];
        if !key.starts_with("--") {
            if !needs_target || parsed.positional.is_some() {
                return Err(usage("Unexpected administrative argument"));
            }
            parsed.positional = Some(key.clone());
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or(usage("Administrative option requires a value"))?;
        match (key.as_str(), parsed.operation.as_str()) {
            ("--output", "export") => parsed.output = Some(PathBuf::from(value)),
            ("--why", "approve" | "reject" | "merge" | "forget") => {
                parsed.why = Some(value.clone())
            }
            ("--command-id", "approve" | "reject" | "merge" | "forget") => {
                parsed.command_id = Some(value.clone())
            }
            ("--into", "merge") => parsed.into = Some(value.clone()),
            ("--exclude", "merge") => parsed.exclude.push(value.clone()),
            ("--include", "merge") => parsed.include.push(value.clone()),
            _ => return Err(usage("Unknown administrative option")),
        }
        i += 2;
    }
    if needs_target && parsed.positional.is_none() {
        return Err(usage("Administrative target is required"));
    }
    if parsed.operation == "export" && parsed.output.is_none() {
        return Err(usage("--output is required"));
    }
    if parsed.operation == "history"
        && !["park", "status"].contains(&parsed.positional.as_deref().unwrap_or(""))
    {
        return Err(usage("History action must be park or status"));
    }
    if ["approve", "reject", "merge", "forget"].contains(&parsed.operation.as_str())
        && parsed.why.is_none()
    {
        return Err(usage("--why is required"));
    }
    Ok(parsed)
}
fn print(console: &mut dyn Console, value: &Value) -> CliResult<()> {
    console
        .output(&(serde_json::to_string_pretty(value).map_err(|_| unavailable())? + "\n"))
        .map_err(|_| unavailable())
}
fn confirm(console: &mut dyn Console, prompt: &str, expected: &str) -> CliResult<bool> {
    let answer = console.confirm(prompt).map_err(|_| unavailable())?;
    if answer == expected {
        Ok(true)
    } else {
        console
            .output("Not confirmed; nothing changed.\n")
            .map_err(|_| unavailable())?;
        Ok(false)
    }
}
pub fn run(args: &[String], console: &mut dyn Console, request: &Requester<'_>) -> CliResult<i32> {
    if args.iter().any(|s| s == "--help" || s == "-h") {
        console.output(HELP).map_err(|_| unavailable())?;
        return Ok(0);
    }
    let args = parse(args)?;
    let mut command = json!({"operation":args.operation});
    match args.operation.as_str() {
        "export" => {
            if !console.interactive() {
                return Err(usage(
                    "Plaintext archive export requires an interactive human terminal",
                ));
            }
            let path =
                std::path::absolute(args.output.as_ref().unwrap()).map_err(|_| unavailable())?;
            let path = path
                .to_str()
                .ok_or(usage("Archive path must be valid UTF-8"))?;
            let expected = format!("export private archive to {path}");
            let prompt = format!(
                "This plaintext archive contains private memory and Drive references; remote files stay on Drive. Type {expected:?} to confirm: "
            );
            if !confirm(console, &prompt, &expected)? {
                return Ok(1);
            }
            command["output"] = json!(path);
        }
        "history" => {
            command["operation"] = json!(format!("history_{}", args.positional.as_ref().unwrap()))
        }
        "approve" | "reject" | "merge" | "forget" => {
            if !console.interactive() {
                return Err(usage(
                    "Administrative decisions require an interactive human terminal",
                ));
            }
            let expected = match args.operation.as_str() {
                "merge" => {
                    let preview=request(&args.data,&json!({"operation":"merge_preview","cluster":args.positional,"into":args.into,"exclude":args.exclude,"include":args.include})).map_err(|_|unavailable())?;
                    if preview.get("error").is_some() {
                        return Err(CliError {
                            status: 1,
                            message: "Administrative preview was rejected",
                        });
                    }
                    let plan = preview.get("result").ok_or_else(unavailable)?;
                    let survivor = plan["survivor"]["id"].as_str().ok_or_else(unavailable)?;
                    let absorbed = plan["absorbed"].as_array().ok_or_else(unavailable)?;
                    let token = plan["token"].as_str().ok_or_else(unavailable)?;
                    print(console, plan)?;
                    command["cluster"] = json!(args.positional);
                    command["into"] = json!(survivor);
                    command["exclude"] = json!(args.exclude);
                    command["include"] = json!(args.include);
                    command["token"] = json!(token);
                    format!("merge {} into {survivor}", absorbed.len())
                }
                "forget" => {
                    let target = args.positional.as_ref().unwrap();
                    let preview = request(
                        &args.data,
                        &json!({"operation":"forget_preview","target":target}),
                    )
                    .map_err(|_| unavailable())?;
                    if preview.get("error").is_some() {
                        return Err(CliError {
                            status: 1,
                            message: "Administrative preview was rejected",
                        });
                    }
                    let plan = preview.get("result").ok_or_else(unavailable)?;
                    let token = plan["token"].as_str().ok_or_else(unavailable)?;
                    print(console, plan)?;
                    command["target"] = json!(target);
                    command["token"] = json!(token);
                    format!("forget {target} from active memory")
                }
                _ => {
                    let target = args.positional.as_ref().unwrap();
                    command["proposal_id"] = json!(target);
                    format!("{} {target}", args.operation)
                }
            };
            let prompt =
                format!("Type {expected:?} to confirm (raw history and backups are retained): ");
            if !confirm(console, &prompt, &expected)? {
                return Ok(1);
            }
            command["why"] = json!(args.why);
            command["command_id"] = json!(
                args.command_id
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
            );
        }
        _ => (),
    }
    let result = request(&args.data, &command).map_err(|_| unavailable())?;
    print(console, &result)?;
    Ok(
        if result.get("error").is_some()
            || args.operation == "compact" && result["result"]["complete"] != true
        {
            1
        } else {
            0
        },
    )
}
pub fn main_status(args: &[String]) -> i32 {
    match run(args, &mut Stdio, &owner::request) {
        Ok(status) => status,
        Err(error) => {
            eprintln!("{}", error.message);
            error.status
        }
    }
}
pub fn arguments() -> std::result::Result<Vec<String>, Error> {
    std::env::args_os()
        .skip(1)
        .map(|a| {
            a.into_string()
                .map_err(|_| Error::Validation("Arguments must be valid UTF-8"))
        })
        .collect()
}
