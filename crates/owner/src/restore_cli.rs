//! Native archive or explicitly declared original Drive connection dispatch.
use crate::{
    owner,
    owner_cli::{CliError, CliResult, expanded},
};
use memento_core::{Error, Result};
use memento_maintenance::{
    cold::{ColdManager, ColdStore},
    drive::DriveColdStore,
    portable,
    transport::{NativeTransport, Transport},
};
use serde_json::{Value, json};
use std::{
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};
pub const HELP: &str = "Restore an owner archive or personal Drive backup\nUsage: memento-restore (--archive PATH --destination PATH | --connection PATH [--backup-id ID --destination PATH] [--allow-incomplete])\n";
#[derive(Debug)]
pub struct RestoreArgs {
    pub archive: Option<PathBuf>,
    pub connection: Option<PathBuf>,
    pub backup_id: Option<String>,
    pub destination: Option<PathBuf>,
    pub allow_incomplete: bool,
}
fn usage(message: &'static str) -> CliError {
    CliError { status: 2, message }
}
pub fn parse(args: &[String]) -> CliResult<RestoreArgs> {
    let args = expanded(args);
    let mut parsed = RestoreArgs {
        archive: None,
        connection: None,
        backup_id: None,
        destination: None,
        allow_incomplete: false,
    };
    let mut i = 0;
    while i < args.len() {
        let key = args[i].as_str();
        if key == "--allow-incomplete" {
            parsed.allow_incomplete = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or(usage("Restore option requires a value"))?;
        match key {
            "--archive" => parsed.archive = Some(value.into()),
            "--connection" => parsed.connection = Some(value.into()),
            "--backup-id" => parsed.backup_id = Some(value.clone()),
            "--destination" => parsed.destination = Some(value.into()),
            _ => return Err(usage("Unknown restore option")),
        }
        i += 2;
    }
    if parsed.archive.is_some() == parsed.connection.is_some() {
        return Err(usage("Choose exactly one of --archive or --connection"));
    }
    if parsed.archive.is_some() {
        if parsed.destination.is_none() || parsed.backup_id.is_some() || parsed.allow_incomplete {
            return Err(usage(
                "--archive requires --destination and cannot use Drive backup options",
            ));
        }
    } else {
        if parsed.backup_id.as_ref().is_some_and(|s| s.is_empty())
            || parsed.backup_id.is_some() != parsed.destination.is_some()
        {
            return Err(usage(
                "--backup-id and --destination must be supplied together",
            ));
        }
        if parsed.allow_incomplete && parsed.backup_id.is_none() {
            return Err(usage(
                "--allow-incomplete requires --backup-id and --destination",
            ));
        }
    }
    Ok(parsed)
}
pub fn execute(args: &RestoreArgs, transport: Arc<dyn Transport>) -> Result<Value> {
    if let Some(archive) = &args.archive {
        return Ok(serde_json::to_value(portable::import_archive(
            archive,
            args.destination
                .as_ref()
                .ok_or(Error::Validation("Restore destination is required"))?,
        )?)?);
    }
    let file = args.connection.as_ref().ok_or(Error::Validation(
        "An explicit Drive connection is required",
    ))?;
    let backend = DriveColdStore::from_connection_file(file, None, transport)?;
    if let Some(id) = &args.backup_id {
        Ok(serde_json::to_value(ColdManager::restore_from(
            backend.as_ref(),
            id,
            args.destination
                .as_ref()
                .ok_or(Error::Validation("Restore destination is required"))?,
            args.allow_incomplete,
        )?)?)
    } else {
        Ok(serde_json::to_value(backend.list_backups()?)?)
    }
}
pub fn main_status(args: &[String]) -> i32 {
    if args.iter().any(|s| s == "--help" || s == "-h") {
        print!("{HELP}");
        return 0;
    }
    let parsed = match parse(args) {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("{}", error.message);
            return error.status;
        }
    };
    match execute(&parsed, Arc::new(NativeTransport::new())) {
        Ok(value) => {
            if let Ok(mut bytes) = owner::ascii_json(&value) {
                bytes.push(b'\n');
                if io::stdout().write_all(&bytes).is_ok() {
                    return 0;
                }
            }
            eprintln!("Restore output is unavailable");
            1
        }
        Err(error) => {
            let value = json!({"error":error.to_string()});
            if let Ok(mut bytes) = owner::ascii_json(&value) {
                bytes.push(b'\n');
                let _ = io::stderr().write_all(&bytes);
            }
            1
        }
    }
}
