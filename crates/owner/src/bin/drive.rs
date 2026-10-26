//! Explicit service-owner provisioning. No ambient credential/account discovery.
use memento_core::{Error, Result};
use std::path::PathBuf;
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("configure") {
        return Err(Error::Validation(
            "Use memento-drive configure --client-json PATH --account ACCOUNT --folder-id ID --data-dir PATH",
        ));
    }
    let (mut client, mut account, mut folder, mut data) = (None, None, None, None);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or(Error::Validation("Missing Drive configuration argument"))?;
        let slot = match flag.as_str() {
            "--client-json" => &mut client,
            "--account" => &mut account,
            "--folder-id" => &mut folder,
            "--data-dir" => &mut data,
            _ => return Err(Error::Validation("Invalid Drive configuration argument")),
        };
        if slot.replace(value).is_some() {
            return Err(Error::Validation("Repeated Drive configuration argument"));
        }
    }
    let client = PathBuf::from(client.ok_or(Error::Validation("Desktop client file is required"))?);
    let data = PathBuf::from(data.ok_or(Error::Validation("Service data directory is required"))?);
    memento_maintenance::drive_auth::configure_client(
        &client,
        &account.ok_or(Error::Validation("Exact account is required"))?,
        &folder.ok_or(Error::Validation("Original folder is required"))?,
        &data,
    )?;
    println!(
        "Original Desktop client and existing Drive folder configured for one-link authorization."
    );
    Ok(())
}
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "connect") {
        std::process::exit(memento_owner::drive_connect_cli::main_status(&args[1..]));
    }
    if run().is_err() {
        eprintln!("Drive connection failed; connection setup did not complete.");
        std::process::exit(1);
    }
}
