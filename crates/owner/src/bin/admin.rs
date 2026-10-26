fn main() {
    let args = match memento_owner::owner_cli::arguments() {
        Ok(v) => v,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    };
    // Native compact worker dispatch is wired to the core helper before opening
    // Store; its parent already holds the exclusive service writer fence.
    if args.first().is_some_and(|s| s == "--compact-worker") {
        if args.len() != 2 {
            eprintln!("Invalid compact worker arguments");
            std::process::exit(2)
        }
        let result = memento_core::compact_worker(std::path::Path::new(&args[1]));
        match result {
            Ok(value) => {
                if let Ok(bytes) = serde_json::to_string(&value) {
                    println!("{bytes}");
                    return;
                }
            }
            Err(_) => (),
        }
        eprintln!("Native compact worker failed");
        std::process::exit(1)
    }
    std::process::exit(memento_owner::owner_cli::main_status(&args));
}
