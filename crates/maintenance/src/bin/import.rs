use std::path::Path;
fn main() {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("Usage: memento-import SOURCE_SQLITE NEW_DATA_DIRECTORY");
        std::process::exit(2)
    }
    match memento_maintenance::legacy::import(Path::new(&args[0]), Path::new(&args[1])) {
        Ok(report) => println!(
            "{}",
            serde_json::to_string(&report).expect("fixed report serializes")
        ),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}
