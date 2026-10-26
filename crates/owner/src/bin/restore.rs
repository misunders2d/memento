fn main() {
    let args = match memento_owner::owner_cli::arguments() {
        Ok(v) => v,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    };
    std::process::exit(memento_owner::restore_cli::main_status(&args));
}
