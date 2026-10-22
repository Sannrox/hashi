//! `hashi` prints its version; `hashi run --config <file> [--once]
//! [--interval-ms <n>]` runs the configured instances, once or until stopped.

use std::process::ExitCode;
use std::time::Duration;

use hashi::{Config, Service};

const USAGE: &str = "usage: hashi run --config <file> [--once] [--interval-ms <n>]";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        println!(
            "hashi {} contract/{}",
            env!("CARGO_PKG_VERSION"),
            hashi::CONTRACT_VERSION
        );
        return ExitCode::SUCCESS;
    }
    match run(&args) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("hashi: {message}");
            ExitCode::from(2)
        }
    }
}

fn run(args: &[String]) -> Result<ExitCode, String> {
    let mut config_path = None;
    let mut once = false;
    let mut interval = Duration::from_secs(1);
    let mut rest = args.iter();
    if rest.next().map(String::as_str) != Some("run") {
        return Err(USAGE.into());
    }
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--config" => config_path = rest.next().cloned(),
            "--once" => once = true,
            "--interval-ms" => {
                let value = rest.next().ok_or(USAGE)?;
                interval = Duration::from_millis(value.parse().map_err(|_| USAGE)?);
            }
            _ => return Err(USAGE.into()),
        }
    }
    let config_path = config_path.ok_or(USAGE)?;
    let config = Config::load(config_path.as_ref()).map_err(|error| error.message)?;
    let mut service = Service::from_config(&config).map_err(|error| error.message)?;
    loop {
        let mut failed = false;
        for (name, outcome) in service.pass() {
            match outcome {
                Ok(report) => eprintln!(
                    "{name}: delivered {} quarantined {} checkpoint {}",
                    report.delivered, report.quarantined, report.checkpoint
                ),
                Err(error) => {
                    failed = true;
                    eprintln!("{name}: {}", error.message);
                }
            }
        }
        if once {
            return Ok(if failed {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            });
        }
        std::thread::sleep(interval);
    }
}
