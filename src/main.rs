//! `hashi` prints its version; `hashi run --config <file> [--once]
//! [--interval-ms <n>] [--listen <addr>]` runs the configured instances, once
//! or until stopped, optionally serving `/healthz` and `/metrics` on `<addr>`;
//! `hashi diagnostics --config <file>` prints a support bundle as JSON.

use std::net::TcpListener;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hashi::observe::{Metrics, serve};
use hashi::{Config, Service};

const USAGE: &str = "usage: hashi run --config <file> [--once] [--interval-ms <n>] [--listen <addr>]\n       hashi diagnostics --config <file>";

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
    let outcome = match args[0].as_str() {
        "run" => run(&args[1..]),
        "diagnostics" => diagnostics(&args[1..]),
        _ => Err(USAGE.into()),
    };
    match outcome {
        Ok(code) => code,
        Err(message) => {
            eprintln!("hashi: {message}");
            ExitCode::from(2)
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn diagnostics(args: &[String]) -> Result<ExitCode, String> {
    let [flag, path] = args else {
        return Err(USAGE.into());
    };
    if flag != "--config" {
        return Err(USAGE.into());
    }
    let config = Config::load(path.as_ref()).map_err(|error| error.message)?;
    let bundle = serde_json::to_string_pretty(&hashi::diagnostics::collect(&config))
        .map_err(|error| error.to_string())?;
    println!("{bundle}");
    Ok(ExitCode::SUCCESS)
}

fn run(args: &[String]) -> Result<ExitCode, String> {
    let mut config_path = None;
    let mut once = false;
    let mut interval = Duration::from_secs(1);
    let mut listen = None;
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--config" => config_path = rest.next().cloned(),
            "--once" => once = true,
            "--interval-ms" => {
                let value = rest.next().ok_or(USAGE)?;
                interval = Duration::from_millis(value.parse().map_err(|_| USAGE)?);
            }
            "--listen" => listen = Some(rest.next().ok_or(USAGE)?.clone()),
            _ => return Err(USAGE.into()),
        }
    }
    let config_path = config_path.ok_or(USAGE)?;
    let config = Config::load(config_path.as_ref()).map_err(|error| error.message)?;
    let mut service = Service::from_config(&config).map_err(|error| error.message)?;
    let metrics = Arc::new(Mutex::new(Metrics::default()));
    if let Some(addr) = listen {
        let listener =
            TcpListener::bind(&addr).map_err(|error| format!("--listen {addr}: {error}"))?;
        let bound = listener.local_addr().map_err(|error| error.to_string())?;
        eprintln!("hashi: listening on {bound}");
        serve(listener, metrics.clone(), now_ms);
    }
    loop {
        let mut failed = false;
        let passes = service.pass_with_progress();
        let mut metrics = metrics.lock().unwrap_or_else(|poison| poison.into_inner());
        for pass in &passes {
            let name = &pass.instance;
            match &pass.outcome {
                Ok(report) => eprintln!(
                    "{name}: delivered {} quarantined {} checkpoint {}",
                    report.delivered, report.quarantined, report.checkpoint
                ),
                Err(error) => {
                    failed = true;
                    eprintln!("{name}: {}", error.message);
                }
            }
            metrics.record(name, &pass.progress, pass.outcome.is_err(), now_ms());
        }
        drop(metrics);
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
